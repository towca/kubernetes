package runtime

import (
	"fmt"
	resourceapi "k8s.io/api/resource/v1alpha3"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	resourcelisters "k8s.io/client-go/listers/resource/v1alpha3"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/util/assumecache"
	"sync"
)

var _ framework.SharedDraManager = &DraManager{}

type DraManager struct {
	resourceClaimTracker *claimTracker
	resourceSliceLister  *resourceSliceLister
	deviceClassLister    *deviceClassLister
}

func NewDraManager(claimsCache *assumecache.AssumeCache, informerFactory informers.SharedInformerFactory) *DraManager {
	return &DraManager{
		resourceClaimTracker: &claimTracker{cache: claimsCache, inFlightAllocations: &sync.Map{}},
		resourceSliceLister:  &resourceSliceLister{sliceLister: informerFactory.Resource().V1alpha3().ResourceSlices().Lister()},
		deviceClassLister:    &deviceClassLister{classLister: informerFactory.Resource().V1alpha3().DeviceClasses().Lister()},
	}
}

func (s *DraManager) ResourceClaims() framework.ResourceClaimTracker {
	return s.resourceClaimTracker
}

func (s *DraManager) ResourceSlices() framework.ResourceSliceLister {
	return s.resourceSliceLister
}

func (s *DraManager) DeviceClasses() framework.DeviceClassLister {
	return s.deviceClassLister
}

var _ framework.ResourceSliceLister = &resourceSliceLister{}

type resourceSliceLister struct {
	sliceLister resourcelisters.ResourceSliceLister
}

func (l *resourceSliceLister) List() ([]*resourceapi.ResourceSlice, error) {
	return l.sliceLister.List(labels.Everything())
}

var _ framework.DeviceClassLister = &deviceClassLister{}

type deviceClassLister struct {
	classLister resourcelisters.DeviceClassLister
}

func (l *deviceClassLister) Get(className string) (*resourceapi.DeviceClass, error) {
	return l.classLister.Get(className)
}

func (l *deviceClassLister) List() ([]*resourceapi.DeviceClass, error) {
	return l.classLister.List(labels.Everything())
}

var _ framework.ResourceClaimTracker = &claimTracker{}

type claimTracker struct {
	// cache enables temporarily storing a newer claim object
	// while the scheduler has allocated it and the corresponding object
	// update from the apiserver has not been processed by the claim
	// informer callbacks. ResourceClaimTracker get added here in PreBind and removed by
	// the informer callback (based on the "newer than" comparison in the
	// assume cache).
	//
	// It uses cache.MetaNamespaceKeyFunc to generate object names, which
	// therefore are "<namespace>/<name>".
	//
	// This is necessary to ensure that reconstructing the resource usage
	// at the start of a pod scheduling cycle doesn't reuse the resources
	// assigned to such a claim. Alternatively, claim allocation state
	// could also get tracked across pod scheduling cycles, but that
	// - adds complexity (need to carefully sync state with informer events
	//   for claims and ResourceSlices)
	// - would make integration with cluster autoscaler harder because it would need
	//   to trigger informer callbacks.
	//
	// When implementing cluster autoscaler support, this assume cache or
	// something like it (see https://github.com/kubernetes/kubernetes/pull/112202)
	// might have to be managed by the cluster autoscaler.
	cache *assumecache.AssumeCache
	// inFlightAllocations is map from claim UUIDs to claim objects for those claims
	// for which allocation was triggered during a scheduling cycle and the
	// corresponding claim status update call in PreBind has not been done
	// yet. If another pod needs the claim, the pod is treated as "not
	// schedulable yet". The cluster event for the claim status update will
	// make it schedulable.
	//
	// This mechanism avoids the following problem:
	// - Pod A triggers allocation for claim X.
	// - Pod B shares access to that claim and gets scheduled because
	//   the claim is assumed to be allocated.
	// - PreBind for pod B is called first, tries to update reservedFor and
	//   fails because the claim is not really allocated yet.
	//
	// We could avoid the ordering problem by allowing either pod A or pod B
	// to set the allocation. But that is more complicated and leads to another
	// problem:
	// - Pod A and B get scheduled as above.
	// - PreBind for pod A gets called first, then fails with a temporary API error.
	//   It removes the updated claim from the assume cache because of that.
	// - PreBind for pod B gets called next and succeeds with adding the
	//   allocation and its own reservedFor entry.
	// - The assume cache is now not reflecting that the claim is allocated,
	//   which could lead to reusing the same resource for some other claim.
	//
	// A sync.Map is used because in practice sharing of a claim between
	// pods is expected to be rare compared to per-pod claim, so we end up
	// hitting the "multiple goroutines read, write, and overwrite entries
	// for disjoint sets of keys" case that sync.Map is optimized for.
	inFlightAllocations *sync.Map
}

func (c *claimTracker) ClaimHasPendingAllocation(claimUid types.UID) bool {
	_, found := c.inFlightAllocations.Load(claimUid)
	return found
}

func (c *claimTracker) SignalClaimPendingAllocation(claimUid types.UID, allocatedClaim *resourceapi.ResourceClaim) {
	c.inFlightAllocations.Store(claimUid, allocatedClaim)
}

func (c *claimTracker) RemoveClaimPendingAllocation(claimUid types.UID) (found bool) {
	_, found = c.inFlightAllocations.LoadAndDelete(claimUid)
	return found
}

func (c *claimTracker) Get(namespace, claimName string) (*resourceapi.ResourceClaim, error) {
	obj, err := c.cache.Get(namespace + "/" + claimName)
	if err != nil {
		return nil, err
	}
	claim, ok := obj.(*resourceapi.ResourceClaim)
	if !ok {
		return nil, fmt.Errorf("unexpected object type %T for assumed object %s/%s", obj, namespace, claimName)
	}
	return claim, nil
}

func (c *claimTracker) GetOriginal(namespace, claimName string) (*resourceapi.ResourceClaim, error) {
	obj, err := c.cache.GetAPIObj(namespace + "/" + claimName)
	if err != nil {
		return nil, err
	}
	claim, ok := obj.(*resourceapi.ResourceClaim)
	if !ok {
		return nil, fmt.Errorf("unexpected object type %T for assumed object %s/%s", obj, namespace, claimName)
	}
	return claim, nil
}

func (c *claimTracker) List() ([]*resourceapi.ResourceClaim, error) {
	var result []*resourceapi.ResourceClaim
	// Probably not worth adding an index for?
	objs := c.cache.List(nil)
	for _, obj := range objs {
		claim, ok := obj.(*resourceapi.ResourceClaim)
		if ok {
			result = append(result, claim)
		}
	}
	return result, nil
}

func (c *claimTracker) ListAllAllocated() ([]*resourceapi.ResourceClaim, error) {
	claims, err := c.List()
	if err != nil {
		return nil, err
	}
	allocated := make([]*resourceapi.ResourceClaim, 0, len(claims))
	for _, origClaim := range claims {
		claim := origClaim
		if obj, ok := c.inFlightAllocations.Load(claim.UID); ok {
			claim = obj.(*resourceapi.ResourceClaim)
		}
		if claim.Status.Allocation != nil {
			allocated = append(allocated, claim)
		}
	}
	return allocated, nil
}

func (c *claimTracker) AssumeClaimAfterApiCall(claim *resourceapi.ResourceClaim) error {
	return c.cache.Assume(claim)
}

func (c *claimTracker) AssumedClaimRestore(namespace, claimName string) {
	c.cache.Restore(namespace + "/" + claimName)
}
