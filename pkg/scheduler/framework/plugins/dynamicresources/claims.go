package dynamicresources

import (
	"fmt"

	resourceapi "k8s.io/api/resource/v1alpha3"
	"k8s.io/kubernetes/pkg/scheduler/util/assumecache"
)

type ClaimsAssumeCache struct {
	cache *assumecache.AssumeCache
}

func (c *ClaimsAssumeCache) Get(namespace, claimName string) (*resourceapi.ResourceClaim, error) {
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

func (c *ClaimsAssumeCache) GetOriginal(namespace, claimName string) (*resourceapi.ResourceClaim, error) {
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

func (c *ClaimsAssumeCache) List() ([]*resourceapi.ResourceClaim, error) {
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

func (c *ClaimsAssumeCache) Assume(claim *resourceapi.ResourceClaim) error {
	return c.cache.Assume(claim)
}

func (c *ClaimsAssumeCache) Restore(namespace, claimName string) {
	c.cache.Restore(namespace + "/" + claimName)
}
