#!/bin/bash

CGO_ENABLED=0 LD_FLAGS=-s go build && \
docker build . -t gcr.io/gke-jtuznik-hosted-master/dra-test-driver:latest && \
docker push gcr.io/gke-jtuznik-hosted-master/dra-test-driver:latest

rm -f test-driver
