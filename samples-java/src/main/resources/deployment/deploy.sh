#!/bin/bash
# Usage: IMAGE_REGISTRY=<your-registry> ./deploy.sh
# IMAGE_REGISTRY is the Azure Container Registry name (without .azurecr.io)
# hosting the flink:1.17.1-scala_2.12 image referenced in the manifests.
: "${IMAGE_REGISTRY:?Set IMAGE_REGISTRY to your Azure Container Registry name}"
env | grep -i "FLINK_" >> .test-env
kubectl create ns flink 2>/dev/null
kubectl create secret generic flinkapp --from-env-file=.test-env -n flink
sed "s/<your-registry>/${IMAGE_REGISTRY}/g" jobmanager.yaml | kubectl apply -n flink -f -
sed "s/<your-registry>/${IMAGE_REGISTRY}/g" taskmanager.yaml | kubectl apply -n flink -f -
kubectl apply -f jobmanager-service.yaml  -n flink
rm .test-env