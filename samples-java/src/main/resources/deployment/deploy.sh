#!/bin/bash
env | grep -i "FLINK_" >> .test-env
kubectl create ns flink 2>/dev/null
kubectl create secret generic flinkapp --from-env-file=.test-env -n flink
kubectl apply -f jobmanager.yaml  -n flink
kubectl apply -f taskmanager.yaml  -n flink
kubectl apply -f jobmanager-service.yaml  -n flink
rm .test-env