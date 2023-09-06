#!/bin/bash
kubectl delete deployment jobmanager taskmanager -n flink
kubectl delete svc jobmanager-service -n flink
kubectl delete secret flinkapp -n flink
kubectl delete ns flink