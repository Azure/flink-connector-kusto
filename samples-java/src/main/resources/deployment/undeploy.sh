#!/bin/bash
kubectl delete deployment jobmanager taskmanager -n flink
kubectl delete svc jobmanager -n flink
kubectl delete secret flinkapp -n flink
kubectl delete ns flink