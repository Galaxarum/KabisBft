#!/bin/bash

docker build -t bft-receiver -f bft-receiver.dockerfile .
docker image tag bft-receiver $1/bft-receiver:latest
docker image push $1/bft-receiver:latest

docker build -t bft-sender -f bft-sender.dockerfile .
docker image tag bft-sender $1/bft-sender:latest
docker image push $1/bft-sender:latest

docker build -t bft-service-replica -f bft-service-replica.dockerfile .
docker image tag bft-service-replica $1/bft-service-replica:latest
docker image push $1/bft-service-replica:latest
