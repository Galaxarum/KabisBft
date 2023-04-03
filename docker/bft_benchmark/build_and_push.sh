#!/bin/bash

docker build -t bft-receiver -f bft-receiver.dockerfile .
docker image tag bft-receiver $1/bft-receiver:latest
docker image push $1/bft-receiver:latest

docker build -t bft-sender -f bft-sender.dockerfile .
docker image tag bft-sender $1/bft-sender:latest
docker image push $1/bft-sender:latest

docker build -t bft-sender-service-replica -f bft-sender-service-replica.dockerfile .
docker image tag bft-sender-service-replica $1/bft-sender-service-replica:latest
docker image push $1/bft-sender-service-replica:latest

docker build -t bft-receiver-service-replica -f bft-receiver-service-replica.dockerfile .
docker image tag bft-receiver-service-replica $1/bft-receiver-service-replica:latest
docker image push $1/bft-receiver-service-replica:latest

