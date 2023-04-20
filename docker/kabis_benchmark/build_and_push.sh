#!/bin/bash

docker build -t kabis-receiver -f kabis-receiver.dockerfile .
docker image tag kabis-receiver $1/kabis-receiver:latest
docker image push $1/kabis-receiver:latest

docker build -t kabis-sender -f kabis-sender.dockerfile .
docker image tag kabis-sender $1/kabis-sender:latest
docker image push $1/kabis-sender:latest

docker build -t kabis-service-replica -f kabis-service-replica.dockerfile .
docker image tag kabis-service-replica $1/kabis-service-replica:latest
docker image push $1/kabis-service-replica:latest

