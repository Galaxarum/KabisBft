#!/bin/bash
read -p "Enter docker hub username: " USER

docker build -t kabis-receiver -f kabis-receiver.dockerfile
docker image tag kabis-receiver $USER/kabis-receiver:latest
docker image push $USER/kabis-receiver:latest

docker build -t kabis-sender -f kabis-sender.dockerfile
docker image tag kabis-sender $USER/kabis-sender:latest
docker image push $USER/kabis-sender:latest

docker build -t kabis-service-replica -f kabis-service-replica.dockerfile
docker image tag kabis-service-replica $USER/kabis-service-replica:latest
docker image push $USER/kabis-service-replica:latest

