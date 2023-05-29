#!/bin/bash

docker build -t kafka-receiver -f kafka-receiver.dockerfile .
docker image tag kafka-receiver $1/kafka-receiver:latest
docker image push $1/kafka-receiver:latest

docker build -t kafka-sender -f kafka-sender.dockerfile .
docker image tag kafka-sender $1/kafka-sender:latest
docker image push $1/kafka-sender:latest

