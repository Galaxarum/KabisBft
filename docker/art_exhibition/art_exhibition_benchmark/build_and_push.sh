#!/bin/bash

docker build -t art-estate -f art-estate.dockerfile .
docker image tag art-estate $1/art-estate:latest
docker image push $1/art-estate:latest

docker build -t ensure -f ensure.dockerfile .
docker image tag ensure $1/ensure:latest
docker image push $1/ensure:latest

docker build -t safe-sense -f safe-sense.dockerfile .
docker image tag safe-sense $1/safe-sense:latest
docker image push $1/safe-sense:latest

docker build -t safe-corp -f safe-corp.dockerfile .
docker image tag safe-corp $1/safe-corp:latest
docker image push $1/safe-corp:latest

docker build -t kabis-service-replica -f kabis-service-replica.dockerfile .
docker image tag kabis-service-replica $1/kabis-service-replica:latest
docker image push $1/kabis-service-replica:latest

