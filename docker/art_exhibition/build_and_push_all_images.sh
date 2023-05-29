#!/bin/bash
source ../.env
echo "Reading docker-hub username from .env: ${DOCKERHUB_USERNAME}"

cd bft_benchmark && ./build_and_push.sh "${DOCKERHUB_USERNAME}" && cd ..
cd kabis_benchmark && ./build_and_push.sh "${DOCKERHUB_USERNAME}" && cd ..
cd kafka_benchmark && ./build_and_push.sh "${DOCKERHUB_USERNAME}" && cd ..
