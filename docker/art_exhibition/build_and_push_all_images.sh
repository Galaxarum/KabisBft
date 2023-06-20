#!/bin/bash
source ../../.env
echo "Reading docker-hub username from .env: ${DOCKERHUB_USERNAME}"

cd art_exhibition_benchmark && ./build_and_push.sh "${DOCKERHUB_USERNAME}" && cd ..
