#!/bin/bash

read -p "Enter docker hub username: " USER

cd bft_benchmark && ./build_and_push.sh $USER && cd ..
cd kabis_benchmark && ./build_and_push.sh $USER && cd ..
cd kafka_benchmark && ./build_and_push.sh $USER && cd ..
