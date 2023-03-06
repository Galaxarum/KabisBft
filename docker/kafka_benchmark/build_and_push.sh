USER=flavoriz

docker build -t kafka-receiver -f kafka-receiver.dockerfile
docker image tag kafka-receiver USER/kafka-receiver:latest
docker image push USER/kafka-receiver:latest

docker build -t kafka-sender -f kafka-sender.dockerfile
docker image tag kafka-sender USER/kafka-sender:latest
docker image push USER/kafka-sender:latest

