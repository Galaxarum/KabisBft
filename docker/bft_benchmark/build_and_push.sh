USER=flavoriz

docker build -t bft-receiver -f bft-receiver.dockerfile
docker image tag bft-receiver USER/bft-receiver:latest
docker image push USER/bft-receiver:latest

docker build -t bft-sender -f bft-sender.dockerfile
docker image tag bft-sender USER/bft-sender:latest
docker image push USER/bft-sender:latest

docker build -t bft-service-replica -f bft-service-replica.dockerfile
docker image tag bft-service-replica USER/bft-service-replica:latest
docker image push USER/bft-service-replica:latest

