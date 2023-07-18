docker system prune -a -f

docker network create -d overlay --attachable bft_benchmarks
docker network create -d overlay --attachable kafka_benchmarks