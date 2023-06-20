# ✈️ Distributed Benchmarks
In order to run the benchmarks in a distributed environment, [docker swarm](https://docs.docker.com/engine/swarm/) is used.  
As of April 2023, docker swarm is supported only on Linux.

## ⚙️ Swarm setup
On the leader machine, init the swarm using:
```shell
docker swarm init --advertise-addr=<IP-ADDRESS-OF-MANAGER>
```
If the host only has one network interface, the `--advertise-addr` flag is optional.  
The command to connect to the swarm will be printed after the execution.


On the other machine, connect to the swarm:
```shell
docker swarm join --token <TOKEN> \
  --advertise-addr <IP-ADDRESS-OF-WORKER-1> \
  <IP-ADDRESS-OF-MANAGER>:2377
```

## 🚧 Create the networks
The benchmarks will use 2 networks, one for the **storage** channel and the other for the **validation** channel.   
**On the leader node**:
- Create the `bft_benchmarks` network: 
```shell
docker network create -d overlay --attachable bft_benchmarks
```
- Create the `kafka_benchmarks` network:
```shell
docker network create -d overlay --attachable kafka_benchmarks
```

## 🚀 Run the benchmarks
Start the **senders** compose on a node and the **receivers** compose on the other.

Example:
```shell
 docker compose --file "kabis-compose-receivers.yaml" --env-file "../../envs/kabis/0-0.env" up --force-recreate --abort-on-container-exit
```