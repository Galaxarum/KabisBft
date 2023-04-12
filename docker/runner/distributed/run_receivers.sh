KABIS_ENVS=../envs/kabis
KAFKA_ENVS=../envs/kafka

docker system prune -a -f

for conf in "$KABIS_ENVS"/*; do
  docker compose --file "kabis-compose-receivers.yaml" --env-file "$conf" up --force-recreate --abort-on-container-exit
  echo "kabis: $conf"
done

for type in kafka bft; do
for conf in "$KAFKA_ENVS"/*; do
  docker compose --file "$type-compose-receivers.yaml" --env-file "$conf" up --force-recreate --abort-on-container-exit
  echo "$type: $conf"
done
done