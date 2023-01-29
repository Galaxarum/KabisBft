KABIS_ENVS=envs/kabis
KAFKA_ENVS=envs/kafka

for conf in "$KABIS_ENVS"/*; do
  docker-compose --file "kabis-compose.yaml" --env-file "./$conf" up --force-recreate --abort-on-container-exit &> /dev/null
  echo "kabis: $conf"
done

for type in kafka bft; do
for conf in "$KAFKA_ENVS"/*; do
  docker-compose --file "$type-compose.yaml" --env-file "./$conf" up --force-recreate --abort-on-container-exit &> /dev/null
  echo "$type: $conf"
done
done
