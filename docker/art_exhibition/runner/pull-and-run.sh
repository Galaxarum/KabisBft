docker compose --file $1 --env-file $2 pull

docker compose --file $1 --env-file $2 up --force-recreate --abort-on-container-exit