cd ..
docker compose -f ./docker/docker-compose-server-stage.yml --env-file ./dev/.env-server-stage stop
docker compose -f ./docker/docker-compose-server-stage.yml --env-file ./dev/.env-server-stage rm -f
docker compose -f ./docker/docker-compose-server-stage.yml --env-file ./dev/.env-server-stage up
pause