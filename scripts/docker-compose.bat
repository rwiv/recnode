cd ..
docker compose -f ./docker/docker-compose.yml --env-file ./dev/.env rm
docker compose -f ./docker/docker-compose.yml --env-file ./dev/.env up
pause