cd ..
docker compose -f ./docker/docker-compose-test-batch.yml --env-file ./dev/.env up
pause