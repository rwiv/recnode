cd ..
docker compose -f ./docker/docker-compose-test-watcher.yml --env-file ./dev/.env up
pause