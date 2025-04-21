cd ..
docker compose -f ./docker/docker-compose-server-vpn-stage.yml --env-file ./dev/.env stop
docker compose -f ./docker/docker-compose-server-vpn-stage.yml --env-file ./dev/.env rm -f
docker compose -f ./docker/docker-compose-server-vpn-stage.yml --env-file ./dev/.env up -d
pause