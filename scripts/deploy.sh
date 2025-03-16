#!/bin/sh

cd ~/project/stdl

sudo docker compose -f ./docker/docker-compose-server.yml --env-file ./secret/.env stop
sudo docker compose -f ./docker/docker-compose-server.yml --env-file ./secret/.env rm -f

git pull

sudo docker compose -f ./docker/docker-compose-server.yml --env-file ./secret/.env up -d

docker rmi harbor.rwiv.xyz/private/stdl:0.3.8
docker logs stdl
