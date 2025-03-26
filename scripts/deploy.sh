#!/bin/sh

cd ~/project/stdl

sudo docker compose -f ./docker/docker-compose-server.yml --env-file ./secret/.env stop
sudo docker compose -f ./docker/docker-compose-server.yml --env-file ./secret/.env rm -f

git pull

sudo docker rmi harbor.rwiv.xyz/private/stdl:0.4.1

sudo docker compose -f ./docker/docker-compose-server.yml --env-file ./secret/.env up -d

sleep 3
sudo docker logs stdl
