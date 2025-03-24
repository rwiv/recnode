#!/bin/sh

cd ~/project/stdl

sudo docker compose -f ./docker/docker-compose-server-vpn1.yml --env-file ./secret/.env stop
sudo docker compose -f ./docker/docker-compose-server-vpn1.yml --env-file ./secret/.env rm -f

sudo docker compose -f ./docker/docker-compose-server-vpn2.yml --env-file ./secret/.env stop
sudo docker compose -f ./docker/docker-compose-server-vpn2.yml --env-file ./secret/.env rm -f

git pull

sudo docker compose -f ./docker/docker-compose-server-vpn1.yml --env-file ./secret/.env up -d
sudo docker compose -f ./docker/docker-compose-server-vpn2.yml --env-file ./secret/.env up -d

docker rmi harbor.rwiv.xyz/private/stdl:0.3.9
sleep 3
docker logs stdl
