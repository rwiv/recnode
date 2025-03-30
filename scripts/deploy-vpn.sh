#!/bin/sh

cd ~/project/stdl

sudo docker compose -f ./docker/docker-compose-server.yml --env-file ./secret/.env stop
sudo docker compose -f ./docker/docker-compose-server.yml --env-file ./secret/.env rm -f

sudo docker compose -f ./docker/docker-compose-watcher.yml --env-file ./secret/.env stop
sudo docker compose -f ./docker/docker-compose-watcher.yml --env-file ./secret/.env rm -f

sudo docker compose -f ./docker/docker-compose-server-vpn1.yml --env-file ./secret/.env stop
sudo docker compose -f ./docker/docker-compose-server-vpn1.yml --env-file ./secret/.env rm -f

sudo docker compose -f ./docker/docker-compose-server-vpn2.yml --env-file ./secret/.env stop
sudo docker compose -f ./docker/docker-compose-server-vpn2.yml --env-file ./secret/.env rm -f

git pull

sudo docker rmi harbor.rwiv.xyz/private/stdl:0.4.8
sudo docker pull harbor.rwiv.xyz/private/stdl:0.5.1

sudo docker compose -f ./docker/docker-compose-watcher.yml --env-file ./secret/.env up -d
sudo docker compose -f ./docker/docker-compose-server.yml --env-file ./secret/.env up -d
sudo docker compose -f ./docker/docker-compose-server-vpn1.yml --env-file ./secret/.env up -d
sudo docker compose -f ./docker/docker-compose-server-vpn2.yml --env-file ./secret/.env up -d

sleep 3
sudo docker logs stdl
sudo docker logs stdl-vpn1
sudo docker logs stdl-vpn2
