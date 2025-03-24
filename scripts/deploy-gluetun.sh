#!/bin/sh

cd ~/project/stdl

sudo docker compose -f ./docker/docker-compose-gluetun.yml --env-file ./secret/.env stop
sudo docker compose -f ./docker/docker-compose-gluetun.yml --env-file ./secret/.env rm -f

sudo docker compose -f ./docker/docker-compose-gluetun.yml --env-file ./secret/.env up -d

sleep 3
docker logs gluetun
