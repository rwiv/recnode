#!/bin/sh

cd ..
docker compose -f ./docker/docker-compose-batch.yml --env-file ./secret/.env stop
docker compose -f ./docker/docker-compose-batch.yml --env-file ./secret/.env rm -f
docker compose -f ./docker/docker-compose-batch.yml --env-file ./secret/.env up -d