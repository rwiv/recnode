#!/bin/sh

ENDPOINT=$1
ONCE=$2
UID=$3

curl \
  -d "{ \"reqType\": \"chzzk_live\", \"chzzkLive\": { \"uid\": \"${UID}\", \"once\": ${ONCE} } }" \
  -H "Content-Type: application/json" \
  -X POST ${ENDPOINT}/stdl
