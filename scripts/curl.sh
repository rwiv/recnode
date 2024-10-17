#!/bin/sh

ENDPOINT=$1
ONCE=$2
UID=$3

curl -X POST -v \
   -H "content-type: application/json"  \
   -H "ce-specversion: 1.0" \
   -H "ce-source: my/stdl" \
   -H "ce-type: my.stdl" \
   -H "ce-id: 123" \
   -d "{ \"reqType\": \"chzzk_live\", \"chzzkLive\": { \"uid\": \"${UID}\", \"once\": ${ONCE} } }" \
   ${ENDPOINT}/media/stdl
