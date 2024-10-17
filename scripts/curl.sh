#!/bin/sh

HOST=$1
UID=$2

curl -X POST -v \
   -H "content-type: application/json"  \
   -H "ce-specversion: 1.0" \
   -H "ce-source: my/curl/command" \
   -H "ce-type: my.demo.event" \
   -H "ce-id: 123" \
   -d "{ \"reqType\": \"chzzk_live\", \"chzzkLive\": { \"uid\": \"${UID}\", \"once\": true } }" \
   http://${HOST}/default/job-sink-logger
