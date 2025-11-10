#!/bin/sh

set -e

ENERGY_DATA=hdfs-energy-data.json
METEROLOGICAL_OBSERVATIONS=hdfs-meterological-observations.json
PROCESSED_DATA=hdfs-processed-data.json

curl -X POST \
     -H "Content-Type: application/json" \
     --data @${ENERGY_DATA} \
     http://127.0.0.1:8083/connectors | jq

echo "Deployed ${ENERGY_DATA}"
sleep 1

curl -X POST \
     -H "Content-Type: application/json" \
     --data @${METEROLOGICAL_OBSERVATIONS} \
     http://127.0.0.1:8083/connectors | jq

echo "Deployed ${METEROLOGICAL_OBSERVATIONS}"
sleep 1

curl -X POST \
     -H "Content-Type: application/json" \
     --data @${PROCESSED_DATA} \
     http://127.0.0.1:8083/connectors | jq

echo "Deployed ${PROCESSED_DATA}"
