#!/bin/bash
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka1:29092,kafka2:29092}"
SR_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
TOPIC="${KAFKA_TOPIC:-movie-events}"
PARTITIONS=3
REPLICATION=2
MIN_ISR=1
SCHEMA_FILE="/schemas/movie-event.avsc"

echo "==> Waiting for brokers to be ready …"
sleep 5

echo "==> Creating topic: ${TOPIC}"
kafka-topics \
  --bootstrap-server "${BOOTSTRAP}" \
  --create \
  --if-not-exists \
  --topic "${TOPIC}" \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION}" \
  --config "min.insync.replicas=${MIN_ISR}" \
  --config "retention.ms=604800000"

echo "==> Topic list:"
kafka-topics --bootstrap-server "${BOOTSTRAP}" --list

echo "==> Registering Avro schema with Schema Registry …"

SCHEMA_ESCAPED=$(sed 's/\\/\\\\/g' "${SCHEMA_FILE}" | sed 's/"/\\"/g' | tr -d '\n' | tr -d '\r')
SCHEMA_PAYLOAD="{\"schema\": \"${SCHEMA_ESCAPED}\", \"schemaType\": \"AVRO\"}"

HTTP_CODE=$(curl -s -o /tmp/sr_response.json -w "%{http_code}" \
  -X POST "${SR_URL}/subjects/${TOPIC}-value/versions" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "${SCHEMA_PAYLOAD}")

if [ "${HTTP_CODE}" -ge 200 ] && [ "${HTTP_CODE}" -lt 300 ]; then
  echo "==> Schema registered successfully. Response: $(cat /tmp/sr_response.json)"
else
  echo "==> Schema Registry error (HTTP ${HTTP_CODE}):"
  cat /tmp/sr_response.json
  exit 1
fi

echo "==> Kafka initialisation complete."
