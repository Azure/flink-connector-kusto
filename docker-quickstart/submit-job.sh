#!/bin/bash
# Submit the Flink job to the running JobManager.
# Reads configuration from .env and submits the Kafka-to-Kusto job.
#
# Prerequisites:
#   1. Run ./build.sh first
#   2. docker compose up -d
#   3. Wait for services to be healthy

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load environment variables
if [ ! -f "${SCRIPT_DIR}/.env" ]; then
    echo "ERROR: .env file not found. Copy .env.example to .env and fill in your credentials."
    exit 1
fi
set -a
source "${SCRIPT_DIR}/.env"
set +a

# Find the job JAR
JOB_JAR=$(find "${SCRIPT_DIR}/flink-job/target" -name "flink-kusto-quickstart-*.jar" \
    ! -name "*-original-*" 2>/dev/null | head -1)
if [ -z "${JOB_JAR}" ]; then
    echo "ERROR: Job JAR not found. Run ./build.sh first."
    exit 1
fi
JOB_JAR_NAME=$(basename "${JOB_JAR}")

echo "Waiting for Flink JobManager to be ready..."
until curl -sf http://localhost:8081/overview > /dev/null 2>&1; do
    printf "."
    sleep 2
done
echo " Ready!"

echo "Waiting for Kafka to be ready..."
until docker exec kafka bin/kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    printf "."
    sleep 2
done
echo " Ready!"

# Create Kafka topic if it doesn't exist
TOPIC="${KAFKA_TOPIC:-sensor-readings}"
echo "Creating Kafka topic '${TOPIC}'..."
docker exec kafka bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic "${TOPIC}" --partitions 2 --replication-factor 1 \
    --if-not-exists 2>/dev/null || true

# Upload JAR to Flink
echo "Uploading job JAR (${JOB_JAR_NAME})..."
UPLOAD_RESPONSE=$(curl -sf -X POST http://localhost:8081/jars/upload \
    -F "jarfile=@${JOB_JAR}")
JAR_ID=$(echo "${UPLOAD_RESPONSE}" | grep -o '"filename":"[^"]*"' | sed 's/"filename":"//;s/"//' | sed 's|.*/||')

if [ -z "${JAR_ID}" ]; then
    echo "ERROR: Failed to upload JAR. Response: ${UPLOAD_RESPONSE}"
    exit 1
fi
echo "Uploaded JAR ID: ${JAR_ID}"

# Build program arguments
PROGRAM_ARGS="--kafka.bootstrap.servers kafka:9092"
PROGRAM_ARGS="${PROGRAM_ARGS} --kafka.topic ${TOPIC}"
PROGRAM_ARGS="${PROGRAM_ARGS} --kusto.cluster.url ${KUSTO_CLUSTER_URL}"
PROGRAM_ARGS="${PROGRAM_ARGS} --kusto.database ${KUSTO_DATABASE}"
PROGRAM_ARGS="${PROGRAM_ARGS} --kusto.table ${KUSTO_TABLE}"
PROGRAM_ARGS="${PROGRAM_ARGS} --kusto.app.id ${KUSTO_APP_ID}"
PROGRAM_ARGS="${PROGRAM_ARGS} --kusto.app.key ${KUSTO_APP_KEY}"
PROGRAM_ARGS="${PROGRAM_ARGS} --kusto.tenant.id ${KUSTO_TENANT_ID}"

# Submit job
echo "Submitting Flink job..."
RUN_RESPONSE=$(curl -sf -X POST "http://localhost:8081/jars/${JAR_ID}/run" \
    -H "Content-Type: application/json" \
    -d "{\"programArgs\": \"${PROGRAM_ARGS}\"}")

JOB_ID=$(echo "${RUN_RESPONSE}" | grep -o '"jobid":"[^"]*"' | sed 's/"jobid":"//;s/"//')

if [ -z "${JOB_ID}" ]; then
    echo "ERROR: Failed to submit job. Response: ${RUN_RESPONSE}"
    exit 1
fi

echo ""
echo "Job submitted successfully!"
echo "  Job ID: ${JOB_ID}"
echo "  Flink UI: http://localhost:8081/#/job/${JOB_ID}"
echo ""
echo "Next: ./produce-messages.sh to send test messages to Kafka"
