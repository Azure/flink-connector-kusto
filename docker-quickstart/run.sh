#!/bin/bash
# One-shot quickstart: build, start, submit job, produce messages, and verify.
#
# Usage:
#   ./run.sh              # full run with default 20000 messages in 5 bursts
#   ./run.sh 50000 10     # 50000 messages in 10 bursts

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

TOTAL_MESSAGES="${1:-20000}"
NUM_BURSTS="${2:-5}"
BURST_SIZE=$((TOTAL_MESSAGES / NUM_BURSTS))
PAUSE_SECONDS=15

# ── Colors ───────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

step() { echo -e "\n${GREEN}=== $1 ===${NC}"; }
info() { echo -e "${CYAN}$1${NC}"; }
warn() { echo -e "${YELLOW}$1${NC}"; }

# ── Pre-flight checks ───────────────────────────────────────────────────────
if [ ! -f .env ]; then
    if [ -f .env.example ]; then
        echo "ERROR: .env file not found."
        echo "  cp .env.example .env"
        echo "  # then edit .env with your Kusto credentials"
        exit 1
    fi
fi

set -a
source .env
set +a

for var in KUSTO_CLUSTER_URL KUSTO_DATABASE KUSTO_TABLE KUSTO_APP_ID KUSTO_APP_KEY KUSTO_TENANT_ID; do
    if [ -z "${!var:-}" ] || [[ "${!var}" == your-* ]]; then
        echo "ERROR: ${var} is not set or still has placeholder value in .env"
        exit 1
    fi
done

TOPIC="${KAFKA_TOPIC:-sensor-readings}"

# ── Step 1: Build ────────────────────────────────────────────────────────────
step "Step 1/7: Building connector and job JARs"

REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"
mvn clean install -DskipTests -q
cd "${SCRIPT_DIR}/flink-job"
mvn clean package -DskipTests -q
cd "${SCRIPT_DIR}"

info "Build complete."

# ── Step 2: Build Docker images & start services ────────────────────────────
step "Step 2/7: Starting Docker services (Kafka, Flink, OTEL Collector, Grafana)"

docker compose build --quiet
docker compose up -d

info "Waiting for Kafka..."
until docker exec kafka bin/kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    printf "."
    sleep 2
done
echo " Ready!"

info "Waiting for Flink JobManager..."
until curl -sf http://localhost:8081/overview > /dev/null 2>&1; do
    printf "."
    sleep 2
done
echo " Ready!"

# ── Step 3: Create Kafka topic ──────────────────────────────────────────────
step "Step 3/7: Creating Kafka topic '${TOPIC}'"

docker exec kafka bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic "${TOPIC}" --partitions 2 --replication-factor 1 \
    --if-not-exists 2>/dev/null || true

# ── Step 4: Submit Flink job ────────────────────────────────────────────────
step "Step 4/7: Submitting Flink job"

JOB_JAR=$(find flink-job/target -name "flink-kusto-quickstart-*.jar" \
    ! -name "*original*" 2>/dev/null | head -1)

UPLOAD_RESPONSE=$(curl -sf -X POST http://localhost:8081/jars/upload \
    -F "jarfile=@${JOB_JAR}")
JAR_ID=$(echo "${UPLOAD_RESPONSE}" | grep -o '"filename":"[^"]*"' | \
    sed 's/"filename":"//;s/"//' | sed 's|.*/||')

PROGRAM_ARGS="--kafka.bootstrap.servers kafka:9092"
PROGRAM_ARGS="${PROGRAM_ARGS} --kafka.topic ${TOPIC}"
PROGRAM_ARGS="${PROGRAM_ARGS} --kusto.cluster.url ${KUSTO_CLUSTER_URL}"
PROGRAM_ARGS="${PROGRAM_ARGS} --kusto.database ${KUSTO_DATABASE}"
PROGRAM_ARGS="${PROGRAM_ARGS} --kusto.table ${KUSTO_TABLE}"
PROGRAM_ARGS="${PROGRAM_ARGS} --kusto.app.id ${KUSTO_APP_ID}"
PROGRAM_ARGS="${PROGRAM_ARGS} --kusto.app.key ${KUSTO_APP_KEY}"
PROGRAM_ARGS="${PROGRAM_ARGS} --kusto.tenant.id ${KUSTO_TENANT_ID}"

RUN_RESPONSE=$(curl -sf -X POST "http://localhost:8081/jars/${JAR_ID}/run" \
    -H "Content-Type: application/json" \
    -d "{\"programArgs\": \"${PROGRAM_ARGS}\"}")
JOB_ID=$(echo "${RUN_RESPONSE}" | grep -o '"jobid":"[^"]*"' | sed 's/"jobid":"//;s/"//')

info "Job submitted: ${JOB_ID}"
info "Flink UI: http://localhost:8081/#/job/${JOB_ID}"

# Give the job a moment to initialize
sleep 5

# ── Step 5: Produce messages in bursts ──────────────────────────────────────
step "Step 5/7: Producing ${TOTAL_MESSAGES} messages in ${NUM_BURSTS} bursts of ${BURST_SIZE} (${PAUSE_SECONDS}s pause between bursts)"

generate_batch() {
    local start=$1
    local count=$2
    for i in $(seq "$start" "$((start + count - 1))"); do
        ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
        id=$((RANDOM % 10000))
        value=$((RANDOM % 1000))
        sensor="sensor-$((RANDOM % 5 + 1))"
        echo "{\"id\":${id},\"timestamp\":\"${ts}\",\"sensor\":\"${sensor}\",\"value\":${value},\"unit\":\"celsius\",\"message_number\":${i}}"
    done
}

total_sent=0
for burst in $(seq 1 "${NUM_BURSTS}"); do
    remaining=$((TOTAL_MESSAGES - total_sent))
    chunk=$((remaining < BURST_SIZE ? remaining : BURST_SIZE))

    info "Burst ${burst}/${NUM_BURSTS}: sending ${chunk} messages..."
    generate_batch $((total_sent + 1)) "$chunk" | \
        docker exec -i kafka bin/kafka-console-producer.sh \
            --bootstrap-server localhost:9092 \
            --topic "${TOPIC}" 2>/dev/null
    total_sent=$((total_sent + chunk))
    echo "  Sent ${total_sent}/${TOTAL_MESSAGES} total messages"

    if [ "$burst" -lt "${NUM_BURSTS}" ]; then
        info "Pausing ${PAUSE_SECONDS}s before next burst..."
        sleep "${PAUSE_SECONDS}"
    fi
done

echo ""
info "All ${total_sent} messages produced."

# ── Step 6: Wait for metrics to flow ────────────────────────────────────────
step "Step 6/7: Waiting for metrics to start flowing"

info "Checking Flink Prometheus endpoint..."
METRICS_OK=false
for i in $(seq 1 10); do
    if curl -sf http://localhost:9249/metrics 2>/dev/null | grep -q "flink_jobmanager"; then
        METRICS_OK=true
        break
    fi
    printf "."
    sleep 3
done

if [ "$METRICS_OK" = true ]; then
    echo " Flink metrics are being exported."
    info "OTEL Collector is scraping and forwarding to Kusto."
else
    warn "Flink metrics endpoint not yet responding — OTEL will start scraping once available."
fi

# ── Step 7: Summary ─────────────────────────────────────────────────────────
step "Step 7/7: Done!"

echo ""
echo "Pipeline is running:"
echo "  Kafka topic:  ${TOPIC} (${total_sent} messages)"
echo "  Flink job:    ${JOB_ID}"
echo "  Flink UI:     http://localhost:8081"
echo "  Grafana:      http://localhost:3000  (admin/admin)"
echo "  Prometheus:   http://localhost:9249/metrics"
echo ""
echo "Data should appear in Kusto within ~60 seconds."
echo "Metrics appear in OTELMetrics table within ~30 seconds."
echo ""
echo "Query data:"
echo "  ${KUSTO_TABLE} | take 10"
echo "  ${KUSTO_TABLE} | summarize count(), avg(value) by sensor"
echo ""
echo "Query metrics:"
echo "  OTELMetrics | where MetricName startswith 'flink_taskmanager_job_task_operator_' | summarize arg_max(Timestamp, MetricValue) by MetricName"
echo ""
echo "To stop:  docker compose down"
echo "Cleanup:  docker compose down -v"
