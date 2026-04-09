#!/bin/bash
# Build the Flink connector and the quickstart job JAR.
# Run this from the docker-quickstart/ directory.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=== Step 1: Building and installing flink-connector-kusto ==="
cd "${REPO_ROOT}"
mvn clean install -DskipTests -q
echo "Connector installed to local Maven repository."

echo ""
echo "=== Step 2: Building quickstart Flink job JAR ==="
cd "${SCRIPT_DIR}/flink-job"
mvn clean package -DskipTests -q
echo "Job JAR built successfully."

echo ""
echo "=== Step 3: Building Docker images ==="
cd "${SCRIPT_DIR}"
docker compose build

echo ""
echo "Build complete!"
echo ""
echo "Next steps:"
echo "  1. cp .env.example .env   # then edit .env with your Kusto credentials"
echo "  2. docker compose up -d   # start Kafka + Flink"
echo "  3. ./submit-job.sh        # submit the Flink job"
echo "  4. ./produce-messages.sh  # produce test messages"
echo "  5. Open http://localhost:8081 for the Flink Web UI"
