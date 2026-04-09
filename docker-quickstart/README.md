# Docker Quickstart: Flink Kafka-to-Kusto Pipeline

This quickstart sets up a complete local environment that streams JSON messages from
Apache Kafka into Azure Data Explorer (Kusto) using Apache Flink and the
flink-connector-kusto.

## Architecture

```
┌──────────────────┐     ┌─────────────────────────────┐     ┌──────────────────┐
│                  │     │  Apache Flink                │     │                  │
│  Kafka           │────>│  KafkaSource (JSON)          │────>│  Azure Data      │
│  (Strimzi KRaft) │     │  -> KustoWriteSink (CSV)     │     │  Explorer        │
│                  │     │                              │     │                  │
└──────────────────┘     │  JobManager (:8081)          │     └──────────────────┘
                         │  TaskManager (4 slots)       │
                         └─────────────────────────────┘
```

### Components

| Service | Image | Description | Ports |
|---|---|---|---|
| `kafka` | Strimzi (KRaft) | Kafka broker, no ZooKeeper | 29092 |
| `jobmanager` | `flink:1.20.0-java11` | Flink Job Manager + Web UI | 8081 |
| `taskmanager` | `flink:1.20.0-java11` | Flink Task Manager (4 slots) | — |

### Data Flow

1. `produce-messages.sh` publishes JSON sensor readings to a Kafka topic
2. Flink `KafkaSource` reads the messages as JSON strings
3. A `RichMapFunction` deserializes JSON into `SensorReading` POJOs
4. `KustoWriteSink` serializes POJOs to CSV and ingests into Azure Data Explorer

## Prerequisites

- **Docker** and **Docker Compose** (v2)
- **Java 11+** and **Maven** (to build the connector and job JARs)
- An **Azure Data Explorer cluster** with:
  - A database for the quickstart data
  - An Azure AD Service Principal with `Ingestor` role on the database

## Quick Start

### 1. Create the Kusto Table

Before starting, create the target table in your Azure Data Explorer database.
Open [Kusto Web Explorer](https://dataexplorer.azure.com) and run the commands in
`kusto-config/create-table.kql`:

```kql
.create table FlinkQuickstart (
    id: int,
    message_number: int,
    sensor: string,
    timestamp: string,
    unit: string,
    value: int
)

.alter table FlinkQuickstart policy ingestionbatching
  '{ "MaximumBatchingTimeSpan": "00:00:30" }'
```

> **Note:** Column order matches the POJO field alphabetical order used by
> flink-connector-kusto's CSV serialization.

### 2. Configure Environment Variables

```bash
cd docker-quickstart
cp .env.example .env
# Edit .env with your Azure Data Explorer credentials
```

| Variable | Description |
|---|---|
| `KUSTO_CLUSTER_URL` | Kusto engine endpoint (e.g. `https://cluster.region.kusto.windows.net`) |
| `KUSTO_DATABASE` | Target database name |
| `KUSTO_TABLE` | Target table name (default: `FlinkQuickstart`) |
| `KUSTO_APP_ID` | Azure AD Application (client) ID |
| `KUSTO_APP_KEY` | Azure AD Application secret |
| `KUSTO_TENANT_ID` | Azure AD Tenant ID |
| `KAFKA_TOPIC` | Kafka topic name (default: `sensor-readings`) |

### 3. Build Everything

```bash
./build.sh
```

This will:
1. Build and install `flink-connector-kusto` to your local Maven repository
2. Build the quickstart Flink job JAR (with all dependencies shaded)
3. Build the Docker images

### 4. Start Services

```bash
docker compose up -d
```

Wait for all services to be healthy:

```bash
docker compose ps
```

The Flink Web UI will be available at [http://localhost:8081](http://localhost:8081).

### 5. Submit the Flink Job

```bash
./submit-job.sh
```

This will:
1. Wait for Flink and Kafka to be ready
2. Create the Kafka topic (if it doesn't exist)
3. Upload the job JAR to the Flink cluster
4. Start the Kafka-to-Kusto pipeline

### 6. Produce Test Messages

```bash
# Send 100 messages in batch mode (default)
./produce-messages.sh

# Send 500 messages
./produce-messages.sh 500

# Send messages one per second (serial mode)
./produce-messages.sh 100 1
```

Sample message format:

```json
{"id":4821,"timestamp":"2026-03-18T03:55:00Z","sensor":"sensor-3","value":472,"unit":"celsius","message_number":1}
```

### 7. Verify

**Check Flink job status:**

Open [http://localhost:8081](http://localhost:8081) and verify the job is `RUNNING`.

**Query ingested data in Kusto:**

After ~60 seconds (flush interval + batching policy), data should appear:

```kql
FlinkQuickstart
| take 10
```

```kql
FlinkQuickstart
| summarize count(), avg(value), min(value), max(value) by sensor
```

### 8. View Grafana Dashboard

Grafana starts with a pre-provisioned **Flink Kusto Sink Metrics** dashboard.

1. Open [http://localhost:3000](http://localhost:3000)
2. Login with `admin` / `admin`
3. Navigate to **Dashboards → Flink → Flink Kusto Sink Metrics**

Dashboard panels:

| Panel | Description |
|---|---|
| Records Sent to Kusto | Time series of records written |
| Bytes Sent to Blob Storage | CSV bytes uploaded |
| Ingestion Successes / Failures | Ingestion outcome counters |
| Blobs Uploaded | Blob upload count over time |
| Ingestion Latency (ms) | p50/p95/p99 ingestion poll latency |
| Blob Upload Latency (ms) | Serialization + upload latency |
| Send Latency | Current send time gauge |
| Flink Job Health | Uptime, checkpoint stats |
| All Metrics | Table view of latest values |

> The Grafana ADX datasource is auto-configured using the same credentials
> from `.env`. The Service Principal needs at least **Viewer** role on the database.

### 9. Import Kusto Web Explorer Dashboard

A pre-built Azure Data Explorer dashboard is provided for use directly in
[Kusto Web Explorer](https://dataexplorer.azure.com).

**Two pages:** Kusto Sink (records, bytes, ingestion, latency, blobs) and
Flink Job Health (uptime, checkpoints).

**To import:**

1. Open [Kusto Web Explorer](https://dataexplorer.azure.com)
2. Go to **Dashboards** → **New dashboard** → **Import dashboard from file**
3. Select `dashboards/kusto-dashboard.json`
4. Edit the data source: replace **Cluster URI** and **Database** with your values
5. Click **Save**

## Customization

### Delivery Guarantee

The quickstart uses `AT_LEAST_ONCE` delivery. To use the two-phase commit sink
with exactly-once semantics, modify `KafkaToKustoJob.java` to call
`buildWriteAheadSink()` instead of `build()`:

```java
KustoWriteSink.builder()
    .setConnectionOptions(connectionOptions)
    .setWriteOptions(writeOptions)
    .buildWriteAheadSink(sensorStream, 2);
```

### Batch Size and Interval

Adjust the Kusto write options in `KafkaToKustoJob.java`:

```java
KustoWriteOptions writeOptions = KustoWriteOptions.builder()
    .withDatabase(database)
    .withTable(table)
    .withBatchIntervalMs(10000)   // flush every 10 seconds
    .withBatchSize(5000)          // or after 5000 records
    .withDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    .build();
```

### Parallelism

Change the sink parallelism in the `.build()` call:

```java
KustoWriteSink.builder()
    .setConnectionOptions(connectionOptions)
    .setWriteOptions(writeOptions)
    .build(sensorStream, 4);  // 4 parallel writers
```

## Stopping

```bash
docker compose down
```

To also remove volumes:

```bash
docker compose down -v
```

## Troubleshooting

**Flink job won't start:**
```bash
docker compose logs jobmanager
docker compose logs taskmanager
```

**No data in Kusto:**
- Check the Flink UI at http://localhost:8081 for exceptions
- Verify the Service Principal has `Ingestor` role on the database
- Check the Kusto table's ingestion batching policy (should be ~30s for quickstart)

**Kafka connection issues:**
```bash
# Verify Kafka is running and topic exists
docker exec kafka bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Check topic has messages
docker exec kafka bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic sensor-readings --from-beginning --max-messages 5
```

**"Unable to get ingestion resources" / timeout errors:**

This means the Flink TaskManager can't reach the Kusto cluster. Verify:

1. `KUSTO_CLUSTER_URL` in `.env` is the **engine URL** (e.g. `https://mycluster.eastus.kusto.windows.net`)
   — the connector adds the `ingest-` prefix automatically
2. Docker containers have outbound internet access:
   ```bash
   docker exec flink-taskmanager curl -sf https://mycluster.eastus.kusto.windows.net
   ```
3. The Service Principal has **Ingestor** role on the database
4. If behind a corporate proxy/VPN, add DNS config to docker-compose:
   ```yaml
   taskmanager:
     dns:
       - 8.8.8.8
   ```

## File Structure

```
docker-quickstart/
├── .env.example                # Environment variable template
├── .gitignore                  # Ignores .env and build artifacts
├── README.md                   # This file
├── build.sh                    # Build connector + job + Docker images
├── run.sh                      # One-shot: build, start, submit, produce
├── submit-job.sh               # Submit the Flink job to the cluster
├── produce-messages.sh         # Produce test JSON messages to Kafka
├── docker-compose.yml          # Service orchestration (Kafka + Flink + OTEL + Grafana)
├── Dockerfile.kafka            # Kafka broker image (Strimzi KRaft)
├── dashboards/
│   └── kusto-dashboard.json    # Azure Data Explorer dashboard (import to Kusto Web Explorer)
├── grafana/
│   ├── dashboards/
│   │   └── flink-kusto-metrics.json    # Pre-built Grafana dashboard
│   └── provisioning/
│       ├── dashboards/
│       │   └── dashboard.yaml          # Dashboard provisioning config
│       └── datasources/
│           └── kusto.yaml              # ADX datasource provisioning config
├── kusto-config/
│   └── create-table.kql        # Kusto table + OTELMetrics creation script
├── otel-collector/
│   └── otel-collector-config.yaml  # OTEL Collector pipeline config
└── flink-job/
    ├── pom.xml                 # Maven POM for the Flink job
    └── src/main/java/com/microsoft/azure/flink/quickstart/
        ├── KafkaToKustoJob.java    # Main Flink job (Kafka -> Kusto)
        └── SensorReading.java      # POJO for sensor reading messages
```
