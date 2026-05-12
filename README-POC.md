# NimbusPulse — Docker Compose POC

A working proof-of-concept of the NimbusPulse production architecture, all running locally in Docker. **Five sample connectors** (Python + Go) push synthetic data into **Kafka**; a **Go sink service** fans events out to **Elasticsearch**, **Redis**, and **TimescaleDB**; a **Go Query API** and a **Python FastAPI AI assistant** expose the data; a **single HTML+Tailwind dashboard** gives you one-click access to every UI in the stack.

Apache **NiFi** ships alongside as the visual dataflow tool you'd use to build the production flow.

---

## Table of contents

1. [Quick start](#quick-start)
2. [What's in the box](#whats-in-the-box)
3. [Architecture & data flow](#architecture--data-flow)
4. [Port map & credentials](#port-map--credentials)
5. [Service-by-service tour](#service-by-service-tour)
6. [First-time setup notes](#first-time-setup-notes)
7. [API reference](#api-reference)
8. [Troubleshooting](#troubleshooting)
9. [Extending the POC](#extending-the-poc)
10. [File layout](#file-layout)

---

## Quick start

**Prerequisites:**

- Docker Engine **24+** with Compose v2 (`docker compose`, not `docker-compose`)
- ~**6 GB free RAM** (Elasticsearch alone takes 1 GB; Kafka + NiFi + the rest pushes it up)
- ~**5 GB free disk** for images
- Ports listed in the [port map](#port-map--credentials) free on `localhost`

**Start everything:**

```bash
cd nimbuspulse-poc
docker compose up -d
# (or, equivalently)
make up
```

**Wait ~60 seconds** for Elasticsearch + NiFi to finish booting (Kafka comes up first, then the storage tier, then the sink + connectors). You can watch progress with:

```bash
docker compose ps         # see which services are healthy
docker compose logs -f    # tail all logs
make logs-sink            # tail just the sink to see events flow in
```

**Open the main dashboard:**

→ <http://localhost:8080>

The dashboard auto-updates every few seconds with live stats from the Query API, shows the most recent events, and has tiles linking to every other UI in the stack.

**Stop everything:**

```bash
docker compose down       # stop, keep data
docker compose down -v    # stop + wipe data volumes
```

---

## What's in the box

**18 services** organized into five layers:

| Layer | Service | Image | Purpose |
|---|---|---|---|
| **Entry** | dashboard | nginx:1.27-alpine + custom | Main demo dashboard (HTML/Tailwind/JS) |
| **Dataflow** | nifi | apache/nifi:latest | Visual dataflow orchestrator |
| **Bus** | kafka | apache/kafka:latest | KRaft-mode broker (no ZooKeeper) |
| **Bus** | kafka-ui | provectuslabs/kafka-ui:latest | Web UI for topics & messages |
| **Bus** | kafka-init | apache/kafka:latest | One-shot topic creator |
| **Storage** | elasticsearch | elasticsearch:8.15.3 | Primary event index, full-text search |
| **Storage** | kibana | kibana:8.15.3 | Elasticsearch UI |
| **Storage** | clickhouse | clickhouse/clickhouse-server:latest | OLAP analytics |
| **Storage** | redis | redis:7-alpine | Hot cache + counters + recent list |
| **Storage** | redis-commander | rediscommander/redis-commander | Redis UI |
| **Storage** | timescaledb | timescale/timescaledb:latest-pg16 | Time-series metrics (Postgres) |
| **Storage** | adminer | adminer:latest | SQL UI for Timescale |
| **Storage** | minio | minio/minio:latest | S3-compatible cold storage |
| **Storage** | minio-init | minio/mc:latest | One-shot bucket creator |
| **Observability** | grafana | grafana/grafana:latest | Pre-wired dashboards |
| **Custom** | sink | go 1.23 (built locally) | Consumes Kafka → ES + Redis + Timescale |
| **Custom** | query-api | go 1.23 (built locally) | REST API on top of ES + Redis |
| **Custom** | ai-assistant | python 3.12 (built locally) | FastAPI with simulated AI |
| **Connectors** | connector-cloudwatch | python 3.12 + kafka-python | Synthetic AWS CloudWatch events |
| **Connectors** | connector-k8s-events | go 1.23 + kafka-go | Synthetic Kubernetes events |
| **Connectors** | connector-syslog | python 3.12 + kafka-python | Synthetic syslog RFC 5424 |
| **Connectors** | connector-security | go 1.23 + kafka-go | Synthetic SIEM / WAF / auth events |
| **Connectors** | connector-nginx | python 3.12 + kafka-python | Synthetic Nginx access logs |

**Languages used:**

- **Python** (`kafka-python`, `FastAPI`, `httpx`) where iteration speed matters: 3 connectors + AI assistant
- **Go** (`segmentio/kafka-go`, `pgx/v5`, `redis/go-redis`) where throughput matters: 2 connectors + sink + query API
- **HTML/Tailwind/vanilla JS** for the dashboard

---

## Architecture & data flow

```
┌────────────────┐    ┌───────────────────────────────────────────┐    ┌───────────────────┐
│   5 sample     │    │   Apache Kafka (KRaft, no ZooKeeper)      │    │  Storage tier     │
│   connectors   │    │                                           │    │                   │
│                │    │  raw.cloudwatch                           │    │  Elasticsearch    │
│  cloudwatch (P)│───▶│  raw.k8s-events                           │    │  (np-events idx)  │
│  k8s-events (G)│───▶│  raw.syslog                ┌──────────┐   │───▶│                   │
│  syslog     (P)│───▶│  raw.security        ────▶ │   sink   │ ──┼───▶│  Redis (counters, │
│  security   (G)│───▶│  raw.nginx                 │   (Go)   │   │    │   recent, tput)   │
│  nginx      (P)│───▶│                            └──────────┘   │───▶│                   │
└────────────────┘    │                                  ▲        │    │  TimescaleDB      │
                      │  events.normalized               │        │    │  (event_metrics)  │
       NiFi (visual)  │  events.info / warn / error /    │        │    │                   │
       runs alongside │  critical                        │        │    │  MinIO (cold,     │
       (not on path)  │  metrics.aggregated              │        │    │   bucket created) │
                      └───────────────────────────────────────────┘    └─────────┬─────────┘
                                                                                  │
                      ┌───────────────────────────────┐    ┌──────────────────────▼─────────┐
                      │  Query API (Go) :8090         │    │  AI Assistant (Python) :8091   │
                      │   GET /api/v1/stats           │◀───┤   POST /api/v1/ask             │
                      │   GET /api/v1/recent          │    │   (calls Query API for facts)  │
                      │   GET /api/v1/search          │    └──────────────┬─────────────────┘
                      │   GET /api/v1/throughput      │                   │
                      │   GET /api/v1/sources         │                   │
                      └──────────────┬────────────────┘                   │
                                     │                                    │
                                     ▼                                    ▼
                      ┌─────────────────────────────────────────────────────────┐
                      │  Main Dashboard :8080  (HTML + Tailwind + JS)           │
                      │   live stats · recent events · service tiles · AI chat  │
                      └─────────────────────────────────────────────────────────┘
```

**Why this shape:**

- **Connectors produce directly to Kafka**, each to its own `raw.*` topic. This is what real production agents do (Fluent Bit, Vector, custom collectors).
- **The sink service replaces NiFi in the runtime path** — for a single-machine POC, a Go consumer is simpler than configuring NiFi processors. NiFi is included so you can see how the same flow would be built visually for production (see [nifi/README.md](./nifi/README.md)).
- **Three storage backends**, each for what it's best at: Elasticsearch (search), Redis (hot counters), TimescaleDB (time-series aggregations).
- **The Query API hides the storage tier** — callers don't need to know whether the data came from Redis or ES.
- **The AI assistant is grounded** — it pulls live facts from the Query API before answering, simulating RAG.

---

## Port map & credentials

| Port | Service | URL | Credentials |
|---:|---|---|---|
| **8080** | **Main dashboard** | <http://localhost:8080> | — |
| 8443 | Apache NiFi (HTTPS) | <https://localhost:8443> | `admin` / `ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB` |
| 9092 | Kafka (internal listener) | `kafka:9092` (inside containers) | — |
| 9094 | Kafka (external listener) | `localhost:9094` | — |
| 8181 | Kafka UI | <http://localhost:8181> | — |
| 9200 | Elasticsearch HTTP | <http://localhost:9200> | security disabled |
| 5601 | Kibana | <http://localhost:5601> | — |
| 8123 | ClickHouse HTTP | <http://localhost:8123/play> | `nimbus` / `nimbus` |
| 9100 | ClickHouse native | `localhost:9100` | `nimbus` / `nimbus` |
| 6379 | Redis | `localhost:6379` | — |
| 8182 | Redis Commander | <http://localhost:8182> | — |
| 5432 | TimescaleDB | `localhost:5432` | `nimbus` / `nimbus`, db `metrics` |
| 8888 | Adminer | <http://localhost:8888> | System: PostgreSQL · Server: `timescaledb` · User: `nimbus` · Pass: `nimbus` · DB: `metrics` |
| 9000 | MinIO API (S3) | `localhost:9000` | `nimbus` / `nimbus-secret` |
| 9001 | MinIO Console | <http://localhost:9001> | `nimbus` / `nimbus-secret` |
| 3000 | Grafana | <http://localhost:3000> | `admin` / `nimbus`, or anonymous viewer |
| 8090 | Query API (Go) | <http://localhost:8090> | — |
| 8091 | AI Assistant (Python) | <http://localhost:8091/docs> | — |

> Tip: run `make urls` to print this list with one command.

---

## Service-by-service tour

### Apache NiFi (port 8443)

The visual dataflow orchestrator. In production it would receive data from the ingest gateways and route it to Kafka, performing schema validation, enrichment, and PII masking along the way. In this POC NiFi runs as a clean canvas — see [nifi/README.md](./nifi/README.md) for what the production flow would look like, processor by processor.

**First time you open NiFi:**

1. Browse to <https://localhost:8443>
2. Accept the self-signed certificate warning
3. Log in with `admin` / `ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB`
4. You'll land on an empty canvas — drag processors from the top toolbar to build a flow

### Apache Kafka (KRaft mode, port 9092 internal / 9094 external)

Latest official `apache/kafka` image, configured for KRaft (no ZooKeeper). One broker, three partitions per topic, replication factor 1 (POC-only — production uses 3).

Topics are auto-created by `kafka-init` on startup:

- `raw.cloudwatch`, `raw.k8s-events`, `raw.syslog`, `raw.security`, `raw.nginx` — one per connector
- `events.normalized`, `events.info`, `events.warn`, `events.error`, `events.critical` — production targets (unused by the POC sink, but ready for NiFi)
- `metrics.aggregated` — for Flink-style rollups

**Inspecting messages:**

- **Kafka UI**: <http://localhost:8181> — point and click
- **CLI**:
  ```bash
  docker exec -it np-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic raw.k8s-events --from-beginning
  ```

### Elasticsearch + Kibana (ports 9200 + 5601)

Elasticsearch 8.15.3 with security disabled (POC only). The sink service creates the `np-events` index on startup with an explicit mapping: `time` and `ingestedAt` are dates, `severity`/`source` are keywords, `message` is full-text.

**First time you open Kibana:**

1. Browse to <http://localhost:5601>
2. Sidebar → **Discover**
3. **Create data view** → name: `np-events`, index pattern: `np-events`, time field: `time`
4. **Save**, then explore live events

### ClickHouse (ports 8123 HTTP, 9100 native)

Latest server image with a database `nimbuspulse` and user `nimbus`. Used as a placeholder for OLAP workloads — the sink doesn't write here in the POC, but ClickHouse is provisioned and the Grafana datasource is pre-wired.

Open <http://localhost:8123/play> to use the built-in SQL playground.

### Redis + Redis Commander (ports 6379 + 8182)

Redis 7 alpine stores:

- `np:counter:total` — total events ingested
- `np:counter:sev:{LEVEL}` — count by severity
- `np:counter:source:{ID}` — count by source
- `np:recent` — most recent 200 events as a list
- `np:tput:{0..59}` — 60-bucket per-second throughput ring (90s TTL)

Browse with Redis Commander at <http://localhost:8182>.

### TimescaleDB + Adminer (ports 5432 + 8888)

PostgreSQL 16 with the TimescaleDB extension. The sink creates a hypertable on first run:

```sql
CREATE TABLE event_metrics (
  ts        TIMESTAMPTZ NOT NULL,
  source    TEXT NOT NULL,
  severity  TEXT NOT NULL,
  count     INTEGER NOT NULL DEFAULT 1
);
SELECT create_hypertable('event_metrics', 'ts');
```

Each event becomes one row. Useful queries via Adminer (<http://localhost:8888>):

```sql
SELECT source, severity, count(*)
FROM event_metrics
WHERE ts > now() - interval '5 minutes'
GROUP BY 1, 2
ORDER BY 3 DESC;

SELECT time_bucket('30 seconds', ts) AS bucket, severity, count(*)
FROM event_metrics
WHERE ts > now() - interval '5 minutes'
GROUP BY 1, 2
ORDER BY 1;
```

### MinIO (ports 9000 API + 9001 Console)

S3-compatible object storage. Two buckets are created on startup by `minio-init`:

- `np-cold-events`
- `np-cold-archive`

In production, the sink (or a Kafka Connect S3 sink) would write older events here in Parquet format with an Iceberg manifest. POC doesn't push data here; the buckets are ready when you want to.

Console: <http://localhost:9001> · user `nimbus` · password `nimbus-secret`.

### Grafana (port 3000)

Latest Grafana with anonymous viewer enabled and three datasources pre-wired:

- **TimescaleDB** (default) — query the `event_metrics` hypertable
- **Elasticsearch** — query the `np-events` index
- **ClickHouse** — via the official `grafana-clickhouse-datasource` plugin

To build a dashboard:

1. <http://localhost:3000> → log in (`admin` / `nimbus`) or use anonymous viewer
2. **Explore** → pick TimescaleDB → write SQL or use the visual builder
3. Or **Dashboards → New** to assemble panels

### Sink service (Go, no exposed port)

Internal-only Go service. One Kafka consumer goroutine per `raw.*` topic. For each message:

1. Decode JSON event
2. Enrich (add `ingestedAt`, `enrichment.traceId`, simple PII mask)
3. Write to Elasticsearch (`POST /np-events/_doc/{id}`)
4. Write to Redis (pipeline: counters, recent list, throughput bucket)
5. Write to TimescaleDB (`INSERT INTO event_metrics`)

Watch it work with `make logs-sink`. Stats logged every 15s.

### Query API (Go, port 8090)

Thin REST API on top of Elasticsearch + Redis. CORS-open so the dashboard at `:8080` can call it from the browser.

### AI Assistant (Python FastAPI, port 8091)

A simulated LLM assistant. Routes by keyword to one of ~10 grounded responses, each of which calls the Query API for live facts (stats, top sources, recent events) before answering.

**OpenAPI docs:** <http://localhost:8091/docs>

This is the staffing of the LLM/RAG layer in the production architecture. Plug in a real LLM here (Claude, GPT, local) by replacing `_generate_answer` and adding embedding-based retrieval.

### Sample connectors (5 containers, no exposed ports)

Each connector is a single file that:

1. Waits for Kafka to be ready (with retries)
2. Generates synthetic events at the configured rate (env var `RATE_PER_SEC`)
3. JSON-serializes and publishes to its `raw.*` topic
4. Logs every 25 events

| Connector | Language | Topic | Rate (default) | Sample event types |
|---|---|---|---:|---|
| cloudwatch | Python | `raw.cloudwatch` | 3/s | Lambda timeouts, alarms, runtime errors |
| k8s-events | Go | `raw.k8s-events` | 4/s | Scheduled, Pulled, OOMKilled, ImagePullBackOff |
| syslog | Python | `raw.syslog` | 5/s | sshd failures, kernel OOM, cron, BUG |
| security | Go | `raw.security` | 2/s | Failed logins, WAF blocks, credential stuffing, malware |
| nginx | Python | `raw.nginx` | 8/s | 200/304/401/404/500/502/504 with paths & UAs |

Tweak rates by editing the `RATE_PER_SEC` env in `docker-compose.yml` and running `docker compose up -d connector-X`.

---

## First-time setup notes

After `docker compose up -d`, in order:

1. **(0s)** Kafka starts; healthcheck takes ~20s
2. **(~20s)** `kafka-init` creates topics, exits cleanly (this is expected — `Exited (0)` is success)
3. **(~25s)** All 5 connectors start producing
4. **(~30s)** Elasticsearch becomes healthy
5. **(~35s)** Sink starts consuming, creates ES index + Timescale hypertable
6. **(~60s)** NiFi finishes its long startup
7. **(anytime)** Open <http://localhost:8080>

If you open the dashboard before the sink starts, you'll see "query-api unreachable" for a few seconds. It self-recovers — just wait.

---

## API reference

### Query API — `http://localhost:8090`

| Endpoint | Method | Returns |
|---|---|---|
| `/healthz` | GET | `{status: "ok", uptime}` |
| `/api/v1/stats` | GET | Counters from Redis: total, bySeverity, bySource, errorRate |
| `/api/v1/recent?n=50` | GET | Most recent N events (max 200) from Redis list |
| `/api/v1/search?q=&sev=&source=&size=20` | GET | Elasticsearch search; returns hits + total |
| `/api/v1/throughput` | GET | 60-bucket events-per-second ring from Redis |
| `/api/v1/sources` | GET | Elasticsearch terms aggregation on `source` |

Examples:

```bash
curl http://localhost:8090/api/v1/stats | jq .
curl 'http://localhost:8090/api/v1/search?sev=ERROR&size=5' | jq '.hits[].message'
curl 'http://localhost:8090/api/v1/recent?n=3' | jq .
curl http://localhost:8090/api/v1/throughput | jq '.series[-10:]'
```

### AI Assistant — `http://localhost:8091`

| Endpoint | Method | Body / params | Returns |
|---|---|---|---|
| `/healthz` | GET | — | `{status, service}` |
| `/api/v1/prompts` | GET | — | List of suggested prompts |
| `/api/v1/ask` | POST | `{"question": "..."}` | `{answer, citations, facts, latencyMs}` |
| `/docs` | GET | — | OpenAPI Swagger UI |

Example:

```bash
curl -X POST http://localhost:8091/api/v1/ask \
  -H 'Content-Type: application/json' \
  -d '{"question": "summarize the last few minutes"}' | jq .
```

---

## Troubleshooting

**Elasticsearch dies on startup with exit 137 / OOMKilled**

Raise Docker's memory limit (Docker Desktop → Settings → Resources → Memory ≥ 6 GB), or reduce `ES_JAVA_OPTS` in `docker-compose.yml` from `-Xms1g -Xmx1g` to `-Xms512m -Xmx512m`.

**Port conflict on startup**

Something else is using one of the ports. Find it: `lsof -i :8080` (or whichever port). Either stop the conflicting process, or edit the port mapping in `docker-compose.yml` (e.g. `"8081:80"` for the dashboard).

**`kafka-init` shows as Exited(0) — is that broken?**

No, that's correct. `kafka-init` is a one-shot container: it creates topics, prints the list, and exits cleanly. Compose marks it stopped because it's done.

**Dashboard shows "query-api unreachable"**

The Query API needs Elasticsearch and Redis up first. Check `docker compose ps` for healthy status. If still failing: `docker logs np-query-api`.

**NiFi takes forever to start**

Normal — NiFi's startup is slow (60-90s). Watch `docker logs -f np-nifi`. When you see `JettyServer NiFi has started`, it's ready.

**Connectors not producing events**

Check the connector logs: `docker logs np-conn-cloudwatch` (or whichever). They retry Kafka connection on a loop. If you see repeated "Kafka not ready", ensure `np-kafka` is healthy.

**I want to wipe everything and start fresh**

```bash
docker compose down -v   # or: make reset
docker compose up -d     # or: make up
```

---

## Extending the POC

### Add a new connector (Python version)

1. Copy `connectors/cloudwatch-sim/` to `connectors/myservice-sim/`
2. Edit `app.py`: change `TOPIC` env default and the templates in `EVENT_TEMPLATES`
3. In `docker-compose.yml`, add a new service block (copy `connector-cloudwatch`, change name, build path, container_name, and `TOPIC` env)
4. Add the topic to `kafka-init`'s `for t in ...` list
5. `docker compose up -d --build connector-myservice`

### Add a new connector (Go version)

Same flow, but copy `connectors/k8s-events-sim/` and edit `main.go`.

### Plug in a real LLM

In `services/ai-assistant/app.py`, replace `_generate_answer` with a call to your LLM provider. The `facts` dict is already populated with live stats, recent events, and source counts — pass it as system context.

### Add NiFi to the runtime path

1. Open <https://localhost:8443>
2. Drag in a **ConsumeKafkaRecord_2_6** processor, point at `raw.k8s-events`
3. Chain **EvaluateJsonPath → UpdateAttribute → ReplaceText → RouteOnAttribute → PublishKafkaRecord_2_6** publishing to `events.normalized`
4. Modify the sink service to consume `events.normalized` instead of the raw topics

### Add Grafana dashboards

Datasources are already wired. In Grafana:

1. **Dashboards → New → Add visualization**
2. Pick **TimescaleDB**
3. Query: `SELECT time_bucket('30 seconds', ts) AS time, severity, count(*) FROM event_metrics WHERE $__timeFilter(ts) GROUP BY 1, 2`
4. Visualization: Time series, stacked

---

## File layout

```
nimbuspulse-poc/
├── docker-compose.yml          # 18 services, named volumes, healthchecks
├── Makefile                    # convenience targets (up/down/logs/test/urls)
├── README.md                   # this file
│
├── dashboard/                  # main demo dashboard
│   ├── Dockerfile              # nginx:1.27-alpine
│   ├── nginx.conf
│   └── index.html              # Tailwind + vanilla JS + SVG, ~600 lines
│
├── services/
│   ├── sink/                   # Go: Kafka → ES + Redis + Timescale
│   │   ├── Dockerfile          # multi-stage, distroless-style alpine
│   │   ├── go.mod
│   │   └── main.go
│   ├── query-api/              # Go: REST API on top of storage
│   │   ├── Dockerfile
│   │   ├── go.mod
│   │   └── main.go
│   └── ai-assistant/           # Python FastAPI: simulated LLM
│       ├── Dockerfile
│       ├── requirements.txt
│       └── app.py
│
├── connectors/
│   ├── cloudwatch-sim/         # Python — AWS CloudWatch
│   ├── k8s-events-sim/         # Go — Kubernetes events
│   ├── syslog-sim/             # Python — Syslog RFC 5424
│   ├── security-sim/           # Go — SIEM / WAF / auth
│   └── nginx-sim/              # Python — Nginx access
│
├── nifi/
│   └── README.md               # what the production NiFi flow looks like
│
└── config/
    └── grafana-datasources.yml # provisioned datasources
```

---

## What's intentionally NOT in this POC

To keep the surface area manageable:

- **No authentication or TLS** anywhere (NiFi has its own self-signed cert; everything else is open inside Docker)
- **Single-broker Kafka** (production: 3+ for replication)
- **Single-node Elasticsearch** (production: 3+ master + data nodes)
- **No schema registry** (the simulation has one; here it's implicit in connector code)
- **No real Flink job** (Timescale aggregations + Redis counters cover the analytics need)
- **No real LLM** in the AI assistant (pattern-matched routing)
- **Connectors generate synthetic data** rather than connecting to real cloud APIs

All of these are deliberate POC simplifications; the architecture supports each of them as drop-in upgrades.

---

*Open <http://localhost:8080> and start clicking.*
