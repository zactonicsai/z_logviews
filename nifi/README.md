# Apache NiFi in the NimbusPulse POC

This directory contains everything you need to run a sample NiFi flow against the POC stack.

## What's here

```
nifi/
├── README.md                          # this file
├── HOW-TO.md                          # full load-and-trigger guide (read this!)
├── flows/
│   └── security-events-router.json   # human-readable reference of the sample flow
└── scripts/
    └── load-flow.sh                   # REST-API automation: build, start, stop, delete
```

This directory is **mounted into the NiFi container** read-only at
`/opt/nifi/nifi-current/flows`, so all files here are visible from inside NiFi too.

## The sample flow

A simple but illustrative pipeline that demonstrates NiFi consuming, parsing,
routing, and republishing — using data already flowing through your POC.

```
ConsumeKafka (raw.security)
  → EvaluateJsonPath ($.severity → attribute)
  → RouteOnAttribute (CRITICAL | ERROR | other)
      ├──► PublishKafka (events.critical)
      ├──► PublishKafka (events.error)
      └──► PublishKafka (events.normalized)
```

It takes the `security-sim` connector's output and splits it into three
severity-specific Kafka topics — visible in Kafka UI within seconds of starting.

## Quick start

```bash
# Build & start the flow (one command)
make nifi-load

# See it running
make nifi-status

# Watch one of the output topics
docker exec np-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic events.critical \
  --from-beginning
```

Open the NiFi canvas at <https://localhost:8443> to see the flow visually.

## All three loading options

**Option 1 — automated** (default): `make nifi-load` calls
[`scripts/load-flow.sh`](scripts/load-flow.sh) which uses the NiFi REST API
to build the flow programmatically. Fastest path.

**Option 2 — manual UI build**: drag & wire the six processors in the NiFi
UI by hand. Best for learning. Step-by-step instructions in
[HOW-TO.md](HOW-TO.md).

**Option 3 — reference JSON**: [`flows/security-events-router.json`](flows/security-events-router.json)
is a human-readable description (not a NiFi-importable file) listing every
processor, property, and connection. Use it when adapting the flow.

**See [HOW-TO.md](HOW-TO.md) for the full guide**, including troubleshooting,
verification, version-specific notes, and extension ideas (PutS3Object,
PutElasticsearchJson, masking, etc.).

## Why this flow specifically?

The POC's runtime sink (`services/sink/`, written in Go) already does the
event → ES/Redis/Timescale fan-out. We didn't want NiFi to duplicate that.

Instead, this sample flow shows NiFi doing **complementary** work — Kafka-to-Kafka
routing — which:

1. Demonstrates the four most common NiFi processor types (ingest, parse,
   route, egress) with minimal setup
2. Uses data already flowing in your POC (the `security-sim` connector)
3. Produces visible output (new messages in three Kafka topics) you can
   verify in Kafka UI within seconds
4. Doesn't conflict with the sink service or risk corrupting demo state

## Production usage

In a real deployment, NiFi would replace the Go sink for the
**ingest-normalize-route** stages. Per connector, the flow would be:

```
ListenHTTP / ConsumeKafka      ← receive from gateways or upstream Kafka
        ↓
EvaluateJsonPath               ← parse into attributes
        ↓
ValidateRecord                 ← schema-validate via Schema Registry
        ↓
UpdateAttribute                ← enrich (geo, user, trace, service)
        ↓
ReplaceText  /  JoltTransform  ← PII redaction, restructure
        ↓
RouteOnAttribute               ← split by severity, domain, tenant
        ↓
PublishKafkaRecord             ← write to clean events.* topics
```

The sample flow in this POC covers stages 1-3 and 6-7 of that pipeline.
Extensions in [HOW-TO.md](HOW-TO.md) show how to add masking and S3 archiving.

## NiFi credentials

- URL: <https://localhost:8443> (self-signed cert — accept the warning)
- Username: `admin`
- Password: `ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB`
- All configured in `docker-compose.yml` under the `nifi:` service block
