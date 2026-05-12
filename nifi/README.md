# Apache NiFi — Production Flow Reference

This directory is mounted into the NiFi container at
`/opt/nifi/nifi-current/flows` (read-only). It's intentionally empty of
flow definitions — NiFi runs as a clean canvas you can build on.

## What the POC actually does

For simplicity, the POC bypasses NiFi at runtime: the **sink service** (Go)
consumes Kafka topics directly and fans data out to Elasticsearch, Redis,
and TimescaleDB. NiFi is included so you can **see how the production flow
would be built visually**, but it isn't on the runtime path.

## What you'd build in NiFi for production

Per connector (process group), the flow is:

```
ConsumeKafkaRecord_2_6     (read from raw.<source> topic)
        ↓
EvaluateJsonPath           (parse JSON into FlowFile attributes)
        ↓
ValidateRecord             (validate against schema in Schema Registry)
        ↓
UpdateAttribute            (enrich: geo, user, trace_id, service)
        ↓
ReplaceText / Jolt         (PII redaction — emails, IPs, names)
        ↓
RouteOnAttribute           (split by severity: info / warn / error / critical)
        ↓
PublishKafkaRecord_2_6     (write to events.<severity> topic)
```

Downstream consumers (sink service, alert engine, analytics) read from the
`events.*` topics, which are clean, enriched, and validated.

## Logging into NiFi

- URL: <https://localhost:8443>
- Username: `admin`
- Password: `ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB`

The cert is self-signed — accept the browser warning. Inside, you can drag
processors from the toolbar onto the canvas and wire them together.

## Hot-reloading a flow

NiFi can import a flow definition (`.json`) at runtime via the UI:
`Operate → Upload template` (NiFi 1.x) or `Process Group → Import flow
definition` (NiFi 2.x). The mount on this directory is read-only so
imports go into `/opt/nifi/nifi-current/conf/flow.json.gz` (which lives
on the `nifi-conf` volume and persists across restarts).
