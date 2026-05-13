# NiFi Sample Flow — Load & Trigger Guide

This guide covers three ways to get the **NimbusPulse Security Pipeline** running on NiFi, plus how to verify it's working and extend it.

## What the flow does

```
ConsumeKafka (raw.security)
    │
    ▼
EvaluateJsonPath  ── extract $.severity to a FlowFile attribute
    │
    ▼
RouteOnAttribute  ── split by severity
    │
    ├──────► PublishKafka → events.critical    (when severity == CRITICAL)
    ├──────► PublishKafka → events.error       (when severity == ERROR)
    └──────► PublishKafka → events.normalized  (everything else)
```

The `security-sim` connector continuously publishes synthetic SIEM/WAF/auth events to `raw.security` with a mix of INFO/WARN/ERROR/CRITICAL. This flow demonstrates NiFi consuming that stream and routing it into severity-specific output topics in real time.

---

## Option 1 — Automated load via REST API (fastest)

The shipped script `nifi/scripts/load-flow.sh` builds the entire flow programmatically using NiFi's REST API and starts it.

### Prerequisites

```bash
# bash, curl, and jq must be available on the host
which jq || brew install jq         # macOS
which jq || apt-get install -y jq   # Debian/Ubuntu
```

### Run it

From the project root:

```bash
make nifi-load           # build + start the flow
make nifi-status         # show current throughput / queue state
make nifi-stop           # pause the flow (keep it on canvas)
make nifi-start          # resume after stopping
make nifi-delete         # remove the flow from canvas
```

Or directly:

```bash
./nifi/scripts/load-flow.sh             # build + start
./nifi/scripts/load-flow.sh --status
./nifi/scripts/load-flow.sh --stop
./nifi/scripts/load-flow.sh --delete
```

The script logs in with the default NiFi single-user credentials. Override via env if you've changed them:

```bash
NIFI_USER=admin NIFI_PASS='my-pass' ./nifi/scripts/load-flow.sh
```

### What you'll see

```
Logging in to NiFi at https://localhost:8443
  ✓ got token (244 bytes)
  ✓ root process group: 9a7e...
Creating process group 'NimbusPulse Security Pipeline'
  ✓ process group: 4b12...
Creating ConsumeKafka (raw.security)
  ✓ ConsumeKafka: 8e3f...
... (etc) ...
Starting process group
  ✓ started

Flow loaded & running
  Process Group: NimbusPulse Security Pipeline (4b12...)
  Open NiFi UI:  https://localhost:8443
```

### If it fails

| Error | Cause / fix |
|---|---|
| `login failed` | NiFi is still booting. Wait 30s and retry — startup is slow. |
| `Unable to perform the desired action` | NiFi rejects requests until fully initialized. Wait and retry. |
| Processor type not found (`...ConsumeKafka_2_6`) | Your NiFi version uses a different Kafka NAR. See [§ NiFi version notes](#nifi-version-notes) below. |
| `jq: command not found` | Install `jq` (it's required to parse responses). |

---

## Option 2 — Build it manually in the NiFi UI (most educational)

This is the best way to actually learn NiFi. It takes ~10 minutes.

### 1. Open NiFi

- Browse to <https://localhost:8443>
- Accept the self-signed cert warning
- Log in: `admin` / `ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB`

You'll land on an empty canvas with a toolbar across the top.

### 2. Create a process group (optional but tidy)

Drag the **Process Group** icon (the folder-looking one in the top toolbar) onto the canvas, name it `NimbusPulse Security Pipeline`, click **ADD**. Double-click it to enter.

### 3. Add the six processors

Drag the **Processor** icon (it looks like a chip/square) onto the canvas. A dialog opens for processor selection. For each row below, type the name into the filter, select it, click **ADD**:

| # | Processor type | Position (rough) |
|---|---|---|
| 1 | `ConsumeKafka_2_6` | left |
| 2 | `EvaluateJsonPath` | left-center |
| 3 | `RouteOnAttribute` | center |
| 4 | `PublishKafka_2_6` | right (top) |
| 5 | `PublishKafka_2_6` | right (middle) |
| 6 | `PublishKafka_2_6` | right (bottom) |

> NiFi 2.x exposes Kafka 3.x processors with names like `ConsumeKafka` (no suffix). If you don't see `_2_6`, just pick the version available. Properties are the same — the script supports passing the broker via `bootstrap.servers`.

### 4. Configure each processor

**Right-click each processor → Configure**. Set these properties.

**(1) ConsumeKafka_2_6**

| Property | Value |
|---|---|
| Kafka Brokers (or `bootstrap.servers`) | `kafka:9092` |
| Topic Name(s) | `raw.security` |
| Topic Name Format | `names` |
| Group ID | `nifi-security-pipeline` |
| Offset Reset | `latest` |
| Max Poll Records | `100` |
| Message Demarcator | `\n` (single newline) |

Right-click → rename to **Consume raw.security**.

**(2) EvaluateJsonPath**

| Property | Value |
|---|---|
| Destination | `flowfile-attribute` |
| Return Type | `auto-detect` |
| Path Not Found Behavior | `ignore` |
| Null Value Representation | `empty string` |

Then click the **+** button to add **dynamic properties** (each becomes an extracted attribute):

| Property name | Value |
|---|---|
| `severity` | `$.severity` |
| `event_source` | `$.source` |
| `event_message` | `$.message` |

Go to the **Relationships** tab and check **Terminate** on `failure` and `unmatched`.

Rename to **Extract severity from JSON**.

**(3) RouteOnAttribute**

| Property | Value |
|---|---|
| Routing Strategy | `Route to Property name` |

Add dynamic properties:

| Property name | Value (NiFi Expression Language) |
|---|---|
| `critical` | `${severity:equals('CRITICAL')}` |
| `error` | `${severity:equals('ERROR')}` |

Rename to **Route by severity**.

**(4–6) PublishKafka_2_6** (three copies)

For all three:

| Property | Value |
|---|---|
| Kafka Brokers (or `bootstrap.servers`) | `kafka:9092` |
| Delivery Guarantee | `Guarantee Replicated Delivery` |
| Use Transactions | `false` |

What differs is the **Topic Name** and the processor name:

| # | Topic Name | Rename to |
|---|---|---|
| 4 | `events.critical` | `Publish → events.critical` |
| 5 | `events.error` | `Publish → events.error` |
| 6 | `events.normalized` | `Publish → events.normalized` |

For each, **Relationships** tab → **Terminate** on `success` and `failure`.

### 5. Wire the connections

Hover over a processor — a directional arrow appears at its center. Drag from one processor to another. A dialog asks which relationship to use:

| Drag from → to | Relationship to select |
|---|---|
| Consume raw.security → Extract severity | `success` |
| Extract severity → Route by severity | `matched` |
| Route by severity → Publish → events.critical | `critical` |
| Route by severity → Publish → events.error | `error` |
| Route by severity → Publish → events.normalized | `unmatched` |

### 6. Start the flow

Either:
- **Right-click the process group → Start**, or
- Select all (Ctrl/Cmd-A) → click the green play button in the **Operate** palette

All six processors should turn green within a few seconds.

### 7. Verify (see § [Verifying](#verifying) below)

---

## Option 3 — Manually inspect / adapt the reference JSON

`nifi/flows/security-events-router.json` is a **human-readable description** of the flow. It's not a directly importable NiFi flow definition (the import format requires runtime UUIDs and bundle versions), but it's the canonical reference for processor types, properties, positions, and connections — useful when:

- You're following the manual UI walkthrough and want to copy/paste property values
- You want to adapt the flow for a different topic / different routing rules
- You want to feed it into your own automation that calls the NiFi REST API

---

## Verifying

Once the flow is running:

### In the NiFi UI

- Each processor tile shows In / Read / Write / Out counts in real time
- Right-click a processor → **View data provenance** → see actual FlowFile contents flowing through
- Right-click a connection → **List queue** → see queued FlowFiles

### In Kafka UI (port 8181)

Open <http://localhost:8181>, select your cluster, go to **Topics**. You should now see:

- `events.critical` — receives ~1 message every 30-60s (CRITICAL events are rare)
- `events.error` — receives a few messages per minute
- `events.normalized` — receives most of the security stream (INFO + WARN routed here)

Click into each topic → **Messages** to see actual routed events.

### From the command line

Tail one of the output topics:

```bash
docker exec -it np-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic events.critical \
  --from-beginning
```

Or run the flow status:

```bash
make nifi-status
# or:
./nifi/scripts/load-flow.sh --status
```

Output looks like:

```json
{
  "running": "Running",
  "input":  "1,234 (1.5 MB)",
  "queued": "0 (0 bytes)",
  "output": "1,234 (1.5 MB)",
  "read":   "1.5 MB",
  "written":"1.5 MB"
}
```

---

## NiFi version notes

The script targets **NiFi 2.x** running the `apache/nifi:latest` image. Processor classnames have changed across versions:

| NiFi 1.x | NiFi 2.x | NiFi 2.5+ (current) |
|---|---|---|
| `ConsumeKafka_2_0` | `ConsumeKafka_2_6` | `ConsumeKafka` |
| `PublishKafka_2_0` | `PublishKafka_2_6` | `PublishKafka` |

If `load-flow.sh` fails with a "processor type not found" error, edit the script and update the `type` strings to match what's available in your NiFi instance. Easiest way to discover the correct name:

1. In the NiFi UI, drag a Processor onto the canvas
2. Type "ConsumeKafka" in the filter
3. The dialog shows the full classname under each match

Or via the API:

```bash
curl -sk -H "Authorization: Bearer $TOKEN" \
  https://localhost:8443/nifi-api/flow/processor-types \
  | jq '.processorTypes[] | select(.type | contains("Kafka")) | .type'
```

---

## Extending the flow

Once the basic pipeline is running, try these variations:

### A. Add a fourth route — WAF blocks

Open the **Route by severity** processor and add a new dynamic property:

| Property | Value |
|---|---|
| `waf_block` | `${event_message:contains('WAF blocked')}` |

Then add a fifth PublishKafka pointing at `events.security.waf`, and wire `Route → waf_block` to it. WAF events will now be siphoned into their own topic.

### B. Archive critical events to MinIO

Replace the **Publish → events.critical** processor with a **PutS3Object**:

| Property | Value |
|---|---|
| Object Key | `critical/${now():format("yyyy-MM-dd")}/${uuid}.json` |
| Bucket | `np-cold-events` |
| Access Key ID | `nimbus` |
| Secret Access Key | `nimbus-secret` |
| Endpoint Override URL | `http://minio:9000` |
| Region | `us-east-1` |
| Path Style Access | `true` |

Now every CRITICAL event ends up as an object in the MinIO bucket alongside being published to Kafka.

### C. Send to Elasticsearch directly

Add a **PutElasticsearchJson** processor:

| Property | Value |
|---|---|
| Index | `np-events-nifi` |
| Type | `_doc` |
| Hosts | `http://elasticsearch:9200` |

Wire `Route → critical` to it (in addition to the Kafka publisher). The sink service already writes to `np-events`; this NiFi-managed index lives alongside, so you can compare them in Kibana.

### D. Enrich, mask, transform

Insert these processors between **Extract severity** and **Route by severity**:

- **UpdateAttribute** — add `${pipeline:='nifi'}`, `${trace_id:=${uuid()}}`
- **ReplaceText** — regex-mask emails / IPs in the FlowFile content
- **JoltTransformJSON** — restructure the payload (e.g. rename fields)

This mirrors the full production pipeline pattern.

---

## Removing the flow

```bash
make nifi-delete
# or:
./nifi/scripts/load-flow.sh --delete
```

This stops the process group and removes it from the root canvas. Other NiFi state (e.g., flow.json.gz in `nifi-conf` volume) is unaffected.

To wipe NiFi completely:

```bash
docker compose down
docker volume rm nimbuspulse_nifi-conf nimbuspulse_nifi-data
docker compose up -d nifi
```
