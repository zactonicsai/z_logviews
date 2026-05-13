# Step-by-Step: Build a Kafka → Kafka Flow in NiFi

A complete walk-through for building **one** NiFi flow from scratch using the NiFi 2.x UI. The flow reads security events from one Kafka topic, splits them by severity, and writes to three output Kafka topics — all visible in real time.

**Time required:** ~15 minutes
**You will learn:** How to consume from Kafka, extract JSON attributes, route on conditions, and publish back to Kafka — the four most-used NiFi patterns.

---

## What you'll build

```
┌──────────────────────┐
│  Kafka topic         │
│  raw.security        │ ◀─── filled by the security-sim connector (already running)
└──────────┬───────────┘
           │
           ▼
   ┌───────────────┐
   │ ConsumeKafka  │  ① pull messages off the topic
   └───────┬───────┘
           │
           ▼
   ┌─────────────────────┐
   │ EvaluateJsonPath    │  ② extract $.severity into an attribute
   └───────┬─────────────┘
           │
           ▼
   ┌─────────────────────┐
   │ RouteOnAttribute    │  ③ branch on severity value
   └─┬─────┬─────────┬───┘
     │     │         │
     ▼     ▼         ▼
   ┌──────────┐  ┌──────────┐  ┌──────────────┐
   │ Publish  │  │ Publish  │  │ Publish      │  ④ write to three different topics
   │ critical │  │ error    │  │ normalized   │
   └────┬─────┘  └────┬─────┘  └──────┬───────┘
        │             │               │
        ▼             ▼               ▼
   events.critical  events.error  events.normalized   ← visible in Kafka UI
```

---

## Prerequisites

Before starting, make sure the POC stack is up:

```bash
docker compose ps
```

You need these services **healthy** or **running**:

| Service | Container | Why |
|---|---|---|
| Kafka | `np-kafka` | Source + destination |
| NiFi | `np-nifi` | What you're configuring |
| Security connector | `np-conn-security` | Producing data into `raw.security` |
| Kafka UI | `np-kafka-ui` | For verifying |

If anything is missing, run `docker compose up -d` and wait ~60s.

### Verify Kafka has data first

You want to see actual messages flowing through `raw.security` before configuring NiFi. Run:

```bash
docker exec np-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic raw.security \
  --max-messages 3
```

You should see 3 JSON events scroll by — each with a `severity` field. If not, check `docker logs np-conn-security`.

---

## Step 1 — Open NiFi

1. Browse to **<https://localhost:8443>**
2. Your browser will warn about the certificate (NiFi uses a self-signed cert by default in single-user mode). Click **Advanced → Proceed to localhost (unsafe)** — this is expected for local dev.
3. NiFi shows a login screen. Enter:
   - **Username:** `admin`
   - **Password:** `ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB`
4. Click **LOG IN**

You land on the NiFi canvas — a large empty grid with toolbars across the top.

### What you're looking at

- **Top toolbar (left side):** drag-and-drop palette. Icons for Processor, Input/Output Port, Process Group, Funnel, Label, etc.
- **Top toolbar (right side):** search, global menu (hamburger)
- **Right-side floating panel ("Operate"):** Start, Stop, Configure, Disable, Enable, Delete buttons
- **Status bar (bottom right):** cluster info, queue stats
- **Canvas:** the big empty grid where you'll drag processors

---

## Step 2 — Create a process group (optional but recommended)

A "process group" is a named container for related processors. Keeps your canvas tidy.

1. Drag the **Process Group** icon (looks like a folder with arrows) from the top toolbar onto the canvas.
2. A dialog opens. In **Process Group Name** type: `Kafka Severity Router`
3. Click **ADD**
4. A labeled box appears on the canvas. **Double-click it** to enter the group. The breadcrumb at the top now shows `NiFi Flow › Kafka Severity Router`.

Everything from here on happens inside this group.

---

## Step 3 — Add the ConsumeKafka processor

1. Drag the **Processor** icon (chip-like icon with a triangle) from the top toolbar onto the canvas.
2. A search dialog opens, listing every processor type available.
3. In the filter box, type: `ConsumeKafka`
4. You'll see one or more matches. The recommended one is `ConsumeKafka_2_6` (org.apache.nifi.processors.kafka.pubsub). Click it to select.
5. Click **ADD**.

A box appears on the canvas labeled "ConsumeKafka_2_6" with a yellow caution triangle (because it's not configured yet).

### Configure it

1. **Right-click the box → Configure** (or double-click)
2. A dialog opens with five tabs: SETTINGS | SCHEDULING | PROPERTIES | RELATIONSHIPS | COMMENTS

**SETTINGS tab:**

| Field | Value |
|---|---|
| Name | `Consume raw.security` |
| Penalty Duration | `30 sec` (leave default) |
| Yield Duration | `1 sec` (leave default) |
| Bulletin Level | `WARN` |

**PROPERTIES tab** — this is the important one. Set:

| Property | Value | Why |
|---|---|---|
| Kafka Brokers | `kafka:9092` | The Kafka container's internal hostname. NOT `localhost:9092` — that's only reachable from the host, not from inside NiFi. |
| Topic Name(s) | `raw.security` | The topic the security-sim connector writes to. |
| Topic Name Format | `names` | Treat the value as a literal name, not a regex. |
| Group ID | `nifi-severity-router` | Kafka consumer group. Unique to this flow so it doesn't conflict with the sink service. |
| Offset Reset | `latest` | Start with new messages only. (Use `earliest` if you want to also catch up on history.) |
| Max Poll Records | `100` | How many messages to pull per fetch. |
| Message Demarcator | leave empty for now | We want one FlowFile per message. |

3. **RELATIONSHIPS tab** — leave defaults (we'll route the `success` relationship next).

4. Click **APPLY**.

The yellow triangle should disappear or change to a red square (Stopped, but valid).

---

## Step 4 — Add EvaluateJsonPath

This processor reads the JSON content of the FlowFile and pulls out fields as attributes.

1. Drag another **Processor** onto the canvas, to the right of ConsumeKafka.
2. Filter: `EvaluateJsonPath`
3. Add it.

### Configure

Right-click → Configure.

**SETTINGS tab:**

| Field | Value |
|---|---|
| Name | `Extract severity from JSON` |

**PROPERTIES tab:**

| Property | Value |
|---|---|
| Destination | `flowfile-attribute` |
| Return Type | `auto-detect` |
| Path Not Found Behavior | `ignore` |
| Null Value Representation | `empty string` |

Now click the **+** button (top-right of the Properties table) to add **dynamic properties**. These define the JSONPath expressions and what attribute names to store them under:

| Property name (you'll be prompted) | Value |
|---|---|
| `severity` | `$.severity` |
| `event_source` | `$.source` |
| `event_message` | `$.message` |

Each row says "give me the value at this JSONPath and store it as a FlowFile attribute called X".

**RELATIONSHIPS tab:**

The processor has three output relationships: `failure`, `matched`, `unmatched`. Check the **Terminate** checkbox for `failure` and `unmatched` (we don't want to wire those — we just discard them). Leave `matched` unchecked.

Click **APPLY**.

---

## Step 5 — Add RouteOnAttribute

The branching processor. Reads the `severity` attribute we just extracted and routes the FlowFile to different output relationships.

1. Drag another **Processor** onto the canvas, to the right of EvaluateJsonPath.
2. Filter: `RouteOnAttribute`
3. Add.

### Configure

Right-click → Configure.

**SETTINGS tab:**

| Field | Value |
|---|---|
| Name | `Route by severity` |

**PROPERTIES tab:**

| Property | Value |
|---|---|
| Routing Strategy | `Route to Property name` |

Add dynamic properties via the **+** button. Each becomes a new output relationship:

| Property name | Value (NiFi Expression Language) |
|---|---|
| `critical` | `${severity:equals('CRITICAL')}` |
| `error` | `${severity:equals('ERROR')}` |

> ⚠️ **Watch the quotes.** NiFi Expression Language uses single quotes inside `${}`. If you paste this from a doc that converted them to smart quotes, the expression won't evaluate. Type them by hand if unsure.

After adding these, the processor will have four output relationships: `critical`, `error`, `unmatched`, and the built-in `success`/`failure` (depending on version). Don't terminate anything yet — we'll wire all three (`critical`, `error`, `unmatched`) to publishers.

Click **APPLY**.

---

## Step 6 — Add the three PublishKafka processors

Now we need three publishers — one per output topic.

### 6a. PublishKafka → events.critical

1. Drag a Processor onto the canvas, top-right.
2. Filter: `PublishKafka`
3. Pick `PublishKafka_2_6`
4. Add.

Configure:

**SETTINGS tab:**

| Field | Value |
|---|---|
| Name | `Publish → events.critical` |

**PROPERTIES tab:**

| Property | Value |
|---|---|
| Kafka Brokers | `kafka:9092` |
| Topic Name | `events.critical` |
| Delivery Guarantee | `Guarantee Replicated Delivery` |
| Use Transactions | `false` |
| Compression Type | `none` |

**RELATIONSHIPS tab:** terminate `success` and `failure` (this is the end of the pipeline — nothing downstream).

**APPLY.**

### 6b. PublishKafka → events.error

Repeat 6a exactly, but:

- Name: `Publish → events.error`
- Topic Name: `events.error`

Place it below the critical one on the canvas.

### 6c. PublishKafka → events.normalized

Repeat 6a once more:

- Name: `Publish → events.normalized`
- Topic Name: `events.normalized`

Place it below the error one.

---

## Step 7 — Wire the connections

Connections are drawn between processors and carry FlowFiles on a chosen relationship.

### How to draw a connection in the NiFi UI

1. **Hover over the source processor.** A directional arrow appears in the middle of its tile.
2. **Click and drag** that arrow onto the destination processor. As you drag, you'll see a line stretch to follow your mouse.
3. **Release** when the destination is highlighted.
4. A dialog opens: "Create Connection". It asks which relationship(s) you want this connection to carry.
5. Check the appropriate relationship name(s). Click **ADD**.

### The five connections you need

| # | From | To | Relationship to select |
|---|---|---|---|
| 1 | Consume raw.security | Extract severity from JSON | `success` |
| 2 | Extract severity from JSON | Route by severity | `matched` |
| 3 | Route by severity | Publish → events.critical | `critical` |
| 4 | Route by severity | Publish → events.error | `error` |
| 5 | Route by severity | Publish → events.normalized | `unmatched` |

Once drawn, you'll see five arrows on the canvas — each labeled with its relationship and queue depth.

### Sanity check before starting

Every processor should now show a **red square** icon (stopped but valid). If any still has a **yellow triangle** (invalid):

- Right-click → **Configure** → **RELATIONSHIPS** tab
- Make sure every relationship is either connected (wired to another processor) OR terminated (checkbox)
- An "Unconnected & not terminated" relationship makes the whole processor invalid

---

## Step 8 — Start the flow

Two ways to start everything at once:

### Option A — Start the process group

If you created the "Kafka Severity Router" process group in Step 2:

1. Click outside the group to deselect everything
2. Navigate back up to the parent canvas (breadcrumb at top → click `NiFi Flow`)
3. Right-click the `Kafka Severity Router` group → **Start**

All six processors inside light up green.

### Option B — Select all and start

Inside the group:

1. Press **Ctrl/Cmd + A** to select all processors
2. In the **Operate** panel on the right side, click the **green play button** (▶)

Same effect.

### What happens

Within seconds:

- The **In** counter on `Consume raw.security` starts incrementing
- FlowFiles flow rightward through the pipeline
- The connection queues briefly show traffic (then drain to 0 as messages publish)
- The **Out** counters on the three publishers tick up

---

## Step 9 — Verify it's working

### In the NiFi canvas (live counters)

Each processor tile shows running stats:
```
   In: 24 (12.4 KB)
   Read/Write: 12.4 KB
   Out: 24 (12.4 KB)
   Tasks: 240
```

You should see In > 0 on the consumer and Out > 0 on the publishers.

### In Kafka UI (port 8181)

1. Open **<http://localhost:8181>**
2. Click your cluster name
3. Go to **Topics**
4. You should now see (alongside the `raw.*` topics) three new ones receiving messages:
   - `events.critical` — receives ~1 message every minute (CRITICAL is rare)
   - `events.error` — receives a few per minute
   - `events.normalized` — receives most of the stream (INFO + WARN)
5. Click any topic → **Messages** tab → see the actual routed events

### From the command line

```bash
# Watch the critical-events topic live
docker exec -it np-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic events.critical \
  --max-messages 3
```

You'll see three security events — each with `"severity": "CRITICAL"` — proving the routing worked.

Compare with `events.normalized`:

```bash
docker exec -it np-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic events.normalized \
  --max-messages 3
```

You'll see INFO/WARN events instead.

### In NiFi's data provenance

For deeper inspection of what flowed through the pipeline:

1. Right-click any processor (e.g. `Route by severity`) → **View data provenance**
2. A list of every FlowFile that passed through opens
3. Click any row → see the FlowFile's content, attributes, and lineage

This is one of NiFi's killer features — full lineage of every record.

---

## Common issues

| Symptom | Cause / fix |
|---|---|
| Consumer shows In: 0 forever | Either `raw.security` is empty (check `docker logs np-conn-security`) or the broker setting is wrong. **Must be `kafka:9092`, not `localhost:9092`** — NiFi runs inside Docker. |
| RouteOnAttribute always routes to `unmatched` | The `severity` attribute is empty or has unexpected casing. Right-click EvaluateJsonPath → View data provenance → check the attributes. The `severity` value should literally be `CRITICAL` / `ERROR` etc. |
| Connection queue keeps growing without draining | The downstream processor is stopped or has a problem. Look for red icons. |
| PublishKafka fails repeatedly | Check the broker address and topic name. If the topic doesn't exist and auto-create is off, you'll get errors. (POC topics are pre-created by `kafka-init`.) |
| Yellow triangle on a processor | Configuration invalid. Right-click → Configure → check Properties (missing required values?) and Relationships (unconnected and not terminated?). |
| "Unable to authenticate" on login | Wrong password. Reset by recreating the NiFi container: `docker compose rm -sf nifi && docker volume rm nimbuspulse_nifi-conf && docker compose up -d nifi` (wait 60s). |

---

## Stopping and resuming

**To pause the flow:**

- Select the process group (or all processors inside)
- Click the **red stop button** (■) in the Operate panel
- Counters freeze; queues retain any in-flight FlowFiles

**To resume:**

- Click the **green play button** (▶) again

**To delete the flow entirely:**

- Stop it first
- Select the process group
- Press Delete (or right-click → Delete)

---

## What you just demonstrated

This minimal pipeline exercises the **four NiFi patterns** you'll use 90% of the time:

1. **Ingest** (`ConsumeKafka`) — pulling data from an upstream source
2. **Parse / extract** (`EvaluateJsonPath`) — turning raw content into queryable attributes
3. **Route** (`RouteOnAttribute`) — branching on a condition
4. **Publish** (`PublishKafka`) — sending data to a downstream sink

Every more-complex NiFi flow is just variations and combinations of these — add enrichment with `UpdateAttribute`, masking with `ReplaceText`, validation with `ValidateRecord`, archive with `PutS3Object`, persistence with `PutElasticsearchJson` or `PutDatabaseRecord`. The skeleton stays the same.

---

## Next steps

- Try **Extension B** in [HOW-TO.md](./HOW-TO.md): replace `Publish → events.critical` with `PutS3Object` to archive CRITICAL events to MinIO instead of (or in addition to) Kafka.
- Add a `UpdateAttribute` between EvaluateJsonPath and RouteOnAttribute to inject `pipeline=nifi`, `trace_id=${uuid()}`.
- Try the same flow shape with `raw.k8s-events` instead — route on `${event_message:contains('OOMKilled')}`.
- Or skip the manual build and just run `make nifi-load`, which builds this exact flow programmatically via REST.
