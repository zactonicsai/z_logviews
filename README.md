# NimbusPulse

**A unified data-ingestion & observability platform simulation — Datadog-style, Simple design language.**

NimbusPulse is a single-page interactive simulation of a production-grade observability platform. It demonstrates how a real system would ingest data from AWS, Azure, GCP, Kubernetes, system logs, security tools, databases, and custom sources — then normalize, route, store, and surface that data through a live dashboard and an AI assistant.

Every button, modal, chart, connector, custom-schema form, and AI prompt is fully interactive. No backend, no build step, no network calls — just open `index.html`.

---

## Table of Contents

1. [What this is (and isn't)](#what-this-is-and-isnt)
2. [Quick start](#quick-start)
3. [File structure](#file-structure)
4. [Walkthrough — every section, in order](#walkthrough--every-section-in-order)
5. [Connector schema format](#connector-schema-format)
6. [Adding a custom connector](#adding-a-custom-connector)
7. [The AI assistant](#the-ai-assistant)
8. [Production architecture (the target build)](#production-architecture-the-target-build)
9. [Tech stack & design decisions](#tech-stack--design-decisions)
10. [localStorage keys](#localstorage-keys)
11. [Customization & extending](#customization--extending)

---

## What this is (and isn't)

**What it is:**

- A working **front-end simulation** of a unified observability platform.
- A demonstration of **how the UX should feel** when ingesting and exploring data from 22 different source types simultaneously.
- A reference for the **production architecture** that would sit behind this UI — centered on Apache NiFi, Kafka, Flink, and polyglot storage.
- A self-contained file you can open offline.

**What it isn't:**

- A real ingestion system. There is no backend; events are synthesized in the browser.
- A general-purpose dashboard framework. The simulation is purpose-built for this demo.
- Production code. The code prioritizes clarity over hardening (no auth, no rate limits, no error retry).

---

## Quick start

```bash
# 1. Unzip / clone wherever you keep things
# 2. Open the file
open index.html        # macOS
xdg-open index.html    # Linux
start index.html       # Windows
```

That's it. No `npm install`, no build step, no server. The page pulls Tailwind from the CDN and Simple Plex fonts from Google Fonts on first load.

**Offline use:** if you need to run without internet, you can swap the Tailwind CDN script for a built Tailwind file and download the fonts locally — but for evaluation, online is fine.

---

## File structure

```
nimbuspulse/
├── index.html        # the entire app — HTML, CSS, JS, SVG, data
└── README.md         # this file
```

Everything is in one HTML file by design — easy to share, easy to read end-to-end. The file is organized top-to-bottom as:

1. `<head>` — meta, Tailwind config, Simple Plex fonts, custom CSS
2. `<header>` — sticky nav with logo, anchors, live status, theme toggle
3. `<main>` — eight numbered sections (§01 through §08)
4. `<footer>` — version & build info
5. Modal + toast components
6. `<script>` — all simulation logic at the bottom

---

## Walkthrough — every section, in order

The page is built as a top-to-bottom narrative. You can scroll through it as a presentation, or jump to any section via the header.

### § 01 — System Overview

A five-stage SVG flow diagram: **Sources → Ingest → Normalize → Store → Consume**. This is the conceptual model. Each stage is a column with the relevant tech/sources.

Below the diagram, three buttons open explainer modals:

- "Explain this diagram" — what each stage does
- "What's in an event schema?" — the common envelope format every connector normalizes to
- "Why normalize & enrich?" — rationale for the middle stage

### § 02 — Live Dashboard

Real-time KPI tiles + charts driven by an in-browser event simulator that fires every 2 seconds.

**Four KPI tiles (clickable for an explainer):**

- **Events / sec** — rolling 20-second average across all enabled connectors
- **Error rate** — % of events at severity ERROR/CRITICAL, compared to a 1% SLO
- **Active sources** — count of currently-enabled connectors (out of 22+)
- **Ingest volume / hour** — estimated bytes-per-hour after gzip compression

**Two charts:**

- **Throughput sparkline** — events per tick over the last 60 ticks (~2 minutes)
- **Severity bars** — stacked counts of DEBUG / INFO / WARN / ERROR / CRITICAL

**Controls:**

- `⏸ PAUSE` / `▶ RESUME` — stops/restarts the event generator
- `↻ RESET` — clears all state and starts fresh

### § 03 — Source Connectors

The 22 built-in connectors, displayed as a grid. Each card shows the connector's icon, name, category, protocol, schedule, and status.

**Per-card actions:**

- `pause` / `start` — toggles ingestion for that connector (persisted to localStorage)
- `inspect` — opens a modal showing the connector spec, schema, status, and a description of what its NiFi process group looks like in production

**Built-in connector categories:**

- **Cloud** — AWS CloudWatch, AWS CloudTrail, AWS S3 Access, Azure Monitor, Azure App Insights, GCP Logging
- **Platform** — Kubernetes Events, Kubernetes Pod Logs, Docker Container
- **System** — Syslog (RFC 5424), Application Health
- **Security** — Security/SIEM, Auth/SSO Logs, WAF/Firewall
- **Web** — Nginx, Apache, CDN/Edge
- **Database** — PostgreSQL, MySQL Slow Query
- **Custom** — Generic Webhook, Kafka Topic, WebSocket Feed

**Below the grid:** a form to add your own connectors (see [Adding a custom connector](#adding-a-custom-connector)).

### § 04 — Live Event Stream

A scrolling, filterable feed of every event the simulator produces. New events animate in at the top.

**Filters:**

- By severity (DEBUG / INFO / WARN / ERROR / CRITICAL)
- By source (all 22+ connectors plus any custom ones)

**Per-row interaction:** click any event to open a detail modal with:

- Full structured payload (JSON)
- Original raw log line
- Action buttons: `Acknowledge`, `Investigate`, `Mute source`, `Replay`
- **`✦ Explain with AI`** — generates a static-but-contextual AI explanation of what likely happened, why it matters, and what to do

### § 05 — Logs Explorer

Same event stream, but in an expandable-row format optimized for forensic search. Type a query in the search box to filter by message text or source name. Click a row to expand it inline and see the structured fields, then jump to the full detail modal or AI explanation.

### § 06 — Ingestion Protocols

A 5-column grid covering the protocols the platform supports. Each card has a one-line description and a mini SVG flow diagram:

- **REST** — POST /v1/ingest, best for periodic exports
- **gRPC** — bidirectional binary stream, best for high-volume agents
- **GraphQL** — selective field subscriptions, best for UIs
- **WebSocket** — persistent duplex, best for browsers and IoT
- **Kafka** — durable log-based queue, best for backpressure and replay

### § 07 — Production Architecture (the target build)

**This is the diagram of what we would actually build behind this UI.** The simulation above is the front-end; section 07 is the back-end blueprint.

The SVG diagram covers, left to right:

1. **Sources** — AWS, Azure, GCP, Kubernetes, Syslog, Security tools, IoT, DB CDC (Debezium), Custom apps
2. **Ingest gateways** — REST (Nginx + OpenAPI), gRPC (Envoy + protobuf), GraphQL (Apollo), WebSocket (sticky sessions), Kafka Connect, Pull Scheduler
3. **Apache NiFi** — the central dataflow orchestrator (more below)
4. **Stream bus & processing** — Apache Kafka, Schema Registry (Confluent), Apache Flink (windowed aggregations), Anomaly Detector (ML), Alert Engine
5. **Storage tier** — Elasticsearch (hot search), ClickHouse (OLAP), S3 + Iceberg (cold), Redis (cache), TimescaleDB (metrics)
6. **Query / API / AI** — Query API, Auth (Keycloak + OIDC + OPA), RAG + Vector Store (pgvector), LLM Assistant
7. **Consumers** — Web Dashboard, Alerts & Paging, Mobile/CLI, Downstream Apps, Compliance Export
8. **Cross-cutting** — Observability (OTel), Secrets (Vault), Infra (Terraform/K8s), CI/CD (ArgoCD/GitOps), Cost (FinOps tags)

**Apache NiFi's role specifically (the highlighted block in the diagram):**

```
ListenHTTP / ListenTCP        ← receive from gateways
   ↓
EvaluateJsonPath · Schema     ← parse + validate against the registry
   ↓
UpdateAttribute · Enrich      ← add geo, user, trace id
   ↓
ReplaceText · PII mask        ← regex redaction + tokenization
   ↓
RouteOnAttribute              ← split by severity / domain
   ↓
PublishKafkaRecord_2_6        ← publish to topic.events.{severity}
```

Every connector you define in JSON/YAML (in §03) compiles into a NiFi **process group** with exactly these steps. That's the design contract between the simulation and the real system.

**Three explainer cards below the diagram:**

- Why Apache NiFi at the center?
- Why Kafka + Flink?
- Why polyglot storage?

### § 08 — AI Assistant

A simulated AI chat interface modeled on how the real assistant would work (LLM + RAG over indexed events). See [The AI assistant](#the-ai-assistant) below for full details.

---

## Connector schema format

Every connector — built-in or custom — is described by a small declarative spec. Five fields are required:

| Field | Type | Description |
|---|---|---|
| `id` | string | Unique slug, used in event records (`source: my-connector`). |
| `name` | string | Human-readable display name. |
| `category` | string | Free-form grouping (Cloud, Platform, System, Security, Web, Database, Custom). |
| `protocol` | string | Wire protocol (`rest`, `grpc`, `kafka`, `websocket`, `pull`, `tail`, `udp/tcp`, `unix-socket`). |
| `schema` | object | Field-name → type map describing the event payload shape. |

Optional fields:

| Field | Default | Description |
|---|---|---|
| `icon` | `✦` | One-character icon shown on the card. |
| `color` | `#8a3ffc` | Hex color used for the icon background and the event-stream dot. |
| `schedule` | `realtime` | Cron expression for pull-style connectors, or `realtime` for push. |
| `enabled` | `true` | Whether ingestion starts on save. |

### JSON example

```json
{
  "id": "stripe-webhook",
  "name": "Stripe Payment Webhook",
  "category": "Custom",
  "protocol": "rest",
  "icon": "$",
  "color": "#635bff",
  "schedule": "realtime",
  "schema": {
    "id": "string",
    "type": "string",
    "created": "unix_ts",
    "data": "object"
  }
}
```

### YAML example (equivalent)

```yaml
id: stripe-webhook
name: Stripe Payment Webhook
category: Custom
protocol: rest
icon: $
color: '#635bff'
schedule: realtime
schema:
  id: string
  type: string
  created: unix_ts
  data: object
```

The YAML parser supports flat keys plus one level of nesting (the `schema:` block) — enough for connector specs. JSON allows arbitrary nesting.

### Supported schema field types

The schema field types are descriptive labels (NiFi enforces them in production):

- Primitives: `string`, `int`, `float`, `bool`
- Time: `iso8601`, `unix_ts`
- Special: `ip`, `enum`, `object`, `array`

---

## Adding a custom connector

In §03, scroll to **"Add a custom connector"**.

1. Click **Load JSON sample** or **Load YAML sample** to prefill the textarea (or paste your own spec).
2. Click **Validate**. The right panel reports:
   - Parse success and detected format (JSON vs YAML)
   - Whether each required field is present
   - Number of schema fields detected
3. Click **Save connector**. The spec is written to `localStorage` under `np_custom_connectors`.
4. The new connector appears in the grid above with a purple `CUSTOM` chip.
5. The connector starts producing events immediately (a generic INFO event template is used unless you've added a template — see [Customization](#customization--extending)).

To remove all custom connectors: click **Clear all custom connectors** under the form (confirmation prompt).

---

## The AI assistant

§08 simulates a RAG-grounded LLM assistant. In production this would:

1. Embed your question into a vector.
2. Retrieve the most relevant recent events from Elasticsearch + pgvector.
3. Pull metric snapshots from TimescaleDB for the time window.
4. Send question + retrieved context to an LLM with strict guardrails.
5. Stream a grounded answer with citations back to the UI.

Here it's a static, pattern-matched simulation — but it's contextualized: it reads the current `STATE.events`, `STATE.sevCounts`, and connector status to make replies feel live.

### Suggested prompts (in the left sidebar)

- Summarize the last 5 minutes of events
- Why is the error rate climbing?
- Show me failed logins
- Which sources are silent?
- What does OOMKilled mean?
- Are there any security threats?
- Top 3 noisy sources today
- Explain this slow Postgres query

Click any to fire it. They also work as a tutorial for what the assistant can answer.

### Pattern reference

The reply generator matches on keywords. Here's the full catalog:

| If your message mentions… | The assistant returns… |
|---|---|
| `summary`, `last X minutes`, `recent` | A digest of total events, sev breakdown, error rate, top sources |
| `error climb`, `error spike`, `why error`, `fail` (without auth context) | Error analysis pointing at k8s OOMKilled, Nginx 502s, app health flap |
| `login`, `password`, `credential`, `auth fail`, `brute force`, `stuff` | Credential-stuffing analysis with remediation steps |
| `security`, `threat`, `attack`, `breach`, `malware`, `sqli`, `xss` | WAF/SIEM/CloudTrail rollup with severity assessment |
| `slow`, `latency`, `p95`, `p99`, `performance`, `degrad` | App Insights + Postgres + CDN correlation, points to DB indexing |
| `k8s`, `kubernetes`, `pod`, `oom`, `crash`, `container` | OOMKilled, ImagePullBackOff, rollout status |
| `silent`, `missing`, `stop`, `inactive`, `down` | List of paused connectors |
| `aws`, `cloudwatch`, `cloudtrail`, `s3` | AWS-specific signals with root-account warning |
| `noisy`, `top`, `loud`, `chatty` | Top sources by volume from current event window |
| `oom`, `memory kill` (standalone) | Deep explanation of OOMKilled + exit code 137 + fixes |
| `postgres`, `slow query`, `sql` | EXPLAIN ANALYZE workflow + missing-index hypothesis |
| `nifi`, `apache nifi`, `dataflow`, `pipeline` | NiFi's role + process-group concept |
| `explain`, `what mean`, `what is`, `how work` | App orientation overview |
| (anything else) | A list of supported topic areas + prompt suggestions |

Chat history persists to `localStorage` under `np_chat`. Click **Clear chat history** in the sidebar to reset.

### Inline AI explanations from events

You can also invoke the assistant from any event detail modal — click **`✦ Explain with AI`**. The system generates a context-aware static explanation for that specific event (OOMKilled, credential stuffing, root account, slow Postgres, SQLi block, CRITICAL/ERROR/INFO generic). The modal includes an **Open in AI chat →** button to continue the conversation.

---

## Production architecture (the target build)

This is what we'd actually build to make this simulation real. The §07 diagram captures it; here's the narrative:

### 1. Ingest gateways (the perimeter)

Six gateways, each tuned to a protocol family:

- **REST gateway** (Nginx + OpenAPI) — batch POST endpoint, OpenAPI-described
- **gRPC service** (Envoy + protobuf) — bidirectional streaming for high-volume agents
- **GraphQL gateway** (Apollo) — for UIs that need a tailored live subscription
- **WebSocket hub** (socket.io + sticky sessions) — browsers and IoT devices
- **Kafka Connect** — source connectors for systems that publish to Kafka natively
- **Pull scheduler** — cron-driven for cloud APIs (CloudWatch, Azure Monitor, etc.) that don't push

### 2. Apache NiFi (the heart)

Each connector spec in JSON/YAML compiles to a NiFi **process group** with these processors in order:

| Stage | NiFi Processor | Purpose |
|---|---|---|
| Listen | `ListenHTTP` / `ListenTCP` / `ConsumeKafka` | Receive from upstream gateway |
| Parse | `EvaluateJsonPath` + Schema | Validate against schema registry |
| Enrich | `UpdateAttribute` | Add geo, user, trace_id, service metadata |
| Mask | `ReplaceText` | Regex PII redaction + tokenization |
| Route | `RouteOnAttribute` | Split by severity / domain |
| Publish | `PublishKafkaRecord_2_6` | Write to `topic.events.{severity}` |

NiFi gives us **back-pressure** (no dropped data when downstream is slow), **provenance** (lineage for every event), **300+ built-in processors**, and **hot-reloadable flows** (new connectors deploy without restart).

### 3. Stream bus + processing

- **Apache Kafka** — durable log-based bus. Every downstream system is a consumer.
- **Schema Registry** (Confluent) — Avro/Proto schemas enforced at write
- **Apache Flink** — windowed aggregations write to a metrics topic
- **Anomaly Detector** — ML scoring (z-score, isolation forest) on metric streams
- **Alert Engine** — rule evaluator routing to PagerDuty / Slack / email

### 4. Polyglot storage

No single store is good at everything:

| Tier | Store | Retention | Best for |
|---|---|---|---|
| Hot | Redis | 15m | Counters, hot cache |
| Hot | Elasticsearch | 7d | Full-text search |
| Warm | ClickHouse | 90d | Analytics, billions of rows |
| Warm | TimescaleDB | 1y | Metrics / time-series |
| Cold | S3 + Iceberg | 7y | Compliance, replay, parquet |

A unified Query API hides the polyglot layer from callers.

### 5. Query, AI, and consumers

- **Query API** — REST + GraphQL + a small DSL, routes to the right tier by time range
- **Auth** — Keycloak (OIDC) + OPA (policy)
- **RAG / Vector Store** — pgvector holds embeddings of event signatures; the LLM uses retrieval over Elasticsearch + pgvector
- **LLM Assistant** — explains events, summarizes, answers natural-language queries, with full citations
- **Consumers** — Web dashboard (this UI), Alerts/Paging, Mobile/CLI, downstream apps (Snowflake, BI), Compliance export (SOC2 / HIPAA / GDPR)

### 6. Cross-cutting

- **Observability** — OpenTelemetry across all services (the platform observes itself)
- **Secrets** — HashiCorp Vault, no plaintext credentials anywhere
- **Infra** — Terraform + Kubernetes, GitOps-deployed via ArgoCD
- **Cost** — FinOps tags on every resource, ingest GB and storage GB-hour billed back

---

## Tech stack & design decisions

### Front-end (this simulation)

| Choice | Why |
|---|---|
| Single HTML file | Easy to distribute, audit, and read end-to-end |
| Tailwind CSS via CDN | No build step; utility classes keep markup self-documenting |
| Vanilla JS | No framework dependency; all logic visible in one place |
| Inline SVG | Crisp at every zoom, themable via CSS, no external assets |
| Simple Plex Sans + Plex Mono | Simple design language — clean, technical, distinctive |
| Simple Carbon color tokens | Blue `#0f62fe`, gray scale `gray10`-`gray100`, status colors |
| Sharp 90° corners | Simple design language — no rounded corners anywhere |
| Grid background | Subtle technical texture in the hero |
| Dark mode | Class-toggled on `<html>`, persisted to localStorage |

### Why Simple design language specifically?

The brief asked for Simple blue + white + Plex fonts. Simple's Carbon design system happens to fit well with a serious technical product: dense layouts, mono-spaced data, sharp edges, and a single accent blue. The color palette is deliberately limited to keep severity colors meaningful when they appear.

---

## localStorage keys

The app persists four things in `localStorage`:

| Key | Contents |
|---|---|
| `np_theme` | `"dark"` or `"light"` — theme preference |
| `np_custom_connectors` | JSON array of user-defined connector specs |
| `np_disabled` | JSON array of connector IDs that are currently paused |
| `np_chat` | JSON array of AI chat message history |

To fully reset the app: open DevTools → Application → Local Storage → clear the four `np_*` keys, then refresh.

---

## Customization & extending

### Add a new severity color

Edit `SEV_STYLE` in the `<script>` block. Each entry is `{ bg, fg }` of Tailwind classes.

### Add event templates for a custom connector

In `EVENT_TEMPLATES`, add an entry keyed by the connector's `id`. Each template is `{ sev, msg, payload: () => ({...}) }`. The payload function runs every time an event is generated, so it can include random data.

### Change tick rate

In `startSim()`, change `setInterval(tick, 2000)` — the value is milliseconds per tick. Each tick generates 1-5 events.

### Add a new AI pattern

In `generateAIReply(q)`, add a new `if (/your regex/i.test(Q)) return '...'` block above the default fallback. Keep replies short, structured, and contextual to the simulated data.

### Modify the production architecture diagram

The SVG is inline in §07 of `index.html`. It's hand-laid-out with explicit `x`/`y` coordinates — modify the rect/text/line elements directly. Keep `viewBox="0 0 1320 600"` unless rebuilding the whole layout.

---

## License & attribution

Simulation only — no real data, no telemetry, no network requests beyond the Tailwind CDN and Google Fonts on first paint. Use freely as a reference, demo, or starting point.

**External resources loaded:**

- Tailwind CSS — `https://cdn.tailwindcss.com`
- Simple Plex Sans + Plex Mono — `https://fonts.googleapis.com`

That's the complete dependency list.

---

*Built as a single-file simulation of the production architecture documented in §07. Open `index.html` and start clicking.*
