#!/usr/bin/env bash
# =============================================================================
# NimbusPulse — NiFi sample flow loader
# Builds the "Security Pipeline" process group on the NiFi root canvas
# using the NiFi 2.x REST API, then starts it.
#
# Usage:
#   ./load-flow.sh                       # build + start the flow
#   ./load-flow.sh --stop                # stop the flow (keep it on canvas)
#   ./load-flow.sh --start               # restart the flow after stopping
#   ./load-flow.sh --delete              # remove the flow from canvas
#   ./load-flow.sh --status              # show flow status
#
# Env overrides:
#   NIFI_URL       (default: https://localhost:8443)
#   NIFI_USER      (default: admin)
#   NIFI_PASS      (default: ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB)
#   KAFKA_BROKERS  (default: kafka:9092 — internal Docker hostname)
# =============================================================================
set -euo pipefail

NIFI_URL="${NIFI_URL:-https://localhost:8443}"
NIFI_USER="${NIFI_USER:-admin}"
NIFI_PASS="${NIFI_PASS:-ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB}"
KAFKA_BROKERS="${KAFKA_BROKERS:-kafka:9092}"

PG_NAME="NimbusPulse Security Pipeline"

CURL="curl -sk"   # -k accepts NiFi's self-signed cert

# ----- pretty -----
b() { printf "\033[1m%s\033[0m\n" "$*"; }
ok() { printf "  \033[32m✓\033[0m %s\n" "$*"; }
err() { printf "  \033[31m✗\033[0m %s\n" "$*" >&2; }
info() { printf "  \033[36m→\033[0m %s\n" "$*"; }

require() {
  command -v "$1" >/dev/null 2>&1 || { err "missing required tool: $1"; exit 1; }
}
require curl
require jq

# ----- API helpers -----

login() {
  b "Logging in to NiFi at $NIFI_URL"
  TOKEN=$($CURL -X POST \
    -H "Content-Type: application/x-www-form-urlencoded" \
    --data-urlencode "username=$NIFI_USER" \
    --data-urlencode "password=$NIFI_PASS" \
    "$NIFI_URL/nifi-api/access/token")
  if [[ -z "$TOKEN" || "$TOKEN" == *"error"* || "$TOKEN" == *"Unable"* ]]; then
    err "login failed (check NIFI_USER/NIFI_PASS, or NiFi may still be starting up)"
    err "raw response: $TOKEN"
    exit 1
  fi
  AUTH="-H Authorization:Bearer\ $TOKEN"
  ok "got token ($(echo -n "$TOKEN" | wc -c) bytes)"
}

api() {
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    $CURL -X "$method" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d "$body" \
      "$NIFI_URL/nifi-api$path"
  else
    $CURL -X "$method" \
      -H "Authorization: Bearer $TOKEN" \
      "$NIFI_URL/nifi-api$path"
  fi
}

# ----- Find / create process group on root canvas -----

get_root_pg_id() {
  api GET /flow/process-groups/root | jq -r '.processGroupFlow.id'
}

find_pg_by_name() {
  local root_id="$1" name="$2"
  api GET "/process-groups/$root_id" \
    | jq -r --arg n "$name" '.component.contents.processGroups[]? | select(.name==$n) | .id // empty' 2>/dev/null \
    || api GET "/flow/process-groups/$root_id" \
       | jq -r --arg n "$name" '.processGroupFlow.flow.processGroups[]? | select(.component.name==$n) | .id // empty'
}

create_pg() {
  local parent_id="$1" name="$2"
  local body
  body=$(jq -nc --arg n "$name" '{
    revision:{version:0},
    component:{name:$n, position:{x:120,y:120}}
  }')
  api POST "/process-groups/$parent_id/process-groups" "$body" | jq -r '.id'
}

# ----- Processor builders -----

create_processor() {
  local pg_id="$1" name="$2" type="$3" x="$4" y="$5" props_json="$6"
  local body
  body=$(jq -nc \
    --arg n "$name" --arg t "$type" --argjson x "$x" --argjson y "$y" \
    --argjson props "$props_json" '{
      revision:{version:0},
      component:{
        name:$n,
        type:$t,
        position:{x:$x,y:$y},
        config:{
          properties:$props,
          schedulingStrategy:"TIMER_DRIVEN",
          schedulingPeriod:"0 sec",
          penaltyDuration:"30 sec",
          yieldDuration:"1 sec",
          bulletinLevel:"WARN",
          executionNode:"ALL",
          concurrentlySchedulableTaskCount:1
        }
      }
    }')
  api POST "/process-groups/$pg_id/processors" "$body" | jq -r '.id'
}

set_proc_relationships_autoterminate() {
  # Mark certain output relationships as auto-terminate so the processor is "valid"
  # without us wiring every output. Takes processor id and a JSON array of relationship names.
  local proc_id="$1" rels_json="$2"
  local current rev rels_obj
  current=$(api GET "/processors/$proc_id")
  rev=$(echo "$current" | jq -c '.revision')
  # Build relationship objects with autoTerminate=true
  rels_obj=$(jq -c --argjson rels "$rels_json" '[$rels[] | {name:., autoTerminate:true}]' <<< '[]')
  local body
  body=$(jq -nc --argjson rev "$rev" --argjson rels "$rels_obj" --arg id "$proc_id" '{
    revision:$rev,
    component:{id:$id, config:{autoTerminatedRelationships:$rels}}
  }')
  api PUT "/processors/$proc_id" "$body" >/dev/null
}

create_connection() {
  local pg_id="$1" src_id="$2" dst_id="$3" rel="$4"
  local body
  body=$(jq -nc \
    --arg src "$src_id" --arg dst "$dst_id" --arg rel "$rel" --arg pg "$pg_id" '{
      revision:{version:0},
      component:{
        source:{id:$src,groupId:$pg,type:"PROCESSOR"},
        destination:{id:$dst,groupId:$pg,type:"PROCESSOR"},
        selectedRelationships:[$rel],
        flowFileExpiration:"0 sec",
        backPressureDataSizeThreshold:"1 GB",
        backPressureObjectThreshold:10000
      }
    }')
  api POST "/process-groups/$pg_id/connections" "$body" | jq -r '.id'
}

start_pg() {
  local pg_id="$1"
  local body
  body=$(jq -nc --arg id "$pg_id" '{id:$id, state:"RUNNING"}')
  api PUT "/flow/process-groups/$pg_id" "$body" >/dev/null
}

stop_pg() {
  local pg_id="$1"
  local body
  body=$(jq -nc --arg id "$pg_id" '{id:$id, state:"STOPPED"}')
  api PUT "/flow/process-groups/$pg_id" "$body" >/dev/null
}

delete_pg() {
  local pg_id="$1"
  local rev
  rev=$(api GET "/process-groups/$pg_id" | jq -r '.revision.version')
  api DELETE "/process-groups/$pg_id?version=$rev&clientId=loader" >/dev/null
}

flow_status() {
  local pg_id="$1"
  api GET "/flow/process-groups/$pg_id/status?recursive=true" \
    | jq '{running:.processGroupStatus.aggregateSnapshot.runStatus,
           input:.processGroupStatus.aggregateSnapshot.input,
           queued:.processGroupStatus.aggregateSnapshot.queued,
           output:.processGroupStatus.aggregateSnapshot.output,
           read:.processGroupStatus.aggregateSnapshot.read,
           written:.processGroupStatus.aggregateSnapshot.written}'
}

# ============================================================================
# COMMAND DISPATCH
# ============================================================================

cmd="${1:-build}"

login

ROOT_ID=$(get_root_pg_id)
[[ -z "$ROOT_ID" || "$ROOT_ID" == "null" ]] && { err "could not get root process group id"; exit 1; }
ok "root process group: $ROOT_ID"

case "$cmd" in
  --stop|stop)
    PG_ID=$(find_pg_by_name "$ROOT_ID" "$PG_NAME")
    [[ -z "$PG_ID" || "$PG_ID" == "null" ]] && { err "flow '$PG_NAME' not found on canvas"; exit 1; }
    info "stopping process group $PG_ID"
    stop_pg "$PG_ID"
    ok "stopped"
    exit 0
    ;;
  --start|start)
    PG_ID=$(find_pg_by_name "$ROOT_ID" "$PG_NAME")
    [[ -z "$PG_ID" || "$PG_ID" == "null" ]] && { err "flow '$PG_NAME' not found on canvas"; exit 1; }
    info "starting process group $PG_ID"
    start_pg "$PG_ID"
    ok "started"
    exit 0
    ;;
  --status|status)
    PG_ID=$(find_pg_by_name "$ROOT_ID" "$PG_NAME")
    [[ -z "$PG_ID" || "$PG_ID" == "null" ]] && { err "flow '$PG_NAME' not found on canvas"; exit 1; }
    flow_status "$PG_ID"
    exit 0
    ;;
  --delete|delete)
    PG_ID=$(find_pg_by_name "$ROOT_ID" "$PG_NAME")
    [[ -z "$PG_ID" || "$PG_ID" == "null" ]] && { err "flow '$PG_NAME' not found on canvas"; exit 1; }
    info "stopping then deleting process group $PG_ID"
    stop_pg "$PG_ID" || true
    sleep 2
    delete_pg "$PG_ID"
    ok "deleted"
    exit 0
    ;;
  build|--build|"")
    : # fall through to build below
    ;;
  *)
    err "unknown command: $cmd"
    echo "Usage: $0 [build|--start|--stop|--status|--delete]"
    exit 1
    ;;
esac

# ----- BUILD -----

EXISTING=$(find_pg_by_name "$ROOT_ID" "$PG_NAME" || true)
if [[ -n "$EXISTING" && "$EXISTING" != "null" ]]; then
  info "found existing flow '$PG_NAME' (id $EXISTING) — deleting & recreating"
  stop_pg "$EXISTING" || true
  sleep 2
  delete_pg "$EXISTING"
fi

b "Creating process group '$PG_NAME'"
PG_ID=$(create_pg "$ROOT_ID" "$PG_NAME")
ok "process group: $PG_ID"

# Processor 1: ConsumeKafka — reads from raw.security
b "Creating ConsumeKafka (raw.security)"
CONSUME_PROPS=$(jq -nc \
  --arg brokers "$KAFKA_BROKERS" \
  '{
    "bootstrap.servers": $brokers,
    "topic": "raw.security",
    "topic_type": "names",
    "group.id": "nifi-security-pipeline",
    "auto.offset.reset": "latest",
    "Max Poll Records": "100",
    "honor-transactions": "true",
    "header-encoding": "UTF-8",
    "message-demarcator": "\n"
  }')
CONSUME_ID=$(create_processor "$PG_ID" "Consume raw.security" \
  "org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6" \
  0 0 "$CONSUME_PROPS")
ok "ConsumeKafka: $CONSUME_ID"

# Processor 2: EvaluateJsonPath — extract severity to attribute
b "Creating EvaluateJsonPath (extract \$.severity)"
EVAL_PROPS=$(jq -nc '{
  "Destination": "flowfile-attribute",
  "Return Type": "auto-detect",
  "Path Not Found Behavior": "ignore",
  "Null Value Representation": "empty string",
  "severity": "$.severity",
  "event_source": "$.source",
  "event_message": "$.message"
}')
EVAL_ID=$(create_processor "$PG_ID" "Extract severity from JSON" \
  "org.apache.nifi.processors.standard.EvaluateJsonPath" \
  300 0 "$EVAL_PROPS")
# Auto-terminate the failure/unmatched relationships we don't connect
set_proc_relationships_autoterminate "$EVAL_ID" '["failure","unmatched"]'
ok "EvaluateJsonPath: $EVAL_ID"

# Processor 3: RouteOnAttribute — split by severity
b "Creating RouteOnAttribute (severity-based routing)"
ROUTE_PROPS=$(jq -nc '{
  "Routing Strategy": "Route to Property name",
  "critical": "${severity:equals(\"CRITICAL\")}",
  "error":    "${severity:equals(\"ERROR\")}"
}')
ROUTE_ID=$(create_processor "$PG_ID" "Route by severity" \
  "org.apache.nifi.processors.standard.RouteOnAttribute" \
  600 0 "$ROUTE_PROPS")
ok "RouteOnAttribute: $ROUTE_ID"

# Processor 4a: PublishKafka — critical
b "Creating PublishKafka (events.critical)"
PUB_CRIT_PROPS=$(jq -nc --arg brokers "$KAFKA_BROKERS" '{
  "bootstrap.servers": $brokers,
  "topic": "events.critical",
  "acks": "all",
  "compression.type": "none",
  "delivery-guarantee": "guarantee-replicated-delivery",
  "use-transactions": "false",
  "publish-strategy": "FlowFile content as Kafka Record Value"
}')
PUB_CRIT_ID=$(create_processor "$PG_ID" "Publish → events.critical" \
  "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6" \
  900 -150 "$PUB_CRIT_PROPS")
set_proc_relationships_autoterminate "$PUB_CRIT_ID" '["success","failure"]'
ok "PublishKafka critical: $PUB_CRIT_ID"

# Processor 4b: PublishKafka — error
b "Creating PublishKafka (events.error)"
PUB_ERR_PROPS=$(jq -nc --arg brokers "$KAFKA_BROKERS" '{
  "bootstrap.servers": $brokers,
  "topic": "events.error",
  "acks": "all",
  "delivery-guarantee": "guarantee-replicated-delivery",
  "use-transactions": "false"
}')
PUB_ERR_ID=$(create_processor "$PG_ID" "Publish → events.error" \
  "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6" \
  900 0 "$PUB_ERR_PROPS")
set_proc_relationships_autoterminate "$PUB_ERR_ID" '["success","failure"]'
ok "PublishKafka error: $PUB_ERR_ID"

# Processor 4c: PublishKafka — normalized (catch-all)
b "Creating PublishKafka (events.normalized)"
PUB_NORM_PROPS=$(jq -nc --arg brokers "$KAFKA_BROKERS" '{
  "bootstrap.servers": $brokers,
  "topic": "events.normalized",
  "acks": "all",
  "delivery-guarantee": "guarantee-replicated-delivery",
  "use-transactions": "false"
}')
PUB_NORM_ID=$(create_processor "$PG_ID" "Publish → events.normalized" \
  "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6" \
  900 150 "$PUB_NORM_PROPS")
set_proc_relationships_autoterminate "$PUB_NORM_ID" '["success","failure"]'
ok "PublishKafka normalized: $PUB_NORM_ID"

# ----- Connections -----

b "Wiring connections"
create_connection "$PG_ID" "$CONSUME_ID"  "$EVAL_ID"      "success"     >/dev/null
ok "consume → evaluate (success)"
create_connection "$PG_ID" "$EVAL_ID"     "$ROUTE_ID"     "matched"     >/dev/null
ok "evaluate → route (matched)"
create_connection "$PG_ID" "$ROUTE_ID"    "$PUB_CRIT_ID"  "critical"    >/dev/null
ok "route → publish critical"
create_connection "$PG_ID" "$ROUTE_ID"    "$PUB_ERR_ID"   "error"       >/dev/null
ok "route → publish error"
create_connection "$PG_ID" "$ROUTE_ID"    "$PUB_NORM_ID"  "unmatched"   >/dev/null
ok "route → publish normalized (unmatched)"

# ----- Start everything -----

b "Starting process group"
sleep 1
start_pg "$PG_ID"
ok "started"

echo
b "Flow loaded & running"
echo "  Process Group: $PG_NAME ($PG_ID)"
echo "  Open NiFi UI:  $NIFI_URL"
echo "  Watch traffic: docker exec np-kafka /opt/kafka/bin/kafka-console-consumer.sh \\"
echo "                   --bootstrap-server localhost:9092 \\"
echo "                   --topic events.critical --from-beginning"
echo "  Status:        $0 --status"
echo "  Stop:          $0 --stop"
