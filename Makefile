# =====================================================================
# NimbusPulse POC — convenience targets
# Usage:  make up | make down | make logs | make ps | make test
# =====================================================================

.PHONY: help up down restart logs ps clean reset test stats topics urls \
        logs-sink logs-conn logs-api logs-ai logs-etl build etl-test \
        nifi-load nifi-start nifi-stop nifi-status nifi-delete

help:
	@echo "NimbusPulse POC — targets:"
	@echo "  make up        — start the whole stack in the background"
	@echo "  make down      — stop everything (keeps volumes)"
	@echo "  make restart   — restart the whole stack"
	@echo "  make ps        — show running services + their ports"
	@echo "  make logs      — tail all logs"
	@echo "  make logs-sink — tail the sink service"
	@echo "  make logs-conn — tail all 5 connectors at once"
	@echo "  make logs-api  — tail query-api + ai-assistant"
	@echo "  make build     — rebuild custom images (sink, query-api, etc.)"
	@echo "  make test      — smoke-test the Query API endpoints"
	@echo "  make etl-test  — upload a sample file to the ETL API"
	@echo "  make stats     — show event counters from Redis"
	@echo "  make topics    — list Kafka topics"
	@echo "  make urls      — print all service URLs"
	@echo ""
	@echo "  NiFi sample flow ('Security Pipeline'):"
	@echo "  make nifi-load   — build & start the sample flow (idempotent)"
	@echo "  make nifi-status — show flow throughput & queue depth"
	@echo "  make nifi-start  — start a previously-stopped flow"
	@echo "  make nifi-stop   — pause the flow (keeps it on canvas)"
	@echo "  make nifi-delete — remove the flow from the NiFi canvas"
	@echo ""
	@echo "  make clean     — stop & remove containers (keep volumes)"
	@echo "  make reset     — wipe EVERYTHING including data volumes"

up:
	docker compose up -d
	@echo ""
	@echo "▶ Stack starting. Open the dashboard at http://localhost:8080"
	@echo "  It may take ~60s for Elasticsearch + NiFi to be healthy."

down:
	docker compose down

restart:
	docker compose restart

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=50

logs-sink:
	docker compose logs -f --tail=100 sink

logs-conn:
	docker compose logs -f --tail=50 \
		connector-cloudwatch connector-k8s-events connector-syslog \
		connector-security connector-nginx

logs-api:
	docker compose logs -f --tail=50 query-api ai-assistant etl-api

logs-etl:
	docker compose logs -f --tail=100 etl-api

build:
	docker compose build sink query-api ai-assistant etl-api dashboard \
		connector-cloudwatch connector-k8s-events connector-syslog \
		connector-security connector-nginx

etl-test:
	@echo "→ Creating sample CSV..."
	@printf "user,event,severity,count\njane,login,INFO,42\nmark,logout,INFO,18\nguest,fail,ERROR,1\n" > /tmp/etl-sample.csv
	@echo "→ POSTing to ETL API..."
	@curl -s -F "file=@/tmp/etl-sample.csv" http://localhost:8092/api/v1/etl/upload | jq . 2>/dev/null || \
		curl -s -F "file=@/tmp/etl-sample.csv" http://localhost:8092/api/v1/etl/upload
	@echo ""
	@echo "→ Listing recent documents..."
	@curl -s 'http://localhost:8092/api/v1/etl/documents?size=3' | jq '.hits[] | {docId, sourceFile, extractor, wordCount}' 2>/dev/null || \
		curl -s 'http://localhost:8092/api/v1/etl/documents?size=3'
	@rm -f /tmp/etl-sample.csv

test:
	@echo "→ /healthz"
	@curl -s http://localhost:8090/healthz | jq . 2>/dev/null || curl -s http://localhost:8090/healthz
	@echo ""
	@echo "→ /api/v1/stats"
	@curl -s http://localhost:8090/api/v1/stats | jq . 2>/dev/null || curl -s http://localhost:8090/api/v1/stats
	@echo ""
	@echo "→ /api/v1/recent?n=3"
	@curl -s 'http://localhost:8090/api/v1/recent?n=3' | jq . 2>/dev/null || curl -s 'http://localhost:8090/api/v1/recent?n=3'

stats:
	@echo "Total events ingested:"
	@docker exec np-redis redis-cli GET np:counter:total
	@echo "By severity:"
	@for s in DEBUG INFO WARN ERROR CRITICAL; do \
		printf "  $$s: "; docker exec np-redis redis-cli GET np:counter:sev:$$s; \
	done

topics:
	@docker exec np-kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --list

urls:
	@echo "MAIN DASHBOARD  → http://localhost:8080"
	@echo ""
	@echo "── Dataflow & Bus ──"
	@echo "  NiFi          → https://localhost:8443   (admin / ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB)"
	@echo "  Kafka UI      → http://localhost:8181"
	@echo ""
	@echo "── Storage ──"
	@echo "  Elasticsearch → http://localhost:9200"
	@echo "  Kibana        → http://localhost:5601"
	@echo "  ClickHouse    → http://localhost:8123/play"
	@echo "  TimescaleDB   → adminer @ http://localhost:8888  (nimbus/nimbus, db=metrics)"
	@echo "  Redis         → redis-commander @ http://localhost:8182"
	@echo "  MinIO         → http://localhost:9001  (nimbus / nimbus-secret)"
	@echo ""
	@echo "── Observability ──"
	@echo "  Grafana       → http://localhost:3000  (admin / nimbus, or anon viewer)"
	@echo ""
	@echo "── Custom services ──"
	@echo "  Query API     → http://localhost:8090/api/v1/stats"
	@echo "  AI Assistant  → http://localhost:8091/docs"
	@echo "  ETL API       → http://localhost:8092/docs"
	@echo "  ETL Upload    → http://localhost:8080/etl/"

clean:
	docker compose down

reset:
	docker compose down -v
	@echo "▶ All volumes wiped. Run 'make up' to start fresh."

# =====================================================================
# NiFi sample flow management — see nifi/HOW-TO.md
# =====================================================================

nifi-load:
	@echo "▶ Loading & starting the 'NimbusPulse Security Pipeline' flow in NiFi..."
	@bash nifi/scripts/load-flow.sh

nifi-status:
	@bash nifi/scripts/load-flow.sh --status

nifi-start:
	@bash nifi/scripts/load-flow.sh --start

nifi-stop:
	@bash nifi/scripts/load-flow.sh --stop

nifi-delete:
	@bash nifi/scripts/load-flow.sh --delete
