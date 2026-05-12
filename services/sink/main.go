// NimbusPulse — Sink service
// Consumes all raw.* topics from Kafka, enriches, and fans out to:
//   - Elasticsearch (primary event index)
//   - Redis (real-time counters + recent events list)
//   - TimescaleDB (event metrics hypertable)
// This mirrors what Apache NiFi + a Flink job would do in production.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type Event struct {
	ID         string                 `json:"id"`
	Time       string                 `json:"time"`
	Severity   string                 `json:"severity"`
	Source     string                 `json:"source"`
	SourceName string                 `json:"sourceName"`
	Message    string                 `json:"message"`
	Payload    map[string]interface{} `json:"payload"`
	// Added by sink:
	Enrichment map[string]interface{} `json:"enrichment,omitempty"`
	Ingested   string                 `json:"ingestedAt,omitempty"`
}

var (
	rawTopics = []string{
		"raw.cloudwatch", "raw.k8s-events", "raw.syslog", "raw.security", "raw.nginx",
	}
	totalIngested uint64
	totalErrors   uint64
)

func main() {
	brokers := strings.Split(getenv("KAFKA_BROKERS", "kafka:9092"), ",")
	esURL := getenv("ES_URL", "http://elasticsearch:9200")
	redisAddr := getenv("REDIS_ADDR", "redis:6379")
	pgDSN := getenv("PG_DSN", "host=timescaledb user=nimbus password=nimbus dbname=metrics sslmode=disable")

	log.Printf("[sink] brokers=%v es=%s redis=%s pg=%s", brokers, esURL, redisAddr, pgDSN)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() { <-sig; log.Println("[sink] shutdown"); cancel() }()

	// Connect Redis
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()
	for {
		if err := rdb.Ping(ctx).Err(); err == nil {
			break
		}
		log.Printf("[sink] redis not ready, retrying...")
		time.Sleep(2 * time.Second)
	}
	log.Println("[sink] redis OK")

	// Connect Postgres/Timescale, create table + hypertable
	var pool *pgxpool.Pool
	for {
		var err error
		pool, err = pgxpool.New(ctx, pgDSN)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				break
			}
		}
		log.Printf("[sink] pg not ready (%v), retrying...", err)
		time.Sleep(3 * time.Second)
	}
	defer pool.Close()
	if err := initPg(ctx, pool); err != nil {
		log.Fatalf("[sink] pg init: %v", err)
	}
	log.Println("[sink] timescaledb OK")

	// Wait for ES
	for {
		resp, err := http.Get(esURL + "/_cluster/health")
		if err == nil && resp.StatusCode < 500 {
			resp.Body.Close()
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		log.Println("[sink] elasticsearch not ready, retrying...")
		time.Sleep(3 * time.Second)
	}
	if err := initEs(esURL); err != nil {
		log.Printf("[sink] es init warn: %v", err)
	}
	log.Println("[sink] elasticsearch OK")

	// Start a consumer per topic
	var wg sync.WaitGroup
	for _, t := range rawTopics {
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			consume(ctx, brokers, topic, esURL, rdb, pool)
		}(t)
	}

	// Periodic stats logger
	go func() {
		tk := time.NewTicker(15 * time.Second)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C:
				log.Printf("[sink] stats — ingested=%d errors=%d",
					atomic.LoadUint64(&totalIngested), atomic.LoadUint64(&totalErrors))
			}
		}
	}()

	wg.Wait()
	log.Println("[sink] stopped.")
}

func consume(ctx context.Context, brokers []string, topic, esURL string, rdb *redis.Client, pool *pgxpool.Pool) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        "sink-" + topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	defer r.Close()
	log.Printf("[sink] consuming %s", topic)

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[sink/%s] read err: %v", topic, err)
			time.Sleep(time.Second)
			continue
		}
		var ev Event
		if err := json.Unmarshal(m.Value, &ev); err != nil {
			log.Printf("[sink/%s] decode err: %v", topic, err)
			atomic.AddUint64(&totalErrors, 1)
			continue
		}
		enrich(&ev)
		if err := writeES(esURL, &ev); err != nil {
			log.Printf("[sink/%s] es write err: %v", topic, err)
			atomic.AddUint64(&totalErrors, 1)
		}
		writeRedis(ctx, rdb, &ev)
		writePg(ctx, pool, &ev)
		atomic.AddUint64(&totalIngested, 1)
	}
}

func enrich(ev *Event) {
	ev.Ingested = time.Now().UTC().Format(time.RFC3339Nano)
	en := map[string]interface{}{
		"pipeline":    "nimbuspulse-sink",
		"env":         "demo",
		"datacenter":  "local",
		"traceId":     "trace-" + ev.ID[:8],
	}
	// Mask anything that looks like an email or IP in payload (very simple PII demo)
	if p, ok := ev.Payload["user"].(string); ok && strings.Contains(p, "@") {
		parts := strings.SplitN(p, "@", 2)
		if len(parts[0]) > 2 {
			ev.Payload["user"] = parts[0][:2] + "***@" + parts[1]
		}
		en["pii_masked"] = true
	}
	ev.Enrichment = en
}

// --------- Elasticsearch ----------

func initEs(esURL string) error {
	// Create index with mapping if not exists
	mapping := `{
	  "mappings": {
	    "properties": {
	      "id":         {"type": "keyword"},
	      "time":       {"type": "date"},
	      "ingestedAt": {"type": "date"},
	      "severity":   {"type": "keyword"},
	      "source":     {"type": "keyword"},
	      "sourceName": {"type": "keyword"},
	      "message":    {"type": "text"},
	      "payload":    {"type": "object", "enabled": true},
	      "enrichment": {"type": "object"}
	    }
	  }
	}`
	req, _ := http.NewRequest("PUT", esURL+"/np-events", bytes.NewBufferString(mapping))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// 200 = created, 400 = already exists — both fine
	return nil
}

func writeES(esURL string, ev *Event) error {
	b, _ := json.Marshal(ev)
	req, _ := http.NewRequest("POST",
		fmt.Sprintf("%s/np-events/_doc/%s", esURL, ev.ID),
		bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("es status %d", resp.StatusCode)
	}
	return nil
}

// --------- Redis ----------

func writeRedis(ctx context.Context, rdb *redis.Client, ev *Event) {
	pipe := rdb.Pipeline()
	// Counters
	pipe.Incr(ctx, "np:counter:total")
	pipe.Incr(ctx, "np:counter:sev:"+ev.Severity)
	pipe.Incr(ctx, "np:counter:source:"+ev.Source)
	// Recent events list (cap at 200)
	if b, err := json.Marshal(ev); err == nil {
		pipe.LPush(ctx, "np:recent", string(b))
		pipe.LTrim(ctx, "np:recent", 0, 199)
	}
	// Per-second throughput bucket (60s ring)
	bucket := time.Now().Unix() % 60
	pipe.Incr(ctx, fmt.Sprintf("np:tput:%d", bucket))
	pipe.Expire(ctx, fmt.Sprintf("np:tput:%d", bucket), 90*time.Second)

	_, _ = pipe.Exec(ctx)
}

// --------- TimescaleDB ----------

func initPg(ctx context.Context, pool *pgxpool.Pool) error {
	stmts := []string{
		`CREATE EXTENSION IF NOT EXISTS timescaledb`,
		`CREATE TABLE IF NOT EXISTS event_metrics (
			ts          TIMESTAMPTZ NOT NULL,
			source      TEXT NOT NULL,
			severity    TEXT NOT NULL,
			count       INTEGER NOT NULL DEFAULT 1
		)`,
		`SELECT create_hypertable('event_metrics', 'ts', if_not_exists => TRUE)`,
		`CREATE INDEX IF NOT EXISTS idx_em_source ON event_metrics (source, ts DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_em_severity ON event_metrics (severity, ts DESC)`,
	}
	for _, s := range stmts {
		if _, err := pool.Exec(ctx, s); err != nil {
			return fmt.Errorf("stmt failed (%s): %w", s, err)
		}
	}
	return nil
}

func writePg(ctx context.Context, pool *pgxpool.Pool, ev *Event) {
	t, err := time.Parse(time.RFC3339Nano, ev.Time)
	if err != nil {
		t = time.Now().UTC()
	}
	_, err = pool.Exec(ctx,
		`INSERT INTO event_metrics (ts, source, severity, count) VALUES ($1, $2, $3, 1)`,
		t, ev.Source, ev.Severity)
	if err != nil {
		log.Printf("[sink] pg insert err: %v", err)
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
