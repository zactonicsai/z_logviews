// NimbusPulse — Kubernetes Events simulator (Go)
// Produces synthetic K8s-style events to a Kafka topic.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
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
}

var (
	namespaces = []string{"prod", "staging", "analytics", "platform", "kube-system"}
	apps       = []string{"checkout-api", "user-svc", "cart-api", "search-svc", "payment-gateway", "report-worker", "auth-svc"}
	reasons    = []string{"Scheduled", "Pulling", "Pulled", "Created", "Started", "Killing", "BackOff", "Unhealthy", "OOMKilled", "ImagePullBackOff", "FailedScheduling", "FailedMount"}
)

type tmpl struct {
	severity string
	reason   string
	tpl      string
	weight   int
}

var templates = []tmpl{
	{"INFO", "Scheduled", "Successfully assigned %s/%s-%s to node-%d", 30},
	{"INFO", "Pulled", "Container image \"%s:v%d.%d\" already present on machine", 25},
	{"INFO", "Started", "Started container %s", 25},
	{"WARN", "Unhealthy", "Readiness probe failed for pod %s/%s-%s: HTTP 503", 8},
	{"WARN", "BackOff", "Back-off restarting failed container %s in pod %s/%s-%s", 5},
	{"ERROR", "ImagePullBackOff", "Failed to pull image \"gcr.io/co/%s:v%d.%d.%d\": rpc error: code = Unknown", 4},
	{"ERROR", "OOMKilled", "Container %s in pod %s/%s-%s was killed (exit 137, OOM)", 3},
}

func pickTemplate() tmpl {
	total := 0
	for _, t := range templates {
		total += t.weight
	}
	r := rand.Intn(total)
	for _, t := range templates {
		if r < t.weight {
			return t
		}
		r -= t.weight
	}
	return templates[0]
}

func makeEvent() Event {
	t := pickTemplate()
	ns := namespaces[rand.Intn(len(namespaces))]
	app := apps[rand.Intn(len(apps))]
	podHash := fmt.Sprintf("%x", rand.Int63n(0xFFFFFFFFFF))[:8]
	node := rand.Intn(20) + 1

	var msg string
	switch t.reason {
	case "Scheduled":
		msg = fmt.Sprintf(t.tpl, ns, app, podHash, node)
	case "Pulled":
		msg = fmt.Sprintf(t.tpl, app, rand.Intn(5)+1, rand.Intn(20))
	case "Started":
		msg = fmt.Sprintf(t.tpl, app)
	case "Unhealthy", "BackOff":
		msg = fmt.Sprintf(t.tpl, app, ns, app, podHash)
	case "ImagePullBackOff":
		msg = fmt.Sprintf(t.tpl, app, rand.Intn(5)+1, rand.Intn(20), rand.Intn(50))
	case "OOMKilled":
		msg = fmt.Sprintf(t.tpl, app, ns, app, podHash)
	}

	return Event{
		ID:         uuid.NewString(),
		Time:       time.Now().UTC().Format(time.RFC3339Nano),
		Severity:   t.severity,
		Source:     "k8s-events",
		SourceName: "Kubernetes Events",
		Message:    msg,
		Payload: map[string]interface{}{
			"reason":    t.reason,
			"namespace": ns,
			"pod":       fmt.Sprintf("%s-%s", app, podHash),
			"container": app,
			"node":      fmt.Sprintf("node-%d", node),
			"cluster":   "prod-gke-us-east1",
			"involvedObject": map[string]string{
				"kind": "Pod",
				"name": fmt.Sprintf("%s-%s", app, podHash),
				"uid":  uuid.NewString(),
			},
			"count":          rand.Intn(5) + 1,
			"firstTimestamp": time.Now().UTC().Add(-time.Duration(rand.Intn(3600)) * time.Second).Format(time.RFC3339),
			"lastTimestamp":  time.Now().UTC().Format(time.RFC3339),
		},
	}
}

func main() {
	brokers := strings.Split(getenv("KAFKA_BROKERS", "kafka:9092"), ",")
	topic := getenv("TOPIC", "raw.k8s-events")
	rateStr := getenv("RATE_PER_SEC", "4")
	rate, _ := strconv.ParseFloat(rateStr, 64)
	if rate <= 0 {
		rate = 1
	}
	interval := time.Duration(float64(time.Second) / rate)

	log.Printf("[k8s-events] starting; brokers=%v topic=%s rate=%.2f/s", brokers, topic, rate)

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchTimeout: 20 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
	defer w.Close()

	// Wait for Kafka
	for i := 0; i < 60; i++ {
		conn, err := kafka.Dial("tcp", brokers[0])
		if err == nil {
			conn.Close()
			break
		}
		log.Printf("[k8s-events] kafka not ready (%v), retrying...", err)
		time.Sleep(3 * time.Second)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() { <-sigCh; log.Println("[k8s-events] shutdown"); cancel() }()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	n := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ev := makeEvent()
			b, _ := json.Marshal(ev)
			err := w.WriteMessages(ctx, kafka.Message{Key: []byte(ev.Source), Value: b})
			if err != nil {
				log.Printf("[k8s-events] write err: %v", err)
				continue
			}
			n++
			if n%25 == 0 {
				log.Printf("[k8s-events] produced %d events", n)
			}
		}
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
