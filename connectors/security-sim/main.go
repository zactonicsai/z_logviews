// NimbusPulse — Security / SIEM events simulator (Go)
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

var rules = []struct {
	severity string
	category string
	tpl      string
	weight   int
}{
	{"INFO", "auth", "Successful login: %s from %s", 40},
	{"WARN", "auth", "Failed login: %s from %s (invalid credentials)", 25},
	{"WARN", "waf", "WAF blocked %s on %s from %s", 15},
	{"ERROR", "auth", "Account lockout: %s (5 failed attempts in 60s)", 8},
	{"ERROR", "ids", "Suspicious port scan detected from %s", 5},
	{"CRITICAL", "siem", "Possible credential stuffing: %d failed logins from /16 %s", 4},
	{"CRITICAL", "malware", "Malware signature matched on upload: %s (%s)", 3},
}

var users = []string{"jane.doe@example.com", "mark.smith@example.com", "guest", "admin",
	"root", "alice.kim@example.com", "deploy-bot"}
var wafRules = []string{"SQLI-001", "XSS-014", "LFI-007", "RCE-022", "SCANNER-001"}
var endpoints = []string{"/login", "/api/search", "/api/orders", "/admin", "/api/users", "/wp-admin"}
var malware = []struct{ file, sig string }{
	{"invoice.exe", "Trojan.Win32.Generic"},
	{"resume.pdf.exe", "Trojan.Agent.PHJ"},
	{"setup.zip", "Backdoor.MSIL.Bladabindi"},
	{"payload.sh", "Trojan.Linux.Tsunami"},
}

func fakeIP() string {
	return fmt.Sprintf("%d.%d.%d.%d", rand.Intn(223)+1, rand.Intn(255), rand.Intn(255), rand.Intn(254))
}
func fakeSubnet() string {
	return fmt.Sprintf("%d.%d.0.0/16", rand.Intn(223)+1, rand.Intn(255))
}

func pickRule() int {
	total := 0
	for _, r := range rules {
		total += r.weight
	}
	x := rand.Intn(total)
	for i, r := range rules {
		if x < r.weight {
			return i
		}
		x -= r.weight
	}
	return 0
}

func makeEvent() Event {
	idx := pickRule()
	r := rules[idx]
	user := users[rand.Intn(len(users))]
	ip := fakeIP()
	payload := map[string]interface{}{
		"category":  r.category,
		"alertId":   uuid.NewString(),
		"detection": "rule-engine-v2",
	}

	var msg string
	switch r.category {
	case "auth":
		msg = fmt.Sprintf(r.tpl, user, ip)
		payload["user"] = user
		payload["srcIp"] = ip
		payload["mfa"] = rand.Intn(2) == 1
		payload["provider"] = "okta"
	case "waf":
		rule := wafRules[rand.Intn(len(wafRules))]
		ep := endpoints[rand.Intn(len(endpoints))]
		msg = fmt.Sprintf(r.tpl, rule, ep, ip)
		payload["rule"] = rule
		payload["action"] = "block"
		payload["uri"] = ep
		payload["srcIp"] = ip
	case "ids":
		msg = fmt.Sprintf(r.tpl, ip)
		payload["srcIp"] = ip
		payload["portsScanned"] = []int{22, 80, 443, 3306, 5432, 6379}
	case "siem":
		subnet := fakeSubnet()
		attempts := rand.Intn(500) + 300
		msg = fmt.Sprintf(r.tpl, attempts, subnet)
		payload["srcSubnet"] = subnet
		payload["failedAttempts"] = attempts
		payload["targetUsers"] = rand.Intn(80) + 40
		payload["windowMin"] = 10
	case "malware":
		m := malware[rand.Intn(len(malware))]
		msg = fmt.Sprintf(r.tpl, m.file, m.sig)
		payload["file"] = m.file
		payload["signature"] = m.sig
		payload["action"] = "quarantined"
		payload["user"] = user
	}

	return Event{
		ID:         uuid.NewString(),
		Time:       time.Now().UTC().Format(time.RFC3339Nano),
		Severity:   r.severity,
		Source:     "security-siem",
		SourceName: "Security / SIEM",
		Message:    msg,
		Payload:    payload,
	}
}

func main() {
	brokers := strings.Split(getenv("KAFKA_BROKERS", "kafka:9092"), ",")
	topic := getenv("TOPIC", "raw.security")
	rateStr := getenv("RATE_PER_SEC", "2")
	rate, _ := strconv.ParseFloat(rateStr, 64)
	if rate <= 0 {
		rate = 1
	}
	interval := time.Duration(float64(time.Second) / rate)

	log.Printf("[security] starting; brokers=%v topic=%s rate=%.2f/s", brokers, topic, rate)

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchTimeout: 20 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}
	defer w.Close()

	for i := 0; i < 60; i++ {
		conn, err := kafka.Dial("tcp", brokers[0])
		if err == nil {
			conn.Close()
			break
		}
		log.Printf("[security] kafka not ready, retrying...")
		time.Sleep(3 * time.Second)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() { <-sig; cancel() }()

	tk := time.NewTicker(interval)
	defer tk.Stop()
	n := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			ev := makeEvent()
			b, _ := json.Marshal(ev)
			if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(ev.Source), Value: b}); err != nil {
				log.Printf("[security] write err: %v", err)
				continue
			}
			n++
			if n%20 == 0 {
				log.Printf("[security] produced %d events", n)
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
