// NimbusPulse — Query API
// REST endpoints for events, search, stats, and live counters.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	esURL     string
	rdb       *redis.Client
	startedAt = time.Now()
)

func main() {
	esURL = getenv("ES_URL", "http://elasticsearch:9200")
	redisAddr := getenv("REDIS_ADDR", "redis:6379")

	rdb = redis.NewClient(&redis.Options{Addr: redisAddr})
	for {
		if err := rdb.Ping(context.Background()).Err(); err == nil {
			break
		}
		log.Println("[query-api] redis not ready, retrying...")
		time.Sleep(2 * time.Second)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "uptime": time.Since(startedAt).String()})
	})
	mux.HandleFunc("/api/v1/stats", statsHandler)
	mux.HandleFunc("/api/v1/recent", recentHandler)
	mux.HandleFunc("/api/v1/search", searchHandler)
	mux.HandleFunc("/api/v1/throughput", throughputHandler)
	mux.HandleFunc("/api/v1/sources", sourcesHandler)

	addr := ":8090"
	log.Printf("[query-api] listening on %s; es=%s redis=%s", addr, esURL, redisAddr)
	srv := &http.Server{
		Addr:              addr,
		Handler:           cors(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Fatal(srv.ListenAndServe())
}

func cors(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		h.ServeHTTP(w, r)
	})
}

// GET /api/v1/stats — counters & severity breakdown from Redis
func statsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	total, _ := rdb.Get(ctx, "np:counter:total").Int64()
	sevs := map[string]int64{}
	for _, s := range []string{"DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"} {
		v, _ := rdb.Get(ctx, "np:counter:sev:"+s).Int64()
		sevs[s] = v
	}
	srcKeys, _ := rdb.Keys(ctx, "np:counter:source:*").Result()
	sources := map[string]int64{}
	for _, k := range srcKeys {
		v, _ := rdb.Get(ctx, k).Int64()
		sources[strings.TrimPrefix(k, "np:counter:source:")] = v
	}
	errRate := 0.0
	if total > 0 {
		errRate = float64(sevs["ERROR"]+sevs["CRITICAL"]) / float64(total) * 100
	}
	writeJSON(w, 200, map[string]any{
		"total":        total,
		"bySeverity":   sevs,
		"bySource":     sources,
		"errorRate":    errRate,
		"asOf":         time.Now().UTC().Format(time.RFC3339),
	})
}

// GET /api/v1/recent?n=50 — most recent N events from Redis list
func recentHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	n := 50
	if s := r.URL.Query().Get("n"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 && v <= 200 {
			n = v
		}
	}
	vals, err := rdb.LRange(ctx, "np:recent", 0, int64(n-1)).Result()
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": err.Error()})
		return
	}
	out := make([]map[string]any, 0, len(vals))
	for _, v := range vals {
		var m map[string]any
		if err := json.Unmarshal([]byte(v), &m); err == nil {
			out = append(out, m)
		}
	}
	writeJSON(w, 200, map[string]any{"events": out, "count": len(out)})
}

// GET /api/v1/search?q=…&sev=ERROR&source=k8s-events&size=20
func searchHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	sev := r.URL.Query().Get("sev")
	src := r.URL.Query().Get("source")
	size := 20
	if s := r.URL.Query().Get("size"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 && v <= 100 {
			size = v
		}
	}

	must := []map[string]any{}
	if q != "" {
		must = append(must, map[string]any{
			"match": map[string]any{"message": q},
		})
	}
	if sev != "" {
		must = append(must, map[string]any{"term": map[string]any{"severity": sev}})
	}
	if src != "" {
		must = append(must, map[string]any{"term": map[string]any{"source": src}})
	}

	body := map[string]any{
		"size": size,
		"sort": []map[string]any{{"time": map[string]string{"order": "desc"}}},
	}
	if len(must) > 0 {
		body["query"] = map[string]any{"bool": map[string]any{"must": must}}
	} else {
		body["query"] = map[string]any{"match_all": map[string]any{}}
	}

	resp, err := postJSON(esURL+"/np-events/_search", body)
	if err != nil {
		writeJSON(w, 502, map[string]any{"error": "elasticsearch unavailable", "detail": err.Error()})
		return
	}

	// Pass through hits
	hits := []map[string]any{}
	if hh, ok := resp["hits"].(map[string]any); ok {
		if arr, ok := hh["hits"].([]any); ok {
			for _, h := range arr {
				if m, ok := h.(map[string]any); ok {
					if src, ok := m["_source"].(map[string]any); ok {
						hits = append(hits, src)
					}
				}
			}
		}
	}
	total := int64(0)
	if hh, ok := resp["hits"].(map[string]any); ok {
		if t, ok := hh["total"].(map[string]any); ok {
			if v, ok := t["value"].(float64); ok {
				total = int64(v)
			}
		}
	}
	writeJSON(w, 200, map[string]any{"hits": hits, "total": total, "query": map[string]any{"q": q, "sev": sev, "source": src}})
}

// GET /api/v1/throughput — 60-bucket ring from Redis (events per second over last minute)
func throughputHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	now := time.Now().Unix()
	out := make([]map[string]any, 60)
	for i := 0; i < 60; i++ {
		bucket := (now - int64(59-i)) % 60
		v, _ := rdb.Get(ctx, fmt.Sprintf("np:tput:%d", bucket)).Int64()
		out[i] = map[string]any{"t": now - int64(59-i), "n": v}
	}
	writeJSON(w, 200, map[string]any{"series": out})
}

// GET /api/v1/sources — distinct source list with counts (from ES aggs)
func sourcesHandler(w http.ResponseWriter, r *http.Request) {
	body := map[string]any{
		"size": 0,
		"aggs": map[string]any{
			"sources": map[string]any{
				"terms": map[string]any{"field": "source", "size": 50},
			},
		},
	}
	resp, err := postJSON(esURL+"/np-events/_search", body)
	if err != nil {
		writeJSON(w, 502, map[string]any{"error": err.Error()})
		return
	}
	sources := []map[string]any{}
	if a, ok := resp["aggregations"].(map[string]any); ok {
		if s, ok := a["sources"].(map[string]any); ok {
			if b, ok := s["buckets"].([]any); ok {
				for _, x := range b {
					if m, ok := x.(map[string]any); ok {
						sources = append(sources, map[string]any{
							"source": m["key"],
							"count":  m["doc_count"],
						})
					}
				}
			}
		}
	}
	writeJSON(w, 200, map[string]any{"sources": sources})
}

// ---------- helpers ----------

func postJSON(url string, body map[string]any) (map[string]any, error) {
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", url, strings.NewReader(string(b)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(raw))
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
