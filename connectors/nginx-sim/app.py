"""
NimbusPulse — Nginx access log simulator
Produces synthetic Nginx access events to a Kafka topic.
"""
import json, os, random, signal, sys, time, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092").split(",")
TOPIC = os.environ.get("TOPIC", "raw.nginx")
RATE = float(os.environ.get("RATE_PER_SEC", "8"))

PATHS = [
    "/api/v1/products", "/api/v1/orders", "/api/v1/users/me",
    "/api/v1/checkout", "/api/v1/search", "/api/v1/cart",
    "/", "/static/main.css", "/static/app.js", "/static/logo.svg",
    "/health", "/metrics", "/login", "/api/v1/auth/token",
]
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "curl/8.4.0",
    "PostmanRuntime/7.35.0",
    "GoogleBot/2.1 (+http://www.google.com/bot.html)",
    "kube-probe/1.28",
]

def fake_ip():
    return f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,254)}"

def make_event():
    roll = random.random()
    path = random.choice(PATHS)
    method = random.choices(["GET","POST","PUT","DELETE"], weights=[70,20,5,5])[0]
    if roll < 0.02:
        status = 504; sev = "ERROR"; msg = f"{method} {path} 504 (upstream timeout)"
    elif roll < 0.07:
        status = random.choice([500, 502, 503]); sev = "ERROR"; msg = f"{method} {path} {status}"
    elif roll < 0.15:
        status = random.choice([401, 403, 404]); sev = "WARN"; msg = f"{method} {path} {status}"
    else:
        status = random.choice([200,200,200,200,201,304]); sev = "INFO"
        msg = f"{method} {path} {status} {random.randint(20,500)}ms"
    return {
        "id": str(uuid.uuid4()),
        "time": datetime.now(timezone.utc).isoformat(),
        "severity": sev,
        "source": "nginx",
        "sourceName": "Nginx Access Logs",
        "message": msg,
        "payload": {
            "remoteAddr": fake_ip(),
            "method": method,
            "request": f"{method} {path} HTTP/1.1",
            "path": path,
            "status": status,
            "bytes": random.randint(0, 50000),
            "userAgent": random.choice(USER_AGENTS),
            "referer": random.choice(["-", "https://nimbuspulse.local/", "https://google.com"]),
            "responseTimeMs": random.randint(2, 500) if status < 400 else random.randint(100, 30000),
            "upstream": "checkout-api.prod.svc.cluster.local:8080",
        },
    }


def connect():
    while True:
        try:
            return KafkaProducer(bootstrap_servers=BROKERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all", retries=5, linger_ms=20)
        except NoBrokersAvailable:
            print("[nginx] Kafka not ready, retrying...", flush=True); time.sleep(3)

def main():
    print(f"[nginx] starting; brokers={BROKERS} topic={TOPIC} rate={RATE}/s", flush=True)
    p = connect(); running = True
    def stop(_s,_f):
        nonlocal running; running = False
    signal.signal(signal.SIGTERM, stop); signal.signal(signal.SIGINT, stop)
    interval = 1.0/max(RATE,0.1); n=0
    while running:
        ev = make_event()
        p.send(TOPIC, value=ev, key=ev["source"].encode("utf-8"))
        n+=1
        if n%50==0: print(f"[nginx] produced {n}", flush=True)
        time.sleep(interval)
    p.flush(); p.close(); sys.exit(0)

if __name__=="__main__": main()
