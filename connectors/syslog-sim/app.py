"""
NimbusPulse — Syslog (RFC 5424) simulator
Produces synthetic syslog events to a Kafka topic.
"""
import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092").split(",")
TOPIC = os.environ.get("TOPIC", "raw.syslog")
RATE = float(os.environ.get("RATE_PER_SEC", "5"))

HOSTS = ["bastion-01", "bastion-02", "web-prod-03", "web-prod-04", "db-prod-01", "build-ci-01"]
FACILITIES = ["auth", "daemon", "kern", "cron", "user", "syslog"]

TEMPLATES = [
    ("INFO", "auth", "sshd[{pid}]: Accepted publickey for {user} from {ip}",
     lambda: {"user": random.choice(["devops","admin","deploy","app"]), "ip": fake_ip(), "pid": random.randint(1000,30000)}),
    ("WARN", "auth", "sshd[{pid}]: Failed password for invalid user {user} from {ip}",
     lambda: {"user": random.choice(["root","admin","test","postgres"]), "ip": fake_ip(), "pid": random.randint(1000,30000)}),
    ("ERROR", "kern", "kernel: Out of memory: Killed process {pid} ({name})",
     lambda: {"pid": random.randint(1000,30000), "name": random.choice(["node","python","java","ruby"])}),
    ("INFO", "daemon", "systemd[1]: Started Daily apt activities",
     lambda: {}),
    ("WARN", "cron", "CRON[{pid}]: pam_unix(cron:session): session closed for user nobody",
     lambda: {"pid": random.randint(1000,30000)}),
    ("CRITICAL", "kern", "kernel: BUG: unable to handle kernel paging request at {addr}",
     lambda: {"addr": "0x" + uuid.uuid4().hex[:8]}),
]

def fake_ip():
    return f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,254)}"

def make_event():
    sev, facility, msg_tpl, payload_fn = random.choice(TEMPLATES)
    host = random.choice(HOSTS)
    extra = payload_fn()
    msg = msg_tpl.format(**extra) if extra else msg_tpl
    priority = {"INFO": 14, "WARN": 12, "ERROR": 11, "CRITICAL": 10}[sev]
    return {
        "id": str(uuid.uuid4()),
        "time": datetime.now(timezone.utc).isoformat(),
        "severity": sev,
        "source": "syslog",
        "sourceName": "Syslog (RFC 5424)",
        "message": f"{host} {msg}",
        "payload": {
            "priority": priority,
            "facility": facility,
            "host": host,
            "appName": msg.split("[")[0] if "[" in msg else msg.split(":")[0],
            "raw": f"<{priority}>1 {datetime.utcnow().isoformat()}Z {host} - - - {msg}",
            **extra,
        },
    }


def connect():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BROKERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all", retries=5, linger_ms=20,
            )
        except NoBrokersAvailable:
            print(f"[syslog] Kafka not ready, retrying...", flush=True)
            time.sleep(3)


def main():
    print(f"[syslog] starting; brokers={BROKERS} topic={TOPIC} rate={RATE}/s", flush=True)
    p = connect()
    running = True
    def stop(_s,_f):
        nonlocal running; running = False
    signal.signal(signal.SIGTERM, stop); signal.signal(signal.SIGINT, stop)
    interval = 1.0/max(RATE,0.1); n=0
    while running:
        ev = make_event()
        p.send(TOPIC, value=ev, key=ev["source"].encode("utf-8"))
        n+=1
        if n%25==0: print(f"[syslog] produced {n}", flush=True)
        time.sleep(interval)
    p.flush(); p.close(); sys.exit(0)

if __name__=="__main__": main()
