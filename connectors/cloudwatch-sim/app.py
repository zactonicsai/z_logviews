"""
NimbusPulse — AWS CloudWatch Logs simulator
Produces synthetic CloudWatch-style events to a Kafka topic.
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
TOPIC = os.environ.get("TOPIC", "raw.cloudwatch")
RATE = float(os.environ.get("RATE_PER_SEC", "3"))

LAMBDAS = [
    "api-gateway-handler",
    "payment-processor",
    "image-resize-worker",
    "stream-aggregator",
    "auth-token-validator",
    "report-generator",
]
REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-northeast-1"]
LOG_GROUPS = [
    "/aws/lambda/api-gateway-handler",
    "/aws/lambda/payment-processor",
    "/aws/ecs/checkout-api",
    "/aws/rds/postgres/prod-main/postgresql",
    "/aws/apigateway/access-logs",
]


def make_event() -> dict:
    """Generate one realistic-ish CloudWatch event."""
    roll = random.random()
    if roll < 0.02:
        sev, msg_template, payload = (
            "CRITICAL",
            "Lambda {fn} timed out after 30000ms",
            lambda fn: {
                "functionName": fn,
                "errorType": "TimeoutError",
                "duration": 30000,
                "memoryUsed": random.randint(500, 1024),
            },
        )
    elif roll < 0.10:
        sev, msg_template, payload = (
            "ERROR",
            "Lambda {fn} exited with error",
            lambda fn: {
                "functionName": fn,
                "errorType": random.choice(["RuntimeError", "ConnectionError", "ValidationError"]),
                "duration": random.randint(500, 5000),
                "stackTrace": "Traceback (most recent call last):\n  File '/var/task/handler.py', line 42, in <lambda>",
            },
        )
    elif roll < 0.25:
        sev, msg_template, payload = (
            "WARN",
            "CloudWatch alarm '{fn}-HighCPU' triggered",
            lambda fn: {
                "alarmName": f"{fn}-HighCPU",
                "threshold": 80,
                "currentValue": random.randint(81, 98),
                "datapoints": 5,
            },
        )
    else:
        sev, msg_template, payload = (
            "INFO",
            "Lambda {fn} completed in {dur}ms",
            lambda fn: {
                "functionName": fn,
                "duration": random.randint(40, 800),
                "memoryUsed": random.randint(64, 256),
                "billedDuration": random.randint(40, 800),
            },
        )

    fn = random.choice(LAMBDAS)
    body = payload(fn)
    return {
        "id": str(uuid.uuid4()),
        "time": datetime.now(timezone.utc).isoformat(),
        "severity": sev,
        "source": "aws-cloudwatch",
        "sourceName": "AWS CloudWatch Logs",
        "message": msg_template.format(fn=fn, dur=body.get("duration", "—")),
        "payload": {
            **body,
            "region": random.choice(REGIONS),
            "logGroup": random.choice(LOG_GROUPS),
            "logStream": f"{datetime.utcnow().strftime('%Y/%m/%d')}/[$LATEST]{uuid.uuid4().hex[:16]}",
            "requestId": str(uuid.uuid4()),
            "accountId": f"{random.randint(100000000000, 999999999999)}",
        },
    }


def connect_with_retry() -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BROKERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=20,
            )
        except NoBrokersAvailable:
            print(f"[cloudwatch] Kafka not ready at {BROKERS}, retrying in 3s...", flush=True)
            time.sleep(3)


def main() -> None:
    print(f"[cloudwatch] starting; brokers={BROKERS} topic={TOPIC} rate={RATE}/s", flush=True)
    producer = connect_with_retry()

    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False
        print("[cloudwatch] shutdown requested", flush=True)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    interval = 1.0 / max(RATE, 0.1)
    n = 0
    while running:
        ev = make_event()
        producer.send(TOPIC, value=ev, key=ev["source"].encode("utf-8"))
        n += 1
        if n % 25 == 0:
            print(f"[cloudwatch] produced {n} events", flush=True)
        time.sleep(interval)

    producer.flush()
    producer.close()
    print("[cloudwatch] stopped.", flush=True)
    sys.exit(0)


if __name__ == "__main__":
    main()
