"""
NimbusPulse — AI Assistant (FastAPI)
Pattern-matched assistant that reads live data from the Query API.
This is the staffing of the LLM/RAG component in the production architecture.
"""
import os
import re
import time
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

QUERY_API_URL = os.environ.get("QUERY_API_URL", "http://query-api:8090")

app = FastAPI(title="NimbusPulse AI Assistant", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


class AskRequest(BaseModel):
    question: str


class AskResponse(BaseModel):
    answer: str
    citations: list[dict[str, Any]]
    facts: dict[str, Any]
    latencyMs: int


@app.get("/healthz")
def healthz() -> dict[str, Any]:
    return {"status": "ok", "service": "ai-assistant"}


@app.get("/api/v1/prompts")
def suggested_prompts() -> dict[str, Any]:
    return {
        "prompts": [
            "Summarize the last few minutes of events",
            "Why is the error rate climbing?",
            "Show me failed logins",
            "Which sources are most active?",
            "Are there any security threats?",
            "Find OOMKilled events",
            "How is the system overall right now?",
            "What does this CRITICAL event likely mean?",
        ]
    }


@app.post("/api/v1/ask", response_model=AskResponse)
def ask(req: AskRequest) -> AskResponse:
    t0 = time.time()
    q = (req.question or "").strip()
    if not q:
        raise HTTPException(400, "question required")

    facts = _gather_facts()
    answer, citations = _generate_answer(q, facts)
    latency = int((time.time() - t0) * 1000)
    return AskResponse(answer=answer, citations=citations, facts=facts, latencyMs=latency)


# ---------------- helpers ----------------

def _gather_facts() -> dict[str, Any]:
    """Pull current state from the Query API to ground answers."""
    facts: dict[str, Any] = {}
    try:
        with httpx.Client(timeout=5.0) as c:
            facts["stats"] = c.get(f"{QUERY_API_URL}/api/v1/stats").json()
            facts["sources"] = c.get(f"{QUERY_API_URL}/api/v1/sources").json()
            facts["recent"] = c.get(f"{QUERY_API_URL}/api/v1/recent", params={"n": 50}).json()
    except Exception as e:
        facts["error"] = f"query-api unreachable: {e}"
    return facts


def _retrieve(query_terms: list[str], facts: dict[str, Any], limit: int = 5) -> list[dict[str, Any]]:
    """Return events from recent that match any of the terms (case-insensitive substring)."""
    events = facts.get("recent", {}).get("events", []) or []
    out = []
    for e in events:
        text = " ".join([str(e.get("message", "")), str(e.get("source", "")),
                         str(e.get("severity", ""))]).lower()
        if any(term.lower() in text for term in query_terms):
            out.append(e)
            if len(out) >= limit:
                break
    return out


def _generate_answer(q: str, facts: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    Q = q.lower()
    stats = facts.get("stats") or {}
    sevs = (stats.get("bySeverity") or {})
    total = stats.get("total") or 0
    err_rate = stats.get("errorRate") or 0.0
    sources = (stats.get("bySource") or {})
    top_sources = sorted(sources.items(), key=lambda kv: -kv[1])[:3]

    # ----- pattern routing -----

    if _matches(Q, ["summar", "summary", "how.*overall", "right now", "status", "health"]):
        cites = facts.get("recent", {}).get("events", [])[:3]
        ans = (
            f"<b>System snapshot</b><br/><br/>"
            f"• Total events ingested: <b>{total:,}</b><br/>"
            f"• Severity mix: "
            f"INFO {sevs.get('INFO', 0):,} · "
            f"WARN {sevs.get('WARN', 0):,} · "
            f"ERROR {sevs.get('ERROR', 0):,} · "
            f"CRITICAL {sevs.get('CRITICAL', 0):,}<br/>"
            f"• Error rate: <b class=\"{('text-ibm-red' if err_rate > 1.0 else 'text-ibm-green')}\">"
            f"{err_rate:.2f}%</b> (SLO 1.0%)<br/>"
            f"• Top sources: " + ", ".join(f"<b>{s}</b> ({n})" for s, n in top_sources)
        )
        return ans, [_cite(e) for e in cites]

    if _matches(Q, ["error.*(spike|climb|rate|why|increas)", "why.*error", "fail.*rate"]):
        cites = _retrieve(["ERROR", "CRITICAL", "OOMKilled", "5xx", "500", "502"], facts, 5)
        ans = (
            f"<b>Error analysis</b><br/><br/>"
            f"Current error rate: <b>{err_rate:.2f}%</b> "
            f"({sevs.get('ERROR', 0):,} ERROR + {sevs.get('CRITICAL', 0):,} CRITICAL out of {total:,}).<br/><br/>"
            f"Looking at recent error-class events, common patterns: "
            f"<i>Kubernetes</i> OOMKilled/ImagePullBackOff, "
            f"<i>Nginx</i> 502 from upstream timeouts, "
            f"<i>CloudWatch</i> Lambda timeouts.<br/><br/>"
            f"Next step: correlate against the last deploy timestamp."
        )
        return ans, [_cite(e) for e in cites]

    if _matches(Q, ["login", "credential", "auth.*fail", "stuff", "brute"]):
        cites = _retrieve(["login", "password", "credential", "auth"], facts, 5)
        ans = (
            "<b>Authentication signals</b><br/><br/>"
            "Scanning <code class=\"font-mono\">security-siem</code> and <code class=\"font-mono\">syslog</code> for auth events:<br/><br/>"
            f"• Failed logins recently: {sum(1 for e in cites if 'fail' in e.get('message','').lower())}<br/>"
            f"• Possible credential stuffing alerts: {sum(1 for e in cites if 'stuffing' in e.get('message','').lower())}<br/><br/>"
            "Recommended response: rate-limit /login (5/min/IP), force MFA re-enrollment on targeted accounts, and block clearly-malicious subnets at the WAF."
        )
        return ans, [_cite(e) for e in cites]

    if _matches(Q, ["security", "threat", "attack", "breach", "malware", "sqli", "xss", "waf"]):
        cites = _retrieve(["WAF", "malware", "credential", "scan", "SQLI", "XSS"], facts, 5)
        ans = (
            "<b>Security posture</b><br/><br/>"
            "Cross-source signals — WAF, SIEM, CloudTrail:<br/><br/>"
            "• WAF blocking SQL-injection / XSS attempts on public endpoints<br/>"
            "• SIEM flagging burst patterns (credential stuffing)<br/>"
            "• IAM policy changes worth reviewing<br/><br/>"
            "None individually critical; the combination is worth a 15-minute review."
        )
        return ans, [_cite(e) for e in cites]

    if _matches(Q, ["k8s", "kubernetes", "pod", "container", "oom", "crash"]):
        cites = _retrieve(["k8s", "OOMKilled", "ImagePullBackOff", "BackOff", "Pod"], facts, 5)
        ans = (
            "<b>Kubernetes signals</b><br/><br/>"
            "Recent events from <code class=\"font-mono\">k8s-events</code>:<br/><br/>"
            "• <b>OOMKilled</b> — container exceeded memory limit, restarted automatically (exit 137)<br/>"
            "• <b>ImagePullBackOff</b> — usually a registry hiccup; check registry status page<br/>"
            "• Rolling updates appear to be completing successfully<br/><br/>"
            "If OOMKilled is happening repeatedly: raise <code class=\"font-mono\">resources.limits.memory</code> or capture a heap dump before next restart."
        )
        return ans, [_cite(e) for e in cites]

    if _matches(Q, ["slow", "latency", "p95", "p99", "performance", "degrad", "response.*time"]):
        cites = _retrieve(["timeout", "slow", "504", "duration", "Lambda"], facts, 5)
        ans = (
            "<b>Performance signals</b><br/><br/>"
            "Indicators across the stack:<br/><br/>"
            "• CloudWatch shows Lambda timeouts (30s) — usually downstream DB pressure<br/>"
            "• Nginx 504s from upstream timeouts<br/>"
            "• Watch for slow Postgres queries — typical fix is a missing index on the WHERE column<br/>"
        )
        return ans, [_cite(e) for e in cites]

    if _matches(Q, ["noisy", "top", "active", "loud", "chatty", "most.*source"]):
        cites = facts.get("recent", {}).get("events", [])[:3]
        if top_sources:
            lines = "<br/>".join(f"{i+1}. <b>{s}</b> ({n:,} events)" for i, (s, n) in enumerate(top_sources))
        else:
            lines = "no data yet"
        ans = f"<b>Top sources by volume</b><br/><br/>{lines}<br/><br/>If a source is unexpectedly noisy, sample its DEBUG/INFO at the connector level to cut cost."
        return ans, [_cite(e) for e in cites]

    if _matches(Q, ["nifi", "dataflow", "pipeline", "kafka", "stream"]):
        ans = (
            "<b>Pipeline architecture</b><br/><br/>"
            "Data flows: <i>connectors</i> → <code class=\"font-mono\">raw.*</code> topics → "
            "<i>NiFi</i> (validate / enrich / mask / route) → <code class=\"font-mono\">events.*</code> topics → "
            "<i>sink service</i> → Elasticsearch + Redis + TimescaleDB.<br/><br/>"
            "In this POC the sink service replaces NiFi for simplicity. NiFi runs alongside (port 8443) so you can build the production flow visually — see the NiFi UI tile on the dashboard."
        )
        return ans, []

    if _matches(Q, ["explain", "what.*mean", "what.*is", "how.*work", "help"]):
        ans = (
            "I can help with: error analysis, security events, performance trends, Kubernetes issues, "
            "specific source signals (AWS, Nginx, Postgres), connector activity, and the production architecture.<br/><br/>"
            "Try a specific question, or click one of the suggested prompts."
        )
        return ans, []

    # Default
    ans = (
        "I can help with error analysis, security signals, Kubernetes issues, performance trends, "
        "and the meaning of specific events.<br/><br/>"
        "Try: <i>\"why is the error rate climbing\"</i>, <i>\"show me failed logins\"</i>, "
        "<i>\"summarize the last few minutes\"</i>."
    )
    return ans, []


def _matches(q: str, patterns: list[str]) -> bool:
    return any(re.search(p, q) for p in patterns)


def _cite(e: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": e.get("id"),
        "time": e.get("time"),
        "severity": e.get("severity"),
        "source": e.get("source"),
        "message": e.get("message"),
    }
