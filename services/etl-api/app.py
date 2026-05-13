"""
NimbusPulse — ETL API
=====================
Accepts unstructured file uploads, extracts text + images, and stores results in:
  - MinIO (S3-compatible) — raw originals + extracted images
  - Elasticsearch — searchable text + structured metadata
  - Kafka topic `etl.events` — pipeline events (one per stage) for NiFi to consume

Supported file types (auto-detected from MIME + extension):
  - PDF    → text via pdfminer.six + images via PyMuPDF
  - Images → OCR via pytesseract + perceptual hash for dedup
  - CSV    → rows parsed, header detected, sample + stats
  - JSON   → schema flattened, top-level keys inventoried
  - HTML   → tags stripped via BeautifulSoup, links + titles extracted
  - Plain text → as-is, line/word counts

The "Transform" stage normalizes everything into the common document schema:
    { docId, sourceFile, mediaType, text, language, pageCount,
      images: [{ key, page, width, height, ocrText }], metadata, extractedAt }
"""
import hashlib
import io
import json
import logging
import mimetypes
import os
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from minio import Minio
from minio.error import S3Error
from pydantic import BaseModel

# Optional heavy deps — gracefully degrade if missing
try:
    from pdfminer.high_level import extract_text as pdf_extract_text
    HAS_PDF = True
except ImportError:
    HAS_PDF = False

try:
    import fitz  # PyMuPDF
    HAS_PYMUPDF = True
except ImportError:
    HAS_PYMUPDF = False

try:
    from PIL import Image
    import pytesseract
    HAS_OCR = True
except ImportError:
    HAS_OCR = False

# ----- Config -----
ES_URL = os.environ.get("ES_URL", "http://elasticsearch:9200")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS_KEY", "nimbus")
MINIO_SECRET = os.environ.get("MINIO_SECRET_KEY", "nimbus-secret")
BUCKET_RAW = os.environ.get("BUCKET_RAW", "np-cold-archive")
BUCKET_IMAGES = os.environ.get("BUCKET_IMAGES", "np-cold-events")
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092").split(",")
ETL_TOPIC = os.environ.get("ETL_TOPIC", "etl.events")
ES_INDEX = "np-documents"

# ----- Logging -----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("etl-api")

# ----- App -----
app = FastAPI(
    title="NimbusPulse ETL API",
    description="Upload unstructured files; extract text & images; store in MinIO + Elasticsearch.",
    version="1.0.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# ----- Clients (lazy init) -----
_minio: Minio | None = None
_kafka: KafkaProducer | None = None


def minio_client() -> Minio:
    global _minio
    if _minio is None:
        _minio = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
        # Ensure buckets exist
        for b in (BUCKET_RAW, BUCKET_IMAGES):
            try:
                if not _minio.bucket_exists(b):
                    _minio.make_bucket(b)
                    log.info("created bucket %s", b)
            except S3Error as e:
                log.warning("bucket check failed for %s: %s", b, e)
    return _minio


def kafka_producer() -> KafkaProducer | None:
    global _kafka
    if _kafka is None:
        try:
            _kafka = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=20,
            )
            log.info("kafka producer ready: %s", KAFKA_BROKERS)
        except NoBrokersAvailable:
            log.warning("kafka not available; etl.events will be skipped")
            return None
    return _kafka


# ============================================================================
# ETL EVENT EMISSION — published to Kafka for NiFi / sink to consume
# ============================================================================

def emit_event(stage: str, doc_id: str, source_file: str, **extra: Any) -> None:
    """Emit one ETL pipeline event to Kafka and to local logs.

    Stages: received | extract.start | extract.done | transform.done
            | load.minio | load.elasticsearch | error | complete
    """
    severity_map = {
        "received":          "INFO",
        "extract.start":     "INFO",
        "extract.done":      "INFO",
        "transform.done":    "INFO",
        "load.minio":        "INFO",
        "load.elasticsearch": "INFO",
        "complete":          "INFO",
        "error":             "ERROR",
    }
    severity = severity_map.get(stage, "INFO")
    event = {
        "id": str(uuid.uuid4()),
        "time": datetime.now(timezone.utc).isoformat(),
        "severity": severity,
        "source": "etl-api",
        "sourceName": "ETL API",
        "message": f"ETL stage '{stage}' for {source_file}",
        "payload": {
            "stage": stage,
            "docId": doc_id,
            "sourceFile": source_file,
            **extra,
        },
    }
    log.info("[etl] stage=%s doc=%s file=%s extras=%s", stage, doc_id, source_file, extra)
    producer = kafka_producer()
    if producer is not None:
        try:
            producer.send(ETL_TOPIC, value=event, key=doc_id.encode("utf-8"))
        except Exception as e:
            log.warning("failed to emit kafka event: %s", e)


# ============================================================================
# EXTRACTORS — one function per file type. All return a Document dict.
# ============================================================================

def _doc_skel(doc_id: str, source_file: str, media_type: str) -> dict[str, Any]:
    return {
        "docId": doc_id,
        "sourceFile": source_file,
        "mediaType": media_type,
        "text": "",
        "wordCount": 0,
        "pageCount": 0,
        "images": [],
        "metadata": {},
        "extractedAt": datetime.now(timezone.utc).isoformat(),
        "extractor": "unknown",
    }


def extract_pdf(content: bytes, doc_id: str, source_file: str) -> dict[str, Any]:
    """Extract text + embedded images from a PDF."""
    doc = _doc_skel(doc_id, source_file, "application/pdf")
    doc["extractor"] = "pdfminer+pymupdf" if HAS_PDF and HAS_PYMUPDF else "fallback"

    # Text via pdfminer.six (reliable, doesn't need fonts installed)
    if HAS_PDF:
        try:
            doc["text"] = pdf_extract_text(io.BytesIO(content)) or ""
        except Exception as e:
            log.warning("pdfminer failed: %s", e)

    # Page count + images via PyMuPDF
    if HAS_PYMUPDF:
        try:
            with fitz.open(stream=content, filetype="pdf") as pdf:
                doc["pageCount"] = pdf.page_count
                doc["metadata"]["pdfTitle"] = pdf.metadata.get("title", "") if pdf.metadata else ""
                doc["metadata"]["pdfAuthor"] = pdf.metadata.get("author", "") if pdf.metadata else ""
                for page_num in range(pdf.page_count):
                    page = pdf[page_num]
                    for img_idx, img in enumerate(page.get_images(full=True)):
                        try:
                            xref = img[0]
                            pix = fitz.Pixmap(pdf, xref)
                            if pix.n - pix.alpha >= 4:  # CMYK → RGB
                                pix = fitz.Pixmap(fitz.csRGB, pix)
                            img_bytes = pix.tobytes("png")
                            img_key = f"images/{doc_id}/page_{page_num+1}_img_{img_idx+1}.png"
                            ocr_text = _ocr_bytes(img_bytes) if HAS_OCR else ""
                            doc["images"].append({
                                "key": img_key,
                                "page": page_num + 1,
                                "width": pix.width,
                                "height": pix.height,
                                "size": len(img_bytes),
                                "ocrText": ocr_text[:500] if ocr_text else "",
                                "_bytes": img_bytes,  # consumed by load stage
                            })
                            pix = None
                        except Exception as e:
                            log.warning("image extract failed on page %d: %s", page_num, e)
        except Exception as e:
            log.warning("PyMuPDF processing failed: %s", e)

    doc["wordCount"] = len(doc["text"].split())
    return doc


def extract_image(content: bytes, doc_id: str, source_file: str, mime: str) -> dict[str, Any]:
    """Run OCR on an uploaded image."""
    doc = _doc_skel(doc_id, source_file, mime)
    doc["extractor"] = "pytesseract" if HAS_OCR else "no-ocr-available"

    if HAS_OCR:
        try:
            img = Image.open(io.BytesIO(content))
            doc["metadata"]["width"] = img.width
            doc["metadata"]["height"] = img.height
            doc["metadata"]["format"] = img.format
            doc["metadata"]["mode"] = img.mode
            ocr_text = pytesseract.image_to_string(img)
            doc["text"] = ocr_text.strip()
            doc["wordCount"] = len(ocr_text.split())
            # Treat the image itself as an embedded image so it's saved + indexed
            doc["images"].append({
                "key": f"images/{doc_id}/original.{img.format.lower() if img.format else 'bin'}",
                "page": 1,
                "width": img.width,
                "height": img.height,
                "size": len(content),
                "ocrText": ocr_text[:500],
                "_bytes": content,
            })
        except Exception as e:
            log.warning("image processing failed: %s", e)
    return doc


def extract_csv(content: bytes, doc_id: str, source_file: str) -> dict[str, Any]:
    """Parse CSV: detect header, count rows, capture sample, basic stats."""
    import csv as _csv

    doc = _doc_skel(doc_id, source_file, "text/csv")
    doc["extractor"] = "csv"
    text = content.decode("utf-8", errors="replace")
    reader = _csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return doc
    header = rows[0]
    data_rows = rows[1:]
    doc["metadata"]["columns"] = header
    doc["metadata"]["columnCount"] = len(header)
    doc["metadata"]["rowCount"] = len(data_rows)
    doc["metadata"]["sample"] = data_rows[:5]
    # Build searchable text from header + sample rows
    sample_text = " | ".join(header) + "\n"
    for r in data_rows[:50]:
        sample_text += " | ".join(r) + "\n"
    doc["text"] = sample_text
    doc["wordCount"] = len(sample_text.split())
    return doc


def extract_json(content: bytes, doc_id: str, source_file: str) -> dict[str, Any]:
    """Inspect JSON: flatten schema, inventory top-level keys."""
    doc = _doc_skel(doc_id, source_file, "application/json")
    doc["extractor"] = "json"
    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        doc["metadata"]["parseError"] = str(e)
        doc["text"] = content.decode("utf-8", errors="replace")[:5000]
        return doc

    def shape(obj: Any, depth: int = 0) -> Any:
        if depth > 3:
            return type(obj).__name__
        if isinstance(obj, dict):
            return {k: shape(v, depth + 1) for k, v in list(obj.items())[:20]}
        if isinstance(obj, list):
            return [shape(obj[0], depth + 1)] if obj else []
        return type(obj).__name__

    doc["metadata"]["schema"] = shape(data)
    doc["metadata"]["topKeys"] = list(data.keys())[:30] if isinstance(data, dict) else None
    doc["metadata"]["isArray"] = isinstance(data, list)
    doc["metadata"]["arrayLength"] = len(data) if isinstance(data, list) else None
    # Render text representation for full-text search
    rendered = json.dumps(data, indent=2)
    doc["text"] = rendered[:50000]  # cap
    doc["wordCount"] = len(rendered.split())
    return doc


def extract_html(content: bytes, doc_id: str, source_file: str) -> dict[str, Any]:
    """Strip tags, extract title + links."""
    doc = _doc_skel(doc_id, source_file, "text/html")
    doc["extractor"] = "beautifulsoup4"
    html = content.decode("utf-8", errors="replace")
    soup = BeautifulSoup(html, "html.parser")
    doc["metadata"]["title"] = soup.title.string.strip() if soup.title and soup.title.string else ""
    doc["metadata"]["links"] = [a.get("href") for a in soup.find_all("a") if a.get("href")][:50]
    doc["metadata"]["headings"] = [h.get_text(strip=True) for h in soup.find_all(["h1", "h2", "h3"])][:30]
    for s in soup(["script", "style", "noscript"]):
        s.decompose()
    text = soup.get_text(separator="\n", strip=True)
    doc["text"] = text
    doc["wordCount"] = len(text.split())
    return doc


def extract_text(content: bytes, doc_id: str, source_file: str, mime: str) -> dict[str, Any]:
    """Plain text — log files, markdown, source code, etc."""
    doc = _doc_skel(doc_id, source_file, mime)
    doc["extractor"] = "plain-text"
    text = content.decode("utf-8", errors="replace")
    doc["text"] = text
    doc["wordCount"] = len(text.split())
    doc["metadata"]["lineCount"] = text.count("\n") + 1
    doc["metadata"]["charCount"] = len(text)
    return doc


def _ocr_bytes(img_bytes: bytes) -> str:
    """Run OCR on raw image bytes, return text or empty string."""
    if not HAS_OCR:
        return ""
    try:
        img = Image.open(io.BytesIO(img_bytes))
        return pytesseract.image_to_string(img).strip()
    except Exception as e:
        log.debug("OCR failed: %s", e)
        return ""


# ============================================================================
# TRANSFORM — normalize, enrich, redact
# ============================================================================

PII_PATTERNS = [
    # Email
    (re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"), "[EMAIL]"),
    # Credit card (very loose)
    (re.compile(r"\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b"), "[CARD]"),
    # SSN-style 9-digit
    (re.compile(r"\b\d{3}-\d{2}-\d{4}\b"), "[SSN]"),
]


def transform(doc: dict[str, Any]) -> dict[str, Any]:
    """Normalize + enrich + mask PII."""
    # Redact PII in extracted text
    text = doc.get("text", "")
    pii_hits = {}
    for pattern, replacement in PII_PATTERNS:
        matches = pattern.findall(text)
        if matches:
            pii_hits[replacement] = len(matches)
            text = pattern.sub(replacement, text)
    doc["text"] = text
    doc["metadata"]["piiRedacted"] = pii_hits

    # Light language detection (English heuristic)
    if text:
        common_en = {"the", "and", "of", "to", "in", "is", "for", "that", "with"}
        words = text.lower().split()[:500]
        en_score = sum(1 for w in words if w in common_en) / max(len(words), 1)
        doc["metadata"]["language"] = "en" if en_score > 0.02 else "unknown"

    # Hash for dedup
    doc["metadata"]["sha256"] = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
    return doc


# ============================================================================
# LOAD — write to MinIO + Elasticsearch
# ============================================================================

def load_to_minio(doc_id: str, source_file: str, content: bytes, doc: dict[str, Any]) -> None:
    """Store the original file + every extracted image in MinIO."""
    mc = minio_client()
    # 1. Original raw file
    raw_key = f"raw/{doc_id}/{source_file}"
    mc.put_object(BUCKET_RAW, raw_key, io.BytesIO(content), length=len(content),
                  content_type=doc["mediaType"])
    doc["metadata"]["rawObject"] = f"s3://{BUCKET_RAW}/{raw_key}"
    log.info("[load] uploaded raw %s (%d bytes)", raw_key, len(content))

    # 2. Every extracted image
    for img in doc.get("images", []):
        img_bytes = img.pop("_bytes", None)
        if img_bytes is None:
            continue
        mc.put_object(BUCKET_IMAGES, img["key"], io.BytesIO(img_bytes),
                      length=len(img_bytes), content_type="image/png")
        img["uri"] = f"s3://{BUCKET_IMAGES}/{img['key']}"
        log.info("[load] uploaded image %s", img["key"])


def load_to_elasticsearch(doc: dict[str, Any]) -> None:
    """Index the normalized document in Elasticsearch for full-text search."""
    _ensure_es_index()
    # Strip image bytes (already stripped) but keep metadata
    body = {k: v for k, v in doc.items() if k != "_bytes"}
    r = httpx.post(f"{ES_URL}/{ES_INDEX}/_doc/{doc['docId']}", json=body, timeout=10.0)
    if r.status_code >= 300:
        raise RuntimeError(f"ES index failed: {r.status_code} {r.text}")
    log.info("[load] indexed %s in Elasticsearch", doc["docId"])


_es_index_ready = False


def _ensure_es_index() -> None:
    global _es_index_ready
    if _es_index_ready:
        return
    mapping = {
        "mappings": {
            "properties": {
                "docId":       {"type": "keyword"},
                "sourceFile":  {"type": "keyword"},
                "mediaType":   {"type": "keyword"},
                "extractor":   {"type": "keyword"},
                "extractedAt": {"type": "date"},
                "text":        {"type": "text"},
                "wordCount":   {"type": "integer"},
                "pageCount":   {"type": "integer"},
                "images":      {"type": "object"},
                "metadata":    {"type": "object", "enabled": True},
            }
        }
    }
    try:
        r = httpx.put(f"{ES_URL}/{ES_INDEX}", json=mapping, timeout=10.0)
        # 200 = created; 400 = already exists; both OK
        log.info("ES index %s ensure: %d", ES_INDEX, r.status_code)
    except Exception as e:
        log.warning("ES index ensure failed (will retry on demand): %s", e)
    _es_index_ready = True


# ============================================================================
# PIPELINE ORCHESTRATION — Extract → Transform → Load
# ============================================================================

def _pick_extractor(filename: str, mime: str) -> str:
    name = filename.lower()
    if mime == "application/pdf" or name.endswith(".pdf"):
        return "pdf"
    if mime.startswith("image/") or any(name.endswith(x) for x in (".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff", ".webp")):
        return "image"
    if mime == "text/csv" or name.endswith(".csv"):
        return "csv"
    if mime == "application/json" or name.endswith(".json"):
        return "json"
    if mime == "text/html" or name.endswith((".html", ".htm")):
        return "html"
    if mime.startswith("text/") or any(name.endswith(x) for x in (".txt", ".md", ".log", ".py", ".js", ".go", ".yaml", ".yml")):
        return "text"
    return "text"  # default fallback


def run_pipeline(filename: str, content: bytes, mime: str) -> dict[str, Any]:
    """End-to-end ETL: detect type → Extract → Transform → Load."""
    doc_id = str(uuid.uuid4())
    t_start = time.time()
    emit_event("received", doc_id, filename, mediaType=mime, size=len(content))

    extractor = _pick_extractor(filename, mime)
    emit_event("extract.start", doc_id, filename, extractor=extractor)

    try:
        if extractor == "pdf":
            doc = extract_pdf(content, doc_id, filename)
        elif extractor == "image":
            doc = extract_image(content, doc_id, filename, mime)
        elif extractor == "csv":
            doc = extract_csv(content, doc_id, filename)
        elif extractor == "json":
            doc = extract_json(content, doc_id, filename)
        elif extractor == "html":
            doc = extract_html(content, doc_id, filename)
        else:
            doc = extract_text(content, doc_id, filename, mime)
    except Exception as e:
        emit_event("error", doc_id, filename, stage="extract", error=str(e))
        raise HTTPException(500, f"extraction failed: {e}")

    emit_event("extract.done", doc_id, filename,
               wordCount=doc["wordCount"], imageCount=len(doc["images"]),
               pageCount=doc["pageCount"])

    doc = transform(doc)
    emit_event("transform.done", doc_id, filename,
               piiRedacted=doc["metadata"].get("piiRedacted", {}),
               language=doc["metadata"].get("language", "unknown"))

    try:
        load_to_minio(doc_id, filename, content, doc)
        emit_event("load.minio", doc_id, filename,
                   bucketRaw=BUCKET_RAW, bucketImages=BUCKET_IMAGES,
                   imagesUploaded=len(doc["images"]))
    except Exception as e:
        emit_event("error", doc_id, filename, stage="load.minio", error=str(e))
        log.error("minio load failed: %s", e)
        # Continue to ES — partial success is fine

    try:
        load_to_elasticsearch(doc)
        emit_event("load.elasticsearch", doc_id, filename, index=ES_INDEX)
    except Exception as e:
        emit_event("error", doc_id, filename, stage="load.elasticsearch", error=str(e))
        log.error("es load failed: %s", e)
        raise HTTPException(500, f"elasticsearch indexing failed: {e}")

    duration_ms = int((time.time() - t_start) * 1000)
    emit_event("complete", doc_id, filename, durationMs=duration_ms)
    log.info("[etl] complete doc=%s file=%s duration=%dms", doc_id, filename, duration_ms)
    doc["durationMs"] = duration_ms
    return doc


# ============================================================================
# HTTP ENDPOINTS
# ============================================================================

class JobSummary(BaseModel):
    docId: str
    sourceFile: str
    mediaType: str
    extractor: str
    wordCount: int
    pageCount: int
    imageCount: int
    durationMs: int
    extractedAt: str


@app.get("/healthz")
def healthz():
    """Health probe — reports component readiness."""
    return {
        "status": "ok",
        "components": {
            "pdf": HAS_PDF and HAS_PYMUPDF,
            "ocr": HAS_OCR,
            "minio_endpoint": MINIO_ENDPOINT,
            "es_url": ES_URL,
            "kafka_brokers": KAFKA_BROKERS,
        },
    }


@app.get("/api/v1/info")
def info():
    """Pipeline configuration + supported types."""
    return {
        "service": "etl-api",
        "supportedTypes": ["pdf", "image", "csv", "json", "html", "text"],
        "extractors": {
            "pdf": "pdfminer.six + PyMuPDF" if HAS_PDF and HAS_PYMUPDF else "unavailable",
            "image": "pytesseract OCR" if HAS_OCR else "unavailable",
            "csv": "stdlib csv",
            "json": "stdlib json + schema inference",
            "html": "BeautifulSoup4",
            "text": "plain UTF-8",
        },
        "transforms": ["pii-redaction", "language-detection", "sha256-hash"],
        "load": {
            "minio.raw": BUCKET_RAW,
            "minio.images": BUCKET_IMAGES,
            "elasticsearch.index": ES_INDEX,
            "kafka.topic": ETL_TOPIC,
        },
    }


@app.post("/api/v1/etl/upload", response_model=JobSummary)
async def upload(file: UploadFile = File(...)):
    """Single-file upload endpoint. Runs the full ETL pipeline synchronously."""
    if not file.filename:
        raise HTTPException(400, "no filename")
    content = await file.read()
    if len(content) == 0:
        raise HTTPException(400, "empty file")
    if len(content) > 50 * 1024 * 1024:
        raise HTTPException(413, "file exceeds 50 MB limit")
    mime = file.content_type or mimetypes.guess_type(file.filename)[0] or "application/octet-stream"
    doc = run_pipeline(file.filename, content, mime)
    return JobSummary(
        docId=doc["docId"],
        sourceFile=doc["sourceFile"],
        mediaType=doc["mediaType"],
        extractor=doc["extractor"],
        wordCount=doc["wordCount"],
        pageCount=doc["pageCount"],
        imageCount=len(doc["images"]),
        durationMs=doc.get("durationMs", 0),
        extractedAt=doc["extractedAt"],
    )


@app.get("/api/v1/etl/documents")
def list_documents(size: int = 20, q: str | None = None):
    """List recently processed documents."""
    _ensure_es_index()
    if q:
        body = {
            "size": size,
            "sort": [{"extractedAt": "desc"}],
            "query": {"multi_match": {"query": q, "fields": ["text", "sourceFile", "metadata.title"]}},
        }
    else:
        body = {
            "size": size,
            "sort": [{"extractedAt": "desc"}],
            "query": {"match_all": {}},
        }
    try:
        r = httpx.post(f"{ES_URL}/{ES_INDEX}/_search", json=body, timeout=10.0)
        hits = []
        if r.status_code < 300:
            data = r.json()
            for h in data.get("hits", {}).get("hits", []):
                src = h.get("_source", {})
                hits.append({
                    "docId": src.get("docId"),
                    "sourceFile": src.get("sourceFile"),
                    "mediaType": src.get("mediaType"),
                    "extractor": src.get("extractor"),
                    "wordCount": src.get("wordCount"),
                    "pageCount": src.get("pageCount"),
                    "imageCount": len(src.get("images", [])),
                    "extractedAt": src.get("extractedAt"),
                    "preview": (src.get("text", "") or "")[:280],
                    "metadata": src.get("metadata", {}),
                })
        return {"hits": hits, "count": len(hits)}
    except Exception as e:
        return JSONResponse({"error": str(e), "hits": []}, status_code=502)


@app.get("/api/v1/etl/documents/{doc_id}")
def get_document(doc_id: str):
    """Fetch a single processed document."""
    try:
        r = httpx.get(f"{ES_URL}/{ES_INDEX}/_doc/{doc_id}", timeout=10.0)
        if r.status_code == 404:
            raise HTTPException(404, "document not found")
        return r.json().get("_source", {})
    except httpx.HTTPError as e:
        raise HTTPException(502, f"elasticsearch unreachable: {e}")


@app.get("/api/v1/etl/documents/{doc_id}/raw")
def get_raw_file(doc_id: str):
    """Stream the original uploaded file back from MinIO.

    Looks up the doc to find the source filename, then fetches
    raw/{doc_id}/{filename} from the np-cold-archive bucket.
    """
    from fastapi.responses import StreamingResponse

    try:
        r = httpx.get(f"{ES_URL}/{ES_INDEX}/_doc/{doc_id}", timeout=10.0)
        if r.status_code == 404:
            raise HTTPException(404, "document not found")
        doc = r.json().get("_source", {})
    except httpx.HTTPError as e:
        raise HTTPException(502, f"elasticsearch unreachable: {e}")

    source_file = doc.get("sourceFile", "")
    media_type = doc.get("mediaType", "application/octet-stream")
    if not source_file:
        raise HTTPException(404, "source filename missing on document")

    key = f"raw/{doc_id}/{source_file}"
    try:
        mc = minio_client()
        response = mc.get_object(BUCKET_RAW, key)
        # Wrap MinIO response in a generator that closes properly
        def stream():
            try:
                for chunk in response.stream(64 * 1024):
                    yield chunk
            finally:
                response.close()
                response.release_conn()

        return StreamingResponse(
            stream(),
            media_type=media_type,
            headers={"Content-Disposition": f'inline; filename="{source_file}"'},
        )
    except S3Error as e:
        log.error("MinIO get_object failed for %s: %s", key, e)
        raise HTTPException(404, f"raw file not found in MinIO: {key}")


@app.get("/api/v1/etl/documents/{doc_id}/images/{image_index}")
def get_image(doc_id: str, image_index: int):
    """Stream an extracted image back from MinIO by its position in the images array."""
    from fastapi.responses import StreamingResponse

    try:
        r = httpx.get(f"{ES_URL}/{ES_INDEX}/_doc/{doc_id}", timeout=10.0)
        if r.status_code == 404:
            raise HTTPException(404, "document not found")
        doc = r.json().get("_source", {})
    except httpx.HTTPError as e:
        raise HTTPException(502, f"elasticsearch unreachable: {e}")

    images = doc.get("images", [])
    if image_index < 0 or image_index >= len(images):
        raise HTTPException(404, "image index out of range")
    img = images[image_index]
    key = img.get("key")
    if not key:
        raise HTTPException(404, "image key missing")

    try:
        mc = minio_client()
        response = mc.get_object(BUCKET_IMAGES, key)
        def stream():
            try:
                for chunk in response.stream(64 * 1024):
                    yield chunk
            finally:
                response.close()
                response.release_conn()
        # Most extracted images are PNG (PyMuPDF normalizes to PNG)
        ctype = "image/png" if key.endswith(".png") else "image/jpeg"
        return StreamingResponse(stream(), media_type=ctype)
    except S3Error as e:
        log.error("MinIO get_object failed for %s: %s", key, e)
        raise HTTPException(404, f"image not found in MinIO: {key}")


@app.get("/api/v1/etl/examples")
def examples():
    """Sample ETL flows the user can read about and try."""
    return {
        "examples": [
            {
                "name": "PDF → Text + Embedded Images",
                "input": "research-paper.pdf",
                "extract": "pdfminer.six pulls text; PyMuPDF extracts embedded images; OCR runs on each image",
                "transform": "PII redaction, language detection, SHA-256 dedup hash",
                "load": "raw PDF → MinIO (np-cold-archive); images → MinIO (np-cold-events); doc → Elasticsearch (np-documents)",
                "useCase": "Knowledge base: every research PDF becomes searchable along with its diagrams",
            },
            {
                "name": "Image → OCR",
                "input": "screenshot.png",
                "extract": "Tesseract OCR; capture dimensions + format",
                "transform": "Same PII / language pipeline as PDF",
                "load": "Image stored in MinIO; OCR'd text indexed in ES",
                "useCase": "Customer support: drop in screenshots, search by visible text",
            },
            {
                "name": "CSV → Structured Inventory",
                "input": "sales-2024.csv",
                "extract": "Parse rows, detect header, capture first 50 rows as sample text, count columns/rows",
                "transform": "PII redact, hash",
                "load": "Original CSV in MinIO; schema + sample in ES",
                "useCase": "Data catalog: discover CSVs by column names + sample values",
            },
            {
                "name": "JSON → Schema Inference",
                "input": "api-response.json",
                "extract": "Parse JSON; recursively flatten schema (3 levels deep); inventory top-level keys",
                "transform": "Standard pipeline",
                "load": "Schema and full JSON text indexed; original in MinIO",
                "useCase": "API discovery: find all uploaded responses with a given field name",
            },
            {
                "name": "HTML → Strip + Link Extract",
                "input": "saved-page.html",
                "extract": "BeautifulSoup strips tags; capture title, headings, outbound links",
                "transform": "Standard pipeline",
                "load": "Cleaned text + structure metadata in ES; original HTML in MinIO",
                "useCase": "Archive scraping: search by title or content across saved web pages",
            },
            {
                "name": "Text / Logs",
                "input": "app.log, README.md, source.py",
                "extract": "UTF-8 decode; line/word/char counts",
                "transform": "PII redact (catches emails in logs!), hash",
                "load": "Indexed for grep-like search via ES",
                "useCase": "Centralized log archive with PII removed",
            },
        ]
    }
