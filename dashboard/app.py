"""Multi-Agent System Activity Dashboard â€” thin UI, Cloud Run deployable.

Reads from Firestore collection `agent_activity` populated by main.py log_event().
Simple shared-password auth via DASHBOARD_PASSWORD env var.
Chat endpoints proxy to the private multi-agent-system Cloud Run service via IAM identity tokens.
"""
import os
import json
import logging
import sys
import time
import hmac
import hashlib
import re
import secrets
import base64
from datetime import datetime, timezone, timedelta
from functools import wraps
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

from flask import Flask, request, jsonify, render_template, redirect, url_for, session, Response, stream_with_context
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_wtf.csrf import CSRFProtect, generate_csrf
import requests as http_requests
from google import genai
from google.cloud import firestore
from google.cloud.firestore_v1.vector import Vector
from google.oauth2 import id_token as google_id_token
from google.auth.transport import requests as google_auth_requests

# ---------------------------------------------------------------------------
# Config â€” fail loudly on missing or leaked-default secrets (C4 / C5)
# ---------------------------------------------------------------------------
_SECRET_KEY = os.getenv("DASHBOARD_SECRET_KEY")
_ON_CLOUD_RUN = bool(os.environ.get("K_SERVICE"))
if not _SECRET_KEY:
    if _ON_CLOUD_RUN:
        raise RuntimeError("DASHBOARD_SECRET_KEY must be set in production")
    _SECRET_KEY = "local-dev-secret-key"
elif hashlib.sha256(_SECRET_KEY.encode()).hexdigest() == "ab925e644700133d86c23a0ccf59ed7885bcf79da571b35f8dac0269f4c433fc" and _ON_CLOUD_RUN:
    raise RuntimeError("DASHBOARD_SECRET_KEY must not be the old default value in production")

DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD")
if not DASHBOARD_PASSWORD:
    if _ON_CLOUD_RUN:
        raise RuntimeError("DASHBOARD_PASSWORD must be set in production")
    DASHBOARD_PASSWORD = "localdev"
elif hashlib.sha256(DASHBOARD_PASSWORD.encode()).hexdigest() == "1af84fed98eb4d454f88d33e2a9c563a8abc62f65c895908b1f18042cd3a5169" and _ON_CLOUD_RUN:
    raise RuntimeError("DASHBOARD_PASSWORD must not be the old default value in production")

# URL of the private multi-agent-system Cloud Run service â€” required in production.
Acme_MAS_URL = os.getenv("Acme_MAS_URL", "")
if not Acme_MAS_URL:
    if _ON_CLOUD_RUN:
        raise RuntimeError("Acme_MAS_URL must be set in production")
    Acme_MAS_URL = "http://localhost:8080"

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
if not PROJECT_ID:
    if _ON_CLOUD_RUN:
        raise RuntimeError("GOOGLE_CLOUD_PROJECT must be set in production")
    PROJECT_ID = "YOUR_GCP_PROJECT_ID"
CLOUD_RUN_REGION = os.getenv("CLOUD_RUN_REGION", "europe-west2")
MAS_SERVICE_NAME = os.getenv("MAS_SERVICE_NAME", "multi-agent-system")
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "http://localhost:8081")
GMAIL_WEB_CRED_JSON = os.getenv("GMAIL_WEB_CRED_JSON", "")
XERO_CLIENT_ID = os.getenv("XERO_CLIENT_ID", "")
XERO_CLIENT_SECRET = os.getenv("XERO_CLIENT_SECRET", "")

# IAM identity-token cache â€” tokens are valid 60 min; refresh at 55 min.
_token_cache: dict = {"token": None, "expires": 0.0}


def _get_mas_token() -> str | None:
    """Return a cached IAM identity token for calling multi-agent-system. None outside Cloud Run."""
    if not _ON_CLOUD_RUN:
        return None
    now = time.monotonic()
    if _token_cache["token"] and now < _token_cache["expires"]:
        return _token_cache["token"]
    token = google_id_token.fetch_id_token(google_auth_requests.Request(), Acme_MAS_URL)
    _token_cache.update({"token": token, "expires": now + 3300})
    return token


app = Flask(__name__)
app.secret_key = _SECRET_KEY
app.config.update(
    SESSION_COOKIE_SECURE=bool(os.environ.get("K_SERVICE")),
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=timedelta(hours=8),
    MAX_CONTENT_LENGTH=30 * 1024 * 1024,  # 30 MB for up to 4Ã—5 MB images + overhead
)

# CSRF protection for all POST routes
csrf = CSRFProtect(app)

# Rate limiter â€” applied per route where needed (C4)
limiter = Limiter(app=app, key_func=get_remote_address, default_limits=[], storage_uri="memory://")


@app.after_request
def set_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    if os.environ.get("K_SERVICE"):
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    csp = (
        "default-src 'none'; "
        "script-src 'self' cdn.tailwindcss.com cdn.jsdelivr.net cdnjs.cloudflare.com 'unsafe-inline' 'unsafe-eval'; "
        "style-src 'self' fonts.googleapis.com cdn.jsdelivr.net cdnjs.cloudflare.com 'unsafe-inline'; "
        "font-src fonts.gstatic.com; "
        "img-src 'self' data:; "
        "connect-src 'self' cdnjs.cloudflare.com; "
        "manifest-src 'self'; "
        "frame-src 'none'; "
        "base-uri 'self'; "
        "form-action 'self'"
    )
    response.headers["Content-Security-Policy"] = csp
    return response

db = firestore.Client()
COLLECTION = "agent_activity"
KNOWLEDGE_COLLECTION = "Acme_knowledge"
EMBEDDING_MODEL = "text-embedding-004"
MAX_KNOWLEDGE_LENGTH = 20000
MIN_KNOWLEDGE_LENGTH = 10


def _get_monday_token() -> str:
    """Read Monday API token from Secret Manager, falling back to env var."""
    try:
        from google.cloud import secretmanager_v1
        project = os.environ.get("GOOGLE_CLOUD_PROJECT", "YOUR_GCP_PROJECT_ID")
        client = secretmanager_v1.SecretManagerServiceClient()
        name = f"projects/{project}/secrets/monday-api-key/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("utf-8").strip()
    except Exception:
        return os.environ.get("MONDAY_API_KEY", "")


def _save_monday_token(token: str) -> None:
    """Add a new version of the Monday API token in Secret Manager."""
    from google.cloud import secretmanager_v1
    project = os.environ.get("GOOGLE_CLOUD_PROJECT", "YOUR_GCP_PROJECT_ID")
    client = secretmanager_v1.SecretManagerServiceClient()
    parent = f"projects/{project}/secrets/monday-api-key"
    client.add_secret_version(
        request={"parent": parent, "payload": {"data": token.encode("utf-8")}}
    )

_genai_client = genai.Client(
    vertexai=True,
    project=os.environ.get("GOOGLE_CLOUD_PROJECT", "YOUR_GCP_PROJECT_ID"),
    location="us-central1",
)


def _generate_embedding(text: str) -> list:
    """Generate embedding vector via Vertex AI. Blocking â€” runs in sync Flask handler."""
    result = _genai_client.models.embed_content(model=EMBEDDING_MODEL, contents=text)
    return result.embeddings[0].values


def _chunk_text(text: str) -> list[str]:
    """Split text into chunks using --- separator. Returns at least one chunk."""
    if "---" in text:
        chunks = [chunk.strip() for chunk in text.split("---")]
        chunks = [chunk for chunk in chunks if chunk]
        return chunks if chunks else [text]  # Fallback: never return empty list
    return [text]


def _write_document(
    source: str,
    text: str,
    *,
    type_: str,
    display_name: str | None = None,
    edited: bool = False,
    created: bool = False,
    updated_at: str | None = None,
) -> int:
    """Write document chunks to Firestore knowledge collection.

    1. Split text into chunks
    2. Pre-generate all embeddings (fail fast before any Firestore mutation)
    3. Delete all existing chunks for source
    4. Write new chunks
    5. Return chunk count

    Pre-generating embeddings ensures that if any embedding fails, no Firestore mutations occur.
    """
    # 1. Split into chunks
    chunks = _chunk_text(text)

    # 2. Pre-generate all embeddings â€” fail fast before any Firestore mutation
    timestamp = updated_at or datetime.now(timezone.utc).isoformat()
    chunk_docs = []
    for i, chunk in enumerate(chunks):
        embedding_vector = _generate_embedding(chunk)
        doc_id = source if len(chunks) == 1 else f"{source}_{i}"
        doc_data = {
            "text": chunk,
            "source": source,
            "type": type_,
            "chunk_index": i,
            "embedding": Vector(embedding_vector),
            "updated_at": timestamp,
        }
        if display_name and i == 0:  # display_name only on first chunk
            doc_data["display_name"] = display_name
        if edited:
            doc_data["client_edited"] = True
        if created:
            doc_data["client_created"] = True
        chunk_docs.append((doc_id, doc_data))

    # 3. Delete existing chunks (only after all embeddings succeed)
    existing = db.collection(KNOWLEDGE_COLLECTION).where("source", "==", source).stream()
    for doc in existing:
        doc.reference.delete()

    # 4. Write new chunks
    for doc_id, doc_data in chunk_docs:
        db.collection(KNOWLEDGE_COLLECTION).document(doc_id).set(doc_data)

    return len(chunks)

logging.basicConfig(format="%(message)s", level=logging.INFO, stream=sys.stderr)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    if session.get("authenticated"):
        return redirect(url_for("dashboard"))
    return redirect(url_for("login_page"))


@app.route("/login", methods=["GET"])
def login_page():
    return render_template("login.html")


@app.route("/login", methods=["POST"])
@limiter.limit("5/minute")
def login_submit():
    password = request.form.get("password", "")
    if hmac.compare_digest(password.encode(), DASHBOARD_PASSWORD.encode()):
        session.clear()
        session["authenticated"] = True
        session.permanent = True
        return redirect(url_for("dashboard"))
    return render_template("login.html", error="Incorrect password")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login_page"))


@app.route("/dashboard")
@login_required
def dashboard():
    return render_template("index.html")


# ---------------------------------------------------------------------------
# API endpoints (JSON)
# ---------------------------------------------------------------------------
@app.route("/api/feed")
@login_required
def api_feed():
    """Latest N activity docs for the Live Feed panel (N from dashboard config)."""
    try:
        cfg = _get_dashboard_config()
        limit = int(cfg.get("activity_feed_limit", 50))
        raw_limit = min(limit * 5, 1000)
        docs = (
            db.collection(COLLECTION)
            .order_by("timestamp", direction=firestore.Query.DESCENDING)
            .limit(raw_limit)
            .stream()
        )
        items = []
        for doc in docs:
            d = doc.to_dict()
            if d.get("event") in _HIDDEN_EVENTS:
                continue
            label = _human_event_label(d)
            if label is None:
                continue
            d["id"] = doc.id
            d["human_label"] = label
            items.append(d)
            if len(items) >= limit:
                break
        return jsonify(items)
    except Exception as exc:
        logger.error(json.dumps({"event": "api_feed_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Service temporarily unavailable"}), 503


@app.route("/api/summary")
@login_required
def api_summary():
    """Today's counts grouped by event type for the Summary panel."""
    try:
        today_start = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).isoformat()

        docs = (
            db.collection(COLLECTION)
            .where("timestamp", ">=", today_start)
            .order_by("timestamp")
            .limit(5000)
            .stream()
        )

        counts = {
            "bookings_confirmed": 0,
            "invoices_chased": 0,
            "emails_sent": 0,
            "escalations": 0,
            "tool_calls": 0,
            "scheduled_tasks": 0,
        }
        for doc in docs:
            d = doc.to_dict()
            ev = d.get("event", "")
            tool = d.get("tool", "")

            if ev in ("booking_confirmed", "booking_created") or (ev == "tool_call" and "booking" in tool.lower()):
                counts["bookings_confirmed"] += 1
            if ev == "invoice_chased" or (ev == "tool_call" and tool == "xero_send_invoice"):
                counts["invoices_chased"] += 1
            if ev == "tool_call" and tool in ("send_email", "draft_reply"):
                counts["emails_sent"] += 1
            if ev == "escalation" or d.get("needs_review"):
                counts["escalations"] += 1
            if ev == "tool_call":
                counts["tool_calls"] += 1
            if ev == "scheduled_task":
                counts["scheduled_tasks"] += 1

        return jsonify(counts)
    except Exception as exc:
        logger.error(json.dumps({"event": "api_summary_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Service temporarily unavailable"}), 503


@app.route("/api/escalations")
@login_required
def api_escalations():
    """Items tagged NEEDS_REVIEW or containing escalation references."""
    try:
        cfg = _get_dashboard_config()
        docs = (
            db.collection(COLLECTION)
            .order_by("timestamp", direction=firestore.Query.DESCENDING)
            .limit(int(cfg.get("escalation_scan_limit", 200)))
            .stream()
        )

        escalations = []
        for doc in docs:
            d = doc.to_dict()
            blob = json.dumps(d).lower()
            if "needs_review" in blob or "escalat" in blob:
                d["id"] = doc.id
                escalations.append(d)
        return jsonify(escalations)
    except Exception as exc:
        logger.error(json.dumps({"event": "api_escalations_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Service temporarily unavailable"}), 503


@app.route("/api/status")
@login_required
def api_status():
    """Per-agent health based on recency of last event."""
    try:
        return _api_status_impl()
    except Exception as exc:
        logger.error(json.dumps({"event": "api_status_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Service temporarily unavailable"}), 503


def _api_status_impl():
    agents = ["dispatcher", "finance", "ops", "system"]
    now = datetime.now(timezone.utc)

    # Single batched query instead of 4 separate queries
    try:
        docs = (
            db.collection(COLLECTION)
            .where("agent", "in", agents)
            .order_by("timestamp", direction=firestore.Query.DESCENDING)
            .limit(20)
            .stream()
        )
        latest: dict[str, tuple] = {}
        for doc in docs:
            d = doc.to_dict()
            name = d.get("agent")
            if name and name not in latest:
                latest[name] = (doc, d)
            if len(latest) == len(agents):
                break
    except Exception:
        latest = {}

    statuses = []
    for agent_name in agents:
        if agent_name in latest:
            _, d = latest[agent_name]
            ts_str = d.get("timestamp", "")
            try:
                last_seen = datetime.fromisoformat(ts_str)
                if last_seen.tzinfo is None:
                    last_seen = last_seen.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                last_seen = None

            if last_seen:
                delta = now - last_seen
                cfg = _get_dashboard_config()
                thresholds = cfg.get("agent_status_thresholds", {})
                green_min = int(thresholds.get("green_minutes", 30))
                amber_h = int(thresholds.get("amber_hours", 2))
                if delta < timedelta(minutes=green_min):
                    color = "green"
                elif delta < timedelta(hours=amber_h):
                    color = "amber"
                else:
                    color = "red"
            else:
                color = "red"

            statuses.append({
                "agent": agent_name,
                "last_seen": ts_str,
                "color": color,
                "last_event": d.get("event", ""),
            })
        else:
            statuses.append({
                "agent": agent_name,
                "last_seen": None,
                "color": "red",
                "last_event": "no activity",
            })

    return jsonify(statuses)


@app.route("/api/knowledge", methods=["POST"])
@login_required
@limiter.limit("10/hour")
def api_knowledge_add():
    """Add a custom knowledge entry â€” embeds and writes to Acme_knowledge."""
    data = request.get_json(silent=True) or {}
    text = (data.get("text") or "").strip()
    if len(text) < MIN_KNOWLEDGE_LENGTH:
        return jsonify({"error": f"Entry must be at least {MIN_KNOWLEDGE_LENGTH} characters."}), 400
    if len(text) > MAX_KNOWLEDGE_LENGTH:
        return jsonify({"error": f"Entry must be at most {MAX_KNOWLEDGE_LENGTH:,} characters."}), 400

    try:
        embedding = _generate_embedding(text)
    except Exception as exc:
        logger.error(json.dumps({"event": "knowledge_embed_error", "error": str(exc)[:300]}))
        return jsonify({"error": "Failed to process entry. Please try again."}), 503

    doc_id = f"client_{int(time.time() * 1000)}"
    try:
        db.collection(KNOWLEDGE_COLLECTION).document(doc_id).set({
            "text": text,
            "source": "client_dashboard",
            "type": "custom",
            "chunk_index": 0,
            "embedding": Vector(embedding),
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
    except Exception as exc:
        logger.error(json.dumps({"event": "knowledge_write_error", "error": str(exc)[:300]}))
        return jsonify({"error": "Failed to save entry. Please try again."}), 503

    logger.info(json.dumps({"event": "knowledge_added", "doc_id": doc_id, "length": len(text)}))
    return jsonify({"status": "ok", "id": doc_id}), 201


@app.route("/api/knowledge", methods=["GET"])
@login_required
def api_knowledge_list():
    """List client-added knowledge entries (source == client_dashboard)."""
    try:
        docs = (
            db.collection(KNOWLEDGE_COLLECTION)
            .where("source", "==", "client_dashboard")
            .order_by("created_at", direction=firestore.Query.DESCENDING)
            .limit(50)
            .stream()
        )
        entries = []
        for doc in docs:
            d = doc.to_dict()
            entries.append({
                "id": doc.id,
                "text": d.get("text", ""),
                "created_at": d.get("created_at", ""),
            })
        return jsonify({"entries": entries})
    except Exception as exc:
        logger.error(json.dumps({"event": "knowledge_list_error", "error": str(exc)[:300]}))
        return jsonify({"entries": [], "error": "Could not load entries."}), 500


# ---------------------------------------------------------------------------
# Cost tracker â€” BigQuery billing export
# ---------------------------------------------------------------------------
BILLING_DATASET = os.getenv("BILLING_DATASET", "billing_export")
BILLING_TABLE = os.getenv("BILLING_TABLE")  # e.g. "gcp_billing_export_v1_XXXXXX_XXXXXX_XXXXXX"
_BQ_IDENT_RE = re.compile(r"^[a-zA-Z0-9_]+$")
if BILLING_DATASET and not _BQ_IDENT_RE.match(BILLING_DATASET):
    raise ValueError(f"BILLING_DATASET contains invalid characters: {BILLING_DATASET}")
if BILLING_TABLE and not _BQ_IDENT_RE.match(BILLING_TABLE):
    raise ValueError(f"BILLING_TABLE contains invalid characters: {BILLING_TABLE}")
GBP_USD_RATE = float(os.getenv("GBP_USD_RATE", "0.79"))
_cost_cache: dict = {"payload": None, "expires": 0.0}
_COST_CACHE_TTL = 3600  # 1 hour

_BUCKETS: dict[str, tuple] = {
    "AI model calls": ("Vertex AI", "Generative Language API"),
    "Knowledge storage & search": ("Cloud Firestore",),
    "Service runtime": ("Cloud Run", "Cloud Build"),
}

_bq_client = None


def _get_bq():
    global _bq_client
    if _bq_client is None:
        from google.cloud import bigquery
        _bq_client = bigquery.Client()
    return _bq_client


def _classify_service(service_name: str) -> str:
    for bucket, keywords in _BUCKETS.items():
        if any(k.lower() in service_name.lower() for k in keywords):
            return bucket
    return "Other infrastructure"


def _query_costs() -> dict:
    bq = _get_bq()
    project = os.environ.get("GOOGLE_CLOUD_PROJECT", "YOUR_GCP_PROJECT_ID")
    table_ref = f"`{project}.{BILLING_DATASET}.{BILLING_TABLE}`"
    query = f"""
        SELECT
          service.description AS service,
          DATE(usage_start_time, 'Europe/London') AS day,
          SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) AS cost_usd
        FROM {table_ref}
        WHERE _PARTITIONTIME >= TIMESTAMP_TRUNC(
                TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 45 DAY), DAY)
          AND usage_start_time >= TIMESTAMP(
                DATE_TRUNC(DATE_SUB(CURRENT_DATE('Europe/London'), INTERVAL 1 MONTH), MONTH))
        GROUP BY service, day
        ORDER BY day ASC
    """
    rows = list(bq.query(query).result())

    from datetime import date as _date
    today = _date.today()
    current_month_start = today.replace(day=1)
    if current_month_start.month == 1:
        prev_month_start = current_month_start.replace(year=current_month_start.year - 1, month=12)
    else:
        prev_month_start = current_month_start.replace(month=current_month_start.month - 1)

    mtd_by_bucket: dict[str, float] = {b: 0.0 for b in list(_BUCKETS.keys()) + ["Other infrastructure"]}
    prev_total = 0.0
    daily: dict[str, float] = {}

    for row in rows:
        svc = row["service"] or ""
        day = row["day"]
        cost_usd = float(row["cost_usd"] or 0)
        bucket = _classify_service(svc)
        if day >= current_month_start:
            mtd_by_bucket[bucket] = mtd_by_bucket.get(bucket, 0.0) + cost_usd
            day_str = day.isoformat()
            daily[day_str] = daily.get(day_str, 0.0) + cost_usd
        elif day >= prev_month_start:
            prev_total += cost_usd

    rate = GBP_USD_RATE
    mtd_total_gbp = sum(mtd_by_bucket.values()) * rate
    buckets_out = [
        {"label": b, "value": round(v * rate, 2)}
        for b, v in mtd_by_bucket.items()
        if v > 0
    ]
    daily_out = [
        {"day": d, "value": round(v * rate, 2)}
        for d, v in sorted(daily.items())
    ]
    return {
        "currency": "GBP",
        "rate_gbp_usd": rate,
        "month_to_date": {
            "total": round(mtd_total_gbp, 2),
            "buckets": buckets_out,
        },
        "previous_month": {"total": round(prev_total * rate, 2)},
        "daily": daily_out,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.route("/api/costs", methods=["GET"])
@login_required
def api_costs():
    """Return client-friendly AI usage cost summary from BigQuery billing export."""
    now = time.monotonic()
    if _cost_cache["payload"] and now < _cost_cache["expires"]:
        return jsonify(_cost_cache["payload"])
    if not BILLING_TABLE:
        return jsonify({
            "error": "Cost tracking not configured",
            "reason": "BILLING_TABLE env var unset",
        }), 503
    try:
        payload = _query_costs()
    except Exception as exc:
        err_str = str(exc)[:300]
        logger.error(json.dumps({"event": "cost_query_error", "error": err_str}))
        if "Not found" in err_str or "notFound" in err_str:
            return jsonify({
                "error": "awaiting_data",
                "reason": "Billing export table not yet created â€” data arrives within 24h of enabling export.",
            }), 503
        return jsonify({"error": "Could not load costs."}), 500
    _cost_cache.update({"payload": payload, "expires": now + _COST_CACHE_TTL})
    # Inject budget info (not cached â€” budget can change independently)
    try:
        cfg = _get_dashboard_config()
        budget = float(cfg.get("monthly_budget_gbp", 0))
        if budget > 0:
            mtd = payload.get("month_to_date", {}).get("total", 0.0)
            pct = round((mtd / budget) * 100, 1)
            status = "exceeded" if pct >= 100 else "warning" if pct >= 80 else "ok"
            payload = {**payload, "budget": {"monthly_budget_gbp": budget, "budget_pct": pct, "status": status}}
    except Exception:
        pass
    return jsonify(payload)


_AGENT_LABELS_DEFAULT = {
    "dispatcher": "Dispatcher",
    "finance": "Finance Sentinel",
    "ops": "Ops Controller",
    "system": "System",
}

# ---------------------------------------------------------------------------
# Dashboard config â€” Firestore-backed with 60s TTL cache
# ---------------------------------------------------------------------------
_dashboard_config_cache: dict = {"payload": None, "expires": 0.0}
_DASHBOARD_CONFIG_CACHE_TTL = 60


def _get_dashboard_config() -> dict:
    """Return dashboard config from Firestore with 60s TTL. Falls back to defaults on failure."""
    now = time.monotonic()
    if _dashboard_config_cache["payload"] and now < _dashboard_config_cache["expires"]:
        return _dashboard_config_cache["payload"]
    try:
        from config.dashboard_settings import get_config, _DASHBOARD_CACHE_TTL
        cfg = get_config(cache_ttl=_DASHBOARD_CACHE_TTL)
    except Exception as exc:
        logger.warning("dashboard_config read failed (%s) â€” using defaults", exc)
        from config.dashboard_settings import get_defaults
        cfg = get_defaults()
    _dashboard_config_cache.update({"payload": cfg, "expires": now + _DASHBOARD_CONFIG_CACHE_TTL})
    return cfg

_HIDDEN_EVENTS: frozenset[str] = frozenset({
    # Infrastructure / internal â€” never useful to admins
    "llm_usage", "tool_result", "startup", "ready", "shutdown", "mode",
    "tools_loading", "tools_loaded", "tools_warmup_timeout", "tools_warmup_error",
    "tools_partial_failure", "toolset_close_warning", "toolset_respawn",
    "toolset_respawn_skipped", "toolset_respawn_exhausted", "mcp_pipe_error",
    "response", "chat_cancelled", "max_iterations_exceeded", "transfer_loop_abort",
    "input_token_ceiling_exceeded", "session_size_exceeded", "session_cleanup_failed",
    "invalid_event_type", "event_dispatch_error", "event_terminal", "handle_event_error",
    "quota_retry", "monday_webhook_filtered", "reconciliation_cooldown",
    "weekly_board_dup_skipped",
    # Background maintenance â€” invisible plumbing, not admin decisions
    "db_hygiene_complete", "db_hygiene_error",
    "lock_cleanup_complete", "lock_cleanup_error",
    "gmail_watch_renewed", "gmail_watch_error",
    "enroute_precompute_complete", "enroute_precompute_error",
    # Internal routing noise
    "route_complete", "routing", "event_received",
    "unknown_scheduled_task",
})

_TOOL_LABELS: dict[str, str] = {
    # Monday.com / bookings
    "create_inventory_booking": "Creating a booking",
    "amend_booking":            "Amending a booking",
    "create_lead":              "Creating a new lead",
    "create_task_item":         "Creating a task",
    "create_inventory_task":    "Creating a task",
    "get_board_items":          "Checking bookings",
    "get_board_summary":        "Reviewing board summary",
    "search_items":             "Searching bookings",
    "search_items_paginated":   "Searching bookings",
    "get_item":                 "Looking up a booking",
    "update_column_value":      "Updating a booking",
    "update_multiple_columns":  "Updating a booking",
    "update_status":            "Updating booking status",
    "create_item_update":       "Adding a note to booking",
    "archive_item":             "Archiving an item",
    "move_item_to_group":       "Moving item to group",
    "check_availability":       "Checking availability",
    "record_pending_invoice":   "Recording pending invoice",
    "set_invoice_number":       "Setting invoice number",
    "list_pending_invoices":    "Checking pending invoices",
    # Email / comms
    "send_email":               "Sending an email",
    "draft_reply":              "Drafting an email reply",
    "list_unread_emails":       "Checking emails",
    "list_sent_emails":         "Reviewing sent emails",
    "read_email_body":          "Reading an email",
    "mark_email_processed":     "Processing email",
    "mark_thread_booked":       "Marking thread as booked",
    "mark_chase_drafted":       "Marking chase as drafted",
    "claim_email_for_processing": "Claiming email for processing",
    # WhatsApp
    "send_whatsapp_message":    "Sending WhatsApp message",
    "send_whatsapp_template":   "Sending WhatsApp template",
    "send_contractor_status_check": "Checking contractor status",
    "send_admin_alert":         "Sending admin alert",
    "send_clerk_timetable":     "Sending clerk timetable",
    # Xero / finance
    "create_invoice":           "Creating an invoice",
    "list_unpaid_invoices":     "Checking unpaid invoices",
    "get_invoice_details":      "Looking up invoice details",
    "lock_task":                "Locking task for processing",
    # Scheduling / ops
    "get_working_schedule":     "Checking staff schedule",
    "calculate_travel_time":    "Calculating travel time",
    "optimize_route":           "Optimising route",
    "check_job_feasibility":    "Checking job feasibility",
    # Knowledge
    "consult_knowledge":        "Looking up information",
}

# Human-readable labels for scheduled task names (built from EDITABLE_TASKS + system tasks)
try:
    from config.scheduled_tasks import EDITABLE_TASKS as _ET
    _TASK_LABELS: dict[str, str] = {k: v["label"] for k, v in _ET.items()}
except Exception:
    _TASK_LABELS = {}
_TASK_LABELS.update({
    "lock_cleanup":       "Lock Cleanup",
    "db_hygiene":         "Database Hygiene",
    "renew_gmail_watch":  "Gmail Watch Renewal",
    "enroute_check_poll": "En-Route Check",
})

_AGENT_DISPLAY: dict[str, str] = {
    "dispatcher": "Administrator",
    "finance":    "Finance",
    "ops":        "Operations",
    "system":     "System",
    "hub":        "Orchestrator",
}

_EVENT_LABELS: dict[str, str] = {
    "booking_confirmed":  "Booking confirmed",
    "invoice_chased":     "Invoice chased",
    "route_timeout":      "Agent took too long to respond",
    "enroute_poll_error": "En-route check failed â€” some clerks may not have been messaged",
    "contractor_evening_reminder_error": "Evening reminder failed â€” some contractors may not have been messaged",
    "inbox_check_skipped":  "Inbox checked â€” nothing new",
    "invoice_sweep_skipped": "Invoices checked â€” all clear",
    "error":       "Something went wrong",
    "fatal_error": "Critical error â€” check system status",
}


def _human_event_label(d: dict) -> str | None:
    """Return a plain-English, admin-friendly label for a Firestore activity doc, or None to hide it."""
    event = d.get("event", "")
    tool = d.get("tool", "")
    blob = json.dumps(d).lower()

    if "needs_review" in blob or "escalat" in event.lower():
        return "Flagged for your attention"

    if event == "tool_call" and tool:
        return _TOOL_LABELS.get(tool)  # None hides unlisted tool calls

    # Scheduled task â€” show the business name of which task is running
    if event == "scheduled_task":
        task = d.get("task", "")
        label = _TASK_LABELS.get(task, task.replace("_", " ").title() if task else "a scheduled task")
        return f"Running: {label}"

    # Completion events with useful counts
    if event == "enroute_poll_complete":
        sent = d.get("checks_sent", 0)
        return f"Sent check-in messages to {sent} clerk{'s' if sent != 1 else ''}" if sent else "Check-ins sent â€” no clerks due"

    if event == "contractor_evening_reminder_complete":
        sent = d.get("sent", 0)
        return f"Sent tomorrow's confirmation to {sent} contractor{'s' if sent != 1 else ''}" if sent else "Evening reminders â€” no contractors scheduled"

    if event == "clerk_timetables_complete":
        sent = d.get("sent", 0)
        return f"Sent tomorrow's timetable to {sent} clerk{'s' if sent != 1 else ''}" if sent else "Timetables â€” no clerks scheduled tomorrow"

    if event in _EVENT_LABELS:
        return _EVENT_LABELS[event]

    if event:
        return event.replace("_", " ").title()

    return "Agent activity"

# ---------------------------------------------------------------------------
# Chat endpoints (proxy to multi-agent-system)
# ---------------------------------------------------------------------------

@app.route("/api/chat", methods=["POST"])
@login_required
@limiter.limit("20/minute")
def api_chat():
    """Proxy chat to multi-agent-system /chat and return its JSON response."""
    data = request.get_json(silent=True) or {}
    message = (data.get("message") or "").strip()
    if not message:
        return jsonify({"error": "message is required"}), 400

    session_id = (data.get("session_id") or "").strip() or None

    images = data.get("images", [])
    _ALLOWED_IMG_MIMES = {"image/jpeg", "image/png", "image/webp", "image/gif"}
    if len(images) > 4:
        return jsonify({"error": "Maximum 4 images per message"}), 400
    for _img in images:
        if _img.get("mime_type") not in _ALLOWED_IMG_MIMES:
            return jsonify({"error": f"Unsupported image type: {_img.get('mime_type')}"}), 400
        try:
            if len(base64.b64decode(_img["data"])) > 5 * 1024 * 1024:
                return jsonify({"error": "Each image must be under 5 MB"}), 400
        except Exception:
            return jsonify({"error": "Invalid image data"}), 400

    try:
        headers = {"Content-Type": "application/json"}
        token = _get_mas_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        payload = {"message": message}
        if session_id:
            payload["session_id"] = session_id
        if images:
            payload["images"] = images
        resp = http_requests.post(
            f"{Acme_MAS_URL}/chat",
            json=payload,
            headers=headers,
            timeout=260,
        )
    except http_requests.exceptions.Timeout:
        logger.warning(json.dumps({"event": "chat_timeout", "message_len": len(message)}))
        return jsonify({"error": "The agent took too long to respond. Please try again."}), 504
    except http_requests.exceptions.ConnectionError as exc:
        logger.error(json.dumps({"event": "chat_connection_error", "error": str(exc)[:300]}))
        return jsonify({"error": "Lost connection to the agent service. Please try again in a moment."}), 502
    except Exception as exc:
        logger.error(json.dumps({"event": "chat_error", "error": str(exc)[:300], "type": type(exc).__name__}))
        return jsonify({"error": "An unexpected error occurred. Please try again."}), 500

    if resp.status_code == 422:
        try:
            return jsonify(resp.json()), 422
        except ValueError:
            return jsonify({"error": "Agent service returned an error. Please try again."}), 502
    if not resp.ok:
        logger.error(json.dumps({"event": "chat_upstream_error", "status": resp.status_code}))
        return jsonify({"error": "Agent service returned an error. Please try again."}), 502

    try:
        return jsonify(resp.json())
    except ValueError:
        logger.error(json.dumps({"event": "chat_decode_error", "status": resp.status_code}))
        return jsonify({"error": "Agent service returned an unexpected response. Please try again."}), 502


@app.route("/api/csrf-token", methods=["GET"])
@login_required
def api_csrf_token():
    """Return a fresh CSRF token so long-lived pages can refresh before the 1-hour expiry."""
    return jsonify({"csrf_token": generate_csrf()})


@app.route("/api/chat/stream", methods=["POST"])
@login_required
@limiter.limit("20/minute")
def api_chat_stream():
    """SSE passthrough â€” proxies token chunks from multi-agent-system /chat/stream."""
    data = request.get_json(silent=True) or {}
    message = (data.get("message") or "").strip()
    if not message:
        return jsonify({"error": "message is required"}), 400

    session_id = (data.get("session_id") or "").strip() or None

    images = data.get("images", [])
    _ALLOWED_IMG_MIMES = {"image/jpeg", "image/png", "image/webp", "image/gif"}
    if len(images) > 4:
        return jsonify({"error": "Maximum 4 images per message"}), 400
    for _img in images:
        if _img.get("mime_type") not in _ALLOWED_IMG_MIMES:
            return jsonify({"error": f"Unsupported image type: {_img.get('mime_type')}"}), 400
        try:
            if len(base64.b64decode(_img["data"])) > 5 * 1024 * 1024:
                return jsonify({"error": "Each image must be under 5 MB"}), 400
        except Exception:
            return jsonify({"error": "Invalid image data"}), 400

    def generate():
        try:
            headers = {"Content-Type": "application/json", "Accept": "text/event-stream"}
            token = _get_mas_token()
            if token:
                headers["Authorization"] = f"Bearer {token}"
        except Exception as exc:
            logger.error(json.dumps({"event": "chat_stream_token_error", "error": str(exc)[:300]}))
            yield f"data: {json.dumps({'type': 'error', 'text': 'Failed to authenticate with agent service.'})}\n\n"
            return
        payload = {"message": message}
        if session_id:
            payload["session_id"] = session_id
        if images:
            payload["images"] = images
        try:
            with http_requests.post(
                f"{Acme_MAS_URL}/chat/stream",
                json=payload,
                headers=headers,
                stream=True,
                timeout=(10, 375),
            ) as resp:
                if not resp.ok:
                    logger.error(json.dumps({"event": "chat_stream_upstream_error", "status": resp.status_code}))
                    yield f"data: {json.dumps({'type': 'error', 'text': 'Agent service error.'})}\n\n"
                    return
                saw_done = False
                for line in resp.iter_lines():
                    if line:
                        decoded = line.decode("utf-8") if isinstance(line, bytes) else line
                        yield decoded + "\n\n"
                        if '"type": "done"' in decoded or '"type":"done"' in decoded:
                            saw_done = True
                if not saw_done:
                    yield f"data: {json.dumps({'type': 'done'})}\n\n"
        except http_requests.exceptions.Timeout:
            yield f"data: {json.dumps({'type': 'error', 'text': 'Request timed out.'})}\n\n"
        except Exception as exc:
            logger.error(json.dumps({"event": "chat_stream_proxy_error", "error": str(exc)[:300]}))
            yield f"data: {json.dumps({'type': 'error', 'text': 'An internal error occurred.'})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/chat/history", methods=["GET"])
@login_required
@limiter.limit("30/minute")
def api_chat_history():
    """Proxy chat history to multi-agent-system /chat/history."""
    session_id = (request.args.get("session_id") or "").strip()
    if not session_id:
        return jsonify({"error": "session_id is required"}), 400

    try:
        headers = {}
        token = _get_mas_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        resp = http_requests.get(
            f"{Acme_MAS_URL}/chat/history",
            params={"session_id": session_id},
            headers=headers,
            timeout=5,
        )
    except http_requests.exceptions.Timeout:
        return jsonify({"error": "Agent service temporarily unavailable. Please try again shortly."}), 503
    except Exception as exc:
        logger.error(json.dumps({"event": "chat_history_error", "error": str(exc)[:300]}))
        return jsonify({"error": "An unexpected error occurred."}), 500

    if resp.status_code == 404:
        return jsonify({"error": "not_found"}), 404
    if not resp.ok:
        logger.error(json.dumps({"event": "chat_history_upstream_error", "status": resp.status_code}))
        return jsonify({"error": "Agent service returned an error."}), 502

    try:
        return jsonify(resp.json())
    except ValueError:
        return jsonify({"error": "Agent service returned an unexpected response."}), 502


@app.route("/api/chat/sessions", methods=["GET"])
@login_required
@limiter.limit("30/minute")
def api_chat_sessions():
    """Proxy chat sessions list to multi-agent-system /chat/sessions."""
    try:
        headers = {}
        token = _get_mas_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        resp = http_requests.get(
            f"{Acme_MAS_URL}/chat/sessions",
            headers=headers,
            timeout=5,
        )
    except http_requests.exceptions.Timeout:
        return jsonify({"error": "Agent service temporarily unavailable. Please try again shortly."}), 503
    except Exception as exc:
        logger.error(json.dumps({"event": "chat_sessions_error", "error": str(exc)[:300]}))
        return jsonify({"error": "An unexpected error occurred."}), 500

    if not resp.ok:
        logger.error(json.dumps({"event": "chat_sessions_upstream_error", "status": resp.status_code}))
        return jsonify({"error": "Agent service returned an error."}), 502

    try:
        return jsonify(resp.json())
    except ValueError:
        return jsonify({"error": "Agent service returned an unexpected response."}), 502


@app.route("/api/chat/sessions/<session_id>", methods=["DELETE"])
@login_required
@limiter.limit("20/minute")
def api_delete_chat_session(session_id):
    """Proxy chat session delete to multi-agent-system /chat/sessions/<session_id>."""
    try:
        headers = {}
        token = _get_mas_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        resp = http_requests.delete(
            f"{Acme_MAS_URL}/chat/sessions/{session_id}",
            headers=headers,
            timeout=15,
        )
    except http_requests.exceptions.Timeout:
        return jsonify({"error": "Agent service timed out."}), 504
    except Exception as exc:
        logger.error(json.dumps({"event": "chat_delete_session_error", "error": str(exc)[:300]}))
        return jsonify({"error": "An unexpected error occurred."}), 500

    if resp.status_code == 400:
        try:
            return jsonify(resp.json()), 400
        except ValueError:
            return jsonify({"error": "Invalid session_id."}), 400
    if resp.status_code == 404:
        return jsonify({"error": "Session not found."}), 404
    if not resp.ok:
        logger.error(json.dumps({"event": "chat_delete_session_upstream_error", "status": resp.status_code}))
        return jsonify({"error": "Agent service returned an error."}), 502

    try:
        return jsonify(resp.json())
    except ValueError:
        return jsonify({"error": "Agent service returned an unexpected response."}), 502


# ---------------------------------------------------------------------------
# OAuth helpers
# ---------------------------------------------------------------------------
_GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]
_XERO_SCOPES = "offline_access accounting.invoices accounting.contacts.read"


def _sm_client():
    from google.cloud import secretmanager_v1
    return secretmanager_v1.SecretManagerServiceClient()


def _save_secret(secret_name: str, data: str) -> None:
    parent = f"projects/{PROJECT_ID}/secrets/{secret_name}"
    _sm_client().add_secret_version(
        request={"parent": parent, "payload": {"data": data.encode("utf-8")}}
    )


def _create_oauth_state(service: str) -> str:
    state = secrets.token_urlsafe(32)
    expires = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
    db.collection("oauth_states").document(state).set({"service": service, "expires": expires})
    return state


def _validate_oauth_state(state: str, service: str) -> bool:
    ref = db.collection("oauth_states").document(state)
    doc = ref.get()
    if not doc.exists:
        return False
    data = doc.to_dict()
    if data.get("service") != service:
        ref.delete()
        return False
    try:
        exp_dt = datetime.fromisoformat(data.get("expires", ""))
        if exp_dt.tzinfo is None:
            exp_dt = exp_dt.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) > exp_dt:
            ref.delete()
            return False
    except (ValueError, TypeError):
        ref.delete()
        return False
    ref.delete()
    return True


def _check_connected(service: str) -> bool:
    secret_map = {"gmail": "gmail-token-json", "xero": "xero-token-json"}
    secret_name = secret_map.get(service)
    if not secret_name:
        return False
    try:
        from google.cloud import secretmanager_v1
        client = secretmanager_v1.SecretManagerServiceClient()
        parent = f"projects/{PROJECT_ID}/secrets/{secret_name}"
        versions = list(client.list_secret_versions(
            request={"parent": parent, "filter": "state:ENABLED"}
        ))
        return len(versions) > 0
    except Exception:
        return False


def _get_secret(name: str) -> Optional[str]:
    """Fetch the latest enabled version of a Secret Manager secret.

    Returns None on any failure (secret doesn't exist yet, permission error,
    no enabled versions, etc.) â€” callers should treat that as "not configured
    yet" rather than an exceptional condition.
    """
    try:
        resp = _sm_client().access_secret_version(
            request={"name": f"projects/{PROJECT_ID}/secrets/{name}/versions/latest"}
        )
        return resp.payload.data.decode("utf-8")
    except Exception:
        return None


def _xero_connections(access_token: str) -> list:
    """GET /connections â€” the list of Xero orgs authorised for this token.

    Returns the parsed JSON list of {"tenantId": ..., "tenantName": ..., ...}.
    Raises on a non-2xx response; callers decide how to surface that.
    """
    resp = http_requests.get(
        "https://api.xero.com/connections",
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _get_valid_xero_access_token() -> Optional[str]:
    """Return a currently-valid Xero access token, refreshing via Secret Manager
    if the stored one is expired. Returns None if Xero isn't connected; raises
    on a hard refresh failure so callers surface a 'reconnect Xero' error.
    """
    token_json = _get_secret("xero-token-json")
    if not token_json:
        return None
    token = json.loads(token_json)              # may raise ValueError -> caller catches
    if not isinstance(token, dict):             # closes TypeError gap (corrupt secret)
        raise ValueError("stored Xero token is not an object")
    # Refresh a minute early to avoid using a token that expires mid-request.
    if time.time() < token.get("expires_at", 0) - 60:
        return token["access_token"]
    # Expired -> refresh
    import base64 as _b64
    basic = _b64.b64encode(f"{XERO_CLIENT_ID}:{XERO_CLIENT_SECRET}".encode()).decode()
    resp = http_requests.post(
        "https://identity.xero.com/connect/token",
        headers={"Authorization": f"Basic {basic}",
                 "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "refresh_token", "refresh_token": token.get("refresh_token", "")},
        timeout=30,
    )
    if resp.status_code != 200:
        logger.error(json.dumps({"event": "xero_token_refresh_failed", "status": resp.status_code}))
        raise RuntimeError("Xero token refresh failed â€” reconnect Xero")
    new_data = resp.json()
    token.update(new_data)
    token["expires_at"] = time.time() + token.get("expires_in", 1800)
    _save_secret("xero-token-json", json.dumps(token))
    logger.info(json.dumps({"event": "xero_token_refreshed"}))
    return token["access_token"]


def _exchange_gmail_code(code: str, redirect_uri: str) -> dict:
    from google_auth_oauthlib.flow import Flow
    flow = Flow.from_client_config(
        json.loads(GMAIL_WEB_CRED_JSON),
        scopes=_GMAIL_SCOPES,
        redirect_uri=redirect_uri,
    )
    flow.fetch_token(code=code)
    creds = flow.credentials
    return {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": list(creds.scopes) if creds.scopes else [],
    }


def _exchange_xero_code(code: str, redirect_uri: str) -> dict:
    import base64 as _b64
    creds_b64 = _b64.b64encode(f"{XERO_CLIENT_ID}:{XERO_CLIENT_SECRET}".encode()).decode()
    resp = http_requests.post(
        "https://identity.xero.com/connect/token",
        headers={"Authorization": f"Basic {creds_b64}", "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "authorization_code", "code": code, "redirect_uri": redirect_uri},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# OAuth routes â€” Gmail
# ---------------------------------------------------------------------------

@app.route("/oauth/gmail/start", methods=["GET"])
@login_required
@limiter.limit("3/minute")
def oauth_gmail_start():
    if not GMAIL_WEB_CRED_JSON:
        return jsonify({"error": "Gmail OAuth not configured"}), 503
    from urllib.parse import urlencode
    cred_data = json.loads(GMAIL_WEB_CRED_JSON)
    client_id = (cred_data.get("web") or cred_data.get("installed", {})).get("client_id", "")
    state = _create_oauth_state("gmail")
    auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode({
        "client_id": client_id,
        "redirect_uri": f"{DASHBOARD_URL}/oauth/gmail/callback",
        "scope": " ".join(_GMAIL_SCOPES),
        "response_type": "code",
        "state": state,
        "access_type": "offline",
        "prompt": "select_account consent",
    })
    logger.info(json.dumps({"event": "gmail_oauth_start", "auth_url": auth_url}))
    return redirect(auth_url)


@app.route("/oauth/gmail/callback", methods=["GET"])
@csrf.exempt
@login_required
def oauth_gmail_callback():
    error = request.args.get("error")
    if error:
        return redirect(url_for("dashboard") + f"?tab=settings&gmail_error={error}")
    code = request.args.get("code", "")
    state = request.args.get("state", "")
    if not _validate_oauth_state(state, "gmail"):
        return redirect(url_for("dashboard") + "?tab=settings&gmail_error=invalid_state")
    try:
        token_data = _exchange_gmail_code(code, f"{DASHBOARD_URL}/oauth/gmail/callback")
        _save_secret("gmail-token-json", json.dumps(token_data))
        logger.info(json.dumps({"event": "gmail_oauth_connected"}))
    except Exception as exc:
        logger.error(json.dumps({"event": "gmail_oauth_error", "error": str(exc)[:200]}))
        return redirect(url_for("dashboard") + "?tab=settings&gmail_error=exchange_failed")
    return redirect(url_for("dashboard") + "?tab=settings&gmail_connected=true")


# ---------------------------------------------------------------------------
# OAuth routes â€” Xero
# ---------------------------------------------------------------------------

@app.route("/oauth/xero/start", methods=["GET"])
@login_required
@limiter.limit("3/minute")
def oauth_xero_start():
    if not XERO_CLIENT_ID:
        return jsonify({"error": "Xero OAuth not configured"}), 503
    from urllib.parse import urlencode
    # IMPORTANT: After deploying scope change, re-consent at /oauth/xero/start BEFORE
    # setting INVOICE_CREATION_ENABLED=true. Enabling before re-auth causes 403 on every sweep.
    state = _create_oauth_state("xero")
    auth_url = "https://login.xero.com/identity/connect/authorize?" + urlencode({
        "response_type": "code",
        "client_id": XERO_CLIENT_ID,
        "redirect_uri": f"{DASHBOARD_URL}/oauth/xero/callback",
        "scope": _XERO_SCOPES,
        "state": state,
        "prompt": "login consent",
    })
    logger.info(json.dumps({"event": "xero_oauth_start", "auth_url": auth_url}))
    return redirect(auth_url)


@app.route("/oauth/xero/callback", methods=["GET"])
@csrf.exempt
@login_required
def oauth_xero_callback():
    error = request.args.get("error")
    if error:
        return redirect(url_for("dashboard") + f"?tab=settings&xero_error={error}")
    code = request.args.get("code", "")
    state = request.args.get("state", "")
    if not _validate_oauth_state(state, "xero"):
        return redirect(url_for("dashboard") + "?tab=settings&xero_error=invalid_state")
    try:
        token_data = _exchange_xero_code(code, f"{DASHBOARD_URL}/oauth/xero/callback")
        # xero_server.py gates refresh on expires_at (absolute epoch); Xero's token
        # response only gives expires_in (relative seconds). Compute and inject it here
        # so the server doesn't treat the brand-new token as immediately expired.
        token_data["expires_at"] = time.time() + token_data.get("expires_in", 1800)
        _save_secret("xero-token-json", json.dumps(token_data))
        logger.info(json.dumps({"event": "xero_oauth_connected"}))
    except Exception as exc:
        logger.error(json.dumps({"event": "xero_oauth_error", "error": str(exc)[:200]}))
        return redirect(url_for("dashboard") + "?tab=settings&xero_error=exchange_failed")

    # Multiple Xero orgs can share one login â€” pick automatically when there's
    # only one, otherwise send the user to the dashboard org picker rather than
    # silently pinning the wrong one. Never block the OAuth flow on this.
    try:
        connections = _xero_connections(token_data["access_token"])
        if len(connections) == 1:
            _save_secret("xero-tenant-id", connections[0].get("tenantId", ""))
            logger.info(json.dumps({
                "event": "xero_tenant_auto_pinned",
                "tenant_id": connections[0].get("tenantId", ""),
            }))
        elif len(connections) > 1:
            logger.info(json.dumps({"event": "xero_multiple_orgs_connected", "count": len(connections)}))
            return redirect(url_for("dashboard") + "?tab=settings&xero_select=1")
        else:
            logger.warning(json.dumps({"event": "xero_no_connections_returned"}))
    except Exception as exc:
        logger.warning(json.dumps({"event": "xero_connections_fetch_failed", "error": str(exc)[:200]}))

    return redirect(url_for("dashboard") + "?tab=settings&xero_connected=true")


# ---------------------------------------------------------------------------
# Integration status + credential activation
# ---------------------------------------------------------------------------

@app.route("/api/integration-status")
@login_required
def api_integration_status():
    return jsonify({
        "gmail": _check_connected("gmail"),
        "xero": _check_connected("xero"),
        # Raw pinned tenant id, cheap (secret read only â€” no live Xero call).
        # Frontend resolves it to a friendly org name via /api/xero/organisations.
        "xero_org": _get_secret("xero-tenant-id"),
    })


@app.route("/api/xero/organisations")
@login_required
def api_xero_organisations():
    """List the Xero orgs this account's stored token can see, plus the pin."""
    try:
        access_token = _get_valid_xero_access_token()
    except (ValueError, KeyError):
        return jsonify({"error": "Stored Xero token is invalid â€” reconnect Xero"}), 502
    except Exception as exc:
        logger.error(json.dumps({"event": "xero_token_refresh_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Xero token expired and refresh failed â€” reconnect Xero"}), 502
    if access_token is None:
        return jsonify({"error": "Xero is not connected"}), 503

    try:
        connections = _xero_connections(access_token)
    except Exception as exc:
        logger.error(json.dumps({"event": "xero_organisations_fetch_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Failed to reach Xero â€” reconnect Xero if the token has expired"}), 502

    organisations = [
        {"tenantId": c.get("tenantId"), "tenantName": c.get("tenantName")}
        for c in connections
    ]
    return jsonify({"organisations": organisations, "pinned": _get_secret("xero-tenant-id")})


@app.route("/oauth/xero/select-org", methods=["POST"])
@login_required
@limiter.limit("10/minute")
def oauth_xero_select_org():
    """Persist the user's choice of Xero org as the `xero-tenant-id` secret."""
    data = request.get_json(silent=True) or {}
    tenant_id = (data.get("tenantId") or "").strip()
    if not tenant_id:
        return jsonify({"error": "tenantId is required"}), 400

    try:
        access_token = _get_valid_xero_access_token()
    except (ValueError, KeyError):
        return jsonify({"error": "Stored Xero token is invalid â€” reconnect Xero"}), 502
    except Exception as exc:
        logger.error(json.dumps({"event": "xero_token_refresh_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Xero token expired and refresh failed â€” reconnect Xero"}), 502
    if access_token is None:
        return jsonify({"error": "Xero is not connected"}), 503

    try:
        connections = _xero_connections(access_token)
    except Exception as exc:
        logger.error(json.dumps({"event": "xero_select_org_fetch_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Failed to reach Xero â€” reconnect Xero if the token has expired"}), 502

    if not any(c.get("tenantId") == tenant_id for c in connections):
        return jsonify({"error": "Not a connected organisation"}), 400

    try:
        _save_secret("xero-tenant-id", tenant_id)
    except Exception as exc:
        logger.error(json.dumps({"event": "xero_select_org_save_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Failed to save selection"}), 500

    logger.info(json.dumps({"event": "xero_tenant_selected", "tenant_id": tenant_id}))
    return jsonify({"ok": True})


def _force_mas_new_revision(wait: bool = False, reason: str = "reconnect") -> dict:
    """Force multi-agent-system to roll a genuinely new Cloud Run revision so warm
    instances reload :latest secret values (e.g. gmail-token-json).

    Re-submitting an unchanged Service to update_service() is a no-op in
    Cloud Run â€” it dedupes against the current revision spec and skips the
    rollout entirely, leaving warm instances on stale credentials. Clearing
    template.revision lets Cloud Run auto-name a new revision, and bumping a
    template annotation guarantees the spec hash changes so it is never
    deduped even if some other field happens to already match.

    Returns {"ok": bool, "revision": str|None, "no_op": bool, "error": str|None}.
    """
    try:
        from google.cloud import run_v2
        client = run_v2.ServicesClient()
        name = f"projects/{PROJECT_ID}/locations/{CLOUD_RUN_REGION}/services/{MAS_SERVICE_NAME}"
        service = client.get_service(request={"name": name})
        prev_revision = service.latest_created_revision
        service.template.revision = ""
        service.template.annotations["Acme.dev/reconnect-ts"] = str(int(time.time()))
        operation = client.update_service(request={"service": service})
        if wait:
            # Must stay comfortably under gunicorn's --timeout 120 (Dockerfile) so the
            # worker isn't SIGKILLed mid-wait, which would drop the response entirely.
            result = operation.result(timeout=90)
            new_revision = result.latest_created_revision
            logger.info(json.dumps({
                "event": "mas_new_revision", "reason": reason,
                "revision": new_revision, "no_op": new_revision == prev_revision,
            }))
            return {"ok": True, "revision": new_revision, "no_op": new_revision == prev_revision, "error": None}
        logger.info(json.dumps({"event": "mas_new_revision_started", "reason": reason}))
        return {"ok": True, "revision": None, "no_op": False, "error": None}
    except Exception as exc:
        logger.error(json.dumps({"event": "mas_new_revision_error", "reason": reason, "error": str(exc)[:200]}))
        return {"ok": False, "revision": None, "no_op": False, "error": str(exc)[:200]}


@app.route("/api/activate-credentials", methods=["POST"])
@login_required
def api_activate_credentials():
    res = _force_mas_new_revision(wait=True, reason="activate_credentials")
    if res["ok"]:
        return jsonify({"status": "activated", "revision": res["revision"], "no_op": res["no_op"]})
    return jsonify({"error": "Failed to activate credentials"}), 500


# ---------------------------------------------------------------------------
# Monday.com column auto-detection (inlined from config.monday_config)
# ---------------------------------------------------------------------------
_COLUMN_ALIASES: dict[str, list[str]] = {
    "report_status":       ["report status", "status", "report"],
    "inspector":           ["inspector", "clerk", "assignee"],
    "report_type":         ["report type", "type"],
    "reason_delay":        ["reason for delay", "delay reason", "reason delay"],
    "payment_status":      ["payment status", "payment"],
    "payment_reminder":    ["payment reminder", "reminder"],
    "Acme_confirmation":  ["Acme confirmation", "confirmation"],
    "postcode":            ["postcode", "post code", "zip code", "zip"],
    "phone":               ["phone", "telephone", "mobile", "contact phone"],
    "customer":            ["customer", "client", "client name"],  # "name" removed â€” matches item-Name column, not Customer
    "access_info":         ["access info", "access information", "access", "access instructions", "instructions"],
    "contact_number":      ["contact number", "contact", "phone number"],
    "notes":               ["notes", "note", "comments"],
    "inv_number":          ["inv #", "inv number", "invoice number", "invoice #", "inv"],
    "hour":                ["hour", "time", "appointment time", "booking time"],
    "email":               ["email", "e-mail", "email address"],
    "address":             ["address", "property address", "full address"],
    "spec":                ["spec", "specification", "service type", "service"],
    "date":                ["date", "booking date", "appointment date"],
}


def _detect_columns(board_columns: list) -> dict:
    """Auto-detect logical->column_id mappings from a board's column list."""
    result: dict = {}
    for logical_name, aliases in _COLUMN_ALIASES.items():
        best: Optional[dict] = None
        for col in board_columns:
            title = col.get("title", "").lower().strip()
            col_id = col.get("id", "")
            if any(a == title for a in aliases):
                best = {"id": col_id, "title": col.get("title"), "confidence": 1.0}
                break
            if best is None:
                if any(a in title or title in a for a in aliases):
                    best = {"id": col_id, "title": col.get("title"), "confidence": 0.7}
        if best:
            result[logical_name] = best
    return result


# ---------------------------------------------------------------------------
# Monday.com settings API
# ---------------------------------------------------------------------------

def _monday_api_request(token: str, query: str, variables: Optional[dict] = None) -> dict:
    """Synchronous Monday.com API call for dashboard settings endpoints."""
    import requests as _req
    resp = _req.post(
        "https://api.monday.com/v2",
        headers={"Authorization": token, "API-Version": "2026-01", "Content-Type": "application/json"},
        json={"query": query, "variables": variables or {}},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


@app.route("/api/settings/monday", methods=["GET"])
@login_required
def api_settings_monday_get():
    """Return current Monday config with token redacted."""
    try:
        doc = db.collection("monday_config").document("boards").get()
        if not doc.exists:
            return jsonify({"configured": False, "boards": {}})
        data = doc.to_dict()
        out = {
            "configured": True,
            "token_set": bool(data.get("api_token")),
            "boards": {k: {"id": v.get("id"), "name": v.get("name"), "groups": v.get("groups", {})} for k, v in data.get("boards", {}).items()},
            "updated_at": data.get("updated_at"),
        }
        return jsonify(out)
    except Exception as exc:
        logger.error(json.dumps({"event": "monday_settings_get_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Could not load Monday settings"}), 500


@app.route("/api/settings/monday/token", methods=["POST"])
@login_required
@limiter.limit("10/minute")
def api_settings_monday_token():
    """Validate a Monday API token and save it to Firestore."""
    data = request.get_json(silent=True) or {}
    token = (data.get("token") or "").strip()
    if not token:
        return jsonify({"error": "token is required"}), 400
    try:
        result = _monday_api_request(token, "query { me { name } }")
        if "errors" in result:
            return jsonify({"error": result["errors"][0].get("message", "Invalid token")}), 400
        account_name = result.get("data", {}).get("me", {}).get("name", "Unknown")
    except Exception as exc:
        logger.error(json.dumps({"event": "monday_token_validate_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Token validation failed"}), 400

    try:
        _save_monday_token(token)
        db.collection("monday_config").document("boards").set(
            {"updated_at": datetime.now(timezone.utc).isoformat()},
            merge=True,
        )
    except Exception as exc:
        logger.error(json.dumps({"event": "monday_token_save_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Token validated but could not save"}), 500

    logger.info(json.dumps({"event": "monday_token_saved", "account": account_name}))
    return jsonify({"status": "ok", "account_name": account_name})


@app.route("/api/settings/monday/boards", methods=["GET"])
@login_required
def api_settings_monday_boards():
    """List Monday boards for the saved API token."""
    try:
        token = _get_monday_token()
    except Exception:
        token = ""
    if not token:
        return jsonify({"error": "Monday token not configured. Save a token first."}), 400
    try:
        result = _monday_api_request(token, "query { boards(limit: 100) { id name } }")
        boards = result.get("data", {}).get("boards", [])
        return jsonify({"boards": boards})
    except Exception as exc:
        logger.error(json.dumps({"event": "monday_boards_fetch_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Could not fetch boards"}), 500


@app.route("/api/settings/monday/detect-columns", methods=["POST"])
@login_required
def api_settings_monday_detect_columns():
    """Auto-detect column mappings for a given board ID."""
    data = request.get_json(silent=True) or {}
    board_id = data.get("board_id")
    if not board_id:
        return jsonify({"error": "board_id is required"}), 400
    try:
        token = _get_monday_token()
    except Exception:
        token = ""
    if not token:
        return jsonify({"error": "Monday token not configured"}), 400
    try:
        result = _monday_api_request(
            token,
            "query($id: [ID!]) { boards(ids: $id) { columns { id title } groups { id title } } }",
            {"id": [str(board_id)]},
        )
        board_data = (result.get("data", {}).get("boards") or [{}])[0]
        columns = board_data.get("columns", [])
        groups = board_data.get("groups", [])
    except Exception as exc:
        logger.error(json.dumps({"event": "monday_schema_fetch_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Could not fetch board schema"}), 500

    detected = _detect_columns(columns)
    return jsonify({"detected": detected, "all_columns": columns, "groups": groups})


@app.route("/api/settings/monday/save", methods=["POST"])
@login_required
def api_settings_monday_save():
    """Save full board+column config to Firestore."""
    data = request.get_json(silent=True) or {}
    boards_config = data.get("boards")
    if not boards_config or not isinstance(boards_config, dict):
        return jsonify({"error": "boards config is required"}), 400
    inventories = boards_config.get("inventories", {})
    if not inventories.get("id"):
        return jsonify({"error": "inventories board id is required"}), 400
    _required_cols = {"date", "address", "customer"}
    _mapped_cols = set(inventories.get("columns", {}).keys())
    _missing_required = sorted(_required_cols - _mapped_cols)

    try:
        db.collection("monday_config").document("boards").set(
            {"boards": boards_config, "updated_at": datetime.now(timezone.utc).isoformat()},
            merge=True,
        )
        logger.info(json.dumps({"event": "monday_config_saved", "missing_required_columns": _missing_required}))
        response: dict = {"status": "ok"}
        if _missing_required:
            response["warnings"] = [f"Required column(s) not mapped: {', '.join(_missing_required)}. Bookings may fail until configured."]
        return jsonify(response)
    except Exception as exc:
        logger.error(json.dumps({"event": "monday_config_save_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Failed to save config"}), 500


@app.route("/api/settings/monday/activate", methods=["POST"])
@login_required
def api_settings_monday_activate():
    """Trigger Cloud Run service restart to pick up new Monday config."""
    res = _force_mas_new_revision(wait=True, reason="monday_activate")
    if res["ok"]:
        return jsonify({"status": "activated", "revision": res["revision"], "no_op": res["no_op"]})
    return jsonify({"error": "Failed to restart service"}), 500


# ---------------------------------------------------------------------------
# Dashboard config CRUD
# ---------------------------------------------------------------------------
@app.route("/api/config", methods=["GET"])
@login_required
def api_config_get():
    """Return current dashboard config (Firestore-backed, falls back to defaults)."""
    try:
        cfg = _get_dashboard_config()
        return jsonify(cfg)
    except Exception as exc:
        logger.error(json.dumps({"event": "api_config_get_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Service temporarily unavailable"}), 503


@app.route("/api/config/defaults", methods=["GET"])
@login_required
def api_config_defaults():
    """Return hardcoded default config values (for Reset button)."""
    try:
        from config.dashboard_settings import get_defaults
        return jsonify(get_defaults())
    except Exception as exc:
        logger.error(json.dumps({"event": "api_config_defaults_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Service temporarily unavailable"}), 503


@app.route("/api/config", methods=["PUT"])
@login_required
@limiter.limit("30/minute")
def api_config_put():
    """Partial update of dashboard config. Validates fields then merges into Firestore doc."""
    data = request.get_json(silent=True) or {}
    try:
        from config.dashboard_settings import validate_config_update, invalidate_cache
        cleaned, errors = validate_config_update(data)
    except Exception as exc:
        logger.error(json.dumps({"event": "api_config_validate_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Validation failed"}), 500

    if errors:
        return jsonify({"error": "; ".join(errors)}), 400
    if not cleaned:
        return jsonify({"error": "No valid fields provided"}), 400

    cleaned["updated_at"] = datetime.now(timezone.utc).isoformat()
    try:
        db.collection("dashboard_config").document("settings").set(cleaned, merge=True)
        invalidate_cache()
        _dashboard_config_cache.update({"payload": None, "expires": 0.0})
        logger.info(json.dumps({"event": "dashboard_config_updated", "fields": list(cleaned.keys())}))
        return jsonify({"status": "ok"})
    except Exception as exc:
        logger.error(json.dumps({"event": "api_config_put_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Failed to save config"}), 500


# ---------------------------------------------------------------------------
# Admin Contacts API
# ---------------------------------------------------------------------------

@app.route("/api/admin-contacts", methods=["GET"])
@login_required
def api_admin_contacts_get():
    """Return current admin contacts list from Firestore config."""
    try:
        from config.dashboard_settings import get_admin_contacts
        contacts = get_admin_contacts()
        cfg = _get_dashboard_config()
        source = "firestore" if cfg.get("admin_contacts") else "env_var"
        return jsonify({"contacts": contacts, "source": source})
    except Exception as exc:
        logger.error(json.dumps({"event": "api_admin_contacts_get_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Service temporarily unavailable"}), 503


@app.route("/api/admin-contacts", methods=["PUT"])
@login_required
@limiter.limit("30/minute")
def api_admin_contacts_put():
    """Replace admin contacts list. Validates then writes to Firestore."""
    data = request.get_json(silent=True) or {}
    contacts = data.get("contacts")
    if contacts is None:
        return jsonify({"error": "contacts field required"}), 400
    try:
        from config.dashboard_settings import validate_config_update, invalidate_cache
        cleaned, errors = validate_config_update({"admin_contacts": contacts})
    except Exception as exc:
        logger.error(json.dumps({"event": "api_admin_contacts_validate_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Validation failed"}), 500

    if errors:
        return jsonify({"error": "; ".join(errors)}), 400

    cleaned["updated_at"] = datetime.now(timezone.utc).isoformat()
    try:
        db.collection("dashboard_config").document("settings").set(cleaned, merge=True)
        invalidate_cache()
        _dashboard_config_cache.update({"payload": None, "expires": 0.0})
        logger.info(json.dumps({"event": "admin_contacts_updated", "count": len(cleaned.get("admin_contacts", []))}))
        return jsonify({"status": "ok"})
    except Exception as exc:
        logger.error(json.dumps({"event": "api_admin_contacts_put_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Failed to save contacts"}), 500


# ---------------------------------------------------------------------------
# System State API â€” spend cap, kill-switch, reapproval
# ---------------------------------------------------------------------------
_SYSTEM_STATE_COLLECTION = "system_state"
_SYSTEM_STATE_DOC = "spend"
_SPEND_CAP_MAX = 10_000.0  # upper bound on configurable cap


@app.route("/api/system-state", methods=["GET"])
@login_required
@limiter.limit("60/minute")
def api_system_state_get():
    """Return current spend-cap state (enabled, spend_gbp, cap_gbp, reason, timestamps)."""
    try:
        snap = db.collection(_SYSTEM_STATE_COLLECTION).document(_SYSTEM_STATE_DOC).get()
        if snap.exists:
            state = snap.to_dict()
            # Convert Firestore Timestamps to ISO strings for JSON serialisation
            for key in ("disabled_at", "reset_at"):
                val = state.get(key)
                if hasattr(val, "isoformat"):
                    state[key] = val.isoformat()
        else:
            state = {
                "enabled": True,
                "spend_gbp": 0.0,
                "cap_gbp": float(os.environ.get("SPEND_CAP_GBP", "20.0")),
                "disabled_reason": "",
                "disabled_at": None,
                "reset_at": None,
            }
        return jsonify(state)
    except Exception as exc:
        logger.error(json.dumps({"event": "api_system_state_get_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Service temporarily unavailable"}), 503


@app.route("/api/system-state/reapprove", methods=["POST"])
@login_required
@limiter.limit("10/minute")
def api_system_state_reapprove():
    """Re-enable the system and zero the spend counter after human reapproval."""
    try:
        updates = {
            "enabled": True,
            "spend_gbp": 0.0,
            "disabled_reason": "",
            "disabled_at": None,
            "reset_at": datetime.now(timezone.utc),
        }
        db.collection(_SYSTEM_STATE_COLLECTION).document(_SYSTEM_STATE_DOC).set(updates, merge=True)
        logger.info(json.dumps({"event": "system_state_reapproved_from_dashboard"}))
        return jsonify({"status": "ok", "enabled": True, "spend_gbp": 0.0})
    except Exception as exc:
        logger.error(json.dumps({"event": "api_system_state_reapprove_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Failed to reapprove system"}), 500


@app.route("/api/system-state", methods=["PUT"])
@login_required
@limiter.limit("10/minute")
def api_system_state_put():
    """Update spend cap (cap_gbp). Body: {"cap_gbp": <number>}."""
    data = request.get_json(silent=True) or {}
    cap = data.get("cap_gbp")
    if cap is None:
        return jsonify({"error": "cap_gbp is required"}), 400
    try:
        cap = float(cap)
    except (TypeError, ValueError):
        return jsonify({"error": "cap_gbp must be a number"}), 400
    if not (0 < cap <= _SPEND_CAP_MAX):
        return jsonify({"error": f"cap_gbp must be between 0 and {_SPEND_CAP_MAX}"}), 400

    try:
        db.collection(_SYSTEM_STATE_COLLECTION).document(_SYSTEM_STATE_DOC).set(
            {"cap_gbp": cap}, merge=True
        )
        logger.info(json.dumps({"event": "system_state_cap_updated", "cap_gbp": cap}))
        return jsonify({"status": "ok", "cap_gbp": cap})
    except Exception as exc:
        logger.error(json.dumps({"event": "api_system_state_put_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Failed to update cap"}), 500


# ---------------------------------------------------------------------------
# Scheduled Tasks API â€” read live Cloud Scheduler state + per-task updates
# ---------------------------------------------------------------------------

@app.route("/api/scheduled-tasks", methods=["GET"])
@login_required
def api_scheduled_tasks_get():
    """Return live state for all client-editable scheduled tasks.

    Calls Cloud Scheduler Admin API to read real enable/schedule state.
    Degrades gracefully: if a job is missing, returns deployed=False.
    """
    try:
        from google.cloud import scheduler_v1
        from config.scheduled_tasks import EDITABLE_TASKS, cron_to_schedule, PROJECT, LOCATION
    except ImportError as exc:
        logger.error(json.dumps({"event": "scheduled_tasks_import_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Scheduler client unavailable"}), 503

    try:
        client = scheduler_v1.CloudSchedulerClient()
        parent = f"projects/{PROJECT}/locations/{LOCATION}"
        jobs_by_name: dict = {}
        for job in client.list_jobs(request={"parent": parent}):
            # job.name is the full resource path; basename is the job id
            job_id = job.name.rsplit("/", 1)[-1]
            jobs_by_name[job_id] = job
    except Exception as exc:
        logger.error(json.dumps({"event": "scheduled_tasks_list_error", "error": str(exc)[:300]}))
        return jsonify({"error": "Could not reach Cloud Scheduler"}), 502

    results = []
    for key, meta in EDITABLE_TASKS.items():
        job_id = meta["job"]
        job = jobs_by_name.get(job_id)
        if job is None:
            results.append({
                "key": key,
                "label": meta["label"],
                "agent": meta["agent"],
                "description": meta["description"],
                "enabled": False,
                "schedule": meta["default"],
                "deployed": False,
                "allow_interval": meta["allow_interval"],
                "min_interval_minutes": meta["min_interval_minutes"],
            })
        else:
            enabled = job.state == scheduler_v1.Job.State.ENABLED
            schedule = cron_to_schedule(job.schedule)
            results.append({
                "key": key,
                "label": meta["label"],
                "agent": meta["agent"],
                "description": meta["description"],
                "enabled": enabled,
                "schedule": schedule,
                "deployed": True,
                "allow_interval": meta["allow_interval"],
                "min_interval_minutes": meta["min_interval_minutes"],
            })

    return jsonify(results)


@app.route("/api/scheduled-tasks/<key>", methods=["PUT"])
@login_required
@limiter.limit("60/minute")
def api_scheduled_tasks_put(key: str):
    """Update a single scheduled task: enable/disable and/or change schedule.

    Body JSON:
      enabled: bool
      mode: "time" | "interval"
      -- time mode --
      time: "HH:MM"
      days: [0..6]  (0=Mon, 6=Sun)
      -- interval mode --
      interval_value: int
      interval_unit: "minutes" | "hours"
    """
    try:
        from google.cloud import scheduler_v1
        from google.api_core.exceptions import NotFound, PermissionDenied
        from google.protobuf import field_mask_pb2
        from config.scheduled_tasks import EDITABLE_TASKS, schedule_to_cron, PROJECT, LOCATION, TIMEZONE
    except ImportError as exc:
        logger.error(json.dumps({"event": "scheduled_tasks_import_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Scheduler client unavailable"}), 503

    if key not in EDITABLE_TASKS:
        return jsonify({"error": f"Unknown or non-editable task: {key}"}), 404

    data = request.get_json(silent=True) or {}

    # Validate and build cron string
    spec = {
        "mode": data.get("mode"),
        "time": data.get("time"),
        "days": data.get("days"),
        "interval_value": data.get("interval_value"),
        "interval_unit": data.get("interval_unit"),
    }
    try:
        cron = schedule_to_cron(spec, task_cfg=EDITABLE_TASKS[key])
    except (ValueError, KeyError, TypeError) as exc:
        return jsonify({"error": f"Invalid schedule: {exc}"}), 400

    if "enabled" not in data:
        return jsonify({"error": "enabled field is required"}), 400
    enabled = bool(data["enabled"])

    meta = EDITABLE_TASKS[key]
    job_name = f"projects/{PROJECT}/locations/{LOCATION}/jobs/{meta['job']}"

    try:
        client = scheduler_v1.CloudSchedulerClient()

        # Update cron schedule and timezone
        updated_job = scheduler_v1.Job(
            name=job_name,
            schedule=cron,
            time_zone=TIMEZONE,
        )
        mask = field_mask_pb2.FieldMask(paths=["schedule", "time_zone"])
        client.update_job(request={"job": updated_job, "update_mask": mask})

        # Enable / disable
        if enabled:
            client.resume_job(request={"name": job_name})
        else:
            client.pause_job(request={"name": job_name})

        logger.info(json.dumps({
            "event": "scheduled_task_updated",
            "task": key,
            "enabled": enabled,
            "cron": cron,
        }))
        return jsonify({"status": "ok", "cron": cron, "enabled": enabled})

    except NotFound:
        return jsonify({"error": f"Job not found in Cloud Scheduler: {meta['job']}"}), 404
    except PermissionDenied as exc:
        logger.error(json.dumps({"event": "scheduled_tasks_permission_error", "error": str(exc)[:300]}))
        return jsonify({"error": "Dashboard service account lacks Cloud Scheduler permissions"}), 502
    except Exception as exc:
        logger.error(json.dumps({"event": "scheduled_tasks_update_error", "task": key, "error": str(exc)[:300]}))
        return jsonify({"error": "Failed to update scheduler job"}), 500


@app.route("/api/scheduled-tasks/<key>/trigger", methods=["POST"])
@login_required
@limiter.limit("10/minute")
def api_scheduled_tasks_trigger(key: str):
    """Manually trigger a scheduled task immediately via the MAS /schedule endpoint."""
    from config.scheduled_tasks import EDITABLE_TASKS
    if key not in EDITABLE_TASKS:
        return jsonify({"error": f"Unknown or non-editable task: {key}"}), 404

    try:
        headers = {"Content-Type": "application/json"}
        token = _get_mas_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        resp = http_requests.post(
            f"{Acme_MAS_URL}/schedule",
            json={"task": key},
            headers=headers,
            timeout=30,
        )
    except Exception as exc:
        logger.error(json.dumps({"event": "scheduled_task_trigger_error", "task": key, "error": str(exc)[:200]}))
        return jsonify({"error": "Could not reach agent service"}), 502

    if not resp.ok:
        logger.error(json.dumps({"event": "scheduled_task_trigger_upstream_error", "task": key, "status": resp.status_code}))
        return jsonify({"error": "Agent service returned an error"}), 502

    logger.info(json.dumps({"event": "scheduled_task_triggered", "task": key}))
    return jsonify({"status": "ok"})


# ---------------------------------------------------------------------------
# Knowledge base CRUD (Phase 2)
# ---------------------------------------------------------------------------
@app.route("/api/knowledge/<doc_id>", methods=["PUT"])
@login_required
@limiter.limit("10/hour")
def api_knowledge_edit(doc_id: str):
    """Edit a client_dashboard knowledge entry (re-embeds text)."""
    data = request.get_json(silent=True) or {}
    text = (data.get("text") or "").strip()
    if len(text) < MIN_KNOWLEDGE_LENGTH:
        return jsonify({"error": f"Entry must be at least {MIN_KNOWLEDGE_LENGTH} characters."}), 400
    if len(text) > MAX_KNOWLEDGE_LENGTH:
        return jsonify({"error": f"Entry must be at most {MAX_KNOWLEDGE_LENGTH:,} characters."}), 400

    try:
        ref = db.collection(KNOWLEDGE_COLLECTION).document(doc_id)
        existing = ref.get()
        if not existing.exists:
            return jsonify({"error": "Entry not found"}), 404
        if existing.to_dict().get("source") != "client_dashboard":
            return jsonify({"error": "Only client-added entries can be edited"}), 403
    except Exception as exc:
        logger.error(json.dumps({"event": "knowledge_edit_fetch_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Service temporarily unavailable"}), 503

    try:
        embedding = _generate_embedding(text)
    except Exception as exc:
        logger.error(json.dumps({"event": "knowledge_edit_embed_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Failed to process entry. Please try again."}), 503

    try:
        ref.set({"text": text, "embedding": Vector(embedding)}, merge=True)
        logger.info(json.dumps({"event": "knowledge_edited", "doc_id": doc_id}))
        return jsonify({"status": "ok"})
    except Exception as exc:
        logger.error(json.dumps({"event": "knowledge_edit_write_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Failed to save entry"}), 503


@app.route("/api/knowledge/<doc_id>", methods=["DELETE"])
@login_required
@limiter.limit("20/hour")
def api_knowledge_delete(doc_id: str):
    """Delete a client_dashboard knowledge entry."""
    try:
        ref = db.collection(KNOWLEDGE_COLLECTION).document(doc_id)
        existing = ref.get()
        if not existing.exists:
            return jsonify({"error": "Entry not found"}), 404
        if existing.to_dict().get("source") != "client_dashboard":
            return jsonify({"error": "Only client-added entries can be deleted"}), 403
        ref.delete()
        logger.info(json.dumps({"event": "knowledge_deleted", "doc_id": doc_id}))
        return jsonify({"status": "deleted"})
    except Exception as exc:
        logger.error(json.dumps({"event": "knowledge_delete_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Service temporarily unavailable"}), 503


@app.route("/api/knowledge/preloaded", methods=["GET"])
@login_required
def api_knowledge_preloaded():
    """List pre-loaded (non-client) knowledge entries grouped by source file.

    Returns document metadata: source_file, display_name, type, chunk_count,
    word_count, char_count, and client_created flag.
    """
    try:
        docs = (
            db.collection(KNOWLEDGE_COLLECTION)
            .where("source", "!=", "client_dashboard")
            .limit(200)
            .stream()
        )
        seen: dict[str, dict] = {}
        for doc in docs:
            d = doc.to_dict()
            src = d.get("source", "unknown")
            if src not in seen:
                # Extract display_name: prefer stored field, fall back to _source_to_name()
                display_name = d.get("display_name") or _source_to_name(src)
                seen[src] = {
                    "source_file": src,
                    "display_name": display_name,
                    "type": d.get("type", ""),
                    "chunk_count": 0,
                    "text_content": "",  # Accumulate text for word/char counts
                    "client_created": False,
                }
            seen[src]["chunk_count"] += 1
            # Accumulate text content (sorted by chunk_index)
            text = d.get("text", "")
            if text:
                if seen[src]["text_content"]:
                    seen[src]["text_content"] += "\n" + text
                else:
                    seen[src]["text_content"] = text
            # Check if any chunk is client_created
            if d.get("client_created"):
                seen[src]["client_created"] = True

        # Calculate word and char counts, remove text_content from response
        result_docs = []
        for src, entry in seen.items():
            text = entry.pop("text_content", "")
            entry["word_count"] = len(text.split()) if text else 0
            entry["char_count"] = len(text)
            result_docs.append(entry)

        return jsonify({"documents": result_docs})
    except Exception as exc:
        logger.error(json.dumps({"event": "knowledge_preloaded_error", "error": str(exc)[:200]}))
        return jsonify({"documents": [], "error": "Could not load documents."}), 500


def _source_to_name(source: str) -> str:
    """Convert a source filename to a display name."""
    name_map = {
        "company_overview.txt": "Company Overview",
        "pricing_inventory.txt": "Pricing â€” Inventory",
        "pricing_cleaning.txt": "Pricing â€” Cleaning",
        "pricing_compliance.txt": "Pricing â€” Compliance",
        "terms_and_conditions.txt": "Terms & Conditions",
        "faq.txt": "FAQ",
        "sop_cleaning.txt": "SOP â€” Cleaning",
        "daily_tasks.csv": "Daily Tasks Schedule",
    }
    if source in name_map:
        return name_map[source]
    return source.replace("_", " ").replace(".txt", "").replace(".csv", "").title()


# ---------------------------------------------------------------------------
# Knowledge document CRUD
# ---------------------------------------------------------------------------
@app.route("/api/knowledge/document/<source>", methods=["GET"])
@login_required
@limiter.limit("30/hour")
def api_knowledge_document_get(source):
    """Get a single knowledge document by source.

    Returns: {source, display_name, text, char_count, word_count, client_created}
    404 if document not found.
    """
    try:
        docs = (
            db.collection(KNOWLEDGE_COLLECTION)
            .where("source", "==", source)
            .stream()
        )
        chunks = [doc.to_dict() for doc in docs]
        chunks.sort(key=lambda c: c.get("chunk_index", 0))
        if not chunks:
            return jsonify({"error": "Document not found"}), 404

        # Join chunks with \n
        text_parts = [chunk.get("text", "") for chunk in chunks]
        full_text = "\n".join(text_parts)

        # Determine display_name and client_created from first chunk
        first_chunk = chunks[0]
        display_name = first_chunk.get("display_name") or _source_to_name(source)
        client_created = any(chunk.get("client_created", False) for chunk in chunks)

        return jsonify({
            "source": source,
            "display_name": display_name,
            "text": full_text,
            "char_count": len(full_text),
            "word_count": len(full_text.split()) if full_text else 0,
            "client_created": client_created,
        })
    except Exception as exc:
        logger.error(json.dumps({"event": "knowledge_document_get_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Could not retrieve document"}), 500


@app.route("/api/knowledge/document/<source>", methods=["PUT"])
@login_required
@limiter.limit("10/hour")
def api_knowledge_document_put(source):
    """Update a knowledge document.

    Body: {text: str, display_name?: str}
    Validates text length, marks edited=True if pre-loaded, returns chunk count.
    503 on embedding or write error.
    """
    try:
        data = request.get_json() or {}
        text = data.get("text", "").strip()

        # Validate text length
        if not (MIN_KNOWLEDGE_LENGTH <= len(text) <= MAX_KNOWLEDGE_LENGTH):
            return jsonify({
                "error": f"Text must be between {MIN_KNOWLEDGE_LENGTH} and {MAX_KNOWLEDGE_LENGTH} characters."
            }), 400

        # Fetch one chunk to determine if source is client_created
        existing = list(db.collection(KNOWLEDGE_COLLECTION).where("source", "==", source).limit(1).stream())
        if not existing:
            return jsonify({"error": "Document not found"}), 404

        existing_chunk = existing[0].to_dict()
        is_client_created = existing_chunk.get("client_created", False)
        existing_type = existing_chunk.get("type", "")

        # Get display_name from request, fall back to stored value
        request_display_name = (data.get("display_name") or "").strip()
        display_name = request_display_name or existing_chunk.get("display_name")

        # Determine flags: edited=True if NOT client_created; created=True if IS client_created
        edited = not is_client_created
        created = is_client_created

        # Write document
        try:
            chunk_count = _write_document(
                source,
                text,
                type_=existing_type,
                display_name=display_name,
                edited=edited,
                created=created,
            )
        except Exception as exc:
            logger.error(json.dumps({"event": "knowledge_document_put_write_error", "error": str(exc)[:200]}))
            return jsonify({"error": "Embedding or write error"}), 503

        return jsonify({"status": "ok", "chunks": chunk_count})
    except Exception as exc:
        logger.error(json.dumps({"event": "knowledge_document_put_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Could not update document"}), 503


@app.route("/api/knowledge/document", methods=["POST"])
@login_required
@limiter.limit("10/hour")
def api_knowledge_document_post():
    """Create a new knowledge document.

    Body: {title: str, text: str}
    Validates inputs, generates source ID, returns 201 with source and chunk count.
    503 on embedding or write error.
    """
    try:
        data = request.get_json() or {}
        title = data.get("title", "").strip()
        text = data.get("text", "").strip()

        # Validate title
        if not (1 <= len(title) <= 100):
            return jsonify({"error": "Title must be between 1 and 100 characters."}), 400

        # Validate text length
        if not (MIN_KNOWLEDGE_LENGTH <= len(text) <= MAX_KNOWLEDGE_LENGTH):
            return jsonify({
                "error": f"Text must be between {MIN_KNOWLEDGE_LENGTH} and {MAX_KNOWLEDGE_LENGTH} characters."
            }), 400

        # Generate source ID with entropy to avoid collisions
        source = f"doc_{int(time.time() * 1000)}_{secrets.token_hex(4)}"

        # Write document
        try:
            chunk_count = _write_document(
                source,
                text,
                type_="custom_doc",
                display_name=title,
                created=True,
            )
        except Exception as exc:
            logger.error(json.dumps({"event": "knowledge_document_post_write_error", "error": str(exc)[:200]}))
            return jsonify({"error": "Embedding or write error"}), 503

        return jsonify({"status": "ok", "source": source, "chunks": chunk_count}), 201
    except Exception as exc:
        logger.error(json.dumps({"event": "knowledge_document_post_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Could not create document"}), 503


@app.route("/api/knowledge/document/<source>", methods=["DELETE"])
@login_required
@limiter.limit("20/hour")
def api_knowledge_document_delete(source):
    """Delete a knowledge document.

    Only client_created documents can be deleted. Pre-loaded documents return 403.
    404 if document not found.
    """
    try:
        # Fetch one chunk to verify existence and check client_created flag
        existing = list(db.collection(KNOWLEDGE_COLLECTION).where("source", "==", source).limit(1).stream())
        if not existing:
            return jsonify({"error": "Document not found"}), 404

        existing_chunk = existing[0].to_dict()
        is_client_created = existing_chunk.get("client_created", False)

        # Reject deletion of pre-loaded documents
        if not is_client_created:
            return jsonify({
                "error": "Pre-loaded documents cannot be deleted. Use Re-process to restore originals."
            }), 403

        # Delete all chunks for this source
        chunks_to_delete = db.collection(KNOWLEDGE_COLLECTION).where("source", "==", source).stream()
        for chunk in chunks_to_delete:
            chunk.reference.delete()

        return jsonify({"status": "deleted"})
    except Exception as exc:
        logger.error(json.dumps({"event": "knowledge_document_delete_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Could not delete document"}), 500


# ---------------------------------------------------------------------------
# Knowledge re-ingestion proxy
# ---------------------------------------------------------------------------
@app.route("/api/knowledge/reingest", methods=["POST"])
@login_required
@limiter.limit("2/hour")
def api_knowledge_reingest():
    """Proxy re-ingestion request to agent service, which runs ingest_knowledge.run_ingestion()."""
    try:
        headers = {"Content-Type": "application/json"}
        token = _get_mas_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        resp = http_requests.post(
            f"{Acme_MAS_URL}/api/knowledge/reingest",
            headers=headers,
            timeout=120,
        )
        if resp.status_code == 200:
            logger.info(json.dumps({"event": "knowledge_reingest_triggered"}))
            return jsonify(resp.json()), 200
        logger.error(json.dumps({"event": "reingest_proxy_error", "status": resp.status_code}))
        return jsonify({"error": "Re-ingestion failed on agent service"}), 502
    except Exception as exc:
        logger.error(json.dumps({"event": "reingest_proxy_exc", "error": str(exc)[:200]}))
        return jsonify({"error": "Could not reach agent service"}), 503


# ---------------------------------------------------------------------------
# Password change
# ---------------------------------------------------------------------------
@app.route("/api/settings/password", methods=["PUT"])
@login_required
@limiter.limit("5/hour")
def api_settings_password():
    """Change dashboard password. Updates Secret Manager and in-memory value."""
    global DASHBOARD_PASSWORD
    data = request.get_json(silent=True) or {}
    current = str(data.get("current") or "")
    new_pw = str(data.get("new") or "")
    confirm = str(data.get("confirm") or "")

    if not hmac.compare_digest(current, DASHBOARD_PASSWORD):
        return jsonify({"error": "Current password is incorrect"}), 400
    if len(new_pw) < 8:
        return jsonify({"error": "New password must be at least 8 characters"}), 400
    if new_pw != confirm:
        return jsonify({"error": "Passwords do not match"}), 400
    if new_pw == current:
        return jsonify({"error": "New password must differ from current"}), 400

    if _ON_CLOUD_RUN:
        try:
            from google.cloud import secretmanager_v1
            client = secretmanager_v1.SecretManagerServiceClient()
            parent = f"projects/{PROJECT_ID}/secrets/dashboard-password"
            new_version = client.add_secret_version(
                request={"parent": parent, "payload": {"data": new_pw.encode("utf-8")}}
            )
            # Disable all previous versions
            for v in client.list_secret_versions(request={"parent": parent, "filter": "state:ENABLED"}):
                if v.name != new_version.name:
                    client.disable_secret_version(request={"name": v.name})
        except Exception as exc:
            logger.error(json.dumps({"event": "password_change_secret_error", "error": str(exc)[:200]}))
            return jsonify({"error": "Failed to update password in Secret Manager"}), 500

    DASHBOARD_PASSWORD = new_pw
    logger.info(json.dumps({"event": "dashboard_password_changed"}))
    return jsonify({"status": "ok"})


# ---------------------------------------------------------------------------
# Agent custom rules CRUD
# ---------------------------------------------------------------------------
@app.route("/api/agent-rules", methods=["GET"])
@login_required
def api_agent_rules_get():
    """Return all agent rules grouped by agent name."""
    try:
        from config.agent_rules import get_all_rules
        return jsonify(get_all_rules())
    except Exception as exc:
        logger.error(json.dumps({"event": "api_agent_rules_get_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Service temporarily unavailable"}), 503


@app.route("/api/agent-rules/<agent>", methods=["POST"])
@login_required
@limiter.limit("30/minute")
def api_agent_rules_add(agent: str):
    """Add a rule to an agent. Body: {rule: string}"""
    from config.agent_rules import VALID_AGENTS, MAX_RULES_PER_AGENT, RULE_MIN_LEN, RULE_MAX_LEN, invalidate_cache as invalidate_rules_cache
    if agent not in VALID_AGENTS:
        return jsonify({"error": f"Unknown agent '{agent}'"}), 400

    data = request.get_json(silent=True) or {}
    rule = str(data.get("rule") or "").strip()
    if len(rule) < RULE_MIN_LEN:
        return jsonify({"error": f"Rule must be at least {RULE_MIN_LEN} characters"}), 400
    if len(rule) > RULE_MAX_LEN:
        return jsonify({"error": f"Rule must be at most {RULE_MAX_LEN} characters"}), 400

    try:
        ref = db.collection("agent_config").document("rules")
        doc = ref.get()
        raw = doc.to_dict() if doc.exists else {}
        entries = raw.get(agent, [])
        if not isinstance(entries, list):
            entries = []
        if len(entries) >= MAX_RULES_PER_AGENT:
            return jsonify({"error": f"Maximum {MAX_RULES_PER_AGENT} rules per agent"}), 400
        entries.append({"rule": rule, "created_at": datetime.now(timezone.utc).isoformat()})
        ref.set({agent: entries, "updated_at": datetime.now(timezone.utc).isoformat()}, merge=True)
        invalidate_rules_cache()
        logger.info(json.dumps({"event": "agent_rule_added", "agent": agent}))
        return jsonify({"status": "ok", "count": len(entries)})
    except Exception as exc:
        logger.error(json.dumps({"event": "api_agent_rules_add_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Failed to save rule"}), 500


@app.route("/api/agent-rules/<agent>/<int:index>", methods=["DELETE"])
@login_required
@limiter.limit("30/minute")
def api_agent_rules_delete(agent: str, index: int):
    """Remove a rule by index from an agent."""
    from config.agent_rules import VALID_AGENTS, invalidate_cache as invalidate_rules_cache
    if agent not in VALID_AGENTS:
        return jsonify({"error": f"Unknown agent '{agent}'"}), 400

    try:
        ref = db.collection("agent_config").document("rules")
        doc = ref.get()
        raw = doc.to_dict() if doc.exists else {}
        entries = raw.get(agent, [])
        if not isinstance(entries, list) or index < 0 or index >= len(entries):
            return jsonify({"error": "Rule not found"}), 404
        entries.pop(index)
        ref.set({agent: entries, "updated_at": datetime.now(timezone.utc).isoformat()}, merge=True)
        invalidate_rules_cache()
        logger.info(json.dumps({"event": "agent_rule_deleted", "agent": agent, "index": index}))
        return jsonify({"status": "ok", "count": len(entries)})
    except Exception as exc:
        logger.error(json.dumps({"event": "api_agent_rules_delete_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Failed to delete rule"}), 500


@app.route("/api/whatsapp/messages")
@login_required
def api_whatsapp_messages():
    direction = request.args.get("direction", "all")
    message_type = request.args.get("type", "all")
    limit = min(int(request.args.get("limit", 50)), 200)
    before = request.args.get("before")
    try:
        query = (db.collection("whatsapp_messages")
                   .order_by("timestamp", direction=firestore.Query.DESCENDING))
        if before:
            try:
                dt = datetime.fromisoformat(before)
                query = query.where("timestamp", "<", dt)
            except (ValueError, TypeError):
                pass
        # Fetch extra to handle in-Python filtering and detect has_more
        docs = list(query.limit(500).stream())
        messages = []
        for doc in docs:
            d = doc.to_dict()
            if direction != "all" and d.get("direction") != direction:
                continue
            if message_type != "all" and d.get("message_type") != message_type:
                continue
            ts = d.get("timestamp")
            ts_str = ts.isoformat() if isinstance(ts, datetime) else (str(ts) if ts else None)
            messages.append({
                "id": doc.id,
                "message_id": d.get("message_id", ""),
                "direction": d.get("direction", ""),
                "contact_name": d.get("contact_name"),
                "contact_phone": d.get("contact_phone", ""),
                "conversation_key": d.get("conversation_key", ""),
                "body": d.get("body", ""),
                "message_type": d.get("message_type", ""),
                "status": d.get("status", ""),
                "timestamp": ts_str,
                "metadata": d.get("metadata", {}),
            })
            if len(messages) >= limit + 1:
                break
        has_more = len(messages) > limit
        if has_more:
            messages = messages[:limit]
        next_cursor = messages[-1]["timestamp"] if has_more and messages else None
        return jsonify({"messages": messages, "has_more": has_more, "next_cursor": next_cursor})
    except Exception as exc:
        logger.error(json.dumps({"event": "api_whatsapp_messages_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Failed to load WhatsApp messages"}), 503


@app.route("/api/whatsapp/conversations")
@login_required
def api_whatsapp_conversations():
    try:
        docs = list(
            db.collection("whatsapp_messages")
              .order_by("timestamp", direction=firestore.Query.DESCENDING)
              .limit(500)
              .stream()
        )
        convs = {}
        for doc in docs:
            d = doc.to_dict()
            key = d.get("conversation_key", "")
            if not key:
                continue
            ts = d.get("timestamp")
            ts_str = ts.isoformat() if isinstance(ts, datetime) else (str(ts) if ts else None)
            if key not in convs:
                convs[key] = {
                    "conversation_key": key,
                    "contact_phone": d.get("contact_phone", ""),
                    "contact_name": d.get("contact_name"),
                    "last_message": d.get("body", ""),
                    "last_timestamp": ts_str,
                    "message_count": 0,
                }
            convs[key]["message_count"] += 1
        sorted_convs = sorted(convs.values(), key=lambda c: c["last_timestamp"] or "", reverse=True)
        return jsonify({"conversations": sorted_convs})
    except Exception as exc:
        logger.error(json.dumps({"event": "api_whatsapp_conversations_error", "error": str(exc)[:200]}))
        return jsonify({"error": "Failed to load conversations"}), 503


@app.route("/api/whatsapp/conversations/<key>/messages")
@login_required
def api_whatsapp_conversation_messages(key: str):
    try:
        docs = list(
            db.collection("whatsapp_messages")
              .where("conversation_key", "==", key)
              .stream()
        )
        messages = []
        for doc in docs:
            d = doc.to_dict()
            ts = d.get("timestamp")
            ts_str = ts.isoformat() if isinstance(ts, datetime) else (str(ts) if ts else None)
            messages.append({
                "id": doc.id,
                "direction": d.get("direction", ""),
                "contact_name": d.get("contact_name"),
                "body": d.get("body", ""),
                "message_type": d.get("message_type", ""),
                "status": d.get("status", ""),
                "timestamp": ts_str,
                "metadata": d.get("metadata", {}),
            })
        messages.sort(key=lambda m: m["timestamp"] or "")
        return jsonify({"messages": messages, "has_more": False})
    except Exception as exc:
        logger.error(json.dumps({"event": "api_whatsapp_conv_msgs_error", "key": key[:20], "error": str(exc)[:200]}))
        return jsonify({"error": "Failed to load conversation"}), 503


@app.route("/health")
def health():
    return jsonify({"status": "healthy", "service": "Acme-dashboard"}), 200


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8081))
    debug = os.environ.get("FLASK_DEBUG", "").lower() in ("1", "true")
    # Never enable Werkzeug debugger on Cloud Run â€” it allows arbitrary code execution
    if os.environ.get("K_SERVICE"):
        debug = False
    app.run(host="0.0.0.0", port=port, debug=debug)
