"""HTTP Server Wrapper for Cloud Run.

Provides HTTP interface for Cloud Scheduler and webhook triggers.
Wraps main.py orchestration logic with Flask endpoints.

Threading model:
  Gunicorn spawns multiple threads (--threads 4). A single background
  thread owns the asyncio event loop and all MCP connections. Request
  threads submit coroutines via run_coroutine_threadsafe and block on
  the future, avoiding "event loop already running" crashes.
"""
import faulthandler
faulthandler.enable()  # dump Python traceback to stderr on SIGSEGV/SIGABRT before process dies

import os
import sys
import json
import logging
import asyncio
import threading
import atexit
import hmac
import hashlib
import base64
import queue
import time
import re
import concurrent.futures
from contextlib import AsyncExitStack
from flask import Flask, request, jsonify, Response, stream_with_context
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from werkzeug.middleware.proxy_fix import ProxyFix
from google.api_core.exceptions import AlreadyExists

from main import AcmeOrchestrator, Acme_app, run_scheduled_task, handle_event, get_mcp_health
from servers.circuit_breaker import get_all_breaker_states
from plugins.cost_guard import ERR_MAX_ITERATIONS, ERR_TRANSFER_LOOP, ERR_INPUT_CEILING, ERR_SPEND_CAP
from telemetry import setup_telemetry
from config.dashboard_settings import email_listening_active, get_gmail_push_cooldown
from servers.whatsapp_logging import log_whatsapp_message
import system_state

_SESSION_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")

# Webhook secrets â€” read once at startup
MONDAY_WEBHOOK_SECRET = os.environ.get("MONDAY_WEBHOOK_SECRET", "")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "")
PUBSUB_VERIFICATION_TOKEN = os.environ.get("PUBSUB_VERIFICATION_TOKEN", "")
XERO_WEBHOOK_KEY = os.environ.get("XERO_WEBHOOK_KEY", "")

# Configure logging
logging.basicConfig(
    format='%(message)s',
    level=logging.INFO,
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Cloud Run terminates TLS at its edge and forwards over plain HTTP â€”
# trust its single-hop X-Forwarded-* headers so request.url reflects the
# real https:// URL (required for Twilio signature validation to match).
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)

# Rate limiter â€” no global default; limits applied per route only.
limiter = Limiter(app=app, key_func=get_remote_address, default_limits=[])

# ---------------------------------------------------------------------------
# Lazy Firestore client for scheduler execution dedup (fail-open if unavailable)
# ---------------------------------------------------------------------------
_sched_db = None

# Manual-trigger overlap guard â€” prevents duplicate concurrent runs for same task name.
_manual_running: set = set()
_manual_lock = threading.Lock()

def _sched_firestore():
    """Return a google.cloud.firestore.Client, or None if unavailable."""
    global _sched_db
    if _sched_db is None:
        try:
            from google.cloud import firestore as _fs
            _sched_db = _fs.Client()
        except Exception as _e:
            logger.warning(json.dumps({"event": "sched_firestore_unavailable", "error": str(_e)[:100]}))
    return _sched_db


def _handle_enroute_response(from_phone: str, response: str) -> dict | None:
    """Check if an inbound YES/NO WhatsApp is an en-route button response.

    Returns a result dict if handled (caller should return 200 immediately),
    or None to fall through to normal LLM dispatch.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    db = _sched_firestore()
    if db is None:
        return None

    today = _dt.now(ZoneInfo("Europe/London")).strftime("%Y-%m-%d")
    normalized = from_phone.replace("whatsapp:", "").strip()

    # Try exact phone match, then with/without leading +
    for phone_candidate in [normalized, f"+{normalized.lstrip('+')}",
                             normalized.lstrip("+")]:
        docs = list(
            db.collection("pending_enroute_checks")
            .where("date", "==", today)
            .where("sent", "==", True)
            .where("responded", "==", False)
            .where("clerk_phone", "==", phone_candidate)
            .limit(1)
            .stream()
        )
        if docs:
            break

    if not docs:
        return None  # Not an en-route response â€” let LLM handle it

    doc = docs[0]
    doc_ref = doc.reference
    now_iso = _dt.now(ZoneInfo("UTC")).isoformat()

    from google.cloud import firestore as _fs_mod

    @_fs_mod.transactional
    def _claim_response(transaction, ref):
        snap = ref.get(transaction=transaction)
        if not snap.exists or snap.get("responded"):
            return None
        transaction.update(ref, {
            "responded": True,
            "response": response,
            "responded_at": now_iso,
        })
        return snap.to_dict()

    doc_data = _claim_response(db.transaction(), doc_ref)
    if doc_data is None:
        return None  # raced â€” another request already claimed this

    logger.info(json.dumps({
        "event": "enroute_response",
        "clerk_name": doc_data.get("clerk_name"),
        "response": response,
        "job_address": doc_data.get("job_address"),
    }))

    if response == "no":
        _send_enroute_no_alerts(doc_data)

    return {
        "status": "success",
        "type": "enroute_response",
        "clerk": doc_data.get("clerk_name"),
        "response": response,
    }


def _send_enroute_no_alerts(check_data: dict) -> None:
    """Send WhatsApp alert to Kinza and Jaye when a clerk responds NO to en-route check."""
    from servers.whatsapp_server import (
        send_twilio_api_request,
        TWILIO_ACCOUNT_SID,
        TWILIO_WHATSAPP_NUMBER,
        TWILIO_ADMIN_ALERT_TEMPLATE_SID,
    )

    if not TWILIO_ADMIN_ALERT_TEMPLATE_SID:
        logger.error(json.dumps({
            "event": "enroute_no_alert_skipped",
            "reason": "TWILIO_ADMIN_ALERT_TEMPLATE_SID not set â€” required for business-initiated WhatsApp",
        }))
        return

    admin_phones_str = os.environ.get("ENROUTE_ADMIN_PHONES", "")
    if not admin_phones_str:
        logger.error(json.dumps({
            "event": "enroute_no_alert_skipped",
            "reason": "ENROUTE_ADMIN_PHONES not set",
        }))
        return

    admin_phones = [p.strip() for p in admin_phones_str.split(",") if p.strip()]
    clerk_name = check_data.get("clerk_name", "Unknown clerk")
    job_address = check_data.get("job_address", "unknown location")
    job_time = check_data.get("job_time", "")

    message = (
        f"EN-ROUTE ALERT: {clerk_name} has responded NO to their en-route check "
        f"for job at {job_address}"
        f"{f' (scheduled {job_time})' if job_time else ''}. "
        f"Immediate cover required."
    )

    from_formatted = TWILIO_WHATSAPP_NUMBER
    if not from_formatted.startswith("whatsapp:"):
        from_formatted = f"whatsapp:{from_formatted}"

    for phone in admin_phones:
        to_formatted = phone if phone.startswith("whatsapp:") else f"whatsapp:{phone}"
        try:
            _wa_result = send_twilio_api_request(
                f"Accounts/{TWILIO_ACCOUNT_SID}/Messages.json",
                {
                    "From": from_formatted,
                    "To": to_formatted,
                    "ContentSid": TWILIO_ADMIN_ALERT_TEMPLATE_SID,
                    "ContentVariables": json.dumps({"1": message}),
                },
            )
            logger.info(json.dumps({
                "event": "enroute_no_alert_sent",
                "to_suffix": phone[-4:],
                "clerk_name": clerk_name,
            }))
            log_whatsapp_message(
                direction="outbound",
                message_id=_wa_result.get("sid", ""),
                from_phone=TWILIO_WHATSAPP_NUMBER,
                to_phone=phone,
                body=message,
                message_type="admin_alert",
                status="sent",
                metadata={"alert_type": "enroute_no", "clerk_name": clerk_name, "job_address": job_address},
            )
        except Exception as exc:
            logger.error(json.dumps({
                "event": "enroute_no_alert_failed",
                "to_suffix": phone[-4:],
                "error": str(exc)[:150],
            }))


@app.after_request
def set_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    if os.environ.get("K_SERVICE"):
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response


# --- Background event loop (single thread, shared by all request threads) ---
_loop: asyncio.AbstractEventLoop | None = None
_loop_thread: threading.Thread | None = None
_orchestrator: AcmeOrchestrator | None = None
_stack: AsyncExitStack | None = None
_init_lock = threading.Lock()
_init_done = threading.Event()
_initialized = False
_loop_ready = threading.Event()  # set once the background loop is running

# In-flight fire_async task tracking â€” used by _cleanup to drain before shutdown
_inflight_lock = threading.Lock()
_inflight_count = 0
_inflight_drain = threading.Event()
_inflight_drain.set()  # starts set (zero in-flight = already drained)

# Cloud Run request timeout
_REQUEST_TIMEOUT = 540

# Populated by _validate_secrets_and_config() during orchestrator init
_MISSING_SECRETS: set[str] = set()
_CONFIG_DRIFT: dict[str, tuple[str, str]] = {}  # var -> (expected, actual)

# Secrets that must be non-empty for the service to function correctly
_REQUIRED_SECRETS = [
    "MONDAY_API_KEY", "TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN",
    "XERO_CLIENT_ID", "XERO_CLIENT_SECRET", "GMAIL_TOKEN_JSON", "GMAIL_CRED_JSON",
    "MONDAY_WEBHOOK_SECRET", "PUBSUB_VERIFICATION_TOKEN", "XERO_WEBHOOK_KEY",
    "GOOGLE_MAPS_API_KEY", "ADMIN_PHONE",
]

# Expected values for safety-critical env vars â€” drift from these is logged as WARNING/CRITICAL
_EXPECTED_ENV: dict[str, str] = {
    "XERO_INVOICE_STATUS": "DRAFT",
}


def _start_background_loop() -> None:
    """Entry point for the background event-loop thread."""
    global _loop
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    _loop_ready.set()  # signal that _loop is now assigned
    _loop.run_forever()


def _ensure_loop() -> None:
    """Start the background loop thread if not already running."""
    global _loop_thread
    if _loop_thread is None or not _loop_thread.is_alive():
        _loop_ready.clear()
        _loop_thread = threading.Thread(target=_start_background_loop, daemon=True)
        _loop_thread.start()
        _loop_ready.wait()  # block until _start_background_loop sets the event


def run_async(coro):
    """Submit a coroutine to the background loop; block until done."""
    _ensure_loop()
    future = asyncio.run_coroutine_threadsafe(coro, _loop)
    return future.result(timeout=_REQUEST_TIMEOUT)


_fire_async_sem: asyncio.Semaphore | None = None
_FIRE_ASYNC_MAX = 20


def fire_async(coro):
    """Schedule a coroutine on the background loop without blocking (fire-and-forget).

    Returns 204 to the caller immediately; the coroutine runs on the shared asyncio
    loop after the HTTP response is sent. Safe because --no-cpu-throttling keeps the
    event loop running between requests.

    Tracks in-flight count so _cleanup() can drain tasks before shutdown.
    Bounded by _FIRE_ASYNC_MAX concurrent coroutines to prevent runaway resource use.
    """
    global _inflight_count, _fire_async_sem

    async def _gated():
        global _fire_async_sem
        if _fire_async_sem is None:
            _fire_async_sem = asyncio.Semaphore(_FIRE_ASYNC_MAX)
        async with _fire_async_sem:
            await coro

    _ensure_loop()
    with _inflight_lock:
        _inflight_count += 1
        _inflight_drain.clear()
    future = asyncio.run_coroutine_threadsafe(_gated(), _loop)

    def _on_done(fut):
        global _inflight_count
        with _inflight_lock:
            _inflight_count -= 1
            if _inflight_count <= 0:
                _inflight_drain.set()
        try:
            exc = fut.exception()
            if exc:
                logger.error(json.dumps({
                    "event": "background_task_error",
                    "error": str(exc)[:300],
                    "error_type": type(exc).__name__,
                }))
        except Exception:
            pass

    future.add_done_callback(_on_done)


def _validate_secrets_and_config() -> None:
    """Log missing secrets and config drift from expected values at startup."""
    global _MISSING_SECRETS, _CONFIG_DRIFT
    missing: set[str] = set()
    for key in _REQUIRED_SECRETS:
        if not os.environ.get(key):
            missing.add(key)
            logger.critical(json.dumps({
                "event": "missing_secret_at_startup",
                "var": key,
                "severity": "CRITICAL",
            }))
    _MISSING_SECRETS = missing

    drift: dict[str, tuple[str, str]] = {}
    for var, expected in _EXPECTED_ENV.items():
        actual = os.environ.get(var, "")
        if actual != expected:
            drift[var] = (expected, actual)
            is_dangerous = var == "XERO_INVOICE_STATUS" and actual == "AUTHORISED"
            level = logging.CRITICAL if is_dangerous else logging.WARNING
            logger.log(level, json.dumps({
                "event": "config_drift_detected",
                "var": var,
                "expected": expected,
                "actual": actual,
                "severity": "CRITICAL" if is_dangerous else "WARNING",
            }))
    _CONFIG_DRIFT = drift


async def _init_orchestrator() -> AcmeOrchestrator:
    """Initialize orchestrator with managed AsyncExitStack lifecycle."""
    global _orchestrator, _stack
    setup_telemetry()  # no-op if opentelemetry-exporter-gcp-trace not installed
    _stack = AsyncExitStack()
    await _stack.__aenter__()
    _orchestrator = AcmeOrchestrator()
    await _orchestrator.initialize(_stack)
    _validate_secrets_and_config()
    logger.info(json.dumps({
        "event": "server_startup",
        "status": "orchestrator_initialized",
        "missing_secrets": sorted(_MISSING_SECRETS),
        "config_drift": list(_CONFIG_DRIFT.keys()),
    }))
    # Re-register Gmail push watch on every deploy â€” watch expires every 7 days and
    # without this a redeployment can go silent until the next scheduled renewal job fires.
    try:
        import threading as _threading
        from servers.gmail_server import _register_gmail_watch_sync
        _threading.Thread(target=_register_gmail_watch_sync, daemon=True).start()
    except Exception as _e:
        logger.warning(json.dumps({"event": "gmail_watch_startup_failed", "error": str(_e)}))
        try:
            from servers.alerting import alert_admin, CRITICAL
            alert_admin(CRITICAL, "gmail_watch_startup_failed", str(_e)[:300])
        except Exception:
            pass
    return _orchestrator


def get_orchestrator() -> AcmeOrchestrator:
    """Thread-safe, lazy orchestrator initialization."""
    global _initialized
    if _init_done.is_set():
        return _orchestrator
    with _init_lock:
        if not _initialized:
            run_async(_init_orchestrator())
            _initialized = True
            _init_done.set()
    _init_done.wait(timeout=60)
    return _orchestrator


async def _shutdown() -> None:
    """Gracefully close MCP connections and the AsyncExitStack."""
    global _orchestrator, _stack, _initialized, _init_done
    if _stack is not None:
        try:
            await _stack.__aexit__(None, None, None)
        except Exception as e:
            logger.error(json.dumps({
                "event": "shutdown_error",
                "error": str(e)
            }))
        _stack = None
    _orchestrator = None
    _initialized = False
    _init_done.clear()
    logger.info(json.dumps({
        "event": "server_shutdown",
        "status": "cleanup_complete"
    }))


def _cleanup() -> None:
    """atexit handler â€” drain in-flight tasks, tear down async resources, stop loop."""
    if _inflight_count > 0:
        logger.info(json.dumps({"event": "shutdown_drain_start", "inflight": _inflight_count}))
        drained = _inflight_drain.wait(timeout=120)
        logger.info(json.dumps({"event": "shutdown_drain_done", "clean": drained, "inflight": _inflight_count}))
    if _loop is not None and _loop.is_running():
        future = asyncio.run_coroutine_threadsafe(_shutdown(), _loop)
        try:
            future.result(timeout=10)
        except Exception:
            pass
        _loop.call_soon_threadsafe(_loop.stop)


atexit.register(_cleanup)


@app.route('/api/knowledge/reingest', methods=['POST'])
@limiter.limit("2/hour")
def knowledge_reingest():
    """Re-embed all preloaded knowledge files. Called from dashboard."""
    try:
        from ingest_knowledge import run_ingestion
        result = run_ingestion()
        logger.info(json.dumps({"event": "knowledge_reingested", **result}))
        return jsonify(result), 200
    except Exception as exc:
        logger.error(json.dumps({"event": "reingest_error", "error": str(exc)[:300]}))
        return jsonify({"error": "Re-ingestion failed"}), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Deep health check â€” checks Firestore, MCP subprocesses, spend cap, and config."""
    fs_ok, fs_err = system_state.ping()

    mcp_health = {"monday": False, "gmail": False, "xero": False}
    if _init_done.is_set():
        try:
            mcp_health = get_mcp_health()
        except Exception as _e:
            logger.warning(json.dumps({"event": "health_mcp_check_failed", "error": str(_e)[:200]}))

    state = system_state.get_state()
    spend_cap_active = state.get("enabled", True)

    breaker_states = {}
    try:
        breaker_states = get_all_breaker_states()
    except Exception as _e:
        logger.warning(json.dumps({"event": "health_breaker_check_failed", "error": str(_e)[:200]}))
    open_breakers = [name for name, s in breaker_states.items() if s.get("state") != "closed"]

    checks = {
        "firestore": "ok" if fs_ok else "error",
        "mcp_monday": "ok" if mcp_health.get("monday") else "down",
        "mcp_gmail": "ok" if mcp_health.get("gmail") else "down",
        "mcp_xero": "ok" if mcp_health.get("xero") else "down",
        "mcp_rag": "ok" if mcp_health.get("rag") else "down",
        "mcp_locking": "ok" if mcp_health.get("locking") else "down",
        "mcp_ops": "ok" if mcp_health.get("ops") else "down",
        "mcp_reporting": "ok" if mcp_health.get("reporting") else "down",
        "mcp_whatsapp": "ok" if mcp_health.get("whatsapp") else "down",
        "spend_cap": "active" if spend_cap_active else "latched_off",
        "secrets_ok": len(_MISSING_SECRETS) == 0,
        "config_ok": len(_CONFIG_DRIFT) == 0,
        "circuit_breakers": breaker_states,
    }

    if not _init_done.is_set():
        status = "starting"
    elif not fs_ok or _MISSING_SECRETS or _CONFIG_DRIFT or open_breakers:
        status = "degraded"
    else:
        status = "healthy"

    return jsonify({"status": status, "service": "multi-agent-system", "checks": checks}), 200


@app.route('/ready', methods=['GET'])
def readiness_check():
    """Readiness probe â€” returns 200 only when orchestrator and critical MCP toolsets are warm."""
    if not _init_done.is_set():
        return jsonify({"ready": False, "reason": "orchestrator_initializing"}), 503
    if _MISSING_SECRETS:
        return jsonify({"ready": False, "reason": f"missing_secrets: {sorted(_MISSING_SECRETS)}"}), 503
    try:
        mcp_health = get_mcp_health()
    except Exception as e:
        return jsonify({"ready": False, "reason": f"mcp_check_failed: {e}"}), 503
    _CRITICAL_MCP = {"monday", "gmail", "xero", "whatsapp"}
    _critical_down = [k for k in _CRITICAL_MCP if not mcp_health.get(k)]
    if _critical_down:
        return jsonify({"ready": False, "reason": f"mcp_down: {_critical_down}"}), 503
    return jsonify({"ready": True}), 200


@app.route('/webhook/gmail-push', methods=['POST'])
@limiter.limit("30/minute")
def gmail_push_handler():
    """Handle Gmail push notifications via Google Pub/Sub.

    Pub/Sub sends a push message when Gmail detects new mail.
    Endpoint URL must include ?token=<PUBSUB_VERIFICATION_TOKEN> in the
    Pub/Sub subscription push URL for authentication.

    GCP setup (one-time):
      gcloud pubsub topics create gmail-push-notifications
      # REQUIRED: grant Gmail's system account permission to publish to the topic.
      # Without this, users().watch() returns HTTP 403 "Error sending test message".
      gcloud pubsub topics add-iam-policy-binding gmail-push-notifications \\
        --member="serviceAccount:gmail-api-push@system.gserviceaccount.com" \\
        --role="roles/pubsub.publisher"
      gcloud pubsub subscriptions create gmail-push-sub \\
        --topic=gmail-push-notifications \\
        --push-endpoint=https://<CLOUD_RUN_URL>/webhook/gmail-push?token=<TOKEN> \\
        --push-auth-service-account=<SA>@<PROJECT>.iam.gserviceaccount.com
      # Call gmail.users.watch() to register (see renew_gmail_watch scheduled task)
    """
    # Token check â€” Pub/Sub appends ?token= to the push URL we registered
    if PUBSUB_VERIFICATION_TOKEN:
        token = request.args.get("token", "")
        if not hmac.compare_digest(token, PUBSUB_VERIFICATION_TOKEN):
            logger.warning(json.dumps({"event": "gmail_push_invalid_token"}))
            return jsonify({"error": "forbidden"}), 403
    elif os.environ.get("K_SERVICE"):
        logger.error(json.dumps({"event": "gmail_push_no_token_configured"}))
        return jsonify({"error": "forbidden"}), 403

    try:
        envelope = request.get_json(force=True, silent=True) or {}
        message = envelope.get("message", {})
        data_b64 = message.get("data", "")
        if not data_b64:
            # Pub/Sub keepalive with no payload â€” acknowledge and skip
            return ("", 204)

        notification = json.loads(base64.b64decode(data_b64).decode("utf-8"))
        email_address = notification.get("emailAddress", "")
        history_id = notification.get("historyId", "")

        logger.info(json.dumps({
            "event": "gmail_push_received",
            "emailAddress": (f"{email_address[0]}***@{email_address.rsplit('@',1)[1]}" if "@" in email_address else "***"),
            "historyId": history_id,
        }))

        active, reason = email_listening_active()
        if not active:
            logger.info(json.dumps({"event": "gmail_push_skipped", "reason": reason}))
            return ("", 204)

        try:
            incoming_history_id = int(history_id)
        except (TypeError, ValueError):
            incoming_history_id = None

        # Note: Pub/Sub push delivery doesn't guarantee ordering, so an out-of-order
        # redelivery could in rare cases skip a push here. That's acceptable because
        # each push re-scans the full unprocessed-mail set (not incremental) â€” a
        # skipped push doesn't lose mail, it's just picked up by the next push/sweep.
        if incoming_history_id is not None:
            prior_history_id = system_state.get_last_gmail_history_id()
            if not system_state.try_claim_gmail_history_id(incoming_history_id):
                logger.info(json.dumps({
                    "event": "gmail_push_dedup_skip",
                    "historyId": history_id,
                }))
                return ("", 204)

            # Self-mutation filter: a label write (AI_PROCESSED/AI_BOOKED) from
            # the PRIOR turn bumps historyId and fires a new push here. Inspect
            # the delta since the prior claimed historyId â€” if it's nothing but
            # our own AI label writes (no new messages), skip spawning a turn.
            # Fail-open: any error/uncertainty proceeds with the turn as normal.
            if prior_history_id is not None:
                from servers.gmail_server import _is_gmail_push_self_mutation_only
                if _is_gmail_push_self_mutation_only(prior_history_id, incoming_history_id):
                    logger.info(json.dumps({
                        "event": "gmail_push_self_mutation_skip",
                        "historyId": history_id,
                    }))
                    return ("", 204)

        cooldown_secs = get_gmail_push_cooldown()
        if cooldown_secs > 0:
            allowed, remaining = system_state.try_claim_gmail_dispatch(cooldown_secs)
            if not allowed:
                logger.info(json.dumps({
                    "event": "gmail_push_cooldown_skip",
                    "historyId": history_id,
                    "cooldown_remaining_seconds": round(remaining, 1),
                }))
                return ("", 204)

        orch = get_orchestrator()
        fire_async(handle_event(orch, "email_received", {
            "source": "gmail_push",
            "emailAddress": email_address,
            "historyId": history_id,
            "subject": "New mail notification",
            "sender": email_address,
        }))

        # 204 = acknowledged immediately; Pub/Sub will not redeliver.
        # handle_event continues in the background via fire_async.
        return ("", 204)

    except Exception as e:
        logger.error(json.dumps({
            "event": "gmail_push_error",
            "error": str(e),
            "error_type": type(e).__name__,
        }))
        try:
            from servers.alerting import alert_admin, CRITICAL
            alert_admin(CRITICAL, "gmail_push_processing_failed", str(e)[:300])
        except Exception:
            pass
        # Return 204 to avoid Pub/Sub infinite retry on permanent errors
        return ("", 204)


@app.route('/schedule', methods=['POST'])
@limiter.limit("30/minute")
def schedule_handler():
    """Handle Cloud Scheduler POST requests.

    Expected JSON payload:
    {
        "task": "inbox_sweep",
        "timestamp": "2024-01-01T12:00:00Z"
    }

    Idempotency: Cloud Scheduler retries on 5xx. This handler deduplicates
    retries using the X-CloudScheduler-JobName + X-CloudScheduler-ScheduleTime
    headers (unique per scheduled execution) stored in Firestore/_sched_dedup.
    Terminal errors (max_iterations, transfer_loop, input_ceiling) return 2xx so
    Cloud Scheduler does NOT retry a task that will fail identically every time.
    """
    task_name = ""
    sched_dedup_ref = None  # dedup claim this run owns; freed on failure so a retry can re-run (S1)
    try:
        data = request.get_json(force=True, silent=True) or {}
        task_name = data.get('task', '')

        if not task_name:
            logger.error(json.dumps({"event": "schedule_error", "error": "missing_task_name"}))
            return jsonify({"error": "task name required"}), 400

        if task_name in {"inbox_check", "inbox_sweep"}:
            active, reason = email_listening_active()
            if not active:
                logger.info(json.dumps({"event": "schedule_skipped", "task": task_name, "reason": reason}))
                return jsonify({"status": "skipped", "reason": reason}), 200

        # --- Idempotency guard ---
        # Cloud Scheduler sets these headers on every attempt of the same scheduled execution.
        # Together they form a stable, unique execution ID that survives retries.
        job_name = request.headers.get("X-CloudScheduler-JobName", "")
        sched_time = request.headers.get("X-CloudScheduler-ScheduleTime", "")
        if job_name and sched_time:
            short_job = job_name.rsplit("/", 1)[-1]  # strip project/location prefix
            dedup_id = f"sched_{short_job}_{sched_time}".replace(":", "_").replace("+", "_")
            db = _sched_firestore()
            if db is not None:
                try:
                    ref = db.collection("_sched_dedup").document(dedup_id)
                    # Atomic claim: create() fails with AlreadyExists if another
                    # concurrent Scheduler retry already claimed this execution ID.
                    # A plain read-then-write let two near-simultaneous retries both
                    # pass the existence check before either wrote, double-running the
                    # task. Matches the transactional idempotency guards elsewhere
                    # (system_state claim helpers, create_inventory_booking lock).
                    try:
                        ref.create({"task": task_name, "started_at": time.time()})
                        sched_dedup_ref = ref  # this run now owns the claim
                    except AlreadyExists:
                        logger.info(json.dumps({
                            "event": "schedule_deduplicated",
                            "task": task_name,
                            "schedule_time": sched_time,
                        }))
                        return jsonify({"status": "ok", "task": task_name, "deduplicated": True}), 200
                except Exception as _dedup_exc:
                    logger.warning(json.dumps({
                        "event": "schedule_dedup_error",
                        "error": str(_dedup_exc)[:120],
                    }))
                    # fail-open: proceed without dedup rather than blocking the task

        orch = get_orchestrator()

        # Manual trigger from dashboard (no Cloud Scheduler headers) â€” fire-and-forget so the
        # dashboard button returns immediately instead of timing out waiting for the full task.
        if not job_name:
            with _manual_lock:
                if task_name in _manual_running:
                    return jsonify({"status": "already_running", "task": task_name}), 200
                _manual_running.add(task_name)

            def _run_bg():
                try:
                    from main import _flex_enabled as _fe
                    _fe.set(False)  # manual trigger â€” standard tier (someone's watching)
                    run_async(run_scheduled_task(orch, task_name))
                    system_state.record_task_success(task_name)
                except Exception as _bg_exc:
                    logger.error(json.dumps({
                        "event": "manual_trigger_bg_error",
                        "task": task_name,
                        "error": str(_bg_exc)[:300],
                    }))
                    system_state.record_task_failure(task_name, str(_bg_exc)[:300])
                finally:
                    with _manual_lock:
                        _manual_running.discard(task_name)
            threading.Thread(target=_run_bg, daemon=False, name=f"manual-{task_name}").start()
            logger.info(json.dumps({"event": "manual_trigger_accepted", "task": task_name}))
            return jsonify({"status": "accepted", "task": task_name}), 202

        result = run_async(run_scheduled_task(orch, task_name))
        system_state.record_task_success(task_name)
        return jsonify(result), 200

    except Exception as e:
        err_str = str(e)
        # Quota exhaustion (429 RESOURCE_EXHAUSTED): return 2xx so Cloud Scheduler does NOT
        # retry. Retrying under quota pressure amplifies the loop â€” each retry hits 429,
        # returns 500, Scheduler retries again, cascading until quota recovers. Returning 200
        # breaks the loop; the next scheduled execution retries when quota has recovered.
        # The dedup claim is kept (no delete) so any in-flight Scheduler retry is silently
        # dropped by the AlreadyExists guard rather than firing another LLM run.
        if "RESOURCE_EXHAUSTED" in err_str or (
            "429" in err_str and "quota" in err_str.lower()
        ):
            logger.warning(json.dumps({
                "event": "schedule_quota_exhausted",
                "task": task_name,
                "error": err_str[:300],
            }))
            return jsonify({"status": "ok", "quota_exhausted": True}), 200
        # Terminal (non-retryable) errors: return 2xx so Cloud Scheduler does NOT retry.
        # These errors are structural â€” retrying re-bills a full LLM run for identical failure.
        _TERMINAL = (ERR_MAX_ITERATIONS, ERR_TRANSFER_LOOP, ERR_INPUT_CEILING, ERR_SPEND_CAP)
        if any(s in err_str for s in _TERMINAL):
            logger.warning(json.dumps({
                "event": "schedule_terminal_error",
                "task": task_name,
                "error": err_str[:200],
            }))
            system_state.record_task_failure(task_name, err_str[:300])
            return jsonify({"status": "ok", "terminal_error": err_str[:200]}), 200
        # Non-terminal (transient) failure: free our dedup claim so Cloud Scheduler's
        # retry of this same execution can actually re-run it. Without this the retry
        # hits AlreadyExists and is silently dropped, losing the run entirely (audit
        # round 2, finding S1 â€” observed on financial_close 2026-06-25). A SUCCESSFUL
        # run keeps its claim (still blocks concurrent duplicate retries â€” the C2 fix);
        # a TERMINAL error keeps it too (returned 2xx above â€” retry is pointless).
        if sched_dedup_ref is not None:
            try:
                sched_dedup_ref.delete()
            except Exception:
                pass
        logger.error(json.dumps({
            "event": "schedule_handler_error",
            "task": task_name,
            "error": err_str,
            "error_type": type(e).__name__,
        }))
        try:
            from servers.alerting import alert_admin, HIGH
            alert_admin(HIGH, "schedule_handler_failed", f"{task_name}: {err_str[:200]}")
        except Exception:
            pass
        system_state.record_task_failure(task_name, err_str[:300])
        return jsonify({"error": "Internal error processing scheduled task."}), 500


@app.route('/webhook', methods=['POST'])
@limiter.limit("30/minute")
def webhook_handler():
    """Handle webhook events (Monday.com, Twilio/WhatsApp).

    Security:
    - Monday webhooks must carry a valid X-Monday-Signature HMAC-SHA256 header.
    - Twilio webhooks must carry a valid X-Twilio-Signature header.
    - Requests with neither header are rejected with 401.
    - Monday URL-verification handshake (challenge field) is answered before
      signature checks so registration always succeeds.

    Expected JSON payload (after verification):
    {
        "type": "email_received" | "monday_webhook" | "whatsapp_received",
        "payload": { ... event-specific data ... }
    }
    """
    # 1. Parse raw body first (needed for signature validation)
    #    Twilio posts application/x-www-form-urlencoded, not JSON â€” treat a
    #    non-JSON body as an empty dict instead of rejecting outright, so the
    #    Twilio signature branch below still gets a chance to run.
    raw_body = request.get_data()
    try:
        data = json.loads(raw_body) if raw_body else {}
    except (json.JSONDecodeError, ValueError):
        data = {}

    # 2. Monday URL-verification handshake â€” answer before enforcing signatures
    #    Monday sends {"challenge": "<token>"} once when registering the webhook.
    if isinstance(data, dict) and "challenge" in data:
        return jsonify({"challenge": data["challenge"]}), 200

    # 3. Signature validation
    monday_sig = request.headers.get("X-Monday-Signature", "")
    twilio_sig = request.headers.get("X-Twilio-Signature", "")

    if monday_sig:
        if not MONDAY_WEBHOOK_SECRET:
            logger.error(json.dumps({"event": "webhook_misconfigured", "reason": "MONDAY_WEBHOOK_SECRET not set"}))
            return jsonify({"error": "webhook not configured"}), 500
        expected = "sha256=" + hmac.new(
            MONDAY_WEBHOOK_SECRET.encode(), raw_body, hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected, monday_sig):
            logger.warning(json.dumps({"event": "webhook_invalid_signature", "source": "monday"}))
            return jsonify({"error": "invalid signature"}), 401

    elif twilio_sig:
        if not TWILIO_AUTH_TOKEN:
            logger.error(json.dumps({"event": "webhook_misconfigured", "reason": "TWILIO_AUTH_TOKEN not set"}))
            return jsonify({"error": "webhook not configured"}), 500
        try:
            from twilio.request_validator import RequestValidator
            validator = RequestValidator(TWILIO_AUTH_TOKEN)
            if not validator.validate(request.url, request.form, twilio_sig):
                logger.warning(json.dumps({"event": "webhook_invalid_signature", "source": "twilio"}))
                return jsonify({"error": "invalid signature"}), 401
        except ImportError:
            logger.error(json.dumps({"event": "webhook_misconfigured", "reason": "twilio package not installed"}))
            return jsonify({"error": "webhook not configured"}), 500

        # Twilio's inbound body is form-encoded (From/Body/WaId), not JSON â€”
        # synthesize the same {type, payload} shape the dispatch step below expects.
        data = {
            "type": "whatsapp_received",
            "payload": {
                "from": request.form.get("From", "").replace("whatsapp:", ""),
                "message": request.form.get("Body", ""),
                "message_sid": request.form.get("MessageSid", ""),
                "wa_id": request.form.get("WaId", ""),
            },
        }
        log_whatsapp_message(
            direction="inbound",
            message_id=request.form.get("MessageSid", ""),
            from_phone=request.form.get("From", ""),
            to_phone=request.form.get("To", ""),
            body=request.form.get("Body", ""),
            message_type="inbound",
            status="received",
        )

        # Dedup: Twilio retries the same MessageSid on slow 200 responses
        _msg_sid = request.form.get("MessageSid", "")
        if not system_state.try_claim_whatsapp_message_sid(_msg_sid):
            logger.info(json.dumps({"event": "webhook_whatsapp_dedup", "message_sid": _msg_sid}))
            return jsonify({"status": "duplicate"}), 200

    else:
        logger.warning(json.dumps({"event": "webhook_missing_signature"}))
        return jsonify({"error": "missing signature"}), 401

    # 3b. En-route button response interception â€” handle YES/NO before LLM dispatch
    if isinstance(data, dict) and data.get("type") == "whatsapp_received":
        _body = (data.get("payload", {}).get("message") or "").strip().lower()
        _from = data.get("payload", {}).get("from", "")
        if _body in ("yes", "no"):
            _enroute_result = _handle_enroute_response(_from, _body)
            if _enroute_result is not None:
                return jsonify(_enroute_result), 200

    # 4. Dispatch the verified event
    try:
        event_type = data.get('type', 'unknown') if isinstance(data, dict) else 'unknown'
        payload = data.get('payload', {}) if isinstance(data, dict) else {}

        orch = get_orchestrator()
        result = run_async(handle_event(orch, event_type, payload))
        return jsonify(result), 200

    except Exception as e:
        err = str(e)
        if ERR_SPEND_CAP in err:
            # Return 200 so webhook sender doesn't retry an event that will always be blocked
            logger.warning(json.dumps({"event": "webhook_skipped_spend_cap"}))
            return jsonify({"status": "skipped", "reason": "spend_cap"}), 200
        _TERMINAL = (ERR_MAX_ITERATIONS, ERR_TRANSFER_LOOP, ERR_INPUT_CEILING)
        if any(s in err for s in _TERMINAL) or "Max number of llm calls limit" in err:
            # Structural terminal error â€” retrying would re-bill for the identical outcome
            logger.warning(json.dumps({"event": "webhook_skipped_terminal", "reason": err[:200]}))
            return jsonify({"status": "skipped", "reason": "terminal_error"}), 200
        logger.error(json.dumps({
            "event": "webhook_handler_error",
            "error": err,
            "error_type": type(e).__name__
        }))
        try:
            from servers.alerting import alert_admin, HIGH
            alert_admin(HIGH, "webhook_handler_failed", err[:200])
        except Exception:
            pass
        # Monday and Twilio both retry on 5xx â€” return 200 for non-terminal errors
        # to prevent retry storms. Error is already logged and alerted above.
        _is_monday = bool(request.headers.get("X-Monday-Signature"))
        _is_twilio = bool(request.headers.get("X-Twilio-Signature"))
        if _is_monday or _is_twilio:
            return jsonify({"status": "error_acknowledged"}), 200
        return jsonify({"error": "Internal error processing webhook."}), 500


@app.route('/webhook/billing-alert', methods=['POST'])
@limiter.limit("30/minute")
def billing_alert_handler():
    """Handle GCP Billing Budget Pub/Sub push notifications (Layer 2 spend-cap backstop).

    When GCP total cost (including infra, not just LLM tokens) crosses the configured
    billing budget, this endpoint latches the system off. Complements the in-app LLM
    token meter in CostGuardPlugin (Layer 1) by catching infra costs that Layer 1 misses
    (e.g. runaway Cloud Scheduler loops, Firestore reads, Cloud Run minimum instances).

    GCP setup (one-time â€” see docs/ops/billing-budget-setup.md):
      1. Create/configure a billing budget in GCP console for the project at just below Â£20.
      2. Add a Pub/Sub notification channel pointing to a topic (e.g. `billing-budget-alerts`).
      3. Create a push subscription on that topic:
           gcloud pubsub subscriptions create billing-alert-sub \\
             --topic=billing-budget-alerts \\
             --push-endpoint=https://<CLOUD_RUN_URL>/webhook/billing-alert?token=<PUBSUB_VERIFICATION_TOKEN> \\
             --push-auth-service-account=<SA>@<PROJECT>.iam.gserviceaccount.com
      4. Set PUBSUB_VERIFICATION_TOKEN env var to the same token value.

    The same PUBSUB_VERIFICATION_TOKEN used for Gmail push is reused here â€” only one token
    secret to manage. A different token can be used by setting BILLING_ALERT_TOKEN separately.
    """
    BILLING_ALERT_TOKEN = os.environ.get("BILLING_ALERT_TOKEN") or PUBSUB_VERIFICATION_TOKEN
    if BILLING_ALERT_TOKEN:
        token = request.args.get("token", "")
        if not hmac.compare_digest(token, BILLING_ALERT_TOKEN):
            logger.warning(json.dumps({"event": "billing_alert_invalid_token"}))
            return jsonify({"error": "forbidden"}), 403
    elif os.environ.get("K_SERVICE"):
        logger.error(json.dumps({"event": "billing_alert_no_token_configured"}))
        return jsonify({"error": "forbidden"}), 403

    try:
        envelope = request.get_json(force=True, silent=True) or {}
        message = envelope.get("message", {})
        data_b64 = message.get("data", "")
        if not data_b64:
            return ("", 204)

        import base64
        notification = json.loads(base64.b64decode(data_b64).decode("utf-8"))
        budget_display = notification.get("budgetDisplayName", "")
        cost_amount = notification.get("costAmount", 0)
        budget_amount = notification.get("budgetAmount", 0)
        currency = notification.get("currencyCode", "")

        logger.warning(json.dumps({
            "event": "billing_budget_alert_received",
            "budgetDisplayName": budget_display,
            "costAmount": cost_amount,
            "budgetAmount": budget_amount,
            "currencyCode": currency,
        }))

        # Gate on actual budget breach â€” GCP sends notifications continuously as cost
        # accrues (not only at 100%), so we must not disable on every message.
        # alertThresholdExceeded is a float fraction (e.g. 1.0) sent by GCP when a
        # configured threshold rule fires; fall back to computing costAmount/budgetAmount.
        trigger = float(os.environ.get("BILLING_DISABLE_THRESHOLD", "1.0"))
        threshold_signal = notification.get("alertThresholdExceeded")  # may be None
        try:
            ratio = (float(cost_amount) / float(budget_amount)) if budget_amount else 0.0
        except (TypeError, ValueError):
            ratio = 0.0
        breach = max(float(threshold_signal or 0.0), ratio)

        if breach >= trigger:
            import system_state as _ss
            from datetime import datetime, timezone as _tz
            state = _ss.get_state()
            cooldown_h = float(os.environ.get("BILLING_REAPPROVE_COOLDOWN_HOURS", "12"))
            reset_at = state.get("reset_at")
            now_utc = datetime.now(_tz.utc)
            if (
                reset_at is not None
                and state.get("enabled", False)
                and (now_utc - reset_at).total_seconds() < cooldown_h * 3600
            ):
                # Human manually reapproved within the cooldown window â€” honour the override.
                # Layer-1 in-app token cap still guards against new runaway spend.
                logger.warning(json.dumps({
                    "event": "billing_alert_suppressed_post_reapprove",
                    "costAmount": cost_amount,
                    "budgetAmount": budget_amount,
                    "breach": round(breach, 4),
                    "cooldown_hours": cooldown_h,
                    "reset_at": reset_at.isoformat() if hasattr(reset_at, "isoformat") else str(reset_at),
                    "seconds_since_reapprove": round((now_utc - reset_at).total_seconds()),
                }))
            else:
                _ss.disable("billing_budget")
                logger.warning(json.dumps({
                    "event": "system_disabled_by_billing_alert",
                    "costAmount": cost_amount,
                    "budgetAmount": budget_amount,
                    "breach": round(breach, 4),
                    "trigger": trigger,
                }))
        else:
            logger.info(json.dumps({
                "event": "billing_alert_below_threshold",
                "costAmount": cost_amount,
                "budgetAmount": budget_amount,
                "breach": round(breach, 4),
                "trigger": trigger,
            }))

        return ("", 204)

    except Exception as e:
        logger.error(json.dumps({
            "event": "billing_alert_error",
            "error": str(e),
            "error_type": type(e).__name__,
        }))
        try:
            from servers.alerting import alert_admin, HIGH
            alert_admin(HIGH, "billing_alert_handler_failed", str(e)[:200])
        except Exception:
            pass
        return ("", 204)


async def _handle_xero_invoice_update(invoice_id: str) -> None:
    """Fire-and-forget: flip Monday 'Invoice drafted' â†’ 'Invoice sent by xero' when Xero approves an invoice.

    Called by the /webhook/xero route when Xero notifies us that an invoice was updated.
    Only acts when the invoice moved to AUTHORISED/SENT and a Firestore mapping exists
    (i.e. it was originally created as DRAFT by the AI).
    """
    try:
        from servers.monday_server import _get_booking_firestore, set_invoice_number
        from servers.xero_server import get_xero_headers, _http_client

        # 1. Look up Firestore mapping written by set_invoice_number when DRAFT
        db = _get_booking_firestore()
        doc_ref = db.collection("xero_invoice_map").document(invoice_id)
        doc = await asyncio.to_thread(doc_ref.get)
        if not doc.exists:
            return  # Not a tracked draft invoice
        mapping = doc.to_dict()

        # 2. Fetch current invoice status from Xero
        headers = await get_xero_headers()
        resp = await _http_client.get(
            f"https://api.xero.com/api.xro/2.0/Invoices/{invoice_id}",
            headers=headers,
        )
        if resp.status_code != 200:
            logger.warning(json.dumps({
                "event": "xero_webhook_fetch_failed",
                "invoice_id": invoice_id,
                "http_status": resp.status_code,
            }))
            return
        invoice_data = resp.json().get("Invoices", [{}])[0]
        xero_status = invoice_data.get("Status", "")
        if xero_status not in ("AUTHORISED", "SENT"):
            return  # Not yet approved/sent

        # 3. Fetch online invoice URL (best-effort â€” only available after AUTHORISED)
        online_url = None
        url_resp = await _http_client.get(
            f"https://api.xero.com/api.xro/2.0/Invoices/{invoice_id}/OnlineInvoice",
            headers=await get_xero_headers(),
        )
        if url_resp.status_code == 200:
            online_url = url_resp.json().get("OnlineInvoices", [{}])[0].get("OnlineInvoiceUrl")

        # 4. Flip Monday status to "Invoice sent by xero" and write pay_link
        await set_invoice_number(
            board_id=mapping["board_id"],
            item_id=mapping["item_id"],
            invoice_number=mapping["invoice_number"],
            pay_link=online_url,
            xero_status="AUTHORISED",
        )

        # 5. Update mapping doc so repeat webhook deliveries are idempotent
        await asyncio.to_thread(lambda: doc_ref.update({"xero_status": "AUTHORISED", "sent_at": time.time()}))

        logger.info(json.dumps({
            "event": "xero_webhook_invoice_sent",
            "invoice_id": invoice_id,
            "invoice_number": mapping.get("invoice_number"),
            "item_id": mapping.get("item_id"),
        }))

    except Exception as e:
        logger.error(json.dumps({
            "event": "xero_webhook_handler_error",
            "invoice_id": invoice_id,
            "error": str(e)[:300],
            "error_type": type(e).__name__,
        }))
        try:
            from servers.alerting import alert_admin, HIGH
            alert_admin(HIGH, "xero_invoice_handler_failed", f"invoice {invoice_id}: {str(e)[:200]}")
        except Exception:
            pass


@app.route('/webhook/xero', methods=['POST'])
@limiter.limit("30/minute")
def xero_webhook():
    """Handle Xero webhook events (invoice created/updated).

    Xero signs the raw request body with HMAC-SHA256 using the webhook key shown in
    Xero Developer portal. The base64-encoded digest is sent in x-xero-signature.

    Intent-to-receive (ITR): Xero sends a validation request (empty events array) when
    you first register the webhook. We validate the signature and return 200 to confirm
    ownership. Invalid signature â†’ 401 (required by Xero's ITR protocol).

    Setup (one-time after deploy):
      1. Create the secret: echo -n "<key>" | gcloud secrets create xero-webhook-key --data-file=-
      2. Set XERO_WEBHOOK_KEY secret on Cloud Run and re-deploy.
      3. Xero Developer portal â†’ My Apps â†’ Webhooks â†’ Add webhook
         URL: https://<CLOUD_RUN_URL>/webhook/xero, subscribe to Invoice events.
      4. Xero sends ITR â†’ this endpoint validates signature â†’ webhook goes ACTIVE.
    """
    if not XERO_WEBHOOK_KEY:
        logger.error(json.dumps({"event": "xero_webhook_no_key_configured"}))
        return "", 401

    payload = request.get_data()
    signature = request.headers.get("x-xero-signature", "")
    expected = base64.b64encode(
        hmac.new(XERO_WEBHOOK_KEY.encode(), payload, hashlib.sha256).digest()
    ).decode()
    if not hmac.compare_digest(signature, expected):
        logger.warning(json.dumps({"event": "xero_webhook_invalid_signature"}))
        return "", 401

    try:
        body = json.loads(payload) if payload else {}
    except (json.JSONDecodeError, ValueError):
        return "", 200  # Malformed body â€” still 200 to avoid Xero retry storm

    events = body.get("events", [])
    _xero_db = _sched_firestore()
    _now_window = int(time.time()) // 300  # 5-minute dedup window
    for event in events:
        if event.get("eventCategory") == "INVOICE" and event.get("eventType") == "UPDATE":
            invoice_id = event.get("resourceId", "")
            if not invoice_id:
                continue
            if _xero_db is not None:
                _claim_id = f"{invoice_id}:{_now_window}"
                try:
                    _xero_db.collection("xero_webhook_events").document(_claim_id).create(
                        {"invoice_id": invoice_id, "window": _now_window, "ts": time.time()}
                    )
                except AlreadyExists:
                    logger.info(json.dumps({"event": "xero_webhook_dedup_skipped", "invoice_id": invoice_id}))
                    continue
            fire_async(_handle_xero_invoice_update(invoice_id))

    return "", 200


@app.route('/', methods=['GET'])
def root():
    """Root endpoint - returns service info."""
    return jsonify({
        "service": "multi-agent-system",
        "status": "operational",
        "endpoints": {
            "health": "/health",
            "schedule": "/schedule (POST)",
            "webhook": "/webhook (POST)",
            "chat": "/chat (POST)",
            "chat_stream": "/chat/stream (POST)",
        }
    }), 200


_AGENT_LABELS = {
    "dispatcher": "Dispatcher",
    "finance": "Finance Sentinel",
    "ops": "Ops Controller",
    "system": "System",
}

_CHAT_TIMEOUT = 365
_STREAM_TIMEOUT = 370
_MAX_CHAT_MESSAGE_LENGTH = 5000


@app.route('/chat', methods=['POST'])
@limiter.limit("10/minute")
def chat():
    """Synchronous chat endpoint. Body: {"message": "<text>", "session_id": "<optional>"}."""
    data = request.get_json(silent=True) or {}
    message = (data.get("message") or "").strip()
    if not message:
        return jsonify({"error": "message is required"}), 400
    if len(message) > _MAX_CHAT_MESSAGE_LENGTH:
        return jsonify({"error": f"message too long (max {_MAX_CHAT_MESSAGE_LENGTH} characters)"}), 400

    session_id = (data.get("session_id") or "").strip() or None
    if session_id and not _SESSION_ID_RE.match(session_id):
        return jsonify({"error": "invalid session_id format"}), 400

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

    orch = get_orchestrator()
    if orch is None:
        return jsonify({"error": "Orchestrator not ready"}), 503

    async def _run():
        return await orch.route_and_execute(
            message, session_id=session_id, source_role="dashboard_admin", persist=True,
            images=images,
        )

    _ensure_loop()
    future = asyncio.run_coroutine_threadsafe(_run(), _loop)
    try:
        result_tuple = future.result(timeout=_CHAT_TIMEOUT)
    except concurrent.futures.TimeoutError:
        future.cancel()
        logger.warning(json.dumps({"event": "chat_timeout", "message_len": len(message)}))
        return jsonify({"error": "The agent took too long to respond. Please try again."}), 504
    except RuntimeError as exc:
        err = str(exc)
        if ERR_SPEND_CAP in err:
            return jsonify({"error": "System paused â€” spend cap reached. Reapprove in the dashboard to resume."}), 402
        if ERR_MAX_ITERATIONS in err or "Max number of llm calls limit" in err:
            return jsonify({"error": "The request required too many steps. Try a simpler question."}), 422
        if ERR_TRANSFER_LOOP in err:
            return jsonify({"error": "The agent got stuck routing your request. Please rephrase and try again."}), 422
        if ERR_INPUT_CEILING in err:
            return jsonify({"error": "This request used too much context. Please start a new conversation or simplify your question."}), 422
        logger.error(json.dumps({"event": "chat_runtime_error", "error": err[:300]}))
        return jsonify({"error": "An internal error occurred. Please try again."}), 500
    except ConnectionError as exc:
        logger.error(json.dumps({"event": "chat_connection_error", "error": str(exc)[:300]}))
        return jsonify({"error": "Lost connection to a backend service. Please try again in a moment."}), 502
    except Exception as exc:
        err = str(exc)
        if "RESOURCE_EXHAUSTED" in err or "429" in err:
            logger.warning(json.dumps({"event": "chat_quota_exhausted", "endpoint": "/chat", "error": err[:200]}))
            return jsonify({"error": "The AI service is temporarily busy. Please try again in a moment."}), 429
        logger.error(json.dumps({"event": "chat_error", "error": err[:300], "type": type(exc).__name__}))
        return jsonify({"error": "An unexpected error occurred. Please try again."}), 500

    raw_agent, result_text, returned_session_id = result_tuple
    return jsonify({
        "agent": raw_agent,
        "agent_label": _AGENT_LABELS.get(raw_agent, raw_agent.title()),
        "result": result_text,
        "session_id": returned_session_id,
    })


@app.route('/chat/stream', methods=['POST'])
@limiter.limit("10/minute")
def chat_stream():
    """SSE streaming chat endpoint. Body: {"message": "<text>", "session_id": "<optional>"}."""
    data = request.get_json(silent=True) or {}
    message = (data.get("message") or "").strip()
    if not message:
        return jsonify({"error": "message is required"}), 400
    if len(message) > _MAX_CHAT_MESSAGE_LENGTH:
        return jsonify({"error": f"message too long (max {_MAX_CHAT_MESSAGE_LENGTH} characters)"}), 400

    session_id = (data.get("session_id") or "").strip() or None
    if session_id and not _SESSION_ID_RE.match(session_id):
        return jsonify({"error": "invalid session_id format"}), 400

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

    orch = get_orchestrator()
    if orch is None:
        return jsonify({"error": "Orchestrator not ready"}), 503

    msg_queue: queue.Queue = queue.Queue(maxsize=200)

    async def _run():
        try:
            async for chunk in orch.stream_chat(
                message, session_id=session_id, source_role="dashboard_admin", persist=True,
                images=images,
            ):
                msg_queue.put(chunk)
        except Exception as exc:
            err = str(exc)
            if ERR_SPEND_CAP in err:
                msg_queue.put({"type": "error", "text": "System paused â€” spend cap reached. Reapprove in the dashboard to resume."})
            elif ERR_MAX_ITERATIONS in err or "Max number of llm calls limit" in err:
                msg_queue.put({"type": "error", "text": "The request required too many steps. Try a simpler question."})
            elif ERR_TRANSFER_LOOP in err:
                msg_queue.put({"type": "error", "text": "The agent got stuck routing your request. Please rephrase and try again."})
            elif ERR_INPUT_CEILING in err:
                msg_queue.put({"type": "error", "text": "This request used too much context. Please start a new conversation or simplify your question."})
            elif "RESOURCE_EXHAUSTED" in err or "429" in err:
                logger.warning(json.dumps({"event": "chat_quota_exhausted", "endpoint": "/chat/stream", "error": err[:200]}))
                msg_queue.put({"type": "error", "text": "The AI service is temporarily busy. Please try again in a moment."})
            else:
                logger.error(json.dumps({"event": "chat_stream_error", "error": err[:300], "type": type(exc).__name__}))
                msg_queue.put({"type": "error", "text": "An internal error occurred.", "code": type(exc).__name__})
        finally:
            msg_queue.put(None)

    _ensure_loop()
    future = asyncio.run_coroutine_threadsafe(_run(), _loop)  # keep handle for cancellation

    def generate():
        completed = False
        try:
            start = time.monotonic()
            while True:
                try:
                    item = msg_queue.get(timeout=3)  # short poll so disconnect is noticed quickly
                except queue.Empty:
                    if time.monotonic() - start > _STREAM_TIMEOUT:
                        yield f"data: {json.dumps({'type': 'error', 'text': 'Request timed out.'})}\n\n"
                        completed = True
                        return
                    yield ": heartbeat\n\n"
                    continue
                if item is None:
                    yield f"data: {json.dumps({'type': 'done'})}\n\n"
                    completed = True
                    return
                yield f"data: {json.dumps(item)}\n\n"
        finally:
            # If the generator exits without completing normally (client disconnected / GeneratorExit),
            # cancel the background task so runner.run_async halts and LLM/tool cost stops.
            if not completed:
                future.cancel()

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route('/chat/history', methods=['GET'])
@limiter.limit("30/minute")
def chat_history():
    """Return reconstructed display transcript for a session.
    Query param: session_id (required).
    """
    session_id = (request.args.get("session_id") or "").strip()
    if not session_id:
        return jsonify({"error": "session_id is required"}), 400
    if not _SESSION_ID_RE.match(session_id):
        return jsonify({"error": "invalid session_id format"}), 400

    orch = get_orchestrator()
    if orch is None:
        return jsonify({"error": "Orchestrator not ready"}), 503

    async def _run():
        return await orch.session_service.get_session(
            app_name=Acme_app.name, user_id=orch.USER_ID, session_id=session_id,
        )

    _ensure_loop()
    future = asyncio.run_coroutine_threadsafe(_run(), _loop)
    try:
        session = future.result(timeout=10)
    except Exception:
        return jsonify({"error": "Failed to retrieve session"}), 500

    if session is None:
        return jsonify({"session_id": session_id, "title": "", "messages": []})

    # Reconstruct display transcript from stored ADK events
    messages = []
    for event in session.events:
        try:
            text = None
            if event.content and event.content.parts:
                for part in event.content.parts:
                    t = getattr(part, "text", None)
                    if t:
                        text = t
                        break
            if not text:
                continue
            author = getattr(event, "author", None)
            if event.content and getattr(event.content, "role", None) == "user":
                messages.append({"role": "user", "agent": "user", "agent_label": "You", "text": text})
            elif author in _AGENT_LABELS and event.is_final_response():
                messages.append({"role": "agent", "agent": author, "agent_label": _AGENT_LABELS.get(author, author.title()), "text": text})
        except Exception as exc:
            logger.debug(json.dumps({"event": "history_event_skip", "error": str(exc)[:200]}))

    title = (session.state or {}).get("_chat_title", "")
    return jsonify({"session_id": session_id, "title": title, "messages": messages})


@app.route('/chat/sessions', methods=['GET'])
@limiter.limit("30/minute")
def chat_sessions():
    """Return list of persisted chat sessions (metadata only, newest first)."""
    orch = get_orchestrator()
    if orch is None:
        return jsonify({"error": "Orchestrator not ready"}), 503

    async def _run():
        return await orch.session_service.list_sessions(
            app_name=Acme_app.name, user_id=orch.USER_ID,
        )

    _ensure_loop()
    future = asyncio.run_coroutine_threadsafe(_run(), _loop)
    try:
        result = future.result(timeout=10)
    except Exception:
        return jsonify({"error": "Failed to list sessions"}), 500

    sessions_out = []
    for s in result.sessions:
        if not s.id.startswith("chat-"):
            continue
        stub = s.state or {}
        sessions_out.append({
            "session_id": s.id,
            "title": stub.get("_chat_title", ""),
            "last_active_at": stub.get("last_active_at", ""),
        })
    return jsonify(sessions_out)


@app.route('/chat/sessions/<session_id>', methods=['DELETE'])
@limiter.limit("20/minute")
def delete_chat_session(session_id):
    """Delete a chat session by ID."""
    if not _SESSION_ID_RE.match(session_id):
        return jsonify({"error": "invalid session_id format"}), 400
    if not session_id.startswith("chat-"):
        return jsonify({"error": "can only delete chat sessions"}), 400

    orch = get_orchestrator()
    if orch is None:
        return jsonify({"error": "Orchestrator not ready"}), 503

    async def _run():
        await orch.session_service.delete_session(
            app_name=Acme_app.name, user_id=orch.USER_ID, session_id=session_id,
        )

    _ensure_loop()
    future = asyncio.run_coroutine_threadsafe(_run(), _loop)
    try:
        future.result(timeout=10)
    except Exception:
        return jsonify({"error": "Failed to delete session"}), 500

    return jsonify({"deleted": True}), 200


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    logger.info(json.dumps({
        "event": "server_starting",
        "port": port,
        "mode": "http"
    }))
    app.run(host='0.0.0.0', port=port)
