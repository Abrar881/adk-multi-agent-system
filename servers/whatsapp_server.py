import sys
import os
import re
import json
import time
import random
import functools
import hashlib
import logging
import asyncio
import threading
from datetime import datetime, timezone, timedelta
from collections import deque
import requests as _requests
from mcp.server.fastmcp import FastMCP

from dotenv import load_dotenv
load_dotenv()

try:
    from servers.whatsapp_logging import async_log_whatsapp_message
except ModuleNotFoundError:
    from whatsapp_logging import async_log_whatsapp_message

# --- FIRESTORE (send-dedup lock) ---
try:
    from google.cloud import firestore as _wa_firestore
    from google.cloud.exceptions import AlreadyExists as _WA_AlreadyExists
    _WA_FIRESTORE_AVAILABLE = True
except ImportError:
    _WA_FIRESTORE_AVAILABLE = False

_WA_FIRESTORE_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "")
_WA_LOCK_COLLECTION = "task_locks"
_WA_SEND_LOCK_TTL_H = 12  # hours; covers any same-day scheduler retry window
_wa_db = None


def _get_wa_firestore():
    """Lazy Firestore client (same pattern as gmail_server._get_firestore)."""
    global _wa_db
    if _wa_db is None:
        if not _WA_FIRESTORE_AVAILABLE:
            raise RuntimeError("google-cloud-firestore not installed")
        if not _WA_FIRESTORE_PROJECT:
            raise RuntimeError("GOOGLE_CLOUD_PROJECT env var must be set for WhatsApp send dedup")
        _wa_db = _wa_firestore.Client(project=_WA_FIRESTORE_PROJECT)
    return _wa_db


def _invalidate_wa_firestore() -> None:
    global _wa_db
    _wa_db = None

# --- STRUCTURED LOGGING ---
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

# --- LOGGING CONSTANTS (for Cloud Logging aggregation) ---
SERVICE_NAME = "whatsapp_server"
AGENT_NAME = "whatsapp_server"


def _redact_phone(phone: str) -> str:
    """Return a masked phone number showing only the last 4 digits."""
    if not isinstance(phone, str) or len(phone) < 4:
        return "***"
    return f"***{phone[-4:]}"


# Log fields that contain phone numbers â€” redacted before writing to Cloud Logging.
_PHONE_LOG_FIELDS = frozenset({"to", "from", "contractor_phone", "admin_phone"})


def _log(level: str, tool: str, payload: dict) -> None:
    """Emit structured JSON logs with required aggregation fields.

    Phone-number fields are masked (***NNNN) to prevent PII appearing in logs.
    """
    out = dict(payload or {})
    out.setdefault("service", SERVICE_NAME)
    out.setdefault("agent", AGENT_NAME)
    out.setdefault("tool", tool)
    for field in _PHONE_LOG_FIELDS:
        if field in out and isinstance(out[field], str):
            out[field] = _redact_phone(out[field])
    getattr(logger, level)(json.dumps(out))

# --- CONFIGURATION ---
# Twilio Sandbox Configuration
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID', '')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN', '')
TWILIO_WHATSAPP_NUMBER = os.getenv('TWILIO_WHATSAPP_NUMBER', 'whatsapp:+14155238886')
TWILIO_STATUS_CHECK_TEMPLATE_SID = os.getenv('TWILIO_STATUS_CHECK_TEMPLATE_SID', '')
TWILIO_TIMETABLE_TEMPLATE_SID = os.getenv('TWILIO_TIMETABLE_TEMPLATE_SID', '')
TWILIO_ADMIN_ALERT_TEMPLATE_SID = os.getenv('TWILIO_ADMIN_ALERT_TEMPLATE_SID', '')
TWILIO_API_BASE_URL = "https://api.twilio.com/2010-04-01"

_TEMPLATE_FRIENDLY_NAMES: dict[str, str] = {}

def _get_template_name(sid: str) -> str:
    if not _TEMPLATE_FRIENDLY_NAMES:
        for label, env_val in [
            ("Status Check", TWILIO_STATUS_CHECK_TEMPLATE_SID),
            ("Timetable", TWILIO_TIMETABLE_TEMPLATE_SID),
            ("Admin Alert", TWILIO_ADMIN_ALERT_TEMPLATE_SID),
        ]:
            if env_val:
                _TEMPLATE_FRIENDLY_NAMES[env_val] = label
    return _TEMPLATE_FRIENDLY_NAMES.get(sid, sid[:12] + "â€¦")

# Rate Limiting Configuration (Phase 5 requirement)
# Token bucket: 10 messages per minute, 50 per hour
MAX_MESSAGES_PER_MINUTE = 10
MAX_MESSAGES_PER_HOUR = 50
RATE_LIMIT_MINUTE_WINDOW = 60  # 1 minute in seconds
RATE_LIMIT_HOUR_WINDOW = 3600  # 1 hour in seconds

mcp = FastMCP("AcmeWhatsApp")

def _error_response(*, tool: str, error_code: str, message: str, latency_ms: int | None = None, **details) -> str:
    """Return a standardized machine-readable error payload."""
    payload = {
        "status": "error",
        "error_code": error_code,
        "message": message[:200],
        "tool": tool,
        "service": SERVICE_NAME,
    }
    if latency_ms is not None:
        payload["latency_ms"] = latency_ms
    payload.update(details)
    return json.dumps(payload)

_log("info", "__init__", {
    "event": "server_startup",
    "status": "initialized"
})

# --- RATE LIMITER (Token Bucket Pattern) ---
class RateLimiter:
    """Token bucket rate limiter for WhatsApp messaging."""
    def __init__(self, max_tokens: int, window_seconds: int):
        self.max_tokens = max_tokens
        self.window_seconds = window_seconds
        self.timestamps = deque()
        self._lock = threading.Lock()

    def check_and_add(self) -> tuple[bool, str]:
        """
        Check if action is allowed and add timestamp if so.
        Returns (allowed: bool, message: str)
        """
        with self._lock:
            now = time.time()
            window_label = "minute" if self.window_seconds == 60 else "hour"

            # Remove timestamps outside the window
            while self.timestamps and self.timestamps[0] < now - self.window_seconds:
                self.timestamps.popleft()

            # Check if we're at the limit
            if len(self.timestamps) >= self.max_tokens:
                oldest = self.timestamps[0]
                wait_time = int((oldest + self.window_seconds) - now)
                return False, f"Rate limit exceeded. Max {self.max_tokens} messages per {window_label}. Try again in {wait_time}s."

            # Allow the action
            self.timestamps.append(now)
            return True, f"OK ({len(self.timestamps)}/{self.max_tokens} used this {window_label})"

# Two-tier rate limiting: per-minute and per-hour
whatsapp_rate_limiter_minute = RateLimiter(MAX_MESSAGES_PER_MINUTE, RATE_LIMIT_MINUTE_WINDOW)
whatsapp_rate_limiter_hour = RateLimiter(MAX_MESSAGES_PER_HOUR, RATE_LIMIT_HOUR_WINDOW)

class TwilioTransientError(Exception):
    """Raised on Twilio 429/5xx so @retry can catch and back off."""


try:
    from servers.circuit_breaker import CircuitBreaker as _CircuitBreaker
except ModuleNotFoundError:
    from circuit_breaker import CircuitBreaker as _CircuitBreaker


def _twilio_cb_alert(name, failures):
    try:
        from servers.alerting import alert_admin, CRITICAL
    except ModuleNotFoundError:
        from alerting import alert_admin, CRITICAL
    alert_admin(CRITICAL, f"circuit_breaker_{name}_open",
                f"Twilio API circuit breaker opened after {failures} consecutive failures")


_twilio_breaker = _CircuitBreaker("twilio", failure_threshold=5, recovery_timeout=120.0,
                                  on_open_callback=_twilio_cb_alert)


# --- RETRY DECORATOR ---
def retry(exceptions=(Exception,), tries=4, delay=1, backoff=2, jitter=0.1):
    """Retry with exponential backoff + jitter."""
    def deco(func):
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries > 1:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    sleep_time = _delay + random.uniform(-jitter, jitter)
                    _log("warning", func.__name__, {
                        "event": "retry",
                        "wait_seconds": sleep_time,
                        "error": str(e),
                        "attempts_remaining": _tries - 1
                    })
                    _invalidate_http_session()
                    time.sleep(max(0, sleep_time))
                    _tries -= 1
                    _delay *= backoff
            return func(*args, **kwargs)

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries > 1:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    sleep_time = _delay + random.uniform(-jitter, jitter)
                    _log("warning", func.__name__, {
                        "event": "retry",
                        "wait_seconds": sleep_time,
                        "error": str(e),
                        "attempts_remaining": _tries - 1
                    })
                    await asyncio.sleep(max(0, sleep_time))
                    _tries -= 1
                    _delay *= backoff
            return await func(*args, **kwargs)
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return deco

# --- FIRESTORE RATE LIMITER ---
# Global across all Cloud Run instances. Mirrors gmail_server's email_rate_limits pattern.
_WA_RATE_LIMIT_COLLECTION = "whatsapp_rate_limits"
_WA_RATE_LIMIT_MINUTE = (MAX_MESSAGES_PER_MINUTE, RATE_LIMIT_MINUTE_WINDOW)
_WA_RATE_LIMIT_HOUR = (MAX_MESSAGES_PER_HOUR, RATE_LIMIT_HOUR_WINDOW)


def _check_wa_firestore_rate_limit(max_count: int, window_seconds: int, window_label: str) -> tuple[bool, str, str | None]:
    """Sliding-window rate check backed by Firestore. Returns (allowed, message, doc_id).

    doc_id is the reservation document to delete on send failure (None on deny or error).
    Fail-open: if Firestore is unavailable, returns (True, "firestore_unavailable", None).
    """
    if not _WA_FIRESTORE_AVAILABLE:
        return True, "firestore_unavailable", None
    try:
        db = _get_wa_firestore()
        window_start = datetime.now(timezone.utc).timestamp() - window_seconds
        window_start_dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
        col = db.collection(_WA_RATE_LIMIT_COLLECTION)
        count_query = col.where("created_at", ">=", window_start_dt).where("window", "==", window_label)
        count = sum(1 for _ in count_query.stream())
        if count >= max_count:
            wait_s = int(window_seconds - (datetime.now(timezone.utc).timestamp() - window_start))
            return False, f"Rate limit exceeded. Max {max_count} per {window_label}. Try again in {wait_s}s.", None
        # Reserve a slot
        doc_ref = col.document()
        doc_ref.set({
            "created_at": datetime.now(timezone.utc),
            "window": window_label,
        })
        return True, f"ok ({count + 1}/{max_count} used this {window_label})", doc_ref.id
    except Exception as exc:
        logger.warning(json.dumps({
            "event": "wa_firestore_rate_limit_error",
            "window": window_label,
            "error": str(exc)[:200],
        }))
        return True, "firestore_error_fail_open", None


def _refund_wa_rate_limit(doc_id: str | None) -> None:
    """Delete a rate-limit reservation doc if the send subsequently fails."""
    if not doc_id:
        return
    db = _get_wa_firestore()
    if db is None:
        return
    try:
        db.collection(_WA_RATE_LIMIT_COLLECTION).document(doc_id).delete()
    except Exception:
        pass


# --- HELPER FUNCTIONS ---

def check_rate_limits() -> tuple[bool, str]:
    """Check per-minute and per-hour rate limits.

    Fast-path: check in-memory limiter first (avoids Firestore read when local instance
    is already near limit). If passes, check Firestore global limiter for cross-instance
    enforcement. Fail-open on Firestore errors.

    Returns (allowed: bool, message: str).
    """
    # Fast-path: in-memory check (catches local-instance saturation without Firestore round-trip)
    allowed_minute, msg_minute = whatsapp_rate_limiter_minute.check_and_add()
    if not allowed_minute:
        return False, msg_minute

    allowed_hour, msg_hour = whatsapp_rate_limiter_hour.check_and_add()
    if not allowed_hour:
        if whatsapp_rate_limiter_minute.timestamps:
            whatsapp_rate_limiter_minute.timestamps.pop()
        return False, msg_hour

    # Global Firestore check (cross-instance enforcement)
    fs_ok_min, fs_msg_min, _doc_min = _check_wa_firestore_rate_limit(*_WA_RATE_LIMIT_MINUTE, "minute")
    if not fs_ok_min:
        # Roll back in-memory additions
        if whatsapp_rate_limiter_minute.timestamps:
            whatsapp_rate_limiter_minute.timestamps.pop()
        if whatsapp_rate_limiter_hour.timestamps:
            whatsapp_rate_limiter_hour.timestamps.pop()
        return False, fs_msg_min

    fs_ok_hr, fs_msg_hr, _doc_hr = _check_wa_firestore_rate_limit(*_WA_RATE_LIMIT_HOUR, "hour")
    if not fs_ok_hr:
        _refund_wa_rate_limit(_doc_min)
        if whatsapp_rate_limiter_minute.timestamps:
            whatsapp_rate_limiter_minute.timestamps.pop()
        if whatsapp_rate_limiter_hour.timestamps:
            whatsapp_rate_limiter_hour.timestamps.pop()
        return False, fs_msg_hr

    return True, f"{msg_minute}; {msg_hour}"

_tl = threading.local()


def _get_http_session() -> _requests.Session:
    """Return a per-thread requests.Session (thread-safe connection pooling)."""
    if not hasattr(_tl, "session") or _tl.session is None:
        _tl.session = _requests.Session()
    return _tl.session


def _invalidate_http_session() -> None:
    if hasattr(_tl, "session"):
        _tl.session = None

# --- PER-SEND IDEMPOTENCY LOCK (scheduler-replay dedup) ---

def _wa_acquire_send_transaction(transaction, ref, now, expires_at, key):
    """Transactional helper: atomically check expiry and overwrite. Returns True=acquired.
    Applied as @_wa_firestore.transactional at call site if available.
    """
    snap = ref.get(transaction=transaction)
    if snap.exists:
        data = snap.to_dict()
        ea = data.get("expires_at")
        if hasattr(ea, "to_pydatetime"):
            ea = ea.to_pydatetime()
        if ea and ea > now:
            _log("info", "_try_claim_send", {
                "event": "wa_send_lock_busy",
                "key": key,
                "expires_at": ea.isoformat() if hasattr(ea, "isoformat") else str(ea),
            })
            return False
    transaction.set(ref, {"owner": "whatsapp_send", "created_at": now, "expires_at": expires_at})
    return True


def _try_claim_send(key: str) -> bool:
    """Attempt to claim a send slot for this key (recipient+template+date).

    Returns True if acquired (caller should send).
    Returns False if another run already sent this (caller should skip).
    Fails OPEN on any error â€” never blocks a real send due to lock infra failure.

    Key format: wasend:<tool>:<recipient_last10>:<YYYY-MM-DD> (London date)
    TTL: _WA_SEND_LOCK_TTL_H hours â€” covers same-day scheduler retry window.
    Released explicitly on send failure so a legitimate retry can resend.
    """
    if not _WA_FIRESTORE_AVAILABLE:
        return True  # fail open â€” no Firestore, skip dedup
    try:
        db = _get_wa_firestore()
        ref = db.collection(_WA_LOCK_COLLECTION).document(key)
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(hours=_WA_SEND_LOCK_TTL_H)
        try:
            ref.create({"owner": "whatsapp_send", "created_at": now, "expires_at": expires_at})
            return True  # acquired
        except _WA_AlreadyExists:
            pass  # fall through to transaction path
        tx_fn = _wa_firestore.transactional(_wa_acquire_send_transaction)
        transaction = db.transaction()
        return tx_fn(transaction, ref, now, expires_at, key)
    except Exception as e:
        _invalidate_wa_firestore()
        _log("warning", "_try_claim_send", {
            "event": "wa_send_lock_error",
            "key": key,
            "error": str(e),
        })
        return True  # fail open


def _release_send_claim(key: str) -> None:
    """Delete the send claim so a legitimate retry can resend after a failure.

    Called on send failure only â€” success leaves the claim to expire by TTL.
    Fails silently (fire-and-forget).
    """
    if not _WA_FIRESTORE_AVAILABLE:
        return
    try:
        db = _get_wa_firestore()
        db.collection(_WA_LOCK_COLLECTION).document(key).delete()
    except Exception as e:
        _invalidate_wa_firestore()
        _log("warning", "_release_send_claim", {
            "event": "wa_send_release_error",
            "key": key,
            "error": str(e),
        })


def _wa_send_key(tool_key: str, phone: str) -> str:
    """Build a Firestore dedup key for a WhatsApp send.

    Key: wasend:<tool_key>:<phone_suffix>:<YYYY-MM-DD> (London date)
    phone_suffix = last 10 chars of phone (avoids PII in doc IDs, still unique per recipient).
    """
    from zoneinfo import ZoneInfo
    today = datetime.now(ZoneInfo("Europe/London")).strftime("%Y-%m-%d")
    suffix = (phone or "")[-10:].replace("+", "").replace(" ", "")
    return f"wasend:{tool_key}:{suffix}:{today}"


def _normalize_phone(phone: str) -> str:
    """Ensure phone number has E.164 '+' prefix for Twilio.

    Monday.com phone columns return digits without '+' (e.g. '393283118945').
    Twilio requires E.164 format (e.g. '+393283118945') or returns error 21656.
    Idempotent â€” '+447xxx' stays '+447xxx'.
    """
    phone = phone.strip()
    if not phone:
        return phone
    if phone.startswith('whatsapp:'):
        phone = phone[len('whatsapp:'):]
    phone = re.sub(r'[\s\-\(\)]', '', phone)
    if phone and not phone.startswith('+'):
        phone = f'+{phone}'
    return phone


@retry(exceptions=(_requests.exceptions.ConnectionError, _requests.exceptions.Timeout, TwilioTransientError), tries=3, delay=1, backoff=2)
def send_twilio_api_request(endpoint: str, payload: dict) -> dict:
    """
    Send request to Twilio WhatsApp API.
    Raises TwilioTransientError on 429/5xx (retried); Exception on 4xx (not retried).
    """
    if not _twilio_breaker.allow_request():
        raise TwilioTransientError("twilio circuit breaker open â€” API unavailable")

    url = f"{TWILIO_API_BASE_URL}/{endpoint}"

    # Twilio uses Basic Auth with Account SID and Auth Token
    auth = (TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

    # Twilio expects form-encoded data, not JSON
    try:
        response = _get_http_session().post(url, auth=auth, data=payload, timeout=30)
    except (_requests.exceptions.ConnectionError, _requests.exceptions.Timeout):
        _twilio_breaker.record_failure()
        raise

    if response.status_code not in (200, 201):
        error_data = response.json() if response.headers.get('content-type', '').startswith('application/json') else {}
        error_msg = error_data.get('message', response.text)
        error_code = error_data.get('code', '')
        more_info = error_data.get('more_info', '')
        msg = f"Twilio API error (HTTP {response.status_code}): {error_msg} [code={error_code}] [info={more_info}]"
        # Transient errors â€” retry; permanent errors â€” fail immediately
        if response.status_code in (429, 500, 502, 503):
            _twilio_breaker.record_failure()
            raise TwilioTransientError(msg)
        raise Exception(msg)

    _twilio_breaker.record_success()
    return response.json()

# --- TOOLS ---

@mcp.tool()
async def send_whatsapp_message(to_phone: str, message_text: str) -> str:
    """
    Send a WhatsApp text message via Twilio.
    Rate limited to 10 messages per minute, 50 per hour.

    IMPORTANT: Only works within the 24-hour WhatsApp session window (recipient must
    have messaged us in the last 24 hours). For business-initiated messages to contacts
    who haven't messaged recently, use send_whatsapp_template or send_admin_alert instead
    â€” free-text Body messages will fail with Twilio error 63016 outside the 24h window.

    Args:
        to_phone: Recipient phone number in international format (e.g., +447123456789)
                  Will be automatically prefixed with 'whatsapp:' for Twilio
        message_text: Text message to send

    Returns:
        JSON with status and message ID
    """
    start_time = time.time()

    # Validate credentials before consuming rate limit
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "send_whatsapp_message", {
            "event": "send_whatsapp_message",
            "status": "config_error",
            "to": to_phone,
            "error": "Twilio credentials not configured",
            "latency_ms": latency_ms
        })
        return _error_response(tool="send_whatsapp_message", error_code="config_error", message="Twilio credentials not configured. Set TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN.", latency_ms=latency_ms)

    # Rate limit check (Phase 5 requirement)
    allowed, rate_msg = check_rate_limits()
    if not allowed:
        _log("warning", "send_whatsapp_message", {
            "event": "send_whatsapp_message",
            "status": "rate_limit_exceeded",
            "to": to_phone,
            "message": rate_msg
        })
        return _error_response(tool="send_whatsapp_message", error_code="rate_limit_exceeded", message=rate_msg)

    # Dedup: prevent duplicate sends on LLM retry/replay
    _body_hash = hashlib.sha256(f"{to_phone}:{message_text}".encode()).hexdigest()[:16]
    _send_key = _wa_send_key(f"msg:{_body_hash}", to_phone)
    if not _try_claim_send(_send_key):
        _log("info", "send_whatsapp_message", {
            "event": "send_whatsapp_message",
            "status": "skipped_duplicate",
            "to": to_phone,
        })
        return json.dumps({
            "status": "skipped_duplicate",
            "message": f"Duplicate message to {to_phone} suppressed",
            "to": to_phone,
        })

    try:
        to_formatted = f'whatsapp:{_normalize_phone(to_phone)}'
        from_formatted = TWILIO_WHATSAPP_NUMBER if TWILIO_WHATSAPP_NUMBER.startswith('whatsapp:') else f'whatsapp:{TWILIO_WHATSAPP_NUMBER}'

        # Prepare Twilio API payload (form-encoded)
        payload = {
            "From": from_formatted,
            "To": to_formatted,
            "Body": message_text
        }
        
        # Send via Twilio API (in thread â€” requests.post is synchronous)
        endpoint = f"Accounts/{TWILIO_ACCOUNT_SID}/Messages.json"
        result = await asyncio.to_thread(send_twilio_api_request, endpoint, payload)

        message_id = result.get('sid', 'unknown')

        latency_ms = int((time.time() - start_time) * 1000)
        _log("info", "send_whatsapp_message", {
            "event": "send_whatsapp_message",
            "status": "success",
            "message_id": message_id,
            "to": to_formatted,
            "from": from_formatted,
            "message_length": len(message_text),
            "rate_limit_status": rate_msg,
            "latency_ms": latency_ms,
            "timestamp_utc": datetime.now(timezone.utc).isoformat()
        })
        await async_log_whatsapp_message(
            direction="outbound", message_id=message_id,
            from_phone=TWILIO_WHATSAPP_NUMBER, to_phone=to_phone,
            body=message_text, message_type="free_text", status="sent",
        )

        return json.dumps({
            "status": "success",
            "message_id": message_id,
            "to": to_formatted,
            "from": from_formatted,
            "message": f"WhatsApp message sent successfully via Twilio (SID: {message_id})",
            "rate_limit_status": rate_msg,
            "timestamp_utc": datetime.now(timezone.utc).isoformat()
        })
        
    except ValueError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "send_whatsapp_message", {
            "event": "send_whatsapp_message",
            "status": "config_error",
            "to": to_phone,
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="send_whatsapp_message", error_code="config_error", message=str(e), latency_ms=latency_ms)
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "send_whatsapp_message", {
            "event": "send_whatsapp_message",
            "status": "api_error",
            "to": to_phone,
            "error": str(e),
            "latency_ms": latency_ms
        })
        _release_send_claim(_send_key)  # allow legitimate retry
        return _error_response(tool="send_whatsapp_message", error_code="api_error", message=f"Twilio API error: {str(e)}", latency_ms=latency_ms)

@mcp.tool()
async def send_whatsapp_template(to_phone: str, template_name: str, template_language: str = "en", template_params: str = "{}") -> str:
    """
    Send a WhatsApp template message via Twilio (Content Template API).
    Rate limited to 10 messages per minute, 50 per hour.

    Note: Twilio Sandbox has limited template support. For production, use approved Content Templates.

    Args:
        to_phone: Recipient phone number in international format
        template_name: Twilio Content SID or friendly name
        template_language: Template language code (default: en)
        template_params: JSON object of parameter values for template placeholders (default: {})

    Returns:
        JSON with status and message ID
    """
    start_time = time.time()

    # Validate credentials before consuming rate limit
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "send_whatsapp_template", {
            "event": "send_whatsapp_template",
            "status": "validation_error",
            "to": to_phone,
            "template": template_name,
            "error": "Twilio credentials not configured",
            "latency_ms": latency_ms
        })
        return _error_response(tool="send_whatsapp_template", error_code="validation_error", message="Twilio credentials not configured. Set TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN.", latency_ms=latency_ms)

    # Rate limit check
    allowed, rate_msg = check_rate_limits()
    if not allowed:
        _log("warning", "send_whatsapp_template", {
            "event": "send_whatsapp_template",
            "status": "rate_limit_exceeded",
            "to": to_phone,
            "template": template_name,
            "message": rate_msg
        })
        return _error_response(tool="send_whatsapp_template", error_code="rate_limit_exceeded", message=rate_msg)

    # Idempotency: skip if this recipient already received this template today
    # (guards against Cloud Scheduler whole-task retry after partial completion)
    _send_key = _wa_send_key(f"template:{template_name}", to_phone)
    if not _try_claim_send(_send_key):
        _log("info", "send_whatsapp_template", {
            "event": "send_whatsapp_template",
            "status": "skipped_duplicate",
            "to": to_phone,
            "template": template_name,
            "key": _send_key,
        })
        return json.dumps({
            "status": "skipped_duplicate",
            "message": f"Template {template_name} already sent to {to_phone} today",
            "to": to_phone,
            "template": template_name,
        })

    try:
        # Parse template parameters
        try:
            params = json.loads(template_params)
            if not isinstance(params, dict):
                raise ValueError("template_params must be a JSON object for Twilio")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid template_params JSON: {str(e)}")
        
        to_formatted = f'whatsapp:{_normalize_phone(to_phone)}'
        from_formatted = TWILIO_WHATSAPP_NUMBER if TWILIO_WHATSAPP_NUMBER.startswith('whatsapp:') else f'whatsapp:{TWILIO_WHATSAPP_NUMBER}'

        # Prepare Twilio Content Template payload
        payload = {
            "From": from_formatted,
            "To": to_formatted,
            "ContentSid": template_name,
            "ContentVariables": json.dumps(params) if params else "{}"
        }
        
        # Send via Twilio API (in thread â€” requests.post is synchronous)
        endpoint = f"Accounts/{TWILIO_ACCOUNT_SID}/Messages.json"
        result = await asyncio.to_thread(send_twilio_api_request, endpoint, payload)

        message_id = result.get('sid', 'unknown')

        latency_ms = int((time.time() - start_time) * 1000)
        _log("info", "send_whatsapp_template", {
            "event": "send_whatsapp_template",
            "status": "success",
            "message_id": message_id,
            "to": to_formatted,
            "template": template_name,
            "rate_limit_status": rate_msg,
            "latency_ms": latency_ms,
            "timestamp_utc": datetime.now(timezone.utc).isoformat()
        })
        await async_log_whatsapp_message(
            direction="outbound", message_id=message_id,
            from_phone=TWILIO_WHATSAPP_NUMBER, to_phone=to_phone,
            body=f"[{_get_template_name(template_name)}] {', '.join(str(v) for v in params.values())}" if params else f"[{_get_template_name(template_name)}]", message_type="template", status="sent",
            metadata={"template_sid": template_name, "template_language": template_language},
        )

        return json.dumps({
            "status": "success",
            "message_id": message_id,
            "to": to_formatted,
            "template": template_name,
            "message": f"WhatsApp template message sent successfully via Twilio (SID: {message_id})",
            "rate_limit_status": rate_msg,
            "timestamp_utc": datetime.now(timezone.utc).isoformat()
        })
        
    except ValueError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _release_send_claim(_send_key)
        _log("error", "send_whatsapp_template", {
            "event": "send_whatsapp_template",
            "status": "validation_error",
            "to": to_phone,
            "template": template_name,
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="send_whatsapp_template", error_code="validation_error", message=str(e), latency_ms=latency_ms)
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _release_send_claim(_send_key)
        _log("error", "send_whatsapp_template", {
            "event": "send_whatsapp_template",
            "status": "api_error",
            "to": to_phone,
            "template": template_name,
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="send_whatsapp_template", error_code="api_error", message=f"Twilio API error: {str(e)}", latency_ms=latency_ms)

@mcp.tool()
def get_whatsapp_status() -> str:
    """
    Get current WhatsApp server status and rate limit usage.
    
    Returns:
        JSON with server status and rate limit information
    """
    start_time = time.time()
    
    try:
        # Calculate current rate limit usage
        now = time.time()
        
        # Minute window usage
        minute_timestamps = [t for t in whatsapp_rate_limiter_minute.timestamps if t >= now - RATE_LIMIT_MINUTE_WINDOW]
        minute_used = len(minute_timestamps)
        minute_available = MAX_MESSAGES_PER_MINUTE - minute_used
        
        # Hour window usage
        hour_timestamps = [t for t in whatsapp_rate_limiter_hour.timestamps if t >= now - RATE_LIMIT_HOUR_WINDOW]
        hour_used = len(hour_timestamps)
        hour_available = MAX_MESSAGES_PER_HOUR - hour_used
        
        # Configuration status
        config_ok = bool(TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN)
        
        status = {
            "status": "success",
            "server_status": "healthy" if config_ok else "misconfigured",
            "configuration": {
                "account_sid_set": bool(TWILIO_ACCOUNT_SID),
                "auth_token_set": bool(TWILIO_AUTH_TOKEN),
                "whatsapp_number": TWILIO_WHATSAPP_NUMBER,
                "api_provider": "twilio_production"
            },
            "rate_limits": {
                "per_minute": {
                    "limit": MAX_MESSAGES_PER_MINUTE,
                    "used": minute_used,
                    "available": minute_available,
                    "usage_percent": round((minute_used / MAX_MESSAGES_PER_MINUTE) * 100, 1)
                },
                "per_hour": {
                    "limit": MAX_MESSAGES_PER_HOUR,
                    "used": hour_used,
                    "available": hour_available,
                    "usage_percent": round((hour_used / MAX_MESSAGES_PER_HOUR) * 100, 1)
                }
            },
            "timestamp_utc": datetime.now(timezone.utc).isoformat()
        }
        
        latency_ms = int((time.time() - start_time) * 1000)
        _log("info", "get_whatsapp_status", {
            "event": "get_whatsapp_status",
            "status": "success",
            "server_status": status["server_status"],
            "minute_usage": f"{minute_used}/{MAX_MESSAGES_PER_MINUTE}",
            "hour_usage": f"{hour_used}/{MAX_MESSAGES_PER_HOUR}",
            "latency_ms": latency_ms
        })
        
        return json.dumps(status)
        
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "get_whatsapp_status", {
            "event": "get_whatsapp_status",
            "status": "error",
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="get_whatsapp_status", error_code="api_error", message=f"Status check error: {str(e)}", latency_ms=latency_ms)

@mcp.tool()
async def send_contractor_status_check(
    contractor_phone: str,
    contractor_name: str,
    job_address: str
) -> str:
    """
    Send a status check WhatsApp message to a contractor via the rate-limited Twilio path.
    Per AGENT GUIDE Fleet Mgmt: "Status Check: Are you enroute to {Job Address}? Tap YES or NO."

    Args:
        contractor_phone: Contractor's phone number in international format
        contractor_name: Contractor's name for personalisation
        job_address: The job address they should be heading to

    Returns:
        JSON with message status and ID
    """
    start_time = time.time()

    # Validate credentials before consuming rate limit
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "send_contractor_status_check", {
            "event": "send_contractor_status_check",
            "status": "config_error",
            "contractor_phone": contractor_phone,
            "error": "Twilio credentials not configured",
            "latency_ms": latency_ms
        })
        return _error_response(tool="send_contractor_status_check", error_code="config_error", message="Twilio credentials not configured. Set TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN.", latency_ms=latency_ms)

    allowed, rate_msg = check_rate_limits()
    if not allowed:
        _log("warning", "send_contractor_status_check", {
            "event": "send_contractor_status_check",
            "status": "rate_limit_exceeded",
            "to": contractor_phone,
            "message": rate_msg
        })
        return _error_response(tool="send_contractor_status_check", error_code="rate_limit_exceeded", message=rate_msg)

    # Idempotency: skip if this recipient already received a status check today
    # (guards against Cloud Scheduler whole-task retry after partial completion)
    _send_key = _wa_send_key("status_check", contractor_phone)
    if not _try_claim_send(_send_key):
        _log("info", "send_contractor_status_check", {
            "event": "send_contractor_status_check",
            "status": "skipped_duplicate",
            "contractor_phone": contractor_phone,
            "key": _send_key,
        })
        return json.dumps({
            "status": "skipped_duplicate",
            "message": f"Status check already sent to {contractor_name} today",
            "contractor_phone": contractor_phone,
            "contractor_name": contractor_name,
        })

    try:
        message_body = f"Status Check: Hi {contractor_name}, are you enroute to {job_address}? Reply YES or NO."

        to_formatted = f'whatsapp:{_normalize_phone(contractor_phone)}'
        from_formatted = TWILIO_WHATSAPP_NUMBER if TWILIO_WHATSAPP_NUMBER.startswith('whatsapp:') else f'whatsapp:{TWILIO_WHATSAPP_NUMBER}'

        if not TWILIO_STATUS_CHECK_TEMPLATE_SID:
            return _error_response(tool="send_contractor_status_check", error_code="config_error",
                                   message="TWILIO_STATUS_CHECK_TEMPLATE_SID not set â€” required for business-initiated WhatsApp")

        endpoint = f"Accounts/{TWILIO_ACCOUNT_SID}/Messages.json"
        payload = {
            "From": from_formatted,
            "To": to_formatted,
            "ContentSid": TWILIO_STATUS_CHECK_TEMPLATE_SID,
            "ContentVariables": json.dumps({"1": contractor_name, "2": job_address}),
        }
        result = await asyncio.to_thread(send_twilio_api_request, endpoint, payload)

        message_id = result.get('sid', 'unknown')
        latency_ms = int((time.time() - start_time) * 1000)
        _log("info", "send_contractor_status_check", {
            "event": "send_contractor_status_check",
            "status": "success",
            "message_id": message_id,
            "contractor_phone": contractor_phone,
            "contractor_name": contractor_name,
            "job_address": job_address,
            "rate_limit_status": rate_msg,
            "latency_ms": latency_ms,
            "timestamp_utc": datetime.now(timezone.utc).isoformat()
        })
        await async_log_whatsapp_message(
            direction="outbound", message_id=message_id,
            from_phone=TWILIO_WHATSAPP_NUMBER, to_phone=contractor_phone,
            body=message_body,
            message_type="contractor_status_check", status="sent",
            contact_name=contractor_name,
            metadata={"job_address": job_address},
        )
        return json.dumps({
            "status": "success",
            "message_id": message_id,
            "contractor_phone": contractor_phone,
            "contractor_name": contractor_name,
            "job_address": job_address,
            "message_sent": message_body,
            "message": f"Status check sent to {contractor_name} (SID: {message_id})",
            "rate_limit_status": rate_msg,
            "timestamp_utc": datetime.now(timezone.utc).isoformat()
        })

    except ValueError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _release_send_claim(_send_key)
        _log("error", "send_contractor_status_check", {
            "event": "send_contractor_status_check",
            "status": "config_error",
            "contractor_phone": contractor_phone,
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="send_contractor_status_check", error_code="config_error", message=str(e), latency_ms=latency_ms)
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _release_send_claim(_send_key)
        _log("error", "send_contractor_status_check", {
            "event": "send_contractor_status_check",
            "status": "api_error",
            "contractor_phone": contractor_phone,
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="send_contractor_status_check", error_code="api_error", message=f"Twilio API error: {str(e)}", latency_ms=latency_ms)


@mcp.tool()
async def send_admin_alert(
    alert_type: str,
    contractor_name: str = "",
    job_address: str = "",
    details: str = "",
    admin_phone: str = ""
) -> str:
    """
    Send an operational alert to admin via the rate-limited WhatsApp path.
    Per AGENT GUIDE: triggers when contractor does not reply YES, feasibility fails, or NEEDS_REVIEW.

    Args:
        alert_type: Type of alert ('no_response', 'contractor_late', 'schedule_conflict',
                    'feasibility_issue', 'needs_review', 'missing_invoice', 'bad_debt')
        contractor_name: Name of the contractor (optional)
        job_address: Job address involved (optional)
        details: Additional context for the alert
        admin_phone: Admin phone number (optional, falls back to ADMIN_PHONE env var)

    Returns:
        JSON with message status
    """
    start_time = time.time()

    if not admin_phone:
        admin_phone = os.getenv('ADMIN_PHONE', '')
    if not admin_phone:
        return _error_response(tool="send_admin_alert", error_code="config_error", message="ADMIN_PHONE not provided and ADMIN_PHONE env var not set.")

    admin_phones = [p.strip() for p in admin_phone.split(",") if p.strip()]

    # Validate credentials before consuming rate limit
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "send_admin_alert", {
            "event": "send_admin_alert",
            "status": "config_error",
            "admin_phones": admin_phones,
            "error": "Twilio credentials not configured",
            "latency_ms": latency_ms
        })
        return _error_response(tool="send_admin_alert", error_code="config_error", message="Twilio credentials not configured. Set TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN.", latency_ms=latency_ms)

    allowed, rate_msg = check_rate_limits()
    if not allowed:
        _log("warning", "send_admin_alert", {
            "event": "send_admin_alert",
            "status": "rate_limit_exceeded",
            "to": admin_phones,
            "message": rate_msg
        })
        return _error_response(tool="send_admin_alert", error_code="rate_limit_exceeded", message=rate_msg)

    try:
        alert_messages = {
            "no_response": f"ALERT: {contractor_name} has not responded to status check for job at {job_address}. Manual intervention required.",
            "contractor_late": f"ALERT: {contractor_name} may be running late for job at {job_address}. {details}",
            "schedule_conflict": f"ALERT: Schedule conflict detected for {contractor_name}. Job at {job_address}. {details}",
            "feasibility_issue": f"ALERT: Route feasibility issue for {contractor_name}. {job_address}. {details}",
            "needs_review": f"NEEDS REVIEW: {details}",
            "missing_confirmation": f"BOOKING ALERT: Missing confirmation email for {contractor_name} at {job_address}. {details}",
            "incomplete_job": f"ALERT: Incomplete job for {contractor_name} at {job_address}. {details}",
            "unassigned_job": f"ALERT: Unassigned job at {job_address}. {details}",
            "missing_invoice": f"FINANCE ALERT: Missing invoice for completed job. {details}",
            "bad_debt": f"FINANCE ALERT: Bad debt escalation. {details}",
            "enroute_no": f"EN-ROUTE ALERT: {contractor_name} has responded NO to the en-route check for job at {job_address}. Immediate cover required. {details}",
            "task_summary": f"{details}",
        }
        message_body = alert_messages.get(
            alert_type,
            f"ALERT ({alert_type}): {contractor_name} - {job_address}. {details}"
        )

        from_formatted = TWILIO_WHATSAPP_NUMBER
        if not from_formatted.startswith('whatsapp:'):
            from_formatted = f'whatsapp:{from_formatted}'

        if not TWILIO_ADMIN_ALERT_TEMPLATE_SID:
            return _error_response(tool="send_admin_alert", error_code="config_error",
                                   message="TWILIO_ADMIN_ALERT_TEMPLATE_SID not set â€” required for business-initiated WhatsApp")

        endpoint = f"Accounts/{TWILIO_ACCOUNT_SID}/Messages.json"
        results = []
        for phone in admin_phones:
            to_formatted = f'whatsapp:{_normalize_phone(phone)}'
            admin_payload = {
                "From": from_formatted,
                "To": to_formatted,
                "ContentSid": TWILIO_ADMIN_ALERT_TEMPLATE_SID,
                "ContentVariables": json.dumps({"1": message_body}),
            }
            try:
                result = await asyncio.to_thread(send_twilio_api_request, endpoint, admin_payload)
                message_id = result.get('sid', 'unknown')
                results.append({"phone": phone, "message_id": message_id})
                _log("info", "send_admin_alert", {
                    "event": "send_admin_alert",
                    "status": "success",
                    "message_id": message_id,
                    "admin_phone": phone,
                    "alert_type": alert_type,
                    "latency_ms": int((time.time() - start_time) * 1000),
                    "timestamp_utc": datetime.now(timezone.utc).isoformat()
                })
                await async_log_whatsapp_message(
                    direction="outbound", message_id=message_id,
                    from_phone=TWILIO_WHATSAPP_NUMBER, to_phone=phone,
                    body=message_body, message_type="admin_alert", status="sent",
                    metadata={"alert_type": alert_type, "contractor_name": contractor_name, "job_address": job_address},
                )
            except Exception as _phone_exc:
                _log("error", "send_admin_alert", {
                    "event": "send_admin_alert_phone_failed",
                    "admin_phone": phone,
                    "alert_type": alert_type,
                    "error": str(_phone_exc)[:200],
                })
                results.append({"phone": phone, "message_id": "failed", "error": str(_phone_exc)[:200]})

        latency_ms = int((time.time() - start_time) * 1000)
        first_sid = results[0]["message_id"] if results else "unknown"
        return json.dumps({
            "status": "success",
            "message_id": first_sid,
            "admin_phones": admin_phones,
            "results": results,
            "alert_type": alert_type,
            "message_sent": message_body,
            "message": f"Admin alert sent to {len(results)} recipient(s)",
            "rate_limit_status": rate_msg,
            "timestamp_utc": datetime.now(timezone.utc).isoformat()
        })

    except ValueError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "send_admin_alert", {
            "event": "send_admin_alert",
            "status": "config_error",
            "admin_phones": admin_phones,
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="send_admin_alert", error_code="config_error", message=str(e), latency_ms=latency_ms)
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "send_admin_alert", {
            "event": "send_admin_alert",
            "status": "api_error",
            "admin_phones": admin_phones,
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="send_admin_alert", error_code="api_error", message=f"Twilio API error: {str(e)}", latency_ms=latency_ms)


@mcp.tool()
async def send_clerk_timetable(
    clerk_phone: str,
    clerk_name: str,
    timetable_date: str,
    timetable_text: str,
) -> str:
    """
    Send a next-day timetable WhatsApp to a clerk via approved Twilio Content Template.
    Business-initiated message (outside 24h session window) â€” requires TWILIO_TIMETABLE_TEMPLATE_SID.

    Args:
        clerk_phone: Clerk's phone number in international format (e.g. +447700900000)
        clerk_name: Clerk's first name for personalisation
        timetable_date: Human-readable date string (e.g. "Friday 25 July 2026")
        timetable_text: Pre-formatted job list â€” one job per line with time, type, address, customer

    Returns:
        JSON with message status and ID
    """
    start_time = time.time()

    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
        latency_ms = int((time.time() - start_time) * 1000)
        return _error_response(tool="send_clerk_timetable", error_code="config_error",
                               message="Twilio credentials not configured.", latency_ms=latency_ms)

    if not TWILIO_TIMETABLE_TEMPLATE_SID:
        latency_ms = int((time.time() - start_time) * 1000)
        return _error_response(tool="send_clerk_timetable", error_code="config_error",
                               message="TWILIO_TIMETABLE_TEMPLATE_SID not set. Create timetable template in Twilio and set secret.", latency_ms=latency_ms)

    allowed, rate_msg = check_rate_limits()
    if not allowed:
        return _error_response(tool="send_clerk_timetable", error_code="rate_limit_exceeded", message=rate_msg)

    _send_key = _wa_send_key("timetable", clerk_phone)
    if not _try_claim_send(_send_key):
        _log("info", "send_clerk_timetable", {
            "event": "send_clerk_timetable",
            "status": "skipped_duplicate",
            "clerk_phone": clerk_phone,
            "key": _send_key,
        })
        return json.dumps({
            "status": "skipped_duplicate",
            "message": f"Timetable already sent to {clerk_name} today",
            "clerk_phone": clerk_phone,
            "clerk_name": clerk_name,
        })

    try:
        to_formatted = f'whatsapp:{_normalize_phone(clerk_phone)}'
        from_formatted = TWILIO_WHATSAPP_NUMBER if TWILIO_WHATSAPP_NUMBER.startswith('whatsapp:') else f'whatsapp:{TWILIO_WHATSAPP_NUMBER}'

        endpoint = f"Accounts/{TWILIO_ACCOUNT_SID}/Messages.json"
        payload = {
            "From": from_formatted,
            "To": to_formatted,
            "ContentSid": TWILIO_TIMETABLE_TEMPLATE_SID,
            "ContentVariables": json.dumps({
                "1": clerk_name,
                "2": timetable_date,
                "3": timetable_text,
            }),
        }
        result = await asyncio.to_thread(send_twilio_api_request, endpoint, payload)

        message_id = result.get('sid', 'unknown')
        latency_ms = int((time.time() - start_time) * 1000)
        _log("info", "send_clerk_timetable", {
            "event": "send_clerk_timetable",
            "status": "success",
            "message_id": message_id,
            "clerk_phone": clerk_phone,
            "clerk_name": clerk_name,
            "timetable_date": timetable_date,
            "rate_limit_status": rate_msg,
            "latency_ms": latency_ms,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        })
        await async_log_whatsapp_message(
            direction="outbound", message_id=message_id,
            from_phone=TWILIO_WHATSAPP_NUMBER, to_phone=clerk_phone,
            body=timetable_text, message_type="timetable", status="sent",
            contact_name=clerk_name,
            metadata={"timetable_date": timetable_date},
        )
        return json.dumps({
            "status": "success",
            "message_id": message_id,
            "clerk_phone": clerk_phone,
            "clerk_name": clerk_name,
            "timetable_date": timetable_date,
            "message": f"Timetable sent to {clerk_name} (SID: {message_id})",
            "rate_limit_status": rate_msg,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        })

    except ValueError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _release_send_claim(_send_key)
        return _error_response(tool="send_clerk_timetable", error_code="config_error",
                               message=str(e), latency_ms=latency_ms)
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _release_send_claim(_send_key)
        _log("error", "send_clerk_timetable", {
            "event": "send_clerk_timetable",
            "status": "api_error",
            "clerk_phone": clerk_phone,
            "error": str(e),
            "latency_ms": latency_ms,
        })
        return _error_response(tool="send_clerk_timetable", error_code="api_error",
                               message=f"Twilio API error: {str(e)}", latency_ms=latency_ms)


def startup_health_check() -> bool:
    """
    Phase 3 requirement: Startup health check.
    Verify Twilio configuration is present.
    """
    # Check if credentials are configured
    if not TWILIO_ACCOUNT_SID:
        _log("error", "startup_health_check", {
            "event": "server_startup_failed",
            "error": "TWILIO_ACCOUNT_SID environment variable not set",
            "status": "critical"
        })
        return False
    
    if not TWILIO_AUTH_TOKEN:
        _log("error", "startup_health_check", {
            "event": "server_startup_failed",
            "error": "TWILIO_AUTH_TOKEN environment variable not set",
            "status": "critical"
        })
        return False
    
    _log("info", "startup_health_check", {
        "event": "health_check_passed",
        "status": "healthy",
        "api_provider": "twilio_production",
        "whatsapp_number": TWILIO_WHATSAPP_NUMBER,
        "rate_limits": f"{MAX_MESSAGES_PER_MINUTE}/min, {MAX_MESSAGES_PER_HOUR}/hour"
    })
    return True

if __name__ == "__main__":
    _log("info", "__main__", {
        "event": "server_start",
        "message": "WhatsApp MCP server starting (Twilio Production)",
        "api_provider": "twilio_production",
        "whatsapp_number": TWILIO_WHATSAPP_NUMBER,
        "rate_limits": f"{MAX_MESSAGES_PER_MINUTE}/min, {MAX_MESSAGES_PER_HOUR}/hour"
    })
    
    # Phase 3 requirement: Startup health check
    if not startup_health_check():
        _log("error", "__main__", {
            "event": "server_startup_aborted",
            "status": "critical",
            "message": "Health check failed. Server will not start."
        })
        sys.exit(1)
    
    _log("info", "__main__", {
        "event": "server_ready",
        "status": "ready"
    })
    mcp.run()
