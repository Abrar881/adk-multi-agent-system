import sys
import os
import re
import html
import base64
import json
import time
import random
import functools
import hashlib
import logging
import asyncio
import threading
from email.message import EmailMessage
from datetime import datetime, timedelta, timezone
from collections import deque
from mcp.server.fastmcp import FastMCP

# Google API Imports
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import firestore
from google.api_core.exceptions import AlreadyExists

# --- STRUCTURED LOGGING ---
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

# --- LOGGING CONSTANTS (for Cloud Logging aggregation) ---
SERVICE_NAME = "gmail_server"
AGENT_NAME = "gmail_server"


def _redact_email(email: str) -> str:
    """Return a masked email showing only first char of local part and full domain."""
    if not isinstance(email, str) or "@" not in email:
        return "***"
    local, domain = email.rsplit("@", 1)
    return f"{local[0] if local else '*'}***@{domain}"


def _markdown_to_html(text: str) -> str:
    """Convert the markdown subset emitted by the booking drafter to HTML.

    Handles: headings (#/##/###/####), bullet lists (* / -), bold (**text**),
    paragraphs (blank-line separated), and line breaks within paragraphs.

    HTML-escapes the raw text FIRST so that literal strings like '<24h' and
    '>24h' in the cancellation policy survive as visible text rather than being
    parsed as HTML tags.
    """
    # 1. HTML-escape raw input (must happen before any tag insertion).
    escaped = html.escape(text, quote=False)

    lines = escaped.splitlines()
    html_parts: list[str] = []
    in_ul = False
    para_lines: list[str] = []

    def flush_para():
        nonlocal para_lines
        if para_lines:
            html_parts.append("<p>" + "<br>".join(para_lines) + "</p>")
            para_lines = []

    def close_ul():
        nonlocal in_ul
        if in_ul:
            html_parts.append("</ul>")
            in_ul = False

    def apply_inline(s: str) -> str:
        """Apply inline bold (**text**) conversion."""
        return re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', s)

    for line in lines:
        stripped = line.strip()

        # --- Headings ---
        heading_match = re.match(r'^(#{1,6})\s+(.*)', stripped)
        if heading_match:
            close_ul()
            flush_para()
            level = len(heading_match.group(1))
            # Cap at h3/h4 so email doesn't get oversized heading hierarchy
            tag = "h3" if level <= 2 else "h4"
            html_parts.append(f"<{tag}>{apply_inline(heading_match.group(2))}</{tag}>")
            continue

        # --- Bullet list items (* or -) ---
        bullet_match = re.match(r'^[*\-]\s+(.*)', stripped)
        if bullet_match:
            flush_para()
            if not in_ul:
                html_parts.append("<ul>")
                in_ul = True
            html_parts.append(f"<li>{apply_inline(bullet_match.group(1))}</li>")
            continue

        # --- Blank line = paragraph break ---
        if not stripped:
            close_ul()
            flush_para()
            continue

        # --- Ordinary text line ---
        close_ul()
        para_lines.append(apply_inline(stripped))

    close_ul()
    flush_para()

    body_html = "\n".join(html_parts)
    return (
        '<div style="font-family:Arial,sans-serif;font-size:14px;line-height:1.6;color:#222;">'
        + body_html
        + "</div>"
    )


def _strip_html(raw: str) -> str:
    """Strip HTML tags from a string and return readable plain text.

    Removes <script>/<style> blocks first, then all other tags, unescapes
    HTML entities, and collapses whitespace. Used to sanitise HTML-only
    inbound emails before they reach the drafting LLM (prevents raw markup
    and hidden prompt-injection payloads from entering the model context).
    """
    # 1. Drop script/style blocks entirely (with their content)
    text = re.sub(r'(?is)<(script|style)[^>]*>.*?</\1>', '', raw)
    # 2. Strip remaining tags, replacing with a space to preserve word boundaries
    text = re.sub(r'<[^>]+>', ' ', text)
    # 3. Unescape HTML entities (&amp; &lt; &gt; &nbsp; â€¦)
    text = html.unescape(text)
    # 4. Collapse runs of whitespace / newlines
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _extract_email_text(payload: dict) -> str:
    """Extract readable plain text from a Gmail message payload.

    Prefers text/plain over text/html. Skips attachment parts. Strips HTML
    tags when falling back to HTML. Never concatenates both types (fixes the
    multipart/alternative plain+HTML concatenation bug in the old get_body).

    Args:
        payload: The 'payload' dict from a Gmail messages.get() or
                 threads.get() response.
    Returns:
        Decoded body string, or "" if nothing extractable.
    """
    plain_text: str = ""
    html_text: str = ""

    def _walk(part: dict) -> None:
        nonlocal plain_text, html_text
        mime = part.get('mimeType', '')
        body = part.get('body', {})

        # Skip attachment parts (large bodies stored separately via attachmentId)
        if body.get('attachmentId'):
            return
        headers = part.get('headers', [])
        disposition = next(
            (h['value'] for h in headers if h['name'].lower() == 'content-disposition'),
            ''
        )
        if 'attachment' in disposition.lower():
            return

        if 'parts' in part:
            # Multipart container â€” recurse into children.
            # For multipart/alternative we want ONE winner (plain beats html),
            # so we collect via nonlocal and let the caller pick.
            for child in part['parts']:
                _walk(child)
        elif mime == 'text/plain' and not plain_text:
            data = body.get('data', '')
            if data:
                plain_text = base64.urlsafe_b64decode(data).decode('utf-8', errors='replace')
        elif mime == 'text/html' and not html_text:
            data = body.get('data', '')
            if data:
                html_text = base64.urlsafe_b64decode(data).decode('utf-8', errors='replace')

    _walk(payload)

    if plain_text.strip():
        return plain_text
    if html_text.strip():
        return _strip_html(html_text)
    return ""


_IMAGE_MIME_TYPES = frozenset({
    "image/jpeg", "image/png", "image/gif", "image/webp",
    "image/bmp", "image/tiff",
})
_SIGNATURE_FILENAMES = frozenset({
    "image001.png", "image002.png", "image003.png",
    "image001.jpg", "image002.jpg", "logo.png", "logo.jpg",
    "signature.png", "signature.jpg", "banner.png",
})
_SIGNATURE_SIZE_THRESHOLD = 10_000  # 10KB â€” below this is almost always a logo/sig


def _extract_attachments(payload: dict) -> list:
    """Walk MIME tree and collect attachment metadata without downloading content."""
    attachments = []

    def _walk(part: dict) -> None:
        body = part.get('body', {})
        attachment_id = body.get('attachmentId')
        if attachment_id:
            mime = part.get('mimeType', 'application/octet-stream')
            filename = part.get('filename', '')
            size = body.get('size', 0)
            headers = part.get('headers', [])
            content_id = next(
                (h['value'] for h in headers if h['name'].lower() == 'content-id'),
                None
            )
            likely_sig = (
                size < _SIGNATURE_SIZE_THRESHOLD
                or filename.lower() in _SIGNATURE_FILENAMES
                or content_id is not None  # inline CID images are typically logos/sigs
            )
            attachments.append({
                "attachment_id": attachment_id,
                "filename": filename,
                "mime_type": mime,
                "size_bytes": size,
                "is_image": mime.lower() in _IMAGE_MIME_TYPES,
                "likely_signature": likely_sig,
            })
        if 'parts' in part:
            for child in part['parts']:
                _walk(child)

    _walk(payload)
    return attachments


# Log fields that contain email addresses â€” redacted before writing to Cloud Logging.
_EMAIL_LOG_FIELDS = frozenset({"to", "from", "sender", "email_address"})


def _log(level: str, tool: str, payload: dict) -> None:
    """Emit structured JSON logs with required aggregation fields.

    Email-address fields are masked (x***@domain) to prevent PII appearing in logs.
    """
    out = dict(payload or {})
    out.setdefault("service", SERVICE_NAME)
    out.setdefault("agent", AGENT_NAME)
    out.setdefault("tool", tool)
    for field in _EMAIL_LOG_FIELDS:
        if field in out and isinstance(out[field], str):
            out[field] = _redact_email(out[field])
    getattr(logger, level)(json.dumps(out))

# --- CONFIGURATION ---
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']
_HERE = os.path.dirname(os.path.abspath(__file__))
_TOKEN_DIR = os.path.realpath(os.path.join(_HERE, '..', 'tokens'))

def _safe_token_path(env_var: str, default_name: str) -> str:
    """Validate token file path to prevent directory traversal attacks."""
    raw = os.getenv(env_var, os.path.join(_TOKEN_DIR, default_name))
    resolved = os.path.realpath(raw)
    if not resolved.startswith(_TOKEN_DIR):
        raise ValueError(f"{env_var} must resolve within {_TOKEN_DIR}, got {resolved}")
    return resolved

TOKEN_FILE = _safe_token_path('GMAIL_TOKEN_FILE', 'token.json')
CRED_FILE = _safe_token_path('GMAIL_CRED_FILE', 'credentials.json')
# Cloud Run: token/credentials can be injected as JSON strings via env vars
GMAIL_TOKEN_JSON = os.getenv('GMAIL_TOKEN_JSON')
GMAIL_CRED_JSON = os.getenv('GMAIL_CRED_JSON')

# Rate Limiting Configuration (Phase 5 requirement)
MAX_EMAILS_PER_HOUR = 50
RATE_LIMIT_WINDOW = 3600  # 1 hour in seconds

mcp = FastMCP("AcmeMail")

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

# --- RATE LIMITER (Firestore sliding-window â€” enforced globally across all instances) ---
# Each draft atomically writes a record into 'email_rate_limits' with a 1-hour TTL.
# A count of records in the window enforces the cap before the draft is created.
# Fail-open: if Firestore is unavailable, the draft proceeds (dedup already prevents
# duplicates; blocking all replies on a transient blip is worse than a brief overage).
_RATE_LIMIT_COLLECTION = "email_rate_limits"


def _check_rate_limit() -> tuple[bool, str, str | None]:
    """Check and reserve a rate-limit slot in Firestore.

    Returns:
        (allowed, message, doc_id) â€” doc_id is the Firestore document to
        delete (refund) if the draft subsequently fails.  doc_id is None when
        the call was allowed via fail-open fallback.
    """
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(seconds=RATE_LIMIT_WINDOW)
    try:
        db = _get_firestore()
        col = db.collection(_RATE_LIMIT_COLLECTION)
        # Count drafts created within the rolling window
        count_q = col.where('created_at', '>=', window_start).count()
        count_result = count_q.get()
        current = count_result[0][0].value if count_result else 0
        if current >= MAX_EMAILS_PER_HOUR:
            oldest_q = col.where('created_at', '>=', window_start).order_by('created_at').limit(1)
            oldest_docs = oldest_q.get()
            if oldest_docs:
                oldest_time = oldest_docs[0].to_dict().get('created_at', window_start)
                if hasattr(oldest_time, 'astimezone'):
                    oldest_time = oldest_time.astimezone(timezone.utc)
                wait_s = int((oldest_time + timedelta(seconds=RATE_LIMIT_WINDOW) - now).total_seconds())
                wait_s = max(0, wait_s)
            else:
                wait_s = RATE_LIMIT_WINDOW
            return False, f"Rate limit exceeded. Max {MAX_EMAILS_PER_HOUR} emails per hour. Try again in {wait_s}s.", None
        # Reserve the slot
        doc_ref = col.document()
        doc_ref.set({'created_at': now, 'expires_at': now + timedelta(seconds=RATE_LIMIT_WINDOW)})
        return True, f"OK ({current + 1}/{MAX_EMAILS_PER_HOUR} used this hour)", doc_ref.id
    except Exception as e:
        # Fail-open: log and allow the draft; dedup prevents true duplicates
        _invalidate_firestore_client()
        _log("warning", "_check_rate_limit", {
            "event": "rate_limit_check_failed",
            "error": str(e),
            "error_type": type(e).__name__,
            "action": "fail_open"
        })
        return True, "OK (rate limit check unavailable â€” fail-open)", None


def _refund_rate_limit(doc_id: str | None) -> None:
    """Delete the rate-limit reservation created by _check_rate_limit if draft failed."""
    if not doc_id:
        return
    try:
        _get_firestore().collection(_RATE_LIMIT_COLLECTION).document(doc_id).delete()
    except Exception as e:
        _invalidate_firestore_client()
        _log("warning", "_refund_rate_limit", {"event": "refund_failed", "error": str(e)})

# --- FIRESTORE (email dedup lock, reuses task_locks collection) ---
_FIRESTORE_PROJECT = os.getenv('GOOGLE_CLOUD_PROJECT') or os.getenv('GCLOUD_PROJECT')
_LOCK_COLLECTION = "task_locks"
_DEDUP_TTL_DAYS = 30
_PROACTIVE_DEDUP_TTL_DAYS = 1  # 1-day lock for proactive sends (no reply_to_message_id)
_db: "firestore.Client | None" = None
_db_init_lock = threading.Lock()


def _get_firestore() -> "firestore.Client":
    global _db
    with _db_init_lock:
        if _db is None:
            if not _FIRESTORE_PROJECT:
                raise RuntimeError("GOOGLE_CLOUD_PROJECT env var must be set for email dedup")
            _db = firestore.Client(project=_FIRESTORE_PROJECT)
    return _db


def _invalidate_firestore_client() -> None:
    global _db
    _db = None


_auto_send_cache: tuple[bool, float] = (False, 0.0)
_AUTO_SEND_CACHE_TTL = 60.0


def _is_auto_send_enabled() -> bool:
    """Read email_auto_send_enabled from Firestore dashboard_config/settings. Fail-safe: returns False."""
    global _auto_send_cache
    now = time.monotonic()
    val, expiry = _auto_send_cache
    if now < expiry:
        return val
    try:
        doc = _get_firestore().collection("dashboard_config").document("settings").get()
        enabled = doc.to_dict().get("email_auto_send_enabled", False) if doc.exists else False
        _auto_send_cache = (bool(enabled), now + _AUTO_SEND_CACHE_TTL)
        return bool(enabled)
    except Exception:
        _invalidate_firestore_client()
        _auto_send_cache = (False, now + _AUTO_SEND_CACHE_TTL)
        return False


# --- PER-MESSAGE PROCESSING LOCK (burst-window dedup) ---
_PROCESS_LOCK_TTL_MIN = 10  # burst window only; short TTL so a crashed run self-heals

@firestore.transactional
def _acquire_message_lock_transaction(transaction, ref: "firestore.DocumentReference", now: datetime, expires_at: datetime, message_id: str) -> bool:
    """Transactional helper: atomically check expiry and overwrite if needed.

    Returns True if acquired; False if another live run owns the lock.
    """
    snap = ref.get(transaction=transaction)
    if snap.exists:
        data = snap.to_dict()
        ea = data.get("expires_at")
        if hasattr(ea, "to_pydatetime"):
            ea = ea.to_pydatetime()
        if ea and ea > now:
            # Live lock â€” another run owns this message.
            _log("info", "_try_acquire_message_lock", {
                "event": "message_lock_busy",
                "message_id": message_id,
                "expires_at": ea.isoformat() if hasattr(ea, "isoformat") else str(ea),
            })
            return False
    # Expired or missing â€” overwrite atomically inside the transaction.
    transaction.set(ref, {"owner": "inbox_processing", "created_at": now, "expires_at": expires_at})
    return True


def _try_acquire_message_lock(message_id: str) -> bool:
    """Attempt to acquire a short-lived processing lock for this message.

    Returns True if acquired (caller should proceed).
    Returns False if another live run owns the lock (caller should skip this message).
    Fails OPEN on any error â€” never blocks processing due to lock infra failure.

    Key format: msgproc:<message_id> (distinct from email:<message_id> reply-dedup locks).
    TTL: _PROCESS_LOCK_TTL_MIN minutes â€” covers a slow run; auto-expires so crashes self-heal.
    Never explicitly released â€” the AI_PROCESSED label is the durable cross-run dedup.
    """
    try:
        db = _get_firestore()
        lock_key = f"msgproc:{message_id}"
        ref = db.collection(_LOCK_COLLECTION).document(lock_key)
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(minutes=_PROCESS_LOCK_TTL_MIN)

        # Fast path: atomic create â€” only one caller succeeds; the other gets AlreadyExists.
        try:
            ref.create({"owner": "inbox_processing", "created_at": now, "expires_at": expires_at})
            return True  # acquired
        except AlreadyExists:
            pass  # fall through to transaction path

        # Slow path: lock doc exists. Use a transaction to atomically check expiry and overwrite,
        # preventing a TOCTOU race where two processes both see an expired lock and both proceed.
        transaction = db.transaction()
        return _acquire_message_lock_transaction(transaction, ref, now, expires_at, message_id)

    except Exception as e:
        _invalidate_firestore_client()
        _log("warning", "_try_acquire_message_lock", {
            "event": "message_lock_error",
            "message_id": message_id,
            "error": str(e),
        })
        return True  # FAIL OPEN â€” never block processing due to lock infra failure


@mcp.tool()
async def claim_email_for_processing(message_id: str) -> str:
    """Claim a short-lived processing lock for a message before acting on it.

    Call this right after list_unread_emails, once per message_id, BEFORE calling
    create_inventory_booking, create_lead, or any other create/mutation tool.
    If claimed=false, skip this message entirely â€” another concurrent run is already
    handling it. The lock expires automatically after 10 minutes; no explicit release needed.

    Args:
        message_id: Gmail message ID from list_unread_emails.

    Returns JSON: {"claimed": true/false, "message_id": "<id>"}
    """
    claimed = await asyncio.to_thread(_try_acquire_message_lock, message_id)
    return json.dumps({"claimed": claimed, "message_id": message_id})


# --- AI_PROCESSED / AI_BOOKED GMAIL LABELS ---
_ai_processed_label_id: str | None = None
_ai_booked_label_id: str | None = None
_ai_chase_drafted_label_id: str | None = None
_label_init_lock = threading.Lock()


def _get_or_create_ai_processed_label_id(service) -> str:
    """Return (creating if needed) the Gmail label ID for 'AI_PROCESSED'. Cached after first call."""
    global _ai_processed_label_id
    with _label_init_lock:
        if _ai_processed_label_id:
            return _ai_processed_label_id
        labels = service.users().labels().list(userId='me').execute().get('labels', [])
        for lbl in labels:
            if lbl.get('name') == 'AI_PROCESSED':
                _ai_processed_label_id = lbl['id']
                return _ai_processed_label_id
        new_label = service.users().labels().create(
            userId='me',
            body={
                'name': 'AI_PROCESSED',
                'labelListVisibility': 'labelHide',
                'messageListVisibility': 'show',
            }
        ).execute()
        _ai_processed_label_id = new_label['id']
        _log("info", "_get_or_create_ai_processed_label_id", {
            "event": "label_created",
            "label_id": _ai_processed_label_id
        })
        return _ai_processed_label_id


def _get_or_create_ai_booked_label_id(service) -> str:
    """Return (creating if needed) the Gmail label ID for 'AI_BOOKED'. Cached after first call.

    Applied thread-scope after a successful booking so every reply in the thread
    carries it â€” prevents reply-triggered re-booking and shows booking status in Gmail.
    """
    global _ai_booked_label_id
    with _label_init_lock:
        if _ai_booked_label_id:
            return _ai_booked_label_id
        labels = service.users().labels().list(userId='me').execute().get('labels', [])
        for lbl in labels:
            if lbl.get('name') == 'AI_BOOKED':
                _ai_booked_label_id = lbl['id']
                return _ai_booked_label_id
        new_label = service.users().labels().create(
            userId='me',
            body={
                'name': 'AI_BOOKED',
                'labelListVisibility': 'labelShow',
                'messageListVisibility': 'show',
            }
        ).execute()
        _ai_booked_label_id = new_label['id']
        _log("info", "_get_or_create_ai_booked_label_id", {
            "event": "label_created",
            "label_id": _ai_booked_label_id
        })
        return _ai_booked_label_id


def _get_or_create_ai_chase_drafted_label_id(service) -> str:
    """Return (creating if needed) the Gmail label ID for 'AI_CHASE_DRAFTED'. Cached after first call.

    Applied message-scope after chase_draft creates a chase email draft so all chase
    drafts are visible under one Gmail label for ops review.
    """
    global _ai_chase_drafted_label_id
    with _label_init_lock:
        if _ai_chase_drafted_label_id:
            return _ai_chase_drafted_label_id
        labels = service.users().labels().list(userId='me').execute().get('labels', [])
        for lbl in labels:
            if lbl.get('name') == 'AI_CHASE_DRAFTED':
                _ai_chase_drafted_label_id = lbl['id']
                return _ai_chase_drafted_label_id
        new_label = service.users().labels().create(
            userId='me',
            body={
                'name': 'AI_CHASE_DRAFTED',
                'labelListVisibility': 'labelShow',
                'messageListVisibility': 'show',
            }
        ).execute()
        _ai_chase_drafted_label_id = new_label['id']
        _log("info", "_get_or_create_ai_chase_drafted_label_id", {
            "event": "label_created",
            "label_id": _ai_chase_drafted_label_id
        })
        return _ai_chase_drafted_label_id


def _is_gmail_push_self_mutation_only(start_history_id, new_history_id) -> bool:
    """True only if the Gmail history delta since `start_history_id` consists
    SOLELY of this agent's own AI_PROCESSED/AI_BOOKED label writes â€” i.e. the
    push notification that triggered this call was self-inflicted (a prior turn
    labelling a message bumps historyId, which fires a new push, which would
    otherwise spawn a new dispatcher turn with nothing new to do).

    Fail-open: returns False (== proceed with the turn) on ANY exception, on an
    empty/uninterpretable delta, or if the delta contains any new message or any
    non-AI label change. Never silently drops what could be a real customer email.
    """
    last_exc = None
    for attempt in range(2):
        try:
            service = get_gmail_service()
            ai_label_ids = {
                _get_or_create_ai_processed_label_id(service),
                _get_or_create_ai_booked_label_id(service),
                _get_or_create_ai_chase_drafted_label_id(service),
            }

            saw_any_history = False
            page_token = None
            for _ in range(5):  # cap pages â€” a self-mutation burst never needs more
                resp = service.users().history().list(
                    userId='me',
                    startHistoryId=start_history_id,
                    historyTypes=['messageAdded', 'labelAdded'],
                    pageToken=page_token,
                ).execute()
                history = resp.get('history', [])
                if history:
                    saw_any_history = True
                for entry in history:
                    for msg_event in entry.get('messagesAdded', []):
                        label_ids = set(msg_event.get('message', {}).get('labelIds', []))
                        if 'INBOX' in label_ids:
                            return False  # real inbound message â€” spawn a turn
                        # SENT-only (agent's own outbound) â€” not actionable, keep checking
                    for added in entry.get('labelsAdded', []):
                        label_ids = set(added.get('labelIds', []))
                        if not label_ids or not label_ids.issubset(ai_label_ids):
                            return False
                page_token = resp.get('nextPageToken')
                if not page_token:
                    break

            if not saw_any_history:
                # Empty/uninterpretable delta (e.g. startHistoryId too old and Gmail
                # returned nothing usable) â€” fail open, proceed with the turn.
                return False

            return True
        except Exception as exc:
            last_exc = exc
            _thread_local.service = None  # stale httplib2 connection â€” force rebuild on retry
    _log("warning", "_is_gmail_push_self_mutation_only", {
        "event": "gmail_self_mutation_check_failed",
        "error": str(last_exc)[:200],
    })
    return False  # fail open â€” never block a possibly-real email


try:
    from servers.circuit_breaker import CircuitBreaker as _CircuitBreaker
except ModuleNotFoundError:
    from circuit_breaker import CircuitBreaker as _CircuitBreaker


def _gmail_cb_alert(name, failures):
    try:
        from servers.alerting import alert_admin, CRITICAL
    except ModuleNotFoundError:
        from alerting import alert_admin, CRITICAL
    alert_admin(CRITICAL, f"circuit_breaker_{name}_open",
                f"Gmail API circuit breaker opened after {failures} consecutive failures")


_gmail_breaker = _CircuitBreaker("gmail", failure_threshold=5, recovery_timeout=120.0,
                                 on_open_callback=_gmail_cb_alert)

_GMAIL_TRANSIENT_STATUSES = {429, 500, 502, 503, 504}


# --- RETRY DECORATOR ---
def retry(exceptions=(HttpError, IOError, ValueError), tries=4, delay=1, backoff=2, jitter=0.1):
    """Retry with exponential backoff + jitter. Includes auth errors."""
    def deco(func):
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            if not _gmail_breaker.allow_request():
                raise HttpError(resp=type('R', (), {'status': 503})(), content=b"gmail circuit breaker open")
            _tries, _delay = tries, delay
            while _tries > 1:
                try:
                    result = func(*args, **kwargs)
                    _gmail_breaker.record_success()
                    return result
                except exceptions as e:
                    # Don't retry permanent HTTP errors (400/403/404) â€” they won't succeed
                    if isinstance(e, HttpError) and getattr(e.resp, 'status', None) not in {429, 500, 502, 503, 504}:
                        raise
                    if isinstance(e, HttpError) and getattr(e.resp, 'status', None) in _GMAIL_TRANSIENT_STATUSES:
                        _gmail_breaker.record_failure()
                    if isinstance(e, IOError):
                        _thread_local.service = None  # force fresh connection on next retry
                    sleep_time = _delay + random.uniform(-jitter, jitter)
                    _log("warning", func.__name__, {
                        "event": "retry",
                        "wait_seconds": sleep_time,
                        "error": str(e),
                        "attempts_remaining": _tries - 1
                    })
                    time.sleep(max(0, sleep_time))
                    _tries -= 1
                    _delay *= backoff
            result = func(*args, **kwargs)
            _gmail_breaker.record_success()
            return result

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            if not _gmail_breaker.allow_request():
                raise HttpError(resp=type('R', (), {'status': 503})(), content=b"gmail circuit breaker open")
            _tries, _delay = tries, delay
            while _tries > 1:
                try:
                    result = await func(*args, **kwargs)
                    _gmail_breaker.record_success()
                    return result
                except exceptions as e:
                    # Don't retry permanent HTTP errors (400/403/404) â€” they won't succeed
                    if isinstance(e, HttpError) and getattr(e.resp, 'status', None) not in {429, 500, 502, 503, 504}:
                        raise
                    if isinstance(e, HttpError) and getattr(e.resp, 'status', None) in _GMAIL_TRANSIENT_STATUSES:
                        _gmail_breaker.record_failure()
                    if isinstance(e, IOError):
                        _thread_local.service = None  # force fresh connection on next retry
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
            result = await func(*args, **kwargs)
            _gmail_breaker.record_success()
            return result

        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return deco

# --- AUTHENTICATION HELPER ---
# Module-level service cache: build() is expensive (HTTP discovery + transport).
# The lock also serialises token refreshes, preventing concurrent writes to TOKEN_FILE.
_thread_local = threading.local()  # per-thread Gmail service â€” httplib2 is not thread-safe
_gmail_creds = None
_gmail_lock = threading.Lock()


def get_gmail_service():
    """
    Gets authenticated Gmail service with token refresh.
    Supports two modes:
      - Cloud Run: loads token from GMAIL_TOKEN_JSON env var (Secret Manager)
      - Local:     loads token from TOKEN_FILE on disk

    The service object is cached after first build. The lock serialises all
    calls so token refresh and file writes are never raced concurrently.
    """
    global _gmail_creds
    with _gmail_lock:
        # Fast path: creds still valid â€” return or build this thread's own service
        if _gmail_creds is not None:
            if _gmail_creds.valid:
                svc = getattr(_thread_local, 'service', None)
                if svc is None:
                    svc = build('gmail', 'v1', credentials=_gmail_creds, cache_discovery=False)
                    _thread_local.service = svc
                return svc
            # Creds expired but refreshable â€” mutate in-place; per-thread services stay valid
            if _gmail_creds.expired and _gmail_creds.refresh_token:
                try:
                    _gmail_creds.refresh(Request())
                    _log("info", "get_gmail_service", {"event": "token_refreshed", "source": "cache"})
                    if not GMAIL_TOKEN_JSON:
                        with open(TOKEN_FILE, 'w') as token:
                            token.write(_gmail_creds.to_json())
                    svc = getattr(_thread_local, 'service', None)
                    if svc is None:
                        svc = build('gmail', 'v1', credentials=_gmail_creds, cache_discovery=False)
                        _thread_local.service = svc
                    return svc
                except Exception as e:
                    _log("warning", "get_gmail_service", {"event": "cached_refresh_failed", "error": str(e)})
                    # Fall through to full rebuild

        # Slow path: load creds and build the service
        creds = None

        # 1. Try env var first (Cloud Run / Secret Manager)
        if GMAIL_TOKEN_JSON:
            try:
                token_data = json.loads(GMAIL_TOKEN_JSON)
                creds = Credentials.from_authorized_user_info(token_data, SCOPES)
                _log("info", "get_gmail_service", {
                    "event": "token_loaded",
                    "source": "env_var"
                })
            except (ValueError, KeyError) as e:
                _log("error", "get_gmail_service", {
                    "event": "token_load_error",
                    "source": "env_var",
                    "error": str(e)
                })
                creds = None
        # 2. Fall back to file (local dev)
        elif os.path.exists(TOKEN_FILE):
            try:
                creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
                _log("info", "get_gmail_service", {
                    "event": "token_loaded",
                    "source": "file"
                })
            except (ValueError, KeyError) as e:
                _log("error", "get_gmail_service", {
                    "event": "token_load_error",
                    "source": "file",
                    "error": str(e)
                })
                creds = None

        # 3. Refresh if expired
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                _log("info", "get_gmail_service", {
                    "event": "token_refresh",
                    "status": "refreshing"
                })
                try:
                    creds.refresh(Request())
                except (ValueError, IOError) as e:
                    _log("error", "get_gmail_service", {
                        "event": "token_refresh_failed",
                        "error": str(e)
                    })
                    try:
                        from servers.alerting import alert_admin, CRITICAL
                        alert_admin(CRITICAL, "gmail_auth_expired",
                                    f"Gmail OAuth refresh failed: {str(e)[:200]}. Re-authenticate.")
                    except Exception:
                        pass
                    raise
            else:
                # Interactive OAuth â€” only possible locally
                if GMAIL_TOKEN_JSON:
                    try:
                        from servers.alerting import alert_admin, CRITICAL
                        alert_admin(CRITICAL, "gmail_auth_expired",
                                    "Gmail token invalid/expired and cannot be refreshed. Re-authenticate.")
                    except Exception:
                        pass
                    raise RuntimeError(
                        "GMAIL_TOKEN_JSON is set but token is invalid/expired and cannot be refreshed. "
                        "Re-authenticate locally, upload the new token.json to Secret Manager, and redeploy."
                    )
                _log("info", "get_gmail_service", {
                    "event": "user_login_required",
                    "status": "awaiting_auth"
                })
                if GMAIL_CRED_JSON:
                    import tempfile
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
                        tmp.write(GMAIL_CRED_JSON)
                        tmp_path = tmp.name
                    try:
                        flow = InstalledAppFlow.from_client_secrets_file(tmp_path, SCOPES)
                    finally:
                        try:
                            os.unlink(tmp_path)
                        except OSError:
                            pass
                elif os.path.exists(CRED_FILE):
                    flow = InstalledAppFlow.from_client_secrets_file(CRED_FILE, SCOPES)
                else:
                    raise FileNotFoundError(f"No credentials available (checked GMAIL_CRED_JSON and '{CRED_FILE}')")
                creds = flow.run_local_server(port=0)

            # Save refreshed token back to file (local dev only)
            if not GMAIL_TOKEN_JSON:
                with open(TOKEN_FILE, 'w') as token:
                    token.write(creds.to_json())
                _log("info", "get_gmail_service", {
                    "event": "token_saved",
                    "status": "success"
                })

        _gmail_creds = creds
        _thread_local.service = build('gmail', 'v1', credentials=creds, cache_discovery=False)
        return _thread_local.service

def normalize_email_date(date_str: str) -> str:
    """
    Convert email date string to UTC ISO format.
    Phase 4 requirement: All timestamps must be stored in UTC.
    """
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(date_str)
        # RFC dates with no zone (or -0000) can return naive datetimes;
        # treat them as UTC rather than letting astimezone() use server local time.
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        # Convert to UTC
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.isoformat()
    except (ValueError, TypeError) as e:
        _log("warning", "normalize_email_date", {
            "event": "date_parse_warning",
            "original_date": date_str,
            "error": str(e)
        })
        return date_str  # Return original if parsing fails

# --- TOOLS ---

GMAIL_PUBSUB_TOPIC = os.getenv(
    "GMAIL_PUBSUB_TOPIC",
    "projects/YOUR_GCP_PROJECT_ID/topics/gmail-push-notifications",
)


@retry(exceptions=(HttpError, IOError), tries=3, delay=1, backoff=2)
def _register_gmail_watch_sync() -> str:
    """Register/renew the Gmail push watch. Idempotent â€” safe to call daily.

    Gmail push watches expire every 7 days. Called by the renew_gmail_watch scheduled
    task and on server startup so real-time push delivery never silently lapses.
    """
    start_time = time.time()
    try:
        service = get_gmail_service()
        resp = service.users().watch(userId='me', body={
            "topicName": GMAIL_PUBSUB_TOPIC,
            "labelIds": ["INBOX"],
            "labelFilterBehavior": "INCLUDE",
        }).execute()
        latency_ms = int((time.time() - start_time) * 1000)
        _log("info", "register_gmail_watch", {
            "event": "gmail_watch_registered",
            "expiration": resp.get("expiration"),
            "historyId": resp.get("historyId"),
            "latency_ms": latency_ms,
        })
        return json.dumps({
            "status": "success",
            "expiration": resp.get("expiration"),
            "historyId": resp.get("historyId"),
        })
    except HttpError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "register_gmail_watch", {
            "event": "gmail_watch_error",
            "error": str(e),
            "latency_ms": latency_ms,
        })
        return _error_response(tool="register_gmail_watch", error_code="api_error",
                               message=str(e), latency_ms=latency_ms)


@retry(exceptions=(HttpError, IOError), tries=3, delay=1, backoff=2)
def _list_unread_emails_sync(limit: int = 5, include_read: bool = True, window: str | None = None, include_processed: bool = False) -> str:
    """List the snippets of the most recent emails.

    Args:
        limit: max messages to return (default 5).
        include_read: if True (default), returns ALL emails within the lookback window,
            read or unread. Pass False for a fast unread-only scan.
        window: lookback window for include_read=True mode (e.g. "2d", "7d").
            Defaults to GMAIL_SCAN_WINDOW env var (default "1d").
            Ignored when include_read=False.
        include_processed: if True, omit the -label:AI_PROCESSED filter so already-handled
            emails are included. Use for display/triage requests ("check emails", "what's in
            my inbox") where the operator needs to see everything. Default False (action/sweep
            mode) so background scans never re-process handled mail.
    """
    start_time = time.time()
    try:
        service = get_gmail_service()
        # Build query without label-scoped exclusion (thread-scoped filters would swallow
        # new replies in previously-handled threads). We post-filter per-message below instead.
        if include_read:
            # Read+unread mode: time-bounded, inbox only (excludes Drafts/Sent)
            _window = window or SCAN_WINDOW
            q = f"newer_than:{_window} in:inbox"
        else:
            # Unread-only fast scan â€” pass include_read=False explicitly to use this
            q = f"is:unread in:inbox"
        # In action mode (include_processed=False) we over-fetch so client-side skipping
        # doesn't starve the requested limit.
        fetch_limit = limit if include_processed else min(limit * 10, 500)
        results = service.users().messages().list(
            userId='me',
            q=q,
            maxResults=fetch_limit
        ).execute()

        if not results.get('messages'):
            _log("info", "list_unread_emails", {
                "event": "list_unread_emails",
                "status": "no_results",
                "include_read": include_read,
                "latency_ms": int((time.time() - start_time) * 1000)
            })
            return json.dumps({
                "status": "success",
                "emails": [],
                "include_read": include_read,
                "message": "No messages found."
            })

        # Resolve AI_PROCESSED label id once â€” needed both for early message-scoped post-filter
        # and for the per-email 'processed' flag in the output.
        try:
            ai_processed_label_id = _get_or_create_ai_processed_label_id(service)
        except Exception:
            ai_processed_label_id = None

        try:
            ai_booked_label_id = _get_or_create_ai_booked_label_id(service)
        except Exception:
            ai_booked_label_id = None

        output = []
        MAX_PAGES = 5  # bound API calls
        page_count = 0

        while True:
            messages = results.get('messages', [])
            for msg in messages:
                if not include_processed and len(output) >= limit:
                    break
                # Fetch details for each email â€” labelIds come back here at no extra cost
                txt = service.users().messages().get(
                    userId='me', id=msg['id'], format='metadata',
                    metadataHeaders=['Subject', 'From']
                ).execute()

                headers = txt['payload']['headers']
                subject = next((h['value'] for h in headers if h['name'] == 'Subject'), "No Subject")
                sender = next((h['value'] for h in headers if h['name'] == 'From'), "Unknown")
                label_ids = txt.get('labelIds', [])
                is_processed = (ai_processed_label_id in label_ids) if ai_processed_label_id else False

                # Message-scoped post-filter: skip messages whose own label is AI_PROCESSED
                # (not thread-scoped â€” new replies in the same thread are unlabeled and pass through)
                if not include_processed and is_processed:
                    continue

                output.append({
                    "id": msg['id'],
                    "thread_id": msg.get('threadId', ''),
                    "from": sender,
                    "subject": subject,
                    "unread": 'UNREAD' in label_ids,
                    "processed": is_processed,
                    # True when the thread already has a confirmed booking (AI_BOOKED label).
                    # If True: do NOT call create_inventory_booking â€” only reply/amend.
                    "already_booked": (ai_booked_label_id in label_ids) if ai_booked_label_id else False,
                })
                if not include_processed and len(output) >= limit:
                    break

            page_count += 1
            next_page = results.get('nextPageToken')
            # Stop if: we have enough rows, no more pages, or we've hit the page cap
            if len(output) >= limit or not next_page or page_count >= MAX_PAGES:
                break

            results = service.users().messages().list(
                userId='me',
                q=q,
                maxResults=fetch_limit,
                pageToken=next_page
            ).execute()
        
        latency_ms = int((time.time() - start_time) * 1000)
        _log("info", "list_unread_emails", {
            "event": "list_unread_emails",
            "status": "success",
            "count": len(output),
            "include_read": include_read,
            "latency_ms": latency_ms
        })

        return json.dumps({
            "status": "success",
            "emails": output,
            "count": len(output),
            "include_read": include_read
        })
        
    except HttpError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "list_unread_emails", {
            "event": "list_unread_emails",
            "status": "http_error",
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="list_unread_emails", error_code="api_error", message="Gmail API request failed", latency_ms=latency_ms)
    except (FileNotFoundError, ValueError, KeyError) as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "list_unread_emails", {
            "event": "list_unread_emails",
            "status": "config_error",
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="list_unread_emails", error_code="config_error", message=str(e), latency_ms=latency_ms)

@mcp.tool()
async def list_unread_emails(limit: int = 5, include_read: bool = True, window: str | None = None, include_processed: bool = False) -> str:
    """List the snippets of the most recent emails.

    Default (include_read=True, include_processed=False): returns all emails â€” read and
    unread â€” within the last GMAIL_SCAN_WINDOW days (default 1d), EXCLUDING AI_PROCESSED
    threads. Standard mode for background sweeps and webhook processing â€” never re-processes
    already-handled mail.

    For DISPLAY/TRIAGE requests ("check emails", "what's in my inbox", "show recent emails"):
    pass include_processed=True so already-handled read emails are visible. Each returned
    email carries unread (bool) and processed (bool) flags â€” annotate handled items as
    "(already handled)" and do NOT draft replies to them unless the operator explicitly asks.

    Pass include_read=False for a fast unread-only scan.
    Pass window="7d" to widen the lookback (interactive only).
    """
    return await asyncio.to_thread(_list_unread_emails_sync, limit, include_read, window, include_processed)


MAX_BODY_CHARS = 2500  # per-body character cap; ~625 tokens; tightened from 4000 to cut input token cost
SCAN_WINDOW = os.getenv("GMAIL_SCAN_WINDOW", "1d")  # time window for read-email sweeps; 1d = tight baseline (webhook+30-min); EOD sweep pins 2d explicitly; override via env


def _cap_body(text: str) -> tuple:
    """Truncate a body string to MAX_BODY_CHARS; return (capped_text, was_truncated)."""
    if text and len(text) > MAX_BODY_CHARS:
        return text[:MAX_BODY_CHARS] + "\nâ€¦[truncated]", True
    return text, False


@retry(exceptions=(HttpError, IOError, ValueError), tries=3, delay=1, backoff=2)
def _read_email_body_sync(message_id: str) -> str:
    """Read the full text content of a specific email by ID, plus the entire thread context."""
    start_time = time.time()
    try:
        service = get_gmail_service()

        # Step 1: resolve thread_id from the triggering message
        meta = service.users().messages().get(
            userId='me', id=message_id, format='metadata',
            metadataHeaders=['Subject', 'From', 'Date', 'Message-Id', 'References']
        ).execute()
        thread_id = meta.get('threadId', '')

        # Step 2: fetch the whole thread (Gmail returns messages oldest â†’ newest)
        thread_data = service.users().threads().get(
            userId='me', id=thread_id, format='full'
        ).execute()
        all_msgs = thread_data.get('messages', [meta])  # fallback to single msg if thread empty

        # Step 3: build thread list â€” newest 2 get full body (capped); older get snippet only
        FULL_BODY_LIMIT = 2  # tightened from 3; 2 messages is enough context to reply in-thread
        total = len(all_msgs)
        cutoff = max(0, total - FULL_BODY_LIMIT)  # messages before this index get snippet only

        thread_list = []
        for i, m in enumerate(all_msgs):
            # Guard: drafts/partial messages may lack payload.headers
            h = m.get('payload', {}).get('headers', [])
            m_from = next((x['value'] for x in h if x['name'] == 'From'), "Unknown")
            m_date_raw = next((x['value'] for x in h if x['name'] == 'Date'), "Unknown")
            m_rfc_id = next((x['value'] for x in h if x['name'].lower() == 'message-id'), None)
            m_snippet = m.get('snippet', '')
            if i < cutoff:
                # Older message â€” snippet only to protect context window
                thread_list.append({
                    "message_id": m['id'],
                    "rfc_message_id": m_rfc_id,
                    "from": m_from,
                    "date": m_date_raw,
                    "date_utc": normalize_email_date(m_date_raw),
                    "body": "",
                    "snippet": m_snippet,
                    "truncated": True,
                })
            else:
                m_body = _extract_email_text(m.get('payload', {}))
                m_attachments = _extract_attachments(m.get('payload', {}))
                m_raw = m_body if m_body.strip() else m_snippet
                m_content, m_body_truncated = _cap_body(m_raw)
                thread_list.append({
                    "message_id": m['id'],
                    "rfc_message_id": m_rfc_id,
                    "from": m_from,
                    "date": m_date_raw,
                    "date_utc": normalize_email_date(m_date_raw),
                    "body": m_content,
                    "snippet": m_snippet,
                    "truncated": False,
                    "body_truncated": m_body_truncated,
                    "attachments": m_attachments,
                })

        # Step 3b: detect whether a human/admin already replied to this thread.
        # The system only ever creates drafts (never sends), so any message with the Gmail
        # SENT label was sent by a human. Check the newest thread message â€” if it's SENT,
        # an admin replied. Callers (sweeps) should call mark_email_processed and skip
        # drafting when admin_replied is True.
        newest_msg = all_msgs[-1]
        newest_labels = newest_msg.get('labelIds', [])
        admin_replied = 'SENT' in newest_labels

        # Step 3c: detect whether this thread already has a confirmed booking.
        # AI_BOOKED is applied thread-scope by mark_thread_booked after a successful
        # create_inventory_booking call, so it appears on all messages in the thread.
        try:
            _booked_lbl = _get_or_create_ai_booked_label_id(service)
            already_booked = any(_booked_lbl in m.get('labelIds', []) for m in all_msgs)
        except Exception:
            already_booked = False
        admin_reply_from = None
        admin_reply_date = None
        if admin_replied:
            newest_h = newest_msg.get('payload', {}).get('headers', [])
            admin_reply_from = next((x['value'] for x in newest_h if x['name'] == 'From'), None)
            admin_reply_date = next((x['value'] for x in newest_h if x['name'] == 'Date'), None)

        # Step 4: extract top-level fields from the triggering message (backward compat)
        # Find it in all_msgs; fall back to the newest message
        trig = next((m for m in all_msgs if m['id'] == message_id), all_msgs[-1])
        trig_headers = trig.get('payload', {}).get('headers', [])
        subject = next((x['value'] for x in trig_headers if x['name'] == 'Subject'), "No Subject")
        sender = next((x['value'] for x in trig_headers if x['name'] == 'From'), "Unknown")
        date_raw = next((x['value'] for x in trig_headers if x['name'] == 'Date'), "Unknown")
        rfc_message_id = next((x['value'] for x in trig_headers if x['name'].lower() == 'message-id'), None)
        references = next((x['value'] for x in trig_headers if x['name'].lower() == 'references'), None)
        date_utc = normalize_email_date(date_raw)
        body = _extract_email_text(trig.get('payload', {}))
        trig_attachments = _extract_attachments(trig.get('payload', {}))
        snippet = trig.get('snippet', '')
        raw_content = body if body.strip() else snippet
        content, trig_body_truncated = _cap_body(raw_content)

        latency_ms = int((time.time() - start_time) * 1000)
        _log("info", "read_email_body", {
            "event": "read_email_body",
            "status": "success",
            "message_id": message_id,
            "thread_id": thread_id,
            "thread_message_count": total,
            "thread_messages_full": total - cutoff,
            "body_length": len(content),
            "admin_replied": admin_replied,
            "latency_ms": latency_ms
        })

        return json.dumps({
            "status": "success",
            # Triggering message fields (unchanged shape for backward compat)
            "message_id": message_id,
            "thread_id": thread_id,
            "rfc_message_id": rfc_message_id,
            "references": references,
            "from": sender,
            "subject": subject,
            "date": date_raw,
            "date_utc": date_utc,
            "body": content,
            "body_truncated": trig_body_truncated,
            "snippet": snippet,
            "attachments": trig_attachments,
            # Admin-reply detection: True if newest thread message has Gmail SENT label
            # (system only creates drafts, so SENT = human/admin sent it).
            # If True: call mark_email_processed and do NOT draft a reply.
            "admin_replied": admin_replied,
            "admin_reply_from": admin_reply_from,
            "admin_reply_date": admin_reply_date,
            # Booking detection: True if the AI_BOOKED label is present on any thread message.
            # Applied thread-scope by mark_thread_booked after a successful booking.
            # If True: do NOT call create_inventory_booking â€” only reply/amend.
            "already_booked": already_booked,
            # Full thread context (oldest â†’ newest; newest 2 have full body capped at MAX_BODY_CHARS)
            "thread": thread_list,
            "messages_total": total,
            "messages_full": total - cutoff,
        })
        
    except HttpError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "read_email_body", {
            "event": "read_email_body",
            "status": "http_error",
            "message_id": message_id,
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="read_email_body", error_code="api_error", message="Gmail API request failed", latency_ms=latency_ms)
    except (ValueError, KeyError, UnicodeDecodeError) as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "read_email_body", {
            "event": "read_email_body",
            "status": "parse_error",
            "message_id": message_id,
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="read_email_body", error_code="validation_error", message=f"Email parsing error: {str(e)}", latency_ms=latency_ms)

@mcp.tool()
async def read_email_body(message_id: str) -> str:
    """Read the full text content of a specific email by ID."""
    return await asyncio.to_thread(_read_email_body_sync, message_id)


@retry(exceptions=(HttpError, IOError, ValueError), tries=3, delay=1, backoff=2)
def _draft_reply_sync(to_email: str, subject: str, body_text: str,
                      reply_to_message_id: str | None = None,
                      thread_id: str | None = None) -> str:
    """
    Create a DRAFT email (does not send it).
    Use this for human-in-the-loop workflows where review is required.
    Rate limited to 50 emails per hour (Phase 5 requirement).

    When reply_to_message_id is provided:
      - Deduplicates: skips draft creation if this message was already handled.
      - Threads: sets In-Reply-To/References headers and threadId.
      - Marks source email AI_PROCESSED and removes UNREAD after draft creation.
    """
    start_time = time.time()
    draft_created = False  # gates lock cleanup: never release the dedup lock once a draft exists (G1)

    # --- Dedup check ---
    # Covers two cases:
    #   Reply (reply_to_message_id present): key = email:<message_id>, TTL 30 days
    #   Proactive (no reply_to_message_id):  key = email:proactive:<sha1(to|subject)>, TTL 1 day
    doc_ref = None
    lock_key = None
    ttl_days = _DEDUP_TTL_DAYS
    if reply_to_message_id:
        lock_key = f"email:{reply_to_message_id}"
    elif to_email and subject:
        digest = hashlib.sha1(
            f"{to_email.strip().lower()}|{subject.strip().lower()}".encode()
        ).hexdigest()
        lock_key = f"email:proactive:{digest}"
        ttl_days = _PROACTIVE_DEDUP_TTL_DAYS
    if lock_key:
        try:
            db = _get_firestore()
            doc_ref = db.collection(_LOCK_COLLECTION).document(lock_key)
            now = datetime.now(timezone.utc)
            expires_at = now + timedelta(days=ttl_days)
            try:
                doc_ref.create({"owner": "gmail_server_draft", "created_at": now, "expires_at": expires_at})
            except AlreadyExists:
                snap = doc_ref.get()
                if snap.exists:
                    data = snap.to_dict()
                    ea = data.get("expires_at")
                    if hasattr(ea, 'to_pydatetime'):
                        ea = ea.to_pydatetime()
                    if ea and ea > now:
                        latency_ms = int((time.time() - start_time) * 1000)
                        _log("info", "draft_reply", {
                            "event": "draft_reply",
                            "status": "skipped_duplicate",
                            "lock_key": lock_key,
                            "latency_ms": latency_ms
                        })
                        # Ensure AI_PROCESSED label is set even on duplicate-draft paths
                        if reply_to_message_id:
                            try:
                                _mark_email_processed_sync(reply_to_message_id)
                            except Exception:
                                pass  # best-effort; skipped_duplicate already means this thread is handled
                        return json.dumps({
                            "status": "skipped",
                            "reason": "already_replied",
                            "lock_key": lock_key
                        })
                    # Expired â€” overwrite
                    doc_ref.set({"owner": "gmail_server_draft", "created_at": now, "expires_at": expires_at})
        except Exception as e:
            _invalidate_firestore_client()
            _log("warning", "draft_reply", {
                "event": "dedup_check_failed",
                "lock_key": lock_key,
                "error": str(e)
            })
            return json.dumps({
                "status": "error",
                "message": "Dedup check unavailable, retry in a moment"
            })

    # Rate limit check â€” Firestore-backed sliding window (global across all Cloud Run instances)
    allowed, rate_msg, rate_doc_id = _check_rate_limit()
    if not allowed:
        if doc_ref:
            try:
                doc_ref.delete()
            except Exception:
                pass
        _log("warning", "draft_reply", {
            "event": "draft_reply",
            "status": "rate_limit_exceeded",
            "to": to_email,
            "message": rate_msg
        })
        return _error_response(tool="draft_reply", error_code="rate_limit_exceeded", message=rate_msg)

    try:
        service = get_gmail_service()

        message = EmailMessage()
        message.set_content(body_text)
        message.add_alternative(_markdown_to_html(body_text), subtype="html")
        message['To'] = to_email
        message['Subject'] = subject

        # Resolve source message: prefer explicit reply_to_message_id; fall back to the last
        # message in the thread when only thread_id is supplied.
        # Rationale: Gmail's threadId only threads within the sender's mailbox. External mail
        # clients thread purely on RFC 5322 headers (In-Reply-To, References, Subject), so we
        # must set those headers on every reply regardless of which parameter the agent passed.
        source_id = reply_to_message_id
        if not source_id and thread_id:
            try:
                thr = service.users().threads().get(
                    userId='me', id=thread_id, format='metadata',
                    metadataHeaders=['Message-Id', 'References', 'Subject']
                ).execute()
                thr_msgs = thr.get('messages', [])
                if thr_msgs:
                    source_id = thr_msgs[-1]['id']
            except Exception as e:
                _log("warning", "draft_reply", {"event": "thread_header_fetch_failed", "error": str(e)})

        if source_id:
            try:
                orig = service.users().messages().get(
                    userId='me', id=source_id, format='metadata',
                    metadataHeaders=['Message-Id', 'References', 'Subject']
                ).execute()
                orig_headers = orig.get('payload', {}).get('headers', [])

                def _get_orig_header(name):
                    # Case-insensitive: Gmail returns headers with on-the-wire casing
                    # (e.g. "Message-ID" not "Message-Id") so exact match silently fails.
                    return next(
                        (h['value'] for h in orig_headers if h['name'].lower() == name.lower()),
                        None
                    )

                orig_message_id = _get_orig_header('Message-ID')
                orig_references = _get_orig_header('References')
                orig_subject    = _get_orig_header('Subject')

                if orig_message_id:
                    message['In-Reply-To'] = orig_message_id
                    refs = (orig_references + ' ' + orig_message_id) if orig_references else orig_message_id
                    message['References'] = refs

                # Subject preservation: external clients fall back to subject-based threading
                # when reference headers are stripped by relays. Only rewrite if the caller
                # didn't already supply a Re: prefix.
                if orig_subject and not subject.lower().startswith('re:'):
                    message.replace_header('Subject', f"Re: {orig_subject}")
            except Exception as e:
                _log("warning", "draft_reply", {"event": "thread_header_fetch_failed", "error": str(e)})

        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        draft_message = {'raw': encoded_message}
        if thread_id:
            draft_message['threadId'] = thread_id
        create_message = {'message': draft_message}

        auto_send = _is_auto_send_enabled()
        try:
            if auto_send:
                result = service.users().messages().send(userId='me', body=draft_message).execute()
            else:
                result = service.users().drafts().create(userId='me', body=create_message).execute()
            draft_created = True
        except (HttpError, IOError, ValueError) as e:
            # Draft create failed. Do NOT re-raise: re-raising fed the outer @retry, which
            # replayed the whole function and produced duplicate customer drafts (audit G1).
            #
            # The per-message dedup lock (email:<reply_to_message_id>, or the proactive
            # recipient+subject hash â€” created above) is what prevents duplicates across
            # re-invocations. Whether we release it here depends on whether a draft could
            # have been created:
            #   - DEFINITE non-creation (HttpError 4xx, or ValueError): the request was
            #     rejected, no draft exists â†’ release the lock + refund the slot so the
            #     message reprocesses cleanly. No duplicate risk â€” nothing was created.
            #   - AMBIGUOUS (IOError/socket timeout, or HttpError 5xx): the draft MAY have
            #     been created server-side with the response lost â†’ KEEP the lock so a
            #     re-invocation is skipped (skipped_duplicate) instead of creating a second
            #     draft. Slot stays consumed. Human-in-the-loop review surfaces the rare
            #     stuck message. This covers both reply and proactive paths.
            status = getattr(getattr(e, "resp", None), "status", None)
            ambiguous = isinstance(e, IOError) or (
                isinstance(e, HttpError) and status is not None and status >= 500
            )
            if not ambiguous:
                if doc_ref:
                    try:
                        doc_ref.delete()
                    except Exception:
                        pass
                _refund_rate_limit(rate_doc_id)
            latency_ms = int((time.time() - start_time) * 1000)
            _log("error", "draft_reply", {
                "event": "draft_reply",
                "status": "draft_create_failed",
                "ambiguous": ambiguous,
                "to": to_email,
                "error": str(e),
                "latency_ms": latency_ms,
            })
            return _error_response(tool="draft_reply", error_code="api_error", message="Draft creation failed", latency_ms=latency_ms)

        # Mark source email as processed
        if reply_to_message_id:
            try:
                label_id = _get_or_create_ai_processed_label_id(service)
                service.users().messages().modify(
                    userId='me',
                    id=reply_to_message_id,
                    body={'removeLabelIds': ['UNREAD'], 'addLabelIds': [label_id]}
                ).execute()
            except Exception as e:
                _log("warning", "draft_reply", {
                    "event": "mark_processed_failed",
                    "message_id": reply_to_message_id,
                    "error": str(e)
                })

        latency_ms = int((time.time() - start_time) * 1000)
        send_mode = "auto_send" if auto_send else "draft"
        result_id = result['id']
        _log("info", "draft_reply", {
            "event": "draft_reply",
            "status": "success",
            "send_mode": send_mode,
            "draft_id": result_id,
            "to": to_email,
            "subject": subject,
            "rate_limit_status": rate_msg,
            "latency_ms": latency_ms
        })

        action_label = "Sent" if auto_send else "Draft created"
        return json.dumps({
            "status": "success",
            "draft_id": result_id,
            "send_mode": send_mode,
            "to": to_email,
            "subject": subject,
            "message": f"{action_label} successfully (ID: {result_id})",
            "rate_limit_status": rate_msg
        })

    except HttpError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        if not draft_created and doc_ref:
            try:
                doc_ref.delete()
            except Exception:
                pass
            _refund_rate_limit(rate_doc_id)
        _log("error", "draft_reply", {
            "event": "draft_reply",
            "status": "http_error",
            "to": to_email,
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="draft_reply", error_code="api_error", message="Gmail API request failed", latency_ms=latency_ms)
    except (ValueError, TypeError) as e:
        latency_ms = int((time.time() - start_time) * 1000)
        if not draft_created and doc_ref:
            try:
                doc_ref.delete()
            except Exception:
                pass
            _refund_rate_limit(rate_doc_id)
        _log("error", "draft_reply", {
            "event": "draft_reply",
            "status": "encoding_error",
            "to": to_email,
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="draft_reply", error_code="validation_error", message=f"Email encoding error: {str(e)}", latency_ms=latency_ms)
    except Exception as e:
        # Catch-all for exceptions outside the handlers above â€” e.g. get_gmail_service()
        # RuntimeError/FileNotFoundError or auth RefreshError raised BEFORE the draft was
        # created. Without this the dedup lock (created before the draft attempt) would
        # persist with no draft ever made, poisoning this message for the lock's TTL
        # (audit G1, poison path). Release the lock + refund the slot when no draft exists.
        latency_ms = int((time.time() - start_time) * 1000)
        if not draft_created and doc_ref:
            try:
                doc_ref.delete()
            except Exception:
                pass
            _refund_rate_limit(rate_doc_id)
        _log("error", "draft_reply", {
            "event": "draft_reply",
            "status": "unexpected_error",
            "to": to_email,
            "error": str(e),
            "draft_created": draft_created,
            "latency_ms": latency_ms
        })
        return _error_response(tool="draft_reply", error_code="internal_error", message=str(e), latency_ms=latency_ms)

@mcp.tool()
async def draft_reply(to_email: str, subject: str, body_text: str,
                      reply_to_message_id: str | None = None,
                      thread_id: str | None = None) -> str:
    """Create a DRAFT email (does not send it). When replying to an inbound email, pass reply_to_message_id and thread_id to deduplicate and thread the draft correctly."""
    return await asyncio.to_thread(_draft_reply_sync, to_email, subject, body_text, reply_to_message_id, thread_id)


@mcp.tool()
async def send_email(to_email: str, subject: str, body_text: str,
                     reply_to_message_id: str | None = None,
                     thread_id: str | None = None) -> str:
    """Create a draft email for human review (does not send). Rate limited to 50 emails per hour. When replying to an inbound email, pass reply_to_message_id and thread_id to deduplicate and thread the draft correctly."""
    return await asyncio.to_thread(_draft_reply_sync, to_email, subject, body_text, reply_to_message_id, thread_id)


@retry(exceptions=(HttpError, IOError), tries=3, delay=1, backoff=2)
def _list_sent_emails_sync(limit: int = 10, after_date: str = "") -> str:
    """
    List recently sent emails.
    Used by the 11:30 verification_emails task to confirm confirmation emails were sent.
    Args:
        limit: Max number of sent emails to return (default 10).
        after_date: Optional date filter in YYYY/MM/DD format (e.g. '2026/04/05').
    """
    start_time = time.time()
    try:
        service = get_gmail_service()
        query = "in:sent"
        if after_date:
            query += f" after:{after_date}"

        results = service.users().messages().list(
            userId='me',
            q=query,
            maxResults=limit,
        ).execute()

        messages = results.get('messages', [])

        if not messages:
            _log("info", "list_sent_emails", {
                "event": "list_sent_emails",
                "status": "no_results",
                "latency_ms": int((time.time() - start_time) * 1000),
            })
            return json.dumps({
                "status": "success",
                "emails": [],
                "message": "No sent messages found.",
            })

        output = []
        for msg in messages:
            txt = service.users().messages().get(
                userId='me', id=msg['id'], format='metadata',
                metadataHeaders=['Subject', 'To', 'Date'],
            ).execute()
            headers = txt['payload']['headers']
            subject = next((h['value'] for h in headers if h['name'] == 'Subject'), "No Subject")
            to = next((h['value'] for h in headers if h['name'] == 'To'), "Unknown")
            date_raw = next((h['value'] for h in headers if h['name'] == 'Date'), "Unknown")
            output.append({
                "id": msg['id'],
                "to": to,
                "subject": subject,
                "date": normalize_email_date(date_raw),
                "snippet": txt.get('snippet', ''),
            })

        latency_ms = int((time.time() - start_time) * 1000)
        _log("info", "list_sent_emails", {
            "event": "list_sent_emails",
            "status": "success",
            "count": len(output),
            "latency_ms": latency_ms,
        })
        return json.dumps({
            "status": "success",
            "emails": output,
            "count": len(output),
        })

    except HttpError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "list_sent_emails", {
            "event": "list_sent_emails",
            "status": "http_error",
            "error": str(e),
            "latency_ms": latency_ms,
        })
        return _error_response(tool="list_sent_emails", error_code="api_error", message="Gmail API request failed", latency_ms=latency_ms)
    except (FileNotFoundError, ValueError, KeyError) as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "list_sent_emails", {
            "event": "list_sent_emails",
            "status": "config_error",
            "error": str(e),
            "latency_ms": latency_ms,
        })
        return _error_response(tool="list_sent_emails", error_code="config_error", message=str(e), latency_ms=latency_ms)


@mcp.tool()
async def list_sent_emails(limit: int = 10, after_date: str = "") -> str:
    """List recently sent emails."""
    return await asyncio.to_thread(_list_sent_emails_sync, limit, after_date)


@retry(exceptions=(HttpError, IOError), tries=3, delay=1, backoff=2)
def _mark_email_processed_sync(message_id: str) -> str:
    """Label an email AI_PROCESSED and mark it read, without drafting a reply.

    Use for spam, no-action-needed, or already-handled emails so they stop
    re-surfacing in list_unread_emails on every agent run.
    """
    # CLI/manual bookings carry no source_message_id â€” clean no-op instead of
    # passing None to the Gmail API (which raises and burns the @retry budget).
    if not message_id:
        return json.dumps({"status": "skipped", "reason": "no_message_id"})
    start_time = time.time()
    try:
        service = get_gmail_service()
        label_id = _get_or_create_ai_processed_label_id(service)
        service.users().messages().modify(
            userId='me',
            id=message_id,
            body={'removeLabelIds': ['UNREAD'], 'addLabelIds': [label_id]}
        ).execute()
        latency_ms = int((time.time() - start_time) * 1000)
        _log("info", "mark_email_processed", {
            "event": "mark_email_processed",
            "status": "success",
            "message_id": message_id,
            "latency_ms": latency_ms
        })
        return json.dumps({"status": "success", "message_id": message_id})
    except HttpError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "mark_email_processed", {
            "event": "mark_email_processed",
            "status": "http_error",
            "message_id": message_id,
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="mark_email_processed", error_code="api_error", message="Gmail API request failed", latency_ms=latency_ms)
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "mark_email_processed", {
            "event": "mark_email_processed",
            "status": "error",
            "message_id": message_id,
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="mark_email_processed", error_code="internal_error", message="Failed to mark email processed", latency_ms=latency_ms)


@mcp.tool()
async def mark_email_processed(message_id: str) -> str:
    """Label an email AI_PROCESSED and mark it read without drafting a reply.
    Use for spam, auto-replies, or no-action emails so they stop appearing in list_unread_emails."""
    return await asyncio.to_thread(_mark_email_processed_sync, message_id)


def _mark_thread_booked_sync(thread_id: str) -> str:
    """Apply the AI_BOOKED label to an entire Gmail thread after a confirmed booking.

    Thread-scoped: every current and future message in the thread carries the label,
    so replies surface as already_booked=True in list_unread_emails and read_email_body.
    This prevents reply-triggered re-booking and gives ops visibility in Gmail.
    """
    if not thread_id:
        return json.dumps({"status": "skipped", "reason": "no_thread_id"})
    start_time = time.time()
    try:
        service = get_gmail_service()
        booked_label_id = _get_or_create_ai_booked_label_id(service)
        service.users().threads().modify(
            userId='me',
            id=thread_id,
            body={'addLabelIds': [booked_label_id]}
        ).execute()
        latency_ms = int((time.time() - start_time) * 1000)
        _log("info", "mark_thread_booked", {
            "event": "mark_thread_booked",
            "status": "success",
            "thread_id": thread_id,
            "latency_ms": latency_ms
        })
        return json.dumps({"status": "success", "thread_id": thread_id})
    except HttpError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "mark_thread_booked", {
            "event": "mark_thread_booked",
            "status": "http_error",
            "thread_id": thread_id,
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="mark_thread_booked", error_code="api_error", message="Gmail API request failed", latency_ms=latency_ms)
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "mark_thread_booked", {
            "event": "mark_thread_booked",
            "status": "error",
            "thread_id": thread_id,
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="mark_thread_booked", error_code="internal_error", message="Failed to mark thread booked", latency_ms=latency_ms)


@mcp.tool()
async def mark_thread_booked(thread_id: str) -> str:
    """Apply the AI_BOOKED label to a Gmail thread after a successful booking.
    Thread-scoped â€” all replies in the thread will show already_booked=True.
    Call immediately after create_inventory_booking returns status 'success' or 'partial_success'."""
    return await asyncio.to_thread(_mark_thread_booked_sync, thread_id)


def _mark_chase_drafted_sync(message_id: str) -> str:
    """Apply the AI_CHASE_DRAFTED label to a chase email draft message.

    Message-scoped: labels the specific draft so all chase drafts are visible
    under one Gmail label for ops review.
    """
    if not message_id:
        return json.dumps({"status": "skipped", "reason": "no_message_id"})
    start_time = time.time()
    try:
        service = get_gmail_service()
        label_id = _get_or_create_ai_chase_drafted_label_id(service)
        service.users().messages().modify(
            userId='me',
            id=message_id,
            body={'addLabelIds': [label_id]}
        ).execute()
        latency_ms = int((time.time() - start_time) * 1000)
        _log("info", "mark_chase_drafted", {
            "event": "mark_chase_drafted",
            "status": "success",
            "message_id": message_id,
            "latency_ms": latency_ms
        })
        return json.dumps({"status": "success", "message_id": message_id})
    except HttpError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "mark_chase_drafted", {
            "event": "mark_chase_drafted",
            "status": "http_error",
            "message_id": message_id,
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="mark_chase_drafted", error_code="api_error", message="Gmail API request failed", latency_ms=latency_ms)
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "mark_chase_drafted", {
            "event": "mark_chase_drafted",
            "status": "error",
            "message_id": message_id,
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="mark_chase_drafted", error_code="internal_error", message="Failed to label chase draft", latency_ms=latency_ms)


@mcp.tool()
async def mark_chase_drafted(message_id: str) -> str:
    """Apply the AI_CHASE_DRAFTED Gmail label to a chase email draft.
    Message-scoped â€” labels the specific draft for ops visibility.
    Call after draft_reply returns successfully for each chased invoice,
    passing the message_id from the draft_reply response."""
    return await asyncio.to_thread(_mark_chase_drafted_sync, message_id)


# ---------------------------------------------------------------------------
# Image attachment vision
# ---------------------------------------------------------------------------

_vision_client = None
_vision_lock = threading.Lock()
_VISION_MODEL = "gemini-3.5-flash-lite"
_MAX_IMAGE_SIZE = 5_000_000  # 5MB


def _get_vision_client():
    global _vision_client
    if _vision_client is None:
        with _vision_lock:
            if _vision_client is None:
                from google import genai as _genai
                _vision_client = _genai.Client(
                    vertexai=True,
                    project=os.environ.get("GOOGLE_CLOUD_PROJECT"),
                    location=os.environ.get("GOOGLE_CLOUD_LOCATION", "europe-west2"),
                )
    return _vision_client


def _invalidate_vision_client() -> None:
    global _vision_client
    _vision_client = None


@mcp.tool()
async def describe_email_attachment(message_id: str, attachment_id: str, filename: str = "") -> str:
    """Download an image attachment and return an AI-generated text description.

    Only call this when the email content references an attached image or the
    image is clearly relevant to understanding the email (e.g. photos of items,
    damage, invoices). Do NOT call for attachments where likely_signature is true.
    """
    def _run() -> str:
        start_time = time.time()
        try:
            service = get_gmail_service()
            att = service.users().messages().attachments().get(
                userId='me', messageId=message_id, id=attachment_id
            ).execute()
            data_b64 = att.get('data', '')
            if not data_b64:
                return json.dumps({"status": "error", "error": "No attachment data returned"})

            image_bytes = base64.urlsafe_b64decode(data_b64)
            if len(image_bytes) > _MAX_IMAGE_SIZE:
                return json.dumps({"status": "error", "error": "Image too large (>5MB), skipping"})

            ext = (filename.rsplit('.', 1)[-1] if '.' in filename else '').lower()
            mime_map = {
                'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png',
                'gif': 'image/gif', 'webp': 'image/webp', 'bmp': 'image/bmp',
                'tiff': 'image/tiff', 'tif': 'image/tiff',
            }
            mime_type = mime_map.get(ext, 'image/jpeg')

            from google.genai import types as _types
            client = _get_vision_client()
            response = client.models.generate_content(
                model=_VISION_MODEL,
                contents=[
                    _types.Part(text=(
                        "Describe this image concisely. Focus on: what it shows, "
                        "any text visible, quantities, conditions, or identifiable items. "
                        "Keep under 200 words."
                    )),
                    _types.Part(inline_data=_types.Blob(
                        data=image_bytes, mime_type=mime_type
                    )),
                ],
            )
            description = response.text if response.text else "Could not describe image."
            latency_ms = int((time.time() - start_time) * 1000)
            _log("info", "describe_email_attachment", {
                "event": "describe_email_attachment",
                "status": "success",
                "message_id": message_id,
                "filename": filename,
                "size_bytes": len(image_bytes),
                "latency_ms": latency_ms,
            })
            return json.dumps({
                "status": "success",
                "filename": filename,
                "description": description,
                "size_bytes": len(image_bytes),
            })
        except HttpError as e:
            latency_ms = int((time.time() - start_time) * 1000)
            _log("error", "describe_email_attachment", {
                "event": "describe_email_attachment",
                "status": "http_error",
                "message_id": message_id,
                "error": str(e),
                "latency_ms": latency_ms,
            })
            return _error_response(tool="describe_email_attachment", error_code="api_error", message="Failed to download attachment", latency_ms=latency_ms)
        except Exception as e:
            _invalidate_vision_client()
            latency_ms = int((time.time() - start_time) * 1000)
            _log("error", "describe_email_attachment", {
                "event": "describe_email_attachment",
                "status": "error",
                "message_id": message_id,
                "error": str(e),
                "latency_ms": latency_ms,
            })
            return _error_response(tool="describe_email_attachment", error_code="internal_error", message=f"Vision error: {str(e)}", latency_ms=latency_ms)

    return await asyncio.to_thread(_run)


# ---------------------------------------------------------------------------
# Document attachment text extraction
# ---------------------------------------------------------------------------

@mcp.tool()
async def read_email_attachment(message_id: str, attachment_id: str, filename: str = "", mime_type: str = "") -> str:
    """Download a document attachment and return its extracted text content.

    Supports PDF, plain text, and CSV files. Use when the email mentions an
    attachment containing property details, specifications, or relevant info.
    Pass mime_type from the attachment metadata when available.
    Do NOT call for attachments where likely_signature is true.
    """
    _MAX_DOC_SIZE = 10_000_000  # 10MB
    _MAX_TEXT_CHARS = 3000

    def _extract_pdf(data: bytes) -> str:
        import io
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(data))
        parts = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                parts.append(text)
        return "\n".join(parts)

    def _run() -> str:
        start_time = time.time()
        try:
            service = get_gmail_service()
            att = service.users().messages().attachments().get(
                userId='me', messageId=message_id, id=attachment_id
            ).execute()
            data_b64 = att.get('data', '')
            if not data_b64:
                return json.dumps({"status": "error", "error": "No attachment data returned"})

            raw_bytes = base64.urlsafe_b64decode(data_b64)
            size_bytes = len(raw_bytes)
            if size_bytes > _MAX_DOC_SIZE:
                return json.dumps({"status": "error", "error": "Attachment too large (>10MB)"})

            ext = (filename.rsplit('.', 1)[-1] if '.' in filename else '').lower()
            effective_mime = mime_type.lower()

            text = None
            if ext == 'pdf' or 'pdf' in effective_mime:
                text = _extract_pdf(raw_bytes)
            elif ext in ('txt', 'csv', 'tsv', 'md') or effective_mime.startswith('text/'):
                text = raw_bytes.decode('utf-8', errors='replace')
            else:
                latency_ms = int((time.time() - start_time) * 1000)
                _log("info", "read_email_attachment", {
                    "event": "read_email_attachment",
                    "status": "unsupported",
                    "filename": filename,
                    "ext": ext,
                    "latency_ms": latency_ms,
                })
                return json.dumps({
                    "status": "unsupported",
                    "filename": filename,
                    "mime_type": mime_type or ext,
                    "size_bytes": size_bytes,
                    "message": "File type not supported for text extraction. Inform the client what was received.",
                })

            truncated = len(text) > _MAX_TEXT_CHARS
            content = text[:_MAX_TEXT_CHARS] if truncated else text
            latency_ms = int((time.time() - start_time) * 1000)
            _log("info", "read_email_attachment", {
                "event": "read_email_attachment",
                "status": "success",
                "filename": filename,
                "size_bytes": size_bytes,
                "chars_extracted": len(content),
                "truncated": truncated,
                "latency_ms": latency_ms,
            })
            return json.dumps({
                "status": "success",
                "filename": filename,
                "content": content,
                "size_bytes": size_bytes,
                "truncated": truncated,
            })
        except HttpError as e:
            latency_ms = int((time.time() - start_time) * 1000)
            _log("error", "read_email_attachment", {
                "event": "read_email_attachment",
                "status": "http_error",
                "message_id": message_id,
                "error": str(e),
                "latency_ms": latency_ms,
            })
            return _error_response(tool="read_email_attachment", error_code="api_error",
                                   message="Failed to download attachment", latency_ms=latency_ms)
        except Exception as e:
            latency_ms = int((time.time() - start_time) * 1000)
            _log("error", "read_email_attachment", {
                "event": "read_email_attachment",
                "status": "error",
                "message_id": message_id,
                "error": str(e),
                "latency_ms": latency_ms,
            })
            return _error_response(tool="read_email_attachment", error_code="internal_error",
                                   message=f"Extraction error: {str(e)}", latency_ms=latency_ms)

    return await asyncio.to_thread(_run)


def startup_health_check() -> bool:
    """
    Phase 3 requirement: Startup health check.
    Verify credentials and test API connectivity.
    Supports both Cloud Run (env var) and local (file) modes.
    """
    # 1. Check that some form of credentials exists
    if not GMAIL_TOKEN_JSON and not os.path.exists(TOKEN_FILE):
        _log("error", "startup_health_check", {
            "event": "server_startup_failed",
            "error": "No Gmail token found (checked GMAIL_TOKEN_JSON env var and token.json file)",
            "status": "critical"
        })
        return False
    
    # 2. Test API connectivity
    try:
        service = get_gmail_service()
        # Simple API call to verify connectivity
        profile = service.users().getProfile(userId='me').execute()
        _log("info", "startup_health_check", {
            "event": "health_check_passed",
            "status": "healthy",
            "email_address": profile.get('emailAddress', 'unknown')
        })
        return True
    except Exception as e:
        _log("error", "startup_health_check", {
            "event": "health_check_failed",
            "error": str(e),
            "status": "critical"
        })
        return False

if __name__ == "__main__":
    _log("info", "__main__", {
        "event": "server_start",
        "message": "Gmail MCP server starting",
        "token_file": TOKEN_FILE,
        "cred_file": CRED_FILE,
        "rate_limit": f"{MAX_EMAILS_PER_HOUR}/hour"
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