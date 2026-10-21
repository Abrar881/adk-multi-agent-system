import sys
import os

# Ensure project root is on sys.path so `config` package resolves
# regardless of CWD when spawned as MCP subprocess by ADK.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import httpx
from servers.circuit_breaker import CircuitBreaker


def _monday_cb_alert(name: str, failures: int) -> None:
    try:
        from servers.alerting import alert_admin, CRITICAL
    except ModuleNotFoundError:
        from alerting import alert_admin, CRITICAL  # type: ignore[no-redef]
    alert_admin(CRITICAL, f"circuit_breaker_{name}_open",
                f"Monday.com API circuit breaker opened after {failures} failures")


_monday_breaker = CircuitBreaker("monday", failure_threshold=5, recovery_timeout=60,
                                  on_open_callback=_monday_cb_alert)
import json
import logging
import time
import random
import functools
import asyncio
import calendar
import hashlib
import html as _html
import threading as _monday_threading
import re
from datetime import datetime, timedelta, timezone
from typing import Optional
from google.cloud import firestore as _firestore
from google.cloud.firestore_v1.field_path import FieldPath as _FieldPath
from google.api_core.exceptions import AlreadyExists as _AlreadyExists
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv

load_dotenv()

# --- STRUCTURED LOGGING ---
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
from config.monday_config import (
    get_api_token, get_board_id, get_column_id, get_group_id,
    require_column_id, invalidate_cache,
)
from config.job_routing import classify_board_key as _classify_board_key

API_URL = "https://api.monday.com/v2"

_MONDAY_RESULT_CAP = 8000  # chars (~2K tokens); prevents Monday responses bloating LLM context

def _cap_tool_result(data: str) -> str:
    if len(data) <= _MONDAY_RESULT_CAP:
        return data
    omitted = len(data) - _MONDAY_RESULT_CAP
    return data[:_MONDAY_RESULT_CAP] + f"\n[truncated â€” {omitted} chars omitted]"

# --- BOOKING IDEMPOTENCY (Firestore) ---
# Prevents duplicate bookings when replies in an already-booked thread re-trigger STEP 1.
# Reuses the same task_locks collection and lock pattern as gmail_server.py email dedup.
_FIRESTORE_PROJECT = os.getenv('GOOGLE_CLOUD_PROJECT') or os.getenv('GCLOUD_PROJECT')
_BOOKING_LOCK_COLLECTION = "task_locks"
_BOOKING_DEDUP_TTL_DAYS = 30
# Durable thread->item resolver: keyed by thread_id (unlike task_locks, which is
# keyed by booking:<thread_id>:<date> and therefore blind to reschedules that
# change the date). Written on every successful create_inventory_booking and
# read by resolve_thread_item() / amend_booking().
_THREAD_BOOKINGS_COLLECTION = "thread_bookings"
_fs_db: "_firestore.Client | None" = None
_fs_db_lock = _monday_threading.Lock()


def _get_booking_firestore() -> "_firestore.Client":
    """Lazy-initialised Firestore client shared across all bookings in this process."""
    global _fs_db
    with _fs_db_lock:
        if _fs_db is None:
            if not _FIRESTORE_PROJECT:
                raise RuntimeError("GOOGLE_CLOUD_PROJECT env var must be set for booking dedup")
            _fs_db = _firestore.Client(project=_FIRESTORE_PROJECT)
    return _fs_db


def _invalidate_booking_firestore() -> None:
    global _fs_db
    _fs_db = None


def resolve_thread_item(thread_id: str) -> dict | None:
    """Resolve the Monday item bound to a Gmail thread, independent of booking date.

    Unlike the task_locks idempotency lock (keyed by booking:<thread_id>:<date>,
    so it goes blind the moment a client reschedules to a different date), this
    looks up thread_bookings/{thread_id} Ã¢â‚¬â€ a durable, date-independent mapping
    written on every successful create_inventory_booking.

    Returns {"item_id", "board_id", "booking_date", "created_at"} on a hit, or
    None if nothing is found. Never raises Ã¢â‚¬â€ callers must handle None explicitly.
    """
    if not thread_id:
        return None

    try:
        db = _get_booking_firestore()
        snap = db.collection(_THREAD_BOOKINGS_COLLECTION).document(thread_id).get()
        if snap.exists:
            data = snap.to_dict() or {}
            if data.get("item_id"):
                return {
                    "item_id": data.get("item_id"),
                    "board_id": data.get("board_id"),
                    "booking_date": data.get("booking_date"),
                    "created_at": data.get("created_at"),
                }
    except Exception as _re:
        _invalidate_booking_firestore()
        logger.warning(json.dumps({
            "event": "resolve_thread_item",
            "service": "monday_server",
            "phase": "thread_bookings_read_failed",
            "thread_id": thread_id,
            "error": str(_re),
        }))

    # Fallback for bookings created before thread_bookings existed (or if that
    # write failed): task_locks doesn't store thread_id as a field, so a normal
    # equality filter can't find it. Range-query on the document ID instead Ã¢â‚¬â€
    # lock keys are "booking:<thread_id>:<date>", so this is a prefix scan.
    try:
        db = _get_booking_firestore()
        prefix = f"booking:{thread_id}:"
        prefix_end = f"booking:{thread_id}:" + chr(0xFFFF)
        query = (
            db.collection(_BOOKING_LOCK_COLLECTION)
            .where(_FieldPath.document_id(), ">=", db.collection(_BOOKING_LOCK_COLLECTION).document(prefix))
            .where(_FieldPath.document_id(), "<", db.collection(_BOOKING_LOCK_COLLECTION).document(prefix_end))
        )
        best_data = None
        best_created = None
        for doc in query.stream():
            data = doc.to_dict() or {}
            if not data.get("item_id"):
                continue
            created = data.get("created_at")
            if hasattr(created, "to_pydatetime"):
                created = created.to_pydatetime()
            if best_created is None or (created and created > best_created):
                best_created = created
                best_data = data
        if best_data:
            return {
                "item_id": best_data.get("item_id"),
                "board_id": None,
                "booking_date": None,
                "created_at": best_data.get("created_at"),
            }
    except Exception as _fe:
        _invalidate_booking_firestore()
        logger.warning(json.dumps({
            "event": "resolve_thread_item",
            "service": "monday_server",
            "phase": "task_locks_fallback_failed",
            "thread_id": thread_id,
            "error": str(_fe),
        }))

    return None


def _board_key_for_id(board_id: int) -> Optional[str]:
    """Resolve a live Monday board_id back to its config board_key.

    create_inventory_booking derives board_key from spec/report_type via
    _classify_board_key; amend_booking is instead given board_id directly, so
    this reverses get_board_id(board_key) against the known board keys.
    """
    for key in ("inventories", "contractors"):
        if get_board_id(key) == board_id:
            return key
    return None


async def _canonicalize_status_label(board_id: int, col_id: str, label: str) -> str:
    """Normalize a single status/color label against the live board's valid labels.

    Same normalization strategy as the pre-flight label check in
    create_inventory_booking (_normalize_label + board schema labels), applied
    to one column value instead of the whole column_values dict. Fail-open:
    returns the raw label unchanged if the schema is unavailable or no
    normalized match exists.
    """
    valid_labels, _ = await _board_schema_info(board_id)
    labels_for_col = valid_labels.get(col_id)
    if not labels_for_col:
        return label
    norm_map = {_normalize_label(bl): bl for bl in labels_for_col}
    return norm_map.get(_normalize_label(label), label)


# Per-(board_id, month_title) asyncio locks Ã¢â‚¬â€ serialise concurrent group check-then-create
# within a single process so two simultaneous bookings for the same month don't both
# read "group absent" and both call create_group.
_month_group_locks: dict[str, asyncio.Lock] = {}
_month_group_locks_meta = asyncio.Lock()  # guards _month_group_locks itself

# Board schema TTL cache Ã¢â‚¬â€ avoids redundant Monday API round-trips within a
# single booking.  Keyed by str(board_id); value is (monotonic_ts, schema_dict).
_BOARD_SCHEMA_CACHE: dict[str, tuple[float, dict]] = {}
_BOARD_SCHEMA_TTL: float = 30.0  # seconds


async def _get_month_group_lock(board_id: int, title: str) -> asyncio.Lock:
    key = f"{board_id}:{title}"
    async with _month_group_locks_meta:
        if key not in _month_group_locks:
            _month_group_locks[key] = asyncio.Lock()
        return _month_group_locks[key]


# Service type Ã¢â€ â€™ canonical Monday "Report type" status label.
# Keys are lowercase/stripped variants; values are exact board labels.
_SERVICE_TYPE_LABELS: dict[str, str] = {
    # Inventory & Check In
    "inventory & check in":          "Inventory & Check In",
    "inventory & check-in":          "Inventory & Check In",
    "inventory and check in":        "Inventory & Check In",
    "inventory and check-in":        "Inventory & Check In",
    "check in":                      "Inventory & Check In",
    "check-in":                      "Inventory & Check In",
    "check in inventory":            "Inventory & Check In",
    # Inventory & Check Out
    "inventory & check out":         "Inventory & Check Out",
    "inventory & check-out":         "Inventory & Check Out",
    "inventory and check out":       "Inventory & Check Out",
    "inventory and check-out":       "Inventory & Check Out",
    "check out inventory":           "Inventory & Check Out",
    # Inventory & Schedule of condition
    "inventory & schedule of condition": "Inventory & Schedule of condition",
    "inventory and schedule of condition": "Inventory & Schedule of condition",
    "inventory":                     "Inventory & Schedule of condition",
    "schedule of condition":         "Inventory & Schedule of condition",
    "soc":                           "Inventory & Schedule of condition",
    # Check out / Property Valuation
    "check out / property valuation": "Check out / Property Valuation",
    "check out":                     "Check out / Property Valuation",
    "check-out":                     "Check out / Property Valuation",
    "checkout":                      "Check out / Property Valuation",
    "property valuation":            "Check out / Property Valuation",
    "valuation":                     "Check out / Property Valuation",
    # Midterm
    "midterm":                       "Midterm",
    "mid-term":                      "Midterm",
    "mid term":                      "Midterm",
    "mid-term inspection":           "Midterm",
    "midterm inspection":            "Midterm",
    # Snagging Report
    "snagging report":               "Snagging Report",
    "snagging":                      "Snagging Report",
    # Property viewing
    "property viewing":              "Property viewing",
    "viewing":                       "Property viewing",
    # Revisit
    "revisit":                       "Revisit",
    # Commercial unit
    "commercial unit":               "Commercial unit",
    "commercial":                    "Commercial unit",
    # Key collection / return
    "key collection":                "Key collection",
    "key return":                    "Key return",
    # Misc
    "task":                          "Task",
    "update":                        "Update",
    "picture report":                "Picture Report",
    "cancelled":                     "Cancelled",
    "professional photography":      "Professional Photography",
    "pictures and valuation visit":  "Pictures and Valutaion Visit",
    "pictures and valutaion visit":  "Pictures and Valutaion Visit",
}


def _resolve_service_type(raw: str) -> str | None:
    """Map a free-text service type to an exact Monday 'Report type' status label.

    Returns the canonical label string, or None if no match found.
    """
    key = " ".join(raw.strip().lower().split())
    # Direct lookup
    if key in _SERVICE_TYPE_LABELS:
        return _SERVICE_TYPE_LABELS[key]
    # Strip common prefixes and retry
    for prefix in ("inventory & ", "inventory and ", "inventory "):
        if key.startswith(prefix):
            stripped = key[len(prefix):]
            if stripped in _SERVICE_TYPE_LABELS:
                return _SERVICE_TYPE_LABELS[stripped]
    return None


def _normalize_label(s: str) -> str:
    """Normalize a Monday status label for fuzzy matching.

    Decodes HTML entities (&amp; Ã¢â€ â€™ &), collapses whitespace, and casefolds.
    Used to reconcile labels from _resolve_service_type (plain &) against
    the board schema which may store them HTML-encoded (&amp;) so that both
    sides compare equal regardless of encoding or casing.
    """
    return " ".join(_html.unescape(str(s)).split()).casefold()


# Maps common LLM-produced cleaning label variants Ã¢â€ â€™ canonical Monday "Service Type" labels
# for the contractors board. Applied before the live-label pre-flight so the value written
# matches the board's actual status options.
_CONTRACTOR_LABEL_MAP: dict[str, str] = {
    "end of tenancy cleaning":              "EOT Cleaning",
    "end of tenancy clean":                 "EOT Cleaning",
    "eot clean":                            "EOT Cleaning",
    "professional end of tenancy cleaning": "EOT Cleaning",
}

# UK postcode regex Ã¢â‚¬â€ matches outward + inward codes with optional whitespace between them.
# Groups: (1) outward e.g. "RM10", (2) inward e.g. "7HT". IGNORECASE so lowercase passes.
_UK_POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}\d[A-Z\d]?)\s*(\d[A-Z]{2})\b", re.IGNORECASE)


def _extract_uk_postcode(text: str) -> str | None:
    """Pull a UK postcode from free text (e.g. an address string), returned in
    normalized 'OUTWARD INWARD' uppercase form (e.g. 'RM10 7HT').
    Returns None if no postcode pattern is found.
    """
    if not text:
        return None
    m = _UK_POSTCODE_RE.search(str(text))
    if not m:
        return None
    return f"{m.group(1).upper()} {m.group(2).upper()}"


async def _geocode_uk_postcode(postcode: str) -> tuple[str, str] | None:
    """Look up lat/lng for a UK postcode via postcodes.io (free, no auth, < 100ms).

    Returns (lat_str, lng_str) or None when the postcode is not found or the
    lookup fails. Failures are intentionally swallowed Ã¢â‚¬â€ callers fall back to
    a London-centre placeholder so the booking is never blocked by a geocode miss.
    """
    try:
        pc_slug = postcode.replace(" ", "").upper()
        client = _get_client()
        resp = await client.get(
            f"https://api.postcodes.io/postcodes/{pc_slug}",
            timeout=5.0,
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == 200 and data.get("result"):
                lat = str(data["result"]["latitude"])
                lng = str(data["result"]["longitude"])
                return lat, lng
    except Exception as exc:
        logger.warning(json.dumps({
            "event": "geocode_postcode_failed",
            "postcode": postcode,
            "error": str(exc)[:200],
        }))
    return None


# Lazy-initialized HTTP client Ã¢â‚¬â€ avoids binding to the import-time event loop
# which gets closed before the MCP subprocess starts its own loop on cold start.
_http_client: httpx.AsyncClient | None = None
_http_client_loop: asyncio.AbstractEventLoop | None = None


def _get_client() -> httpx.AsyncClient:
    global _http_client, _http_client_loop
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None
    if _http_client is None or _http_client.is_closed or _http_client_loop is not current_loop:
        _http_client = httpx.AsyncClient(timeout=30.0)
        _http_client_loop = current_loop
    return _http_client

mcp = FastMCP("AcmeMonday")

SERVICE_NAME = "monday_server"


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


def _ensure_id(val: int | str) -> int:
    """Coerce board/item IDs to int for API calls while accepting both int and str input."""
    return int(val)


logger.info(json.dumps({
    "event": "server_startup",
    "agent": "system",
    "service": "monday_server",
    "status": "initialized"
}))


# --- STARTUP HEALTH CHECK ---
async def _startup_health_check():
    """Validates API key connectivity at startup."""
    try:
        query = "query { boards (limit: 1) { id } }"
        await _monday_request(query)
        logger.info(json.dumps({
            "event": "startup_health_check",
            "agent": "system",
            "service": "monday_server",
            "status": "success"
        }))
        return True
    except Exception as e:
        logger.error(json.dumps({
            "event": "startup_health_check",
            "agent": "system",
            "service": "monday_server",
            "status": "failed",
            "error": str(e),
            "severity": "CRITICAL",
            "action_required": "Admin must verify MONDAY_API_KEY and restart service"
        }))
        return False

# --- RATE LIMIT EXCEPTION ---
class RateLimitError(Exception):
    """Raised when Monday.com returns 429 rate limit."""
    pass


# --- RETRY DECORATOR ---
def retry(exceptions=(httpx.HTTPError, RateLimitError), tries=4, delay=1, backoff=2, jitter=0.1):
    """Retry with exponential backoff + jitter. Handles 429 rate limits."""
    def deco(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries > 1:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    sleep_time = _delay + random.uniform(-jitter, jitter)
                    global _http_client; _http_client = None
                    logger.warning(json.dumps({
                        "event": "retry",
                        "agent": "system",
                        "service": "monday_server",
                        "tool": kwargs.get("_tool_name", "unknown"),
                        "wait_seconds": sleep_time,
                        "error": str(e)
                    }))
                    await asyncio.sleep(max(0, sleep_time))
                    _tries -= 1
                    _delay *= backoff
            return await func(*args, **kwargs)
        
        def sync_wrapper(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries > 1:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    sleep_time = _delay + random.uniform(-jitter, jitter)
                    logger.warning(json.dumps({
                        "event": "retry",
                        "agent": "system",
                        "service": "monday_server",
                        "tool": kwargs.get("_tool_name", "unknown"),
                        "wait_seconds": sleep_time,
                        "error": str(e)
                    }))
                    time.sleep(max(0, sleep_time))
                    _tries -= 1
                    _delay *= backoff
            return func(*args, **kwargs)
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return deco

# --- HELPER FUNCTION ---
@retry(exceptions=(httpx.HTTPError, RateLimitError, json.JSONDecodeError), tries=4, delay=1, backoff=2)
async def _monday_request(query, variables=None):
    """Sends a GraphQL query to Monday.com. Raises RateLimitError on 429 for retry."""
    _api_token = get_api_token()
    if not _api_token:
        raise ValueError("Monday API token not configured Ã¢â‚¬â€ set MONDAY_API_KEY or configure via dashboard")
    start_time = time.time()
    headers = {
        "Authorization": _api_token,
        # API versions: https://developer.monday.com/api-reference/docs/api-versioning
        # 2026-01 is current stable, 2026-04 is release candidate (as of Feb 2026)
        "API-Version": "2026-01",
        "Content-Type": "application/json"
    }
    # Monday 2026 API requires variables to be an object, not null
    data = {"query": query, "variables": variables if variables is not None else {}}
    
    if not _monday_breaker.allow_request():
        return {"errors": [{"message": "monday circuit open - API unavailable"}], "status": "circuit_open"}
    try:
        response = await _get_client().post(API_URL, headers=headers, json=data)
    except Exception:
        _monday_breaker.record_failure()
        raise
    
    latency_ms = int((time.time() - start_time) * 1000)
    
    logger.info(json.dumps({
        "event": "api_call",
        "agent": "system",
        "service": "monday_server",
        "tool": "monday_request",
        "status_code": response.status_code,
        "latency_ms": latency_ms
    }))
    
    # Handle rate limiting with retry
    if response.status_code == 429:
        _monday_breaker.record_failure()
        raise RateLimitError(f"Monday API rate limit exceeded (429): {response.text}")
    
    if response.status_code != 200:
        _monday_breaker.record_failure()
        raise httpx.HTTPError(f"Monday API Error {response.status_code}: {response.text}")
        
    _monday_breaker.record_success()
    return response.json()

# --- TOOLS ---

@mcp.tool()
async def list_boards(limit: int = 50) -> str:
    """Lists the names and IDs of the most recent Monday boards."""
    start_time = time.time()
    query = """
    query ($limit: Int) {
        boards (limit: $limit) {
            id
            name
        }
    }
    """
    try:
        data = await _monday_request(query, {"limit": limit})
        boards = data.get("data", {}).get("boards", [])
        latency_ms = int((time.time() - start_time) * 1000)
        
        if not boards:
            logger.info(json.dumps({
                "event": "list_boards",
                "agent": "system",
                "service": "monday_server",
                "tool": "list_boards",
                "status": "no_results",
                "latency_ms": latency_ms
            }))
            return json.dumps({"status": "success", "boards": [], "message": "No boards found"})
            
        board_list = [{"id": b["id"], "name": b["name"]} for b in boards]
        
        logger.info(json.dumps({
            "event": "list_boards",
            "agent": "system",
            "service": "monday_server",
            "tool": "list_boards",
            "status": "success",
            "count": len(board_list),
            "latency_ms": latency_ms
        }))
        
        return json.dumps({"status": "success", "boards": board_list})
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "list_boards",
            "agent": "system",
            "service": "monday_server",
            "tool": "list_boards",
            "status": "error",
            "error": str(e),
            "latency_ms": latency_ms
        }))
        return _error_response(tool="list_boards", error_code="api_error", message=str(e), latency_ms=latency_ms)

@mcp.tool()
async def create_task_item(board_id: int | str, item_name: str) -> str:
    """Creates a new task (Item) on a specific Monday board."""
    board_id = _ensure_id(board_id)
    start_time = time.time()

    # ---- IDEMPOTENCY PRE-CHECK ----
    # Prevent duplicate NEEDS_REVIEW escalation tasks when the same email/invoice
    # is re-evaluated on scheduler retry or re-processing.
    try:
        dedup_query = """
        query ($board_id: ID!) {
            boards(ids: [$board_id]) {
                items_page(limit: 100) {
                    items { id name }
                }
            }
        }
        """
        dedup_data = await _monday_request(dedup_query, {"board_id": board_id})
        if "errors" not in dedup_data:
            existing_items = (
                dedup_data.get("data", {})
                .get("boards", [{}])[0]
                .get("items_page", {})
                .get("items", [])
            )
            for item in existing_items:
                if item.get("name", "").strip().lower() == item_name.strip().lower():
                    existing_id = item["id"]
                    logger.info(json.dumps({
                        "event": "create_task_item",
                        "phase": "skipped_duplicate",
                        "service": "monday_server",
                        "board_id": board_id,
                        "existing_item_id": existing_id,
                        "item_name": item_name,
                    }))
                    return json.dumps({
                        "status": "skipped_duplicate",
                        "item_id": existing_id,
                        "message": "Task item already exists",
                    })
    except Exception as e:
        # Fail open: dedup check failed, proceed with create
        logger.warning(json.dumps({
            "event": "create_task_item",
            "phase": "dedup_check_failed",
            "service": "monday_server",
            "board_id": board_id,
            "error": str(e),
        }))

    query = """
    mutation ($board_id: ID!, $item_name: String!) {
        create_item (board_id: $board_id, item_name: $item_name) {
            id
        }
    }
    """
    vars = {
        "board_id": board_id,
        "item_name": item_name
    }
    try:
        data = await _monday_request(query, vars)
        latency_ms = int((time.time() - start_time) * 1000)
        
        # Check for GraphQL specific errors (e.g., bad board ID)
        if "errors" in data:
            error_msg = data['errors'][0]['message']
            logger.error(json.dumps({
                "event": "create_task_item",
                "agent": "system",
                "service": "monday_server",
                "tool": "create_task_item",
                "status": "graphql_error",
                "error": error_msg,
                "board_id": board_id,
                "latency_ms": latency_ms
            }))
            return _error_response(tool="create_task_item", error_code="graphql_error", message=error_msg, latency_ms=latency_ms)

        new_id = data["data"]["create_item"]["id"]

        logger.info(json.dumps({
            "event": "create_task_item",
            "agent": "system",
            "service": "monday_server",
            "tool": "create_task_item",
            "status": "success",
            "item_id": new_id,
            "board_id": board_id,
            "item_name": item_name,
            "latency_ms": latency_ms
        }))
        
        return json.dumps({
            "status": "success",
            "item_id": new_id,
            "item_name": item_name,
            "message": f"Created Item '{item_name}' (ID: {new_id})"
        })
        
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "create_task_item",
            "agent": "system",
            "service": "monday_server",
            "tool": "create_task_item",
            "status": "error",
            "error": str(e),
            "board_id": board_id,
            "latency_ms": latency_ms
        }))
        return _error_response(tool="create_task_item", error_code="api_error", message=str(e), latency_ms=latency_ms)

@mcp.tool()
async def get_board_items(board_id: int | str, limit: int = 25) -> str:
    """Gets items from a specific Monday board.

    Returns id + text per column value (raw JSON 'value' field omitted Ã¢â‚¬â€ agents use 'text').
    Default limit lowered to 25 to cap token cost; pass a higher limit if more items needed.
    """
    board_id = _ensure_id(board_id)
    start_time = time.time()
    query = """
    query ($board_id: ID!, $limit: Int) {
        boards (ids: [$board_id]) {
            items_page (limit: $limit) {
                items {
                    id
                    name
                    column_values {
                        id
                        text
                    }
                }
            }
        }
    }
    """
    try:
        data = await _monday_request(query, {"board_id": board_id, "limit": limit})
        latency_ms = int((time.time() - start_time) * 1000)
        
        if "errors" in data:
            error_msg = data['errors'][0]['message']
            logger.error(json.dumps({
                "event": "get_board_items",
                "agent": "system",
                "service": "monday_server",
                "tool": "get_board_items",
                "status": "graphql_error",
                "error": error_msg,
                "board_id": board_id,
                "latency_ms": latency_ms
            }))
            return _error_response(tool="get_board_items", error_code="graphql_error", message=error_msg, latency_ms=latency_ms)

        boards = data.get("data", {}).get("boards", [])
        if not boards:
            logger.info(json.dumps({
                "event": "get_board_items",
                "agent": "system",
                "service": "monday_server",
                "tool": "get_board_items",
                "status": "not_found",
                "board_id": board_id,
                "latency_ms": latency_ms
            }))
            return json.dumps({"status": "success", "items": [], "message": "Board not found"})
        
        items = boards[0].get("items_page", {}).get("items", [])
        
        logger.info(json.dumps({
            "event": "get_board_items",
            "agent": "system",
            "service": "monday_server",
            "tool": "get_board_items",
            "status": "success",
            "board_id": board_id,
            "count": len(items),
            "latency_ms": latency_ms
        }))
        
        return json.dumps({"status": "success", "items": items})
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "get_board_items",
            "agent": "system",
            "service": "monday_server",
            "tool": "get_board_items",
            "status": "error",
            "error": str(e),
            "board_id": board_id,
            "latency_ms": latency_ms
        }))
        return _error_response(tool="get_board_items", error_code="api_error", message=str(e), latency_ms=latency_ms)

@mcp.tool()
async def get_board_summary(board_id: int | str, limit: int = 50) -> str:
    """Returns status counts for a Monday board without raw item data.

    Use for 'how many jobs', 'summary of jobs', 'check Monday jobs', 'jobs status'.
    Returns total count and a status_breakdown dict Ã¢â‚¬â€ DO NOT call get_board_items first.
    """
    board_id = _ensure_id(board_id)
    start_time = time.time()
    _report_status_col = get_column_id("inventories", "report_status")
    if not _report_status_col:
        return json.dumps({
            "status": "error",
            "error": "report_status column not mapped Ã¢â‚¬â€ configure Monday column IDs in the dashboard",
        })
    query = """
    query ($board_id: ID!, $limit: Int, $col_id: String!) {
        boards(ids: [$board_id]) {
            items_page(limit: $limit) {
                items {
                    id
                    name
                    column_values(ids: [$col_id]) {
                        text
                    }
                }
            }
        }
    }
    """
    try:
        data = await _monday_request(query, {"board_id": board_id, "limit": limit, "col_id": _report_status_col})
        latency_ms = int((time.time() - start_time) * 1000)

        if "errors" in data:
            error_msg = data["errors"][0]["message"]
            logger.error(json.dumps({
                "event": "get_board_summary", "service": SERVICE_NAME,
                "status": "graphql_error", "error": error_msg, "board_id": board_id,
                "latency_ms": latency_ms,
            }))
            return _error_response(tool="get_board_summary", error_code="graphql_error",
                                   message=error_msg, latency_ms=latency_ms)

        boards = data.get("data", {}).get("boards", [])
        if not boards:
            return json.dumps({"status": "success", "total": 0, "status_breakdown": {},
                               "message": "Board not found"})

        items = boards[0].get("items_page", {}).get("items", [])
        breakdown: dict[str, int] = {}
        for item in items:
            col_vals = item.get("column_values", [])
            label = (col_vals[0].get("text") or "").strip() if col_vals else ""
            key = label if label else "No Status"
            breakdown[key] = breakdown.get(key, 0) + 1

        logger.info(json.dumps({
            "event": "get_board_summary", "service": SERVICE_NAME, "status": "success",
            "board_id": board_id, "total": len(items), "latency_ms": latency_ms,
        }))
        return json.dumps({
            "status": "success",
            "total": len(items),
            "status_breakdown": breakdown,
            "summary": f"{len(items)} jobs: " + ", ".join(f"{v} {k}" for k, v in breakdown.items()),
        })

    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "get_board_summary", "service": SERVICE_NAME, "status": "error",
            "error": str(e), "board_id": board_id, "latency_ms": latency_ms,
        }))
        return _error_response(tool="get_board_summary", error_code="api_error",
                               message=str(e), latency_ms=latency_ms)

@mcp.tool()
async def search_items(board_id: int | str, column_id: str, value: str) -> str:
    """Searches for items on a board by column value using server-side filtering.

    Uses Monday.com's items_page_by_column_values for efficient server-side search.
    Falls back to client-side filtering if server-side search fails.
    """
    board_id = _ensure_id(board_id)
    start_time = time.time()
    # First, try server-side filtering (more efficient for large boards)
    query_server_side = """
    query ($board_id: ID!, $columns: [ItemsPageByColumnValuesQuery!]!) {
        items_page_by_column_values (board_id: $board_id, columns: $columns, limit: 50) {
            items {
                id
                name
                column_values {
                    id
                    text
                }
            }
        }
    }
    """
    try:
        data = await _monday_request(query_server_side, {
            "board_id": board_id,
            "columns": [{"column_id": column_id, "column_values": [value]}]
        })
        
        if "errors" not in data:
            items = data.get("data", {}).get("items_page_by_column_values", {}).get("items", [])
            latency_ms = int((time.time() - start_time) * 1000)
            
            logger.info(json.dumps({
                "event": "search_items",
                "agent": "system",
                "service": "monday_server",
                "tool": "search_items",
                "status": "success",
                "method": "server_side",
                "board_id": board_id,
                "column_id": column_id,
                "matches": len(items),
                "latency_ms": latency_ms
            }))
            
            return _cap_tool_result(json.dumps({"status": "success", "items": items}))

        # Server-side failed, fall back to client-side
        logger.warning(json.dumps({
            "event": "search_items",
            "agent": "system",
            "service": "monday_server",
            "tool": "search_items",
            "status": "fallback_to_client_side",
            "reason": data.get("errors", [{}])[0].get("message", "unknown")
        }))
    except Exception as e:
        logger.warning(json.dumps({
            "event": "search_items",
            "agent": "system",
            "service": "monday_server",
            "tool": "search_items",
            "status": "fallback_to_client_side",
            "reason": str(e)
        }))
    
    # Fallback: client-side filtering Ã¢â‚¬â€ hard-capped at 200 items to prevent unbounded fetches
    query_fallback = """
    query ($board_id: ID!) {
        boards (ids: [$board_id]) {
            items_page (limit: 50) {
                items {
                    id
                    name
                    column_values {
                        id
                        text
                    }
                }
            }
        }
    }
    """
    try:
        data = await _monday_request(query_fallback, {"board_id": board_id})
        latency_ms = int((time.time() - start_time) * 1000)
        
        if "errors" in data:
            error_msg = data['errors'][0]['message']
            logger.error(json.dumps({
                "event": "search_items",
                "agent": "system",
                "service": "monday_server",
                "tool": "search_items",
                "status": "graphql_error",
                "error": error_msg,
                "latency_ms": latency_ms
            }))
            return _error_response(tool="search_items", error_code="graphql_error", message=error_msg, latency_ms=latency_ms)

        boards = data.get("data", {}).get("boards", [])
        if not boards:
            logger.info(json.dumps({
                "event": "search_items",
                "agent": "system",
                "service": "monday_server",
                "tool": "search_items",
                "status": "not_found",
                "board_id": board_id,
                "latency_ms": latency_ms
            }))
            return json.dumps({"status": "success", "items": [], "message": "Board not found"})
        
        all_items = boards[0].get("items_page", {}).get("items", [])
        
        # Filter items by column value
        matching_items = []
        for item in all_items:
            for col in item.get("column_values", []):
                if col.get("id") == column_id and col.get("text") == value:
                    matching_items.append(item)
                    break
        
        logger.info(json.dumps({
            "event": "search_items",
            "agent": "system",
            "service": "monday_server",
            "tool": "search_items",
            "status": "success",
            "method": "client_side",
            "board_id": board_id,
            "column_id": column_id,
            "matches": len(matching_items),
            "latency_ms": latency_ms
        }))

        return _cap_tool_result(json.dumps({"status": "success", "items": matching_items}))
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "search_items",
            "agent": "system",
            "service": "monday_server",
            "tool": "search_items",
            "status": "error",
            "error": str(e),
            "latency_ms": latency_ms
        }))
        return _error_response(tool="search_items", error_code="api_error", message=str(e), latency_ms=latency_ms)

@mcp.tool()
async def update_column_value(board_id: int | str, item_id: int | str, column_id: str, value: str) -> str:
    """Updates a single column value for an item."""
    board_id = _ensure_id(board_id)
    item_id = _ensure_id(item_id)
    start_time = time.time()
    query = """
    mutation ($board_id: ID!, $item_id: ID!, $column_id: String!, $value: JSON!) {
        change_column_value (board_id: $board_id, item_id: $item_id, column_id: $column_id, value: $value) {
            id
        }
    }
    """
    vars = {
        "board_id": board_id,
        "item_id": item_id,
        "column_id": column_id,
        "value": value
    }
    try:
        data = await _monday_request(query, vars)
        latency_ms = int((time.time() - start_time) * 1000)
        
        if "errors" in data:
            error_msg = data['errors'][0]['message']
            logger.error(json.dumps({
                "event": "update_column_value",
                "agent": "system",
                "service": "monday_server",
                "tool": "update_column_value",
                "status": "graphql_error",
                "error": error_msg,
                "latency_ms": latency_ms
            }))
            return _error_response(tool="update_column_value", error_code="graphql_error", message=error_msg, latency_ms=latency_ms)

        logger.info(json.dumps({
            "event": "update_column_value",
            "agent": "system",
            "service": "monday_server",
            "tool": "update_column_value",
            "status": "success",
            "item_id": item_id,
            "column_id": column_id,
            "latency_ms": latency_ms
        }))
        
        return json.dumps({
            "status": "success",
            "message": f"Updated column {column_id} for item {item_id}"
        })
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "update_column_value",
            "agent": "system",
            "service": "monday_server",
            "tool": "update_column_value",
            "status": "error",
            "error": str(e),
            "latency_ms": latency_ms
        }))
        return _error_response(tool="update_column_value", error_code="api_error", message=str(e), latency_ms=latency_ms)

@mcp.tool()
async def update_multiple_columns(board_id: int | str, item_id: int | str, column_values: str) -> str:
    """Updates multiple column values for an item in a single API call.
    column_values should be a JSON string like: '{"status": {"label": "Done"}, "text": "Updated"}'
    """
    board_id = _ensure_id(board_id)
    item_id = _ensure_id(item_id)
    start_time = time.time()
    query = """
    mutation ($board_id: ID!, $item_id: ID!, $column_values: JSON!) {
        change_multiple_column_values (board_id: $board_id, item_id: $item_id, column_values: $column_values) {
            id
        }
    }
    """
    vars = {
        "board_id": board_id,
        "item_id": item_id,
        "column_values": column_values
    }
    try:
        data = await _monday_request(query, vars)
        latency_ms = int((time.time() - start_time) * 1000)
        
        if "errors" in data:
            error_msg = data['errors'][0]['message']
            logger.error(json.dumps({
                "event": "update_multiple_columns",
                "agent": "system",
                "service": "monday_server",
                "tool": "update_multiple_columns",
                "status": "graphql_error",
                "error": error_msg,
                "latency_ms": latency_ms
            }))
            return _error_response(tool="update_multiple_columns", error_code="graphql_error", message=error_msg, latency_ms=latency_ms)

        logger.info(json.dumps({
            "event": "update_multiple_columns",
            "agent": "system",
            "service": "monday_server",
            "tool": "update_multiple_columns",
            "status": "success",
            "item_id": item_id,
            "board_id": board_id,
            "latency_ms": latency_ms
        }))
        
        return json.dumps({
            "status": "success",
            "message": f"Updated multiple columns for item {item_id}"
        })
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "update_multiple_columns",
            "agent": "system",
            "service": "monday_server",
            "tool": "update_multiple_columns",
            "status": "error",
            "error": str(e),
            "latency_ms": latency_ms
        }))
        return _error_response(tool="update_multiple_columns", error_code="api_error", message=str(e), latency_ms=latency_ms)

@mcp.tool()
async def update_status(
    board_id: int | str,
    item_id: int | str,
    status_column_id: str,
    label: str,
) -> str:
    """Convenience wrapper to set a Status column by label."""
    board_id = _ensure_id(board_id)
    item_id = _ensure_id(item_id)
    start_time = time.time()
    # Monday expects JSON for status, e.g. {"label": "Done"}
    status_value = json.dumps({"label": label})
    query = """
    mutation ($board_id: ID!, $item_id: ID!, $column_id: String!, $value: JSON!) {
        change_column_value (board_id: $board_id, item_id: $item_id, column_id: $column_id, value: $value) {
            id
        }
    }
    """
    vars = {
        "board_id": board_id,
        "item_id": item_id,
        "column_id": status_column_id,
        "value": status_value,
    }
    try:
        data = await _monday_request(query, vars)
        latency_ms = int((time.time() - start_time) * 1000)

        if "errors" in data:
            error_msg = data["errors"][0]["message"]
            logger.error(json.dumps({
                "event": "update_status",
                "agent": "system",
                "service": "monday_server",
                "tool": "update_status",
                "status": "graphql_error",
                "error": error_msg,
                "board_id": board_id,
                "item_id": item_id,
                "status_column_id": status_column_id,
                "latency_ms": latency_ms,
            }))
            return _error_response(tool="update_status", error_code="graphql_error", message=error_msg, latency_ms=latency_ms)

        logger.info(json.dumps({
            "event": "update_status",
            "agent": "system",
            "service": "monday_server",
            "tool": "update_status",
            "status": "success",
            "board_id": board_id,
            "item_id": item_id,
            "status_column_id": status_column_id,
            "label": label,
            "latency_ms": latency_ms,
        }))

        return json.dumps({
            "status": "success",
            "message": f"Updated status for item {item_id} to '{label}'",
        })
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "update_status",
            "agent": "system",
            "service": "monday_server",
            "tool": "update_status",
            "status": "error",
            "error": str(e),
            "board_id": board_id,
            "item_id": item_id,
            "latency_ms": latency_ms,
        }))
        return _error_response(tool="update_status", error_code="api_error", message=str(e), latency_ms=latency_ms)

@mcp.tool()
async def create_inventory_task(
    board_id: int | str,
    item_name: str,
    status_column_id: Optional[str] = None,
    needs_review_label: str = "NEEDS_REVIEW",
    group_id: Optional[str] = None,
    additional_column_values: Optional[str] = None,
    notify_admin: bool = False,
) -> str:
    """Creates an inventory escalation task on a board (optionally sets NEEDS_REVIEW status).

    Notes:
    - If `status_column_id` is provided, the created item will get that Status
      set to `needs_review_label`.
    - `additional_column_values` may be provided as a JSON string mapping
      column_id -> value payload (Monday JSON), and will be merged.
    - If `notify_admin` is True, the response will remind the agent to call send_admin_alert
      from ops_server to notify ADMIN_PHONE via WhatsApp about the NEEDS_REVIEW item.
    """
    board_id = _ensure_id(board_id)
    start_time = time.time()
    merged_values: dict[str, object] = {}

    if additional_column_values:
        try:
            parsed = json.loads(additional_column_values)
            if isinstance(parsed, dict):
                merged_values.update(parsed)
            else:
                return _error_response(tool="create_inventory_task", error_code="validation_error", message="additional_column_values must be a JSON object string")
        except json.JSONDecodeError as e:
            return _error_response(tool="create_inventory_task", error_code="validation_error", message=f"Invalid additional_column_values JSON: {str(e)}")

    if status_column_id:
        merged_values[status_column_id] = {"label": needs_review_label}

    # Monday expects `column_values` to be a JSON string in GraphQL.
    column_values_json = json.dumps(merged_values) if merged_values else None

    # ---- IDEMPOTENCY PRE-CHECK ----
    # Prevent duplicate NEEDS_REVIEW escalation tasks when the same email/invoice
    # is re-evaluated on scheduler retry or re-processing.
    try:
        dedup_query = """
        query ($board_id: ID!) {
            boards(ids: [$board_id]) {
                items_page(limit: 100) {
                    items { id name }
                }
            }
        }
        """
        dedup_data = await _monday_request(dedup_query, {"board_id": board_id})
        if "errors" not in dedup_data:
            existing_items = (
                dedup_data.get("data", {})
                .get("boards", [{}])[0]
                .get("items_page", {})
                .get("items", [])
            )
            for item in existing_items:
                if item.get("name", "").strip().lower() == item_name.strip().lower():
                    existing_id = item["id"]
                    logger.info(json.dumps({
                        "event": "create_inventory_task",
                        "phase": "skipped_duplicate",
                        "service": "monday_server",
                        "board_id": board_id,
                        "existing_item_id": existing_id,
                        "item_name": item_name,
                    }))
                    return json.dumps({
                        "status": "skipped_duplicate",
                        "item_id": existing_id,
                        "message": "Task item already exists",
                    })
    except Exception as e:
        # Fail open: dedup check failed, proceed with create
        logger.warning(json.dumps({
            "event": "create_inventory_task",
            "phase": "dedup_check_failed",
            "service": "monday_server",
            "board_id": board_id,
            "error": str(e),
        }))

    query = """
    mutation ($board_id: ID!, $item_name: String!, $group_id: String, $column_values: JSON) {
        create_item (board_id: $board_id, item_name: $item_name, group_id: $group_id, column_values: $column_values) {
            id
        }
    }
    """
    vars = {
        "board_id": board_id,
        "item_name": item_name,
        "group_id": group_id,
        "column_values": column_values_json,
    }

    try:
        data = await _monday_request(query, vars)
        latency_ms = int((time.time() - start_time) * 1000)

        if "errors" in data:
            error_msg = data["errors"][0]["message"]
            logger.error(json.dumps({
                "event": "create_inventory_task",
                "agent": "system",
                "service": "monday_server",
                "tool": "create_inventory_task",
                "status": "graphql_error",
                "error": error_msg,
                "board_id": board_id,
                "group_id": group_id,
                "latency_ms": latency_ms,
            }))
            return _error_response(tool="create_inventory_task", error_code="graphql_error", message=error_msg, latency_ms=latency_ms)

        new_id = data["data"]["create_item"]["id"]

        logger.info(json.dumps({
            "event": "create_inventory_task",
            "agent": "system",
            "service": "monday_server",
            "tool": "create_inventory_task",
            "status": "success",
            "board_id": board_id,
            "group_id": group_id,
            "item_id": new_id,
            "item_name": item_name,
            "status_column_id": status_column_id,
            "needs_review_label": needs_review_label if status_column_id else None,
            "notify_admin": notify_admin,
            "latency_ms": latency_ms,
        }))

        response = {
            "status": "success",
            "item_id": new_id,
            "message": f"Created inventory escalation task '{item_name}' (ID: {new_id})",
        }
        
        if notify_admin:
            response["admin_notification_required"] = True
            response["next_action"] = "Call send_admin_alert from ops_server to notify ADMIN_PHONE"
        
        return json.dumps(response)
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "create_inventory_task",
            "agent": "system",
            "service": "monday_server",
            "tool": "create_inventory_task",
            "status": "error",
            "error": str(e),
            "board_id": board_id,
            "latency_ms": latency_ms,
        }))
        return _error_response(tool="create_inventory_task", error_code="api_error", message=str(e), latency_ms=latency_ms)

def _get_cached_board_schema(board_id: str) -> dict | None:
    """Return cached schema dict if present and not expired, else None."""
    entry = _BOARD_SCHEMA_CACHE.get(board_id)
    if entry and (time.monotonic() - entry[0]) < _BOARD_SCHEMA_TTL:
        return entry[1]
    return None


def _cache_board_schema(board_id: str, schema: dict) -> None:
    _BOARD_SCHEMA_CACHE[board_id] = (time.monotonic(), schema)


def _invalidate_board_schema(board_id: str) -> None:
    _BOARD_SCHEMA_CACHE.pop(board_id, None)


async def _get_board_schema_fresh(board_id: int | str) -> str:
    """Fetch board schema bypassing the TTL cache. Internal use only."""
    _invalidate_board_schema(str(board_id))
    return await get_board_schema(board_id)


@mcp.tool()
async def get_board_schema(board_id: int | str) -> str:
    """Returns a board's groups + columns (including parsed settings_str where possible)."""
    board_id = _ensure_id(board_id)

    cached = _get_cached_board_schema(str(board_id))
    if cached is not None:
        return json.dumps({"status": "success", "board": cached})

    start_time = time.time()
    query = """
    query ($board_id: [ID!]) {
        boards (ids: $board_id) {
            id
            name
            groups {
                id
                title
            }
            columns {
                id
                title
                type
                settings_str
            }
        }
    }
    """
    try:
        data = await _monday_request(query, {"board_id": [board_id]})
        latency_ms = int((time.time() - start_time) * 1000)

        if "errors" in data:
            error_msg = data["errors"][0]["message"]
            logger.error(json.dumps({
                "event": "get_board_schema",
                "agent": "system",
                "service": "monday_server",
                "tool": "get_board_schema",
                "status": "graphql_error",
                "error": error_msg,
                "board_id": board_id,
                "latency_ms": latency_ms,
            }))
            return _error_response(tool="get_board_schema", error_code="graphql_error", message=error_msg, latency_ms=latency_ms)

        boards = data.get("data", {}).get("boards", [])
        if not boards:
            logger.info(json.dumps({
                "event": "get_board_schema",
                "agent": "system",
                "service": "monday_server",
                "tool": "get_board_schema",
                "status": "not_found",
                "board_id": board_id,
                "latency_ms": latency_ms,
            }))
            return json.dumps({
                "status": "success",
                "board": None,
                "message": "Board not found",
            })

        board = boards[0]
        columns_out = []
        for col in board.get("columns", []) or []:
            settings_str = col.get("settings_str")
            settings: Optional[object] = None
            if settings_str:
                try:
                    settings = json.loads(settings_str)
                except json.JSONDecodeError:
                    settings = {"raw": settings_str}

            columns_out.append({
                "id": col.get("id"),
                "title": col.get("title"),
                "type": col.get("type"),
                "settings": settings,
            })

        result = {
            "id": board.get("id"),
            "name": board.get("name"),
            "groups": board.get("groups", []) or [],
            "columns": columns_out,
        }

        _cache_board_schema(str(board_id), result)

        logger.info(json.dumps({
            "event": "get_board_schema",
            "agent": "system",
            "service": "monday_server",
            "tool": "get_board_schema",
            "status": "success",
            "board_id": board_id,
            "groups_count": len(result["groups"]),
            "columns_count": len(result["columns"]),
            "latency_ms": latency_ms,
        }))
        return json.dumps({"status": "success", "board": result})
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "get_board_schema",
            "agent": "system",
            "service": "monday_server",
            "tool": "get_board_schema",
            "status": "error",
            "error": str(e),
            "board_id": board_id,
            "latency_ms": latency_ms,
        }))
        return _error_response(tool="get_board_schema", error_code="api_error", message=str(e), latency_ms=latency_ms)

@mcp.tool()
async def get_item(item_id: int | str) -> str:
    """Gets a single item by ID (name + column values)."""
    item_id = _ensure_id(item_id)
    start_time = time.time()
    query = """
    query ($item_id: [ID!]) {
        items (ids: $item_id) {
            id
            name
            column_values {
                id
                text
            }
        }
    }
    """
    try:
        data = await _monday_request(query, {"item_id": [item_id]})
        latency_ms = int((time.time() - start_time) * 1000)

        if "errors" in data:
            error_msg = data["errors"][0]["message"]
            logger.error(json.dumps({
                "event": "get_item",
                "agent": "system",
                "service": "monday_server",
                "tool": "get_item",
                "status": "graphql_error",
                "error": error_msg,
                "item_id": item_id,
                "latency_ms": latency_ms,
            }))
            return _error_response(tool="get_item", error_code="graphql_error", message=error_msg, latency_ms=latency_ms)

        items = data.get("data", {}).get("items", [])
        if not items:
            logger.info(json.dumps({
                "event": "get_item",
                "agent": "system",
                "service": "monday_server",
                "tool": "get_item",
                "status": "not_found",
                "item_id": item_id,
                "latency_ms": latency_ms,
            }))
            return json.dumps({
                "status": "success",
                "item": None,
                "message": "Item not found",
            })

        logger.info(json.dumps({
            "event": "get_item",
            "agent": "system",
            "service": "monday_server",
            "tool": "get_item",
            "status": "success",
            "item_id": item_id,
            "latency_ms": latency_ms,
        }))
        return json.dumps({"status": "success", "item": items[0]})
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "get_item",
            "agent": "system",
            "service": "monday_server",
            "tool": "get_item",
            "status": "error",
            "error": str(e),
            "item_id": item_id,
            "latency_ms": latency_ms,
        }))
        return _error_response(tool="get_item", error_code="api_error", message=str(e), latency_ms=latency_ms)

@mcp.tool()
async def create_item_update(item_id: int | str, body: str) -> str:
    """Creates an update (comment) on an item for audit trail / human notes."""
    item_id = _ensure_id(item_id)
    start_time = time.time()
    query = """
    mutation ($item_id: ID!, $body: String!) {
        create_update (item_id: $item_id, body: $body) {
            id
        }
    }
    """
    try:
        data = await _monday_request(query, {"item_id": item_id, "body": body})
        latency_ms = int((time.time() - start_time) * 1000)

        if "errors" in data:
            error_msg = data["errors"][0]["message"]
            logger.error(json.dumps({
                "event": "create_item_update",
                "agent": "system",
                "service": "monday_server",
                "tool": "create_item_update",
                "status": "graphql_error",
                "error": error_msg,
                "item_id": item_id,
                "latency_ms": latency_ms,
            }))
            return _error_response(tool="create_item_update", error_code="graphql_error", message=error_msg, latency_ms=latency_ms)

        update_id = data.get("data", {}).get("create_update", {}).get("id")
        logger.info(json.dumps({
            "event": "create_item_update",
            "agent": "system",
            "service": "monday_server",
            "tool": "create_item_update",
            "status": "success",
            "item_id": item_id,
            "update_id": update_id,
            "latency_ms": latency_ms,
        }))
        return json.dumps({
            "status": "success",
            "update_id": update_id,
            "message": f"Created update on item {item_id}",
        })
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "create_item_update",
            "agent": "system",
            "service": "monday_server",
            "tool": "create_item_update",
            "status": "error",
            "error": str(e),
            "item_id": item_id,
            "latency_ms": latency_ms,
        }))
        return _error_response(tool="create_item_update", error_code="api_error", message=str(e), latency_ms=latency_ms)

@mcp.tool()
async def search_items_paginated(
    board_id: int | str,
    column_id: str,
    value: str,
    limit: int = 50,
    max_pages: int = 10,
) -> str:
    """Searches a board by column value across pages (cursor-based pagination)."""
    board_id = _ensure_id(board_id)
    start_time = time.time()
    query = """
    query ($board_id: ID!, $limit: Int, $cursor: String) {
        boards (ids: [$board_id]) {
            items_page (limit: $limit, cursor: $cursor) {
                cursor
                items {
                    id
                    name
                    column_values {
                        id
                        text
                    }
                }
            }
        }
    }
    """
    try:
        cursor: Optional[str] = None
        matching_items: list[dict[str, object]] = []

        for page in range(max_pages):
            data = await _monday_request(query, {
                "board_id": board_id,
                "limit": limit,
                "cursor": cursor,
            })

            if "errors" in data:
                error_msg = data["errors"][0]["message"]
                latency_ms = int((time.time() - start_time) * 1000)
                logger.error(json.dumps({
                    "event": "search_items_paginated",
                    "agent": "system",
                    "service": "monday_server",
                    "tool": "search_items_paginated",
                    "status": "graphql_error",
                    "error": error_msg,
                    "board_id": board_id,
                    "column_id": column_id,
                    "latency_ms": latency_ms,
                }))
                return _error_response(tool="search_items_paginated", error_code="graphql_error", message=error_msg, latency_ms=latency_ms)

            boards = data.get("data", {}).get("boards", [])
            if not boards:
                latency_ms = int((time.time() - start_time) * 1000)
                logger.info(json.dumps({
                    "event": "search_items_paginated",
                    "agent": "system",
                    "service": "monday_server",
                    "tool": "search_items_paginated",
                    "status": "not_found",
                    "board_id": board_id,
                    "latency_ms": latency_ms,
                }))
                return json.dumps({
                    "status": "success",
                    "items": [],
                    "message": "Board not found",
                })

            items_page = boards[0].get("items_page") or {}
            items = items_page.get("items") or []

            for item in items:
                for col in item.get("column_values", []) or []:
                    if col.get("id") == column_id and col.get("text") == value:
                        matching_items.append(item)
                        break

            cursor = items_page.get("cursor")
            if not cursor:
                break

        latency_ms = int((time.time() - start_time) * 1000)
        logger.info(json.dumps({
            "event": "search_items_paginated",
            "agent": "system",
            "service": "monday_server",
            "tool": "search_items_paginated",
            "status": "success",
            "board_id": board_id,
            "column_id": column_id,
            "matches": len(matching_items),
            "latency_ms": latency_ms,
        }))

        return json.dumps({"status": "success", "items": matching_items})
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "search_items_paginated",
            "agent": "system",
            "service": "monday_server",
            "tool": "search_items_paginated",
            "status": "error",
            "error": str(e),
            "board_id": board_id,
            "column_id": column_id,
            "latency_ms": latency_ms,
        }))
        return _error_response(tool="search_items_paginated", error_code="api_error", message=str(e), latency_ms=latency_ms)

@mcp.tool()
async def archive_item(item_id: int | str) -> str:
    """Archives an item."""
    item_id = _ensure_id(item_id)
    start_time = time.time()
    query = """
    mutation ($item_id: ID!) {
        archive_item (item_id: $item_id) {
            id
        }
    }
    """
    try:
        data = await _monday_request(query, {"item_id": item_id})
        latency_ms = int((time.time() - start_time) * 1000)

        if "errors" in data:
            error_msg = data["errors"][0]["message"]
            logger.error(json.dumps({
                "event": "archive_item",
                "agent": "system",
                "service": "monday_server",
                "tool": "archive_item",
                "status": "graphql_error",
                "error": error_msg,
                "item_id": item_id,
                "latency_ms": latency_ms,
            }))
            return _error_response(tool="archive_item", error_code="graphql_error", message=error_msg, latency_ms=latency_ms)

        archived = data.get("data", {}).get("archive_item", {})
        logger.info(json.dumps({
            "event": "archive_item",
            "agent": "system",
            "service": "monday_server",
            "tool": "archive_item",
            "status": "success",
            "item_id": item_id,
            "latency_ms": latency_ms,
        }))
        return json.dumps({
            "status": "success",
            "archived_item": archived,
            "message": f"Archived item {item_id}",
        })
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "archive_item",
            "agent": "system",
            "service": "monday_server",
            "tool": "archive_item",
            "status": "error",
            "error": str(e),
            "item_id": item_id,
            "latency_ms": latency_ms,
        }))
        return _error_response(tool="archive_item", error_code="api_error", message=str(e), latency_ms=latency_ms)

@mcp.tool()
async def move_item_to_group(item_id: int | str, group_id: str) -> str:
    """Moves an item to another group (within the same board)."""
    item_id = _ensure_id(item_id)
    start_time = time.time()
    query = """
    mutation ($item_id: ID!, $group_id: String!) {
        move_item_to_group (item_id: $item_id, group_id: $group_id) {
            id
        }
    }
    """
    try:
        data = await _monday_request(query, {"item_id": item_id, "group_id": group_id})
        latency_ms = int((time.time() - start_time) * 1000)

        if "errors" in data:
            error_msg = data["errors"][0]["message"]
            logger.error(json.dumps({
                "event": "move_item_to_group",
                "agent": "system",
                "service": "monday_server",
                "tool": "move_item_to_group",
                "status": "graphql_error",
                "error": error_msg,
                "item_id": item_id,
                "group_id": group_id,
                "latency_ms": latency_ms,
            }))
            return _error_response(tool="move_item_to_group", error_code="graphql_error", message=error_msg, latency_ms=latency_ms)

        moved = data.get("data", {}).get("move_item_to_group", {})
        logger.info(json.dumps({
            "event": "move_item_to_group",
            "agent": "system",
            "service": "monday_server",
            "tool": "move_item_to_group",
            "status": "success",
            "item_id": item_id,
            "group_id": group_id,
            "latency_ms": latency_ms,
        }))
        return json.dumps({
            "status": "success",
            "moved_item": moved,
            "message": f"Moved item {item_id} to group {group_id}",
        })
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "move_item_to_group",
            "agent": "system",
            "service": "monday_server",
            "tool": "move_item_to_group",
            "status": "error",
            "error": str(e),
            "item_id": item_id,
            "group_id": group_id,
            "latency_ms": latency_ms,
        }))
        return _error_response(tool="move_item_to_group", error_code="api_error", message=str(e), latency_ms=latency_ms)

@mcp.tool()
async def duplicate_board(
    board_id: int | str,
    duplicate_type: str = "duplicate_board_with_structure"
) -> str:
    """Duplicate a Monday.com board (for weekly schedule preparation).

    Args:
        board_id: ID of the board to duplicate
        duplicate_type: Type of duplication:
            - duplicate_board_with_structure: Structure only (columns, groups) Ã¢â‚¬â€ default
            - duplicate_board_with_pulses: Structure + items
            - duplicate_board_with_pulses_and_updates: Everything including comments
    """
    board_id = _ensure_id(board_id)
    start_time = time.time()
    valid_types = {
        "duplicate_board_with_structure",
        "duplicate_board_with_pulses",
        "duplicate_board_with_pulses_and_updates",
    }
    if duplicate_type not in valid_types:
        return _error_response(tool="duplicate_board", error_code="validation_error", message=f"Invalid duplicate_type '{duplicate_type}'. Must be one of: {sorted(valid_types)}")

    query = """
    mutation ($boardId: ID!, $duplicateType: DuplicateBoardType!) {
        duplicate_board(board_id: $boardId, duplicate_type: $duplicateType) {
            board { id name }
        }
    }
    """
    try:
        data = await _monday_request(query, {"boardId": board_id, "duplicateType": duplicate_type})
        latency_ms = int((time.time() - start_time) * 1000)

        if "errors" in data:
            error_msg = data["errors"][0]["message"]
            logger.error(json.dumps({
                "event": "duplicate_board",
                "agent": "system",
                "service": "monday_server",
                "tool": "duplicate_board",
                "status": "graphql_error",
                "error": error_msg,
                "board_id": board_id,
                "latency_ms": latency_ms,
            }))
            return _error_response(tool="duplicate_board", error_code="graphql_error", message=error_msg, latency_ms=latency_ms)

        new_board = data.get("data", {}).get("duplicate_board", {}).get("board", {})
        logger.info(json.dumps({
            "event": "duplicate_board",
            "agent": "system",
            "service": "monday_server",
            "tool": "duplicate_board",
            "status": "success",
            "source_board_id": board_id,
            "new_board_id": new_board.get("id"),
            "new_board_name": new_board.get("name"),
            "latency_ms": latency_ms,
        }))
        return json.dumps({
            "status": "success",
            "new_board": new_board,
            "message": f"Board {board_id} duplicated as '{new_board.get('name')}' (id: {new_board.get('id')})",
        })
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "duplicate_board",
            "agent": "system",
            "service": "monday_server",
            "tool": "duplicate_board",
            "status": "error",
            "error": str(e),
            "board_id": board_id,
            "latency_ms": latency_ms,
        }))
        return _error_response(tool="duplicate_board", error_code="api_error", message=str(e), latency_ms=latency_ms)


async def _get_month_group(board_id: int, year: int, month: int, force_refresh: bool = False) -> tuple[Optional[str], list[dict], bool]:
    """Finds a monthly group for the given year/month.

    Returns (group_id, groups, schema_ok):
      - group_id   Ã¢â‚¬â€ matching group's id, or None if no match found.
      - groups     Ã¢â‚¬â€ full group list from the board (empty on read failure).
      - schema_ok  Ã¢â‚¬â€ False when the board schema read itself failed (transient
                     API/network error); True on a successful read even if no
                     group matched.  Callers MUST check schema_ok and abort
                     rather than auto-creating a group on read failure.
    force_refresh: when True, bypasses the schema cache (used inside the
                   group-creation lock to see a just-created group).
    """
    month_name = calendar.month_name[month]
    target_title = f"{month_name} {year}"

    schema_ok = False
    schema_data = {}
    for _schema_attempt in range(3):
        if force_refresh and _schema_attempt == 0:
            schema_result = await _get_board_schema_fresh(board_id)
        else:
            schema_result = await get_board_schema(board_id)
        schema_data = json.loads(schema_result)

        if schema_data.get("status") == "success":
            schema_ok = True
            break

        if _schema_attempt < 2:
            await asyncio.sleep(1.5 * (_schema_attempt + 1))

    if not schema_ok:
        logger.warning(json.dumps({
            "event": "get_month_group",
            "agent": "system",
            "service": "monday_server",
            "tool": "create_inventory_booking",
            "status": "schema_read_failed_abort",
            "board_id": board_id,
            "target_title": target_title,
            "schema_status": schema_data.get("status"),
            "schema_message": schema_data.get("message"),
            "schema_error_code": schema_data.get("error_code"),
        }))
        return None, [], False

    board = schema_data.get("board", {})
    groups = board.get("groups", [])

    # Primary: exact case-insensitive title match (e.g. "June 2026")
    for group in groups:
        title = group.get("title", "").strip().lower()
        if title == target_title.lower():
            return group.get("id"), groups, True

    # Fallback: require both the 3-letter abbreviation AND the year as whole
    # whitespace-delimited tokens so loose substrings ("may" in "maybe",
    # "2026" in "112026") cannot route bookings to the wrong month group.
    abbrev = calendar.month_abbr[month].lower()
    year_str = str(year)
    for group in groups:
        tokens = group.get("title", "").lower().split()
        if abbrev in tokens and year_str in tokens:
            return group.get("id"), groups, True

    logger.warning(json.dumps({
        "event": "get_month_group",
        "agent": "system",
        "service": "monday_server",
        "tool": "create_inventory_booking",
        "status": "no_matching_group",
        "board_id": board_id,
        "target_title": target_title,
        "available_groups": [g.get("title") for g in groups]
    }))
    return None, groups, True


async def _create_month_group(board_id: int, title: str) -> Optional[str]:
    """Creates a new group on the board with the given title.

    Returns the new group's id, or None if the API call fails.
    Used by create_inventory_booking to auto-provision missing monthly groups
    so bookings for future months never fail or land in the wrong group.
    """
    query = """
    mutation ($board_id: ID!, $group_name: String!) {
        create_group(board_id: $board_id, group_name: $group_name) {
            id
        }
    }
    """
    try:
        data = await _monday_request(query, {"board_id": board_id, "group_name": title})
        if "errors" in data:
            logger.error(json.dumps({
                "event": "create_month_group",
                "agent": "system",
                "service": "monday_server",
                "status": "graphql_error",
                "board_id": board_id,
                "title": title,
                "error": data["errors"][0]["message"],
            }))
            return None
        return data["data"]["create_group"]["id"]
    except Exception as e:
        logger.error(json.dumps({
            "event": "create_month_group",
            "agent": "system",
            "service": "monday_server",
            "status": "api_error",
            "board_id": board_id,
            "title": title,
            "error": str(e),
        }))
        return None


async def _board_schema_info(board_id: int | str) -> tuple[dict, dict]:
    """Return (valid_labels, col_types) from a single board schema fetch.

    valid_labels: {col_id: set(label_strings)} for status/color columns Ã¢â‚¬â€ used
        for pre-flight validation in create_inventory_booking to drop bad labels.
    col_types:   {col_id: type_str} for all columns Ã¢â‚¬â€ used for type-aware value
        formatting (location, numbers, email, text, etc.).
    Both dicts are empty on any error so callers always get fail-open behaviour.
    """
    try:
        schema_raw = await get_board_schema(board_id)
        schema_data = json.loads(schema_raw)
        if schema_data.get("status") != "success":
            logger.warning(json.dumps({
                "event": "_board_schema_info",
                "service": "monday_server",
                "board_id": str(board_id),
                "status": schema_data.get("status"),
                "message": "Board schema fetch returned non-success Ã¢â‚¬â€ label validation and type-aware formatting disabled",
            }))
            return {}, {}
        board = schema_data.get("board") or {}
        valid_labels: dict = {}
        col_types: dict = {}
        for col in board.get("columns", []) or []:
            col_id = col.get("id")
            col_type = col.get("type", "text")
            if col_id:
                col_types[col_id] = col_type
            if col_type in ("status", "color"):
                settings = col.get("settings") or {}
                if isinstance(settings, dict) and "labels" in settings:
                    labels = settings["labels"]
                    if isinstance(labels, dict):
                        valid_labels[col_id] = set(labels.values())
        return valid_labels, col_types
    except Exception as _schema_exc:
        logger.warning(json.dumps({
            "event": "_board_schema_info",
            "service": "monday_server",
            "board_id": str(board_id),
            "error": str(_schema_exc),
            "message": "Failed to fetch/parse board schema Ã¢â‚¬â€ label validation and type-aware formatting disabled",
        }))
        return {}, {}


def _format_value_by_type(value: object, col_type: str) -> object | None:
    """Shape `value` for a Monday column based on its API column type.

    Returns the correctly-shaped value, or None when the formatted result would
    be empty (e.g. a phone value with no digits for a numbers column).
    Callers that get None should skip writing this column.

    Supported types:
      location  Ã¢â€ â€™ {"address": str}
      numbers   Ã¢â€ â€™ digits-only string (Monday numeric columns reject spaces/dashes)
      email     Ã¢â€ â€™ {"email": str, "text": str}
      text / long_text / unknown Ã¢â€ â€™ plain str (stored verbatim)
    """
    s = str(value).strip() if value is not None else ""
    if not s:
        return None
    if col_type == "location":
        return {"address": s}
    if col_type == "numbers":
        digits = "".join(d for d in s if d.isdigit())
        return digits if digits else None
    if col_type == "email":
        return {"email": s, "text": s}
    # text, long_text, phone (Monday phone column type), and any unrecognised type
    return s


def _merge_text_column(column_values: dict, col: str, text: str) -> None:
    """Append to an already-populated column value with ' | ', else set it.

    access_info/access_instructions and notes both map to the same physical
    column on the inventories board (access___info) Ã¢â‚¬â€ a plain overwrite meant
    the second write always clobbered the first. Using this for both keeps
    them combined instead of last-write-wins. Shared by create_inventory_booking
    and amend_booking so both write paths merge instead of clobber.
    """
    _existing = column_values.get(col)
    if isinstance(_existing, str) and _existing:
        column_values[col] = f"{_existing} | {text}"
    else:
        column_values[col] = text


def _format_hour_interval(hour: object) -> Optional[str]:
    """Normalize a booking time to a 30-minute interval label, e.g. "09:00 - 09:30".

    The slot is [floor to :00 or :30, +30 min] Ã¢â‚¬â€ 09:00 -> "09:00 - 09:30",
    09:15 -> "09:00 - 09:30", 09:45 -> "09:30 - 10:00", 23:45 -> "23:30 - 00:00".
    Accepts "14:30", "2:30 PM", "14", or an int. Returns None if unparseable so
    callers can skip the column rather than write garbage. Shared by
    create_inventory_booking and amend_booking so both write the same format.
    """
    if hour is None:
        return None
    s = str(hour).strip()
    if not s:
        return None
    _ampm = None
    _up = s.upper()
    if _up.endswith("AM") or _up.endswith("PM"):
        _ampm = _up[-2:]
        s = s[:-2].strip()
    try:
        if ":" in s:
            parts = s.split(":")
            h = int(parts[0])
            m = int(parts[1]) if len(parts) > 1 and parts[1] != "" else 0
        elif s.isdigit():
            h, m = int(s), 0
        else:
            return None
    except (ValueError, IndexError):
        return None
    if _ampm == "PM" and h != 12:
        h += 12
    elif _ampm == "AM" and h == 12:
        h = 0
    h %= 24
    m %= 60
    start_m = 0 if m < 30 else 30
    if start_m == 0:
        end_h, end_m = h, 30
    else:
        end_h, end_m = (h + 1) % 24, 0
    return f"{h:02d}:{start_m:02d} - {end_h:02d}:{end_m:02d}"


@mcp.tool()
async def create_inventory_booking(
    board_id: int | str,
    item_name: str,
    booking_date: str,
    group_id: Optional[str] = None,
    report_status: Optional[str] = None,
    inspector: Optional[str] = None,
    hour: Optional[str] = None,
    address: Optional[str] = None,
    postcode: Optional[str] = None,
    report_type: Optional[str] = None,
    spec: Optional[str] = None,
    reason_for_delay: Optional[str] = None,
    phone: Optional[str] = None,
    email: Optional[str] = None,
    customer: Optional[str] = None,
    company_name: Optional[str] = None,
    access_info: Optional[str] = None,
    contact_number: Optional[str] = None,
    notes: Optional[str] = None,
    payment_status: Optional[str] = None,
    inv_number: Optional[str] = None,
    payment_reminder: Optional[str] = None,
    access_instructions: Optional[str] = None,
    Acme_confirmation: Optional[str] = None,
    price: Optional[float] = None,
    job_status: Optional[str] = None,
    source_message_id: Optional[str] = None,
    thread_id: Optional[str] = None,
) -> str:
    # -------------------------------------------------------------------------
    # COLUMN ID MAPPINGS (Board IDs: 18392817962 - inventories, 18397426458 - contractors)
    # Verified 2026-07 against the live boards via get_board_info MCP.
    # Column IDs are loaded at runtime from Firestore monday_config/boards;
    # this block is documentation only Ã¢â‚¬â€ keep in sync with the live boards.
    # -------------------------------------------------------------------------
    # report_status    (status)   -> Report status
    # inspector        (status)   -> Inspector (clerk)   [labels = inspector names]
    # report___type    (status)   -> Report type          [labels = service types only]
    # color_mksnadmq   (status)   -> Reason for Delay     [contractors-only; unmapped on inventories]
    # payment___status (status)   -> Payment status
    # status7          (status)   -> Payment Reminder
    # status2          (status)   -> Acme Confirmation
    # location         (location) -> Postcode            [requires {"address": "..."}, unchanged]
    # contact_number   (text)     -> Phone/Contact number [inventories phone column is text, not numbers]
    # spec             (text)     -> Specification        [unchanged]
    # address          (text)     -> Address               [unchanged]
    # customer         (text)     -> Customer              [unchanged]
    # company_name     (text)     -> Company Name          [inventories: text_mm58vrcb, contractors: text_mm58wdep]
    # access___info    (text)     -> Access information    [unchanged]
    # dup__of_hour0__1 (text)     -> Hour                 [stored as "HH:MM" plain string, unchanged]
    # email            (email)    -> Email                [requires {"email":..,"text":..}, unchanged]
    # date             (date)     -> Date                 [requires {"date":"YYYY-MM-DD"}, unchanged]
    # price            (numbers)  -> Base Cost (inventories) / Acme Price (contractors) [ex-VAT input to Revenue/Gross Income formula]
    # job_status       (status)   -> Job status            [contractors-only; no equivalent column on inventories]
    # -------------------------------------------------------------------------
    """Creates an inventory booking with all columns populated.
    
    Routes to a monthly group based on booking_date, or uses explicit group_id if provided.
    Dynamically looks up groups from the board schema rather than using hardcoded mappings.
    
    Args:
        board_id: 18392817962 for inventories, 18397426458 for contractors
        item_name: Ignored Ã¢â‚¬â€ server always forces the item name to "AI Assisted" regardless
            of the value passed here (prevents the address/customer name from leaking into
            the item name).
        booking_date: Date in ISO format (YYYY-MM-DD) - determines which month group
        group_id: Optional hint group_id. Always validated against the live board Ã¢â‚¬â€ an invalid
                  or stale id is silently ignored and auto-routing takes over.  Auto-routing
                  matches the booking_date month/year to an existing group title; if none exists
                  the group is created automatically ("Month Year", e.g. "October 2026").
        report_status: Inventories-board status label (e.g. "Future report"). Defaults to
            "Not confirmed" (provisional) when omitted Ã¢â‚¬â€ pass the confirmed label only
            after the customer has explicitly confirmed the booking.
        inspector: Inspector name
        hour: Time/hour of booking
        address: Property address
        postcode: Location/postcode
        report_type: A Report/Service-type status label that exists on the board
            (e.g. "Inventory & Check In"). Never pass property attributes like
            furnished/unfurnished here Ã¢â‚¬â€ those belong in spec.
        spec: Specification details
        reason_for_delay: Reason for delay (color column)
        phone: Phone number (text column)
        email: Email address
        customer: Customer name
        company_name: Company or agency name (optional Ã¢â‚¬â€ omit for individual landlords)
        access_info: Access information
        contact_number: Contact number
        notes: Non-access special instructions only. Access/entry detail belongs in
            access_instructions instead Ã¢â‚¬â€ do not split it across both.
        payment_status: Payment status
        inv_number: Invoice number
        payment_reminder: Payment reminder status
        access_instructions: Full, verbatim access details Ã¢â‚¬â€ key-safe code and location,
            gate/entry/alarm codes, parking, on-site contact name/phone, agency key-pickup
            details, timing constraints, pets/hazards. Do not summarize or condense; this
            merges with notes onto the same board column.
        Acme_confirmation: Acme confirmation status
        price: Optional net booking price (ex-VAT). Written to the board's price-input
            column (Base Cost on inventories, Acme Price on contractors) which drives
            the Revenue/Gross Income formula columns. Pass ex-VAT Ã¢â‚¬â€ the board formula
            multiplies by 1.2 for VAT; do not pre-add VAT.
        job_status: Contractors-board-only job status label (e.g. "Booked"). Defaults to
            "Not confirmed with client" (provisional) when omitted on a contractors
            booking Ã¢â‚¬â€ pass "Booked" only after the customer has explicitly confirmed.
            Ignored entirely on inventories bookings Ã¢â‚¬â€ there is no equivalent column
            on that board.
    """
    board_id = _ensure_id(board_id)
    start_time = time.time()

    # --- Required-field guard ---
    # Refuse before any board routing / group lookup / create_item mutation when the
    # booking lacks the two fields every legitimate booking must have. Prevents blank
    # "New" item shells (e.g. items 12488273792, 12488259491 created 2026-07-09) where
    # the LLM called this tool with no real data and the report_status default below
    # masked the emptiness. booking_date is intentionally NOT required here Ã¢â‚¬â€ a null
    # date is legitimate for an in-progress reschedule.
    _missing_required = [
        name for name, val in (("customer", customer), ("address", address))
        if not (val and str(val).strip())
    ]
    if _missing_required:
        logger.warning(json.dumps({
            "event": "create_inventory_booking",
            "phase": "missing_required_fields",
            "service": "monday_server",
            "missing_fields": _missing_required,
            "item_name": item_name,
        }))
        return json.dumps({
            "status": "missing_required_fields",
            "missing_fields": _missing_required,
            "message": (
                "Cannot create booking Ã¢â‚¬â€ missing required field(s): "
                + ", ".join(_missing_required)
                + ". Ask the customer for this information before retrying."
            ),
        })

    # --- Force item name (server-side backstop) ---
    # Override whatever the caller passed Ã¢â‚¬â€ the address or customer name must never
    # leak into the item name (e.g. items logged with the property address as the
    # name, 2026-07-09 Albert Patrick / 7A Sydney Place incident). Every agent-created
    # item is named literally "AI Assisted" regardless of board or caller input.
    item_name = "AI Assisted"

    # --- Board routing (deterministic, by service type) ---
    # classify_board_key maps the job's service type to "inventories" or "contractors".
    # The routed board_id overrides the caller-supplied hint so jobs always land on the
    # correct board regardless of what the agent passes.  If the resolved board is not
    # yet configured (id == 0), we surface a clear error rather than silently writing
    # the job to the wrong board.
    board_key = _classify_board_key(report_type, spec)
    routed_board_id = get_board_id(board_key)
    if routed_board_id == 0:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "create_inventory_booking",
            "agent": "system",
            "service": "monday_server",
            "tool": "create_inventory_booking",
            "status": "board_not_configured",
            "board_key": board_key,
            "spec": spec,
            "latency_ms": latency_ms,
        }))
        return _error_response(
            tool="create_inventory_booking",
            error_code="configuration_error",
            message=(
                f"The '{board_key}' Monday board is not configured. "
                "Connect it in the dashboard under Settings Ã¢â€ â€™ Monday.com, then retry."
            ),
            latency_ms=latency_ms,
        )
    board_id = routed_board_id
    logger.info(json.dumps({
        "event": "booking_routed",
        "agent": "system",
        "service": "monday_server",
        "tool": "create_inventory_booking",
        "board_key": board_key,
        "board_id": board_id,
        "service_type": report_type,
    }))

    # Parse booking date to determine group (if not explicitly provided)
    try:
        date_obj = datetime.fromisoformat(booking_date.replace('Z', '+00:00'))
        year = date_obj.year
        month = date_obj.month
    except (ValueError, AttributeError) as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "create_inventory_booking",
            "agent": "system",
            "service": "monday_server",
            "tool": "create_inventory_booking",
            "status": "date_parse_error",
            "error": str(e),
            "booking_date": booking_date,
            "latency_ms": latency_ms,
        }))
        return _error_response(tool="create_inventory_booking", error_code="validation_error", message=f"Invalid booking_date format: {booking_date}. Use YYYY-MM-DD", latency_ms=latency_ms)
    
    # Resolve which board group to use.
    # Strategy (in order):
    #   1. If caller supplied a group_id, validate it against the live board.
    #      An invalid/stale id is logged and discarded Ã¢â‚¬â€ never forwarded blind to Monday,
    #      because Monday silently places unresolved items in the board's top group
    #      (currently "cancelled_jobs"), corrupting the board.
    #   2. Month-title lookup via _get_month_group (exact match, then token-boundary abbrev).
    #   3. If the schema read failed (transient error), abort with a retryable error Ã¢â‚¬â€
    #      NEVER auto-create in this case, as that would mint duplicate groups whenever
    #      the Monday API is briefly unavailable.
    #   4. If no matching group exists and the read succeeded, auto-create "<Month Year>"
    #      inside a per-(board, title) asyncio lock so concurrent bookings for the same
    #      month serialise: the second caller re-queries inside the lock and reuses the
    #      group just created by the first.
    #   5. If creation also fails, return an error Ã¢â‚¬â€ the mutation is never reached without
    #      a confirmed-valid group id.

    # Fetch live groups once so validation and lookup share the same snapshot.
    month_group_id, available_groups, schema_ok = await _get_month_group(board_id, year, month)
    live_group_ids = {g.get("id") for g in available_groups}

    # Step 1: validate caller-supplied group_id
    resolved_group_id: Optional[str] = None
    if group_id:
        if group_id in live_group_ids:
            resolved_group_id = group_id
        else:
            logger.warning(json.dumps({
                "event": "create_inventory_booking",
                "agent": "system",
                "service": "monday_server",
                "tool": "create_inventory_booking",
                "status": "invalid_group_id_supplied",
                "supplied_group_id": group_id,
                "board_id": board_id,
                "note": "supplied group_id not found on board; falling back to month lookup",
            }))

    # Step 2: use month-matched group from the same snapshot (no extra API call)
    if not resolved_group_id:
        resolved_group_id = month_group_id

    # Step 3: abort if the board schema read failed Ã¢â‚¬â€ do NOT auto-create a group
    # when we can't confirm whether one already exists; that would duplicate it.
    if not resolved_group_id and not schema_ok:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "create_inventory_booking",
            "agent": "system",
            "service": "monday_server",
            "tool": "create_inventory_booking",
            "status": "schema_read_failed_abort",
            "board_id": board_id,
            "year": year,
            "month": month,
            "latency_ms": latency_ms,
        }))
        return _error_response(
            tool="create_inventory_booking",
            error_code="api_error",
            message="Monday board schema read failed; cannot safely determine month group. Please retry.",
            latency_ms=latency_ms,
        )

    # Step 4: auto-create the monthly group if the read succeeded but no group matched.
    # Serialise within this process via a per-(board, title) asyncio lock so that two
    # concurrent bookings for the same month don't both race to create the group.
    if not resolved_group_id and schema_ok:
        new_title = f"{calendar.month_name[month]} {year}"
        lock = await _get_month_group_lock(board_id, new_title)
        async with lock:
            # Re-query inside the lock Ã¢â‚¬â€ another coroutine may have just created it.
            # force_refresh=True bypasses the cache so we see any group created since
            # the initial lookup above.
            re_group_id, _, re_schema_ok = await _get_month_group(board_id, year, month, force_refresh=True)
            if re_group_id:
                resolved_group_id = re_group_id
            elif not re_schema_ok:
                # Schema read failed inside lock Ã¢â‚¬â€ abort rather than blind-create.
                latency_ms = int((time.time() - start_time) * 1000)
                logger.error(json.dumps({
                    "event": "create_inventory_booking",
                    "agent": "system",
                    "service": "monday_server",
                    "tool": "create_inventory_booking",
                    "status": "schema_read_failed_in_lock_abort",
                    "board_id": board_id,
                    "group_title": new_title,
                    "latency_ms": latency_ms,
                }))
                return _error_response(
                    tool="create_inventory_booking",
                    error_code="api_error",
                    message="Monday board schema read failed inside group-creation lock. Please retry.",
                    latency_ms=latency_ms,
                )
            else:
                logger.info(json.dumps({
                    "event": "create_inventory_booking",
                    "agent": "system",
                    "service": "monday_server",
                    "tool": "create_inventory_booking",
                    "status": "auto_creating_group",
                    "board_id": board_id,
                    "group_title": new_title,
                }))
                resolved_group_id = await _create_month_group(board_id, new_title)
                # Invalidate cache so subsequent calls see the new group.
                _invalidate_board_schema(str(board_id))

    # Step 5a: last-resort fallback Ã¢â‚¬â€ use the configured default month group for this board
    # (set via the dashboard "Default Month Group" mapping).  Only used when month lookup
    # AND auto-creation both failed (transient Monday API issues).  The id is validated
    # against live_group_ids so a stale config value is never forwarded blind.
    if not resolved_group_id:
        configured_default = get_group_id(board_key, "default")
        if configured_default and configured_default in live_group_ids:
            resolved_group_id = configured_default
            logger.info(json.dumps({
                "event": "create_inventory_booking",
                "agent": "system",
                "service": "monday_server",
                "tool": "create_inventory_booking",
                "status": "using_configured_default_group",
                "board_id": board_id,
                "board_key": board_key,
                "group_id": resolved_group_id,
            }))

    # Step 5b: hard-fail only if even group creation didn't work
    if not resolved_group_id:
        latency_ms = int((time.time() - start_time) * 1000)
        groups_summary = [{"id": g.get("id"), "title": g.get("title")} for g in available_groups]
        logger.error(json.dumps({
            "event": "create_inventory_booking",
            "agent": "system",
            "service": "monday_server",
            "tool": "create_inventory_booking",
            "status": "no_group_found",
            "year": year,
            "month": month,
            "latency_ms": latency_ms,
        }))
        return json.dumps({
            "status": "error",
            "error_code": "validation_error",
            "tool": "create_inventory_booking",
            "message": f"No group found or created for '{calendar.month_name[month]} {year}'. Pick a group_id from the list below and retry.",
            "available_groups": groups_summary,
            "latency_ms": latency_ms,
        })
    
    # Fetch board schema once Ã¢â‚¬â€ provides both col_types (for type-aware value
    # formatting) and valid_labels (for status pre-flight check below).
    # Fail-open: both dicts are {} on any schema error, which disables validation
    # but never blocks a booking.
    _valid_labels, _col_types = await _board_schema_info(board_id)

    # Build column_values JSON for Monday.com
    column_values: dict[str, object] = {}
    _hour_interval: Optional[str] = None

    def _col(name: str) -> Optional[str]:
        return get_column_id(board_key, name)

    def _require(name: str) -> str:
        return require_column_id(board_key, name)

    # Default to provisional Ã¢â‚¬â€ an agent-created booking is NOT confirmed until the
    # customer explicitly says so. Default the Report status column to "Not confirmed"
    # (exact live-board label) unless the caller explicitly passes the confirmed label
    # ("Future report") after the customer has confirmed the booking.
    if report_status is None:
        report_status = "Not confirmed"

    # Status columns Ã¢â‚¬â€ all require {"label": value} format
    if report_status:
        _c = _col("report_status")
        if _c:
            column_values[_c] = {"label": report_status}
    if inspector:
        _c = _col("inspector")
        if _c:
            column_values[_c] = {"label": inspector}
    if report_type:
        _c = _col("report_type")
        if _c:
            # Inventories: normalize free-text service type to the canonical board label.
            # Contractors: map common LLM alias variants to canonical labels first
            # (e.g. "End of Tenancy Cleaning" Ã¢â€ â€™ "EOT Cleaning"), then the live-label
            # pre-flight below validates/normalizes against the board's actual labels.
            if board_key == "inventories":
                _rt_label = _resolve_service_type(report_type) or report_type
            else:
                _rt_label = _CONTRACTOR_LABEL_MAP.get(
                    " ".join(report_type.lower().strip().split()), report_type
                )
            column_values[_c] = {"label": _rt_label}
    if reason_for_delay:
        _c = _col("reason_delay")
        if _c:
            column_values[_c] = {"label": reason_for_delay}
    if payment_status:
        _c = _col("payment_status")
        if _c:
            column_values[_c] = {"label": payment_status}
    if payment_reminder:
        _c = _col("payment_reminder")
        if _c:
            column_values[_c] = {"label": payment_reminder}
    if Acme_confirmation:
        _c = _col("Acme_confirmation")
        if _c:
            column_values[_c] = {"label": Acme_confirmation}
    # job_status is a contractors-board-only status column Ã¢â‚¬â€ there is no equivalent
    # column on inventories, so this is explicitly gated to board_key == "contractors"
    # (mirroring how report_status above is implicitly inventories-only via config).
    # Default to provisional Ã¢â‚¬â€ a booking is NOT confirmed until the customer explicitly
    # says so. Default to "Not confirmed with client" (verified live label) unless the
    # caller explicitly passes the confirmed label ("Booked") after customer confirmation.
    if board_key == "contractors":
        if job_status is None:
            job_status = "Not confirmed with client"
        _c = _col("job_status")
        if _c:
            column_values[_c] = {"label": job_status}
    # Text columns
    # Auto-derive postcode from address when agent didn't pass it separately.
    # UK addresses almost always include the postcode Ã¢â‚¬â€ regex extracts it reliably.
    # This runs before the postcode write block so the derived value feeds the
    # normal _format_value_by_type path and the idempotency hash unchanged.
    if not postcode and address:
        _derived_pc = _extract_uk_postcode(address)
        if _derived_pc:
            postcode = _derived_pc
            logger.info(json.dumps({
                "event": "create_inventory_booking",
                "phase": "postcode_derived_from_address",
                "service": "monday_server",
                "postcode": _derived_pc,
                "address": address,
            }))
    if postcode:
        _c = _col("postcode")
        if _c:
            # Use live column type from board schema; fall back to board-key heuristic
            # when schema unavailable (inventories=location, contractors=text).
            _ptype = _col_types.get(_c, "location" if board_key == "inventories" else "text")
            if _ptype == "location":
                # Monday API 2026-01 requires lat + lng Ã¢â‚¬â€ {"address": ...} alone is rejected.
                # Geocode the postcode via postcodes.io (free UK lookup, < 100ms).
                # Falls back to London-centre coords so the booking is never blocked.
                _coords = await _geocode_uk_postcode(postcode)
                if _coords:
                    _lat, _lng = _coords
                    logger.info(json.dumps({
                        "event": "create_inventory_booking",
                        "phase": "postcode_geocoded",
                        "service": "monday_server",
                        "postcode": postcode,
                        "lat": _lat,
                        "lng": _lng,
                    }))
                else:
                    _lat, _lng = "51.5074", "-0.1278"  # London centre fallback
                    logger.warning(json.dumps({
                        "event": "create_inventory_booking",
                        "phase": "postcode_geocode_fallback",
                        "service": "monday_server",
                        "postcode": postcode,
                        "message": "postcodes.io lookup failed Ã¢â‚¬â€ using London-centre coords as placeholder",
                    }))
                column_values[_c] = {"lat": _lat, "lng": _lng, "address": postcode}
            else:
                _pval = _format_value_by_type(postcode, _ptype)
                if _pval is not None:
                    column_values[_c] = _pval
    if address:
        _c = _col("address")
        if _c:
            column_values[_c] = address
    if spec:
        # "spec" column Ã¢â‚¬â€ Specification text field on both boards. spec is now
        # PROPERTY SPECIFICATION ONLY (house size / bed/bath/furnished, e.g.
        # "3 bed, 2 bath, Furnished") Ã¢â‚¬â€ service type comes from report_type
        # exclusively (see report_type write above). Verbatim text, never a
        # {"label": ...} status value.
        _c = _col("spec")
        if _c:
            column_values[_c] = spec
    if phone:
        _c = _col("phone")
        if _c:
            # Use live column type; inventories phone is numbers, contractors is text.
            _phtype = _col_types.get(_c, "numbers" if board_key == "inventories" else "text")
            _phval = _format_value_by_type(phone, _phtype)
            if _phval is not None:
                column_values[_c] = _phval
    if customer:
        _c = _col("customer")
        # 'customer' is a REQUIRED column. Refuse to silently drop it when it's unmapped
        # (_c is None) or was mis-detected onto the item-Name column (id "name" Ã¢â‚¬â€ the item
        # title is force-overridden to "AI Assisted" above, so a value written there is
        # discarded). Either way the real Customer column never gets the name; hard-block
        # so no item is created with a missing customer name.
        if _c and _c != "name":
            column_values[_c] = customer
        else:
            logger.error(json.dumps({
                "event": "create_inventory_booking",
                "phase": "customer_column_misconfigured",
                "service": "monday_server",
                "board_key": board_key,
                "resolved_col_id": _c,
                "message": (
                    "Customer column is unmapped or points at the item-Name column Ã¢â‚¬â€ "
                    "customer name would be lost. Re-run Monday column detection "
                    "in the dashboard (Settings -> Monday.com)."
                ),
            }))
            return json.dumps({
                "status": "error",
                "reason": "customer_column_misconfigured",
                "message": (
                    "Cannot create booking Ã¢â‚¬â€ customer column is unmapped or misconfigured. "
                    "Customer name would be silently lost. Fix the column mapping in the "
                    "dashboard (Settings Ã¢â€ â€™ Monday.com) before retrying."
                ),
            })
    if company_name:
        _c = _col("company_name")
        if _c:
            column_values[_c] = company_name
    _access = access_info or access_instructions
    if _access:
        _c = _col("access_info")
        if _c:
            _merge_text_column(column_values, _c, _access)
    if contact_number:
        _c = _col("contact_number")
        if _c:
            column_values[_c] = contact_number
    if notes:
        _c = _col("notes")
        if _c:
            _merge_text_column(column_values, _c, notes)
    if inv_number:
        _c = _col("inv_number")
        if _c:
            column_values[_c] = inv_number
    if price is not None:
        _c = _col("price")
        if _c:
            # Monday `numbers` columns accept a bare number or numeric string; write the
            # value verbatim (as a string) so decimals survive Ã¢â‚¬â€ do NOT route through
            # _format_value_by_type's "numbers" branch, which strips to digits-only and
            # is intended for phone-style values, not currency.
            column_values[_c] = str(price)
            if board_key == "inventories":
                # Revenue = ROUND((Base Cost + Surcharge) * (1 - Discount%) * 1.2, 2) Ã¢â‚¬â€ Monday
                # formula columns propagate NULL through arithmetic, so leaving Surcharge/
                # Discount% unset (their default state on a new item) makes Revenue/Gross
                # Income stay blank even with Base Cost populated. Zero them explicitly on
                # create so the formula actually evaluates; a human can adjust either later.
                column_values["numeric_mm28j2pq"] = "0"  # Surcharge
                column_values["numeric_mm28b3gs"] = "0"  # Discount%

    # Hour column is a text column Ã¢â‚¬â€ store as a 30-minute interval label
    # ("09:00 - 09:30"), not a single "HH:MM" time. Same value is returned in the
    # tool result so the confirmation email quotes the identical arrival window.
    # Skips (does not include in payload) if value cannot be parsed.
    if hour:
        _hour_interval = _format_hour_interval(hour)
        if _hour_interval:
            _hcol = _col("hour")
            if _hcol:
                column_values[_hcol] = _hour_interval
        else:
            logger.warning(json.dumps({
                "event": "create_inventory_booking",
                "service": "monday_server",
                "message": f"Could not parse hour value '{hour}' Ã¢â‚¬â€ skipping hour column",
            }))
    
    # Email column - Monday email type format
    if email:
        _ecol = _col("email")
        if _ecol:
            column_values[_ecol] = {"email": email, "text": email}

    # Date column Ã¢â‚¬â€ required field; hard error if unmapped
    date_only = booking_date.split("T")[0] if "T" in booking_date else booking_date
    _dcol = _col("date")
    if not _dcol:
        latency_ms = int((time.time() - start_time) * 1000)
        return _error_response(
            tool="create_inventory_booking",
            error_code="config_error",
            message="Date column not mapped Ã¢â‚¬â€ configure Monday column IDs in the dashboard",
            latency_ms=latency_ms,
        )
    column_values[_dcol] = {"date": date_only}

    # Pre-flight: validate/canonicalize status/color {"label": ...} entries against the
    # board's live valid labels. Two outcomes:
    #   1. Normalized match found Ã¢â€ â€™ replace payload label with the board's canonical
    #      label string (fixes &amp; vs & encoding divergence between _resolve_service_type
    #      and schema Ã¢â‚¬â€ the root cause of blank Report-type columns on inventory bookings).
    #   2. No match Ã¢â€ â€™ drop the column (prevents create_item rejection) AND preserve the
    #      raw service text in notes so information is not silently lost.
    # _valid_labels fetched above via _board_schema_info Ã¢â‚¬â€ {} on schema failure Ã¢â€ â€™ skipped.
    _dropped_labels: list = []
    if _valid_labels:
        for _vcid in list(column_values.keys()):
            _vval = column_values.get(_vcid)
            if not (isinstance(_vval, dict) and "label" in _vval):
                continue
            if _vcid not in _valid_labels:
                continue
            _payload_label = _vval["label"]
            # Build normalizedÃ¢â€ â€™canonical map from the board's actual label set.
            _norm_map = {_normalize_label(bl): bl for bl in _valid_labels[_vcid]}
            _canonical = _norm_map.get(_normalize_label(_payload_label))
            if _canonical is not None:
                # Match after normalization Ã¢â‚¬â€ use board's canonical label (e.g. &amp; form).
                if column_values[_vcid]["label"] != _canonical:
                    logger.info(json.dumps({
                        "event": "create_inventory_booking",
                        "phase": "status_label_canonicalized",
                        "service": "monday_server",
                        "col_id": _vcid,
                        "from_label": _payload_label,
                        "to_label": _canonical,
                        "message": "Status label normalized to board canonical form",
                    }))
                    column_values[_vcid] = {"label": _canonical}
            else:
                # Genuine mismatch Ã¢â‚¬â€ drop from status column to prevent create_item rejection.
                column_values.pop(_vcid)
                # Preserve the raw service text in notes so information is not silently lost.
                _notes_col = _col("notes")
                if _notes_col and _payload_label:
                    _existing_notes = column_values.get(_notes_col)
                    if isinstance(_existing_notes, str) and _existing_notes:
                        column_values[_notes_col] = f"{_existing_notes} | Service type: {_payload_label}"
                    elif not _existing_notes:
                        column_values[_notes_col] = f"Service type: {_payload_label}"
                _dropped_labels.append({"col_id": _vcid, "label": _payload_label})
                logger.warning(json.dumps({
                    "event": "create_inventory_booking",
                    "phase": "status_label_dropped",
                    "service": "monday_server",
                    "col_id": _vcid,
                    "label": _payload_label,
                    "message": "Status label not valid for column Ã¢â‚¬â€ dropped to prevent create_item rejection; raw value preserved in notes",
                }))

    # Convert to JSON string as Monday expects
    column_values_json = json.dumps(column_values)

    # Fields that should always be populated for a complete booking (by column ID)
    _EXPECTED_BOOKING_FIELDS = {
        c for name in ("spec", "email", "phone", "customer", "address", "postcode", "date", "hour", "notes", "report_type")
        if (c := _col(name))
    }
    _populated = sorted(column_values.keys())
    _missing = sorted(_EXPECTED_BOOKING_FIELDS - set(column_values.keys()))
    # A REQUIRED logical column that isn't mapped at all resolves to no column id, so it
    # can never appear in _missing above Ã¢â‚¬â€ check the required set by logical name too.
    _unmapped_required = sorted(n for n in ("date", "address", "customer") if not _col(n))
    logger.info(json.dumps({
        "event": "create_inventory_booking",
        "phase": "payload_built",
        "service": "monday_server",
        "board_id": board_id,
        "group_id": resolved_group_id,
        "item_name": item_name,
        "populated_fields": _populated,
        "missing_expected_fields": _missing,
        "unmapped_required_columns": _unmapped_required,
        "dropped_labels": _dropped_labels,
    }))

    # ---- LIVE-BOARD DEDUP BACKSTOP (best-effort) ----
    # Catches duplicates the Firestore lock/thread resolver below can't see Ã¢â‚¬â€ e.g. a
    # WhatsApp-originated booking, which (until Edit 5) carries neither thread_id nor
    # source_message_id, so a second channel for the same job would otherwise create a
    # fresh item (see Julie Zhuang double-booking, 2 items 4 min apart, same phone/address).
    # Queries the live board directly by phone digits, or by address confirmed by
    # customer-name match when phone is absent. Reuses the items_page_by_column_values
    # pattern from create_lead's idempotency pre-check (~3483-3560).
    # Scoped by booking_date: a phone/address(+customer) match is only a duplicate if the
    # existing item's date column also matches this booking's date_only. Without this, any
    # phone-less booking for a returning customer/address (e.g. an annual Gas Safety/EICR
    # renewal, or a second inventory job at a previously-booked address) would be
    # misflagged as a duplicate of an unrelated old item and silently dropped Ã¢â‚¬â€ the
    # required-field guard above already forces every call to have customer+address, so
    # this branch would otherwise run on every phone-less booking.
    # Fail-open: wrapped in try/except Ã¢â‚¬â€ any failure here is logged and swallowed, and
    # execution falls through to the Firestore lock below. Must never block a legitimate
    # booking. Caveat: phone/address are text columns, so this is exact-match only.
    try:
        _dedup_phone_col = _col("phone")
        _dedup_addr_col = _col("address")
        _dedup_cust_col = _col("customer")
        _phone_digits = "".join(ch for ch in (phone or "") if ch.isdigit())
        _dedup_columns = None
        _dedup_by_phone = False
        if _phone_digits and _dedup_phone_col:
            _dedup_columns = [{"column_id": _dedup_phone_col, "column_values": [_phone_digits]}]
            _dedup_by_phone = True
        elif address and customer and _dedup_addr_col and _dedup_cust_col:
            _dedup_columns = [{"column_id": _dedup_addr_col, "column_values": [address]}]

        if _dedup_columns:
            _dedup_query = """
            query ($board_id: ID!, $columns: [ItemsPageByColumnValuesQuery!]) {
                items_page_by_column_values(limit: 50, board_id: $board_id, columns: $columns) {
                    items { id name column_values { id text } }
                }
            }
            """
            _dedup_vars = {"board_id": board_id, "columns": _dedup_columns}
            _dedup_data = await _monday_request(_dedup_query, _dedup_vars)
            if "errors" not in _dedup_data:
                _dedup_items = _dedup_data.get("data", {}).get("items_page_by_column_values", {}).get("items", [])
                for _item in _dedup_items:
                    _match = _dedup_by_phone
                    if not _dedup_by_phone:
                        # Address-only path Ã¢â‚¬â€ confirm customer name matches too, to avoid
                        # false positives on shared/common addresses (e.g. blocks of flats).
                        _match = False
                        for _cv in _item.get("column_values", []) or []:
                            if _cv.get("id") == _dedup_cust_col:
                                _match = (_cv.get("text") or "").strip().lower() == customer.strip().lower()
                                break
                    if _match:
                        # Date guard: only treat this as the SAME job re-submitted, not a
                        # new job for a returning customer/address. items_page_by_column_values
                        # already returns every column (no `ids` filter above), so the date
                        # column's text is available here without an extra query round-trip.
                        _item_date = None
                        for _cv in _item.get("column_values", []) or []:
                            if _cv.get("id") == _dcol:
                                _item_date = (_cv.get("text") or "").strip()
                                break
                        _match = _item_date == date_only
                    if _match:
                        logger.info(json.dumps({
                            "event": "create_inventory_booking",
                            "phase": "skipped_duplicate",
                            "service": "monday_server",
                            "board_id": board_id,
                            "existing_item_id": _item.get("id"),
                            "item_name": item_name,
                            "reason": "live_board_dedup_backstop",
                        }))
                        return json.dumps({
                            "status": "skipped_duplicate",
                            "item_id": _item.get("id"),
                            "message": "Booking already exists on the board (live duplicate check by phone/address)",
                            "source_message_id": source_message_id,
                            "lock_key": None,
                        })
    except Exception as _dedup_err:
        logger.warning(json.dumps({
            "event": "create_inventory_booking",
            "phase": "live_dedup_backstop_failed",
            "service": "monday_server",
            "board_id": board_id,
            "error": str(_dedup_err),
        }))

    # ---- BOOKING IDEMPOTENCY LOCK (Firestore) ----
    # Prevents duplicate bookings when replies in an already-booked thread re-trigger STEP 1.
    # Key precedence (most Ã¢â€ â€™ least specific):
    #   booking:<thread_id>:<date>    Ã¢â€ â€™ one booking per email thread per date (primary)
    #   booking:msg:<message_id>      Ã¢â€ â€™ fallback when only message_id is known
    #   booking:proactive:<sha1>      Ã¢â€ â€™ dashboard/CLI bookings with no email context
    # Mirrors the draft_reply dedup lock in gmail_server.py (create/AlreadyExists/expiry).
    _booking_lock_ref = None
    _booking_lock_key = None
    _now = None  # set inside the lock-write try block below; reused for thread_bookings
    if thread_id and date_only:
        _booking_lock_key = f"booking:{thread_id}:{date_only}"
    elif source_message_id:
        _booking_lock_key = f"booking:msg:{source_message_id}"
    elif address and postcode and date_only:
        _digest = hashlib.sha1(
            f"{(address or '').strip().lower()}|{(postcode or '').strip().lower()}|{date_only}".encode()
        ).hexdigest()
        _booking_lock_key = f"booking:proactive:{_digest}"

    if _booking_lock_key:
        try:
            _fs = _get_booking_firestore()
            _booking_lock_ref = _fs.collection(_BOOKING_LOCK_COLLECTION).document(_booking_lock_key)
            _now = datetime.now(timezone.utc)
            _exp = _now + timedelta(days=_BOOKING_DEDUP_TTL_DAYS)
            try:
                _booking_lock_ref.create({
                    "owner": "monday_server_booking",
                    "created_at": _now,
                    "expires_at": _exp,
                    "item_id": None,
                })
            except _AlreadyExists:
                _snap = _booking_lock_ref.get()
                if _snap.exists:
                    _ldata = _snap.to_dict()
                    _ea = _ldata.get("expires_at")
                    if hasattr(_ea, 'to_pydatetime'):
                        _ea = _ea.to_pydatetime()
                    if _ea and _ea > _now:
                        _existing_id = _ldata.get("item_id")
                        # Self-heal: if lock exists but item_id is None, it's poisoned
                        if _existing_id is None:
                            logger.warning(json.dumps({
                                "event": "create_inventory_booking",
                                "phase": "poisoned_lock_recovered",
                                "service": "monday_server",
                                "lock_key": _booking_lock_key,
                                "board_id": board_id,
                            }))
                            # Atomically replace poisoned lock in a transaction (eliminates
                            # TOCTOU window between old delete() + new create()).
                            _new_lock_data = {
                                "owner": "monday_server_booking",
                                "created_at": _now,
                                "expires_at": _exp,
                                "item_id": None,
                            }
                            try:
                                @_firestore.transactional
                                def _recover_poisoned_lock(txn, ref, new_data, now):
                                    snap = ref.get(transaction=txn)
                                    if not snap.exists:
                                        raise _AlreadyExists("lock_gone")
                                    d = snap.to_dict() or {}
                                    ea = d.get("expires_at")
                                    if hasattr(ea, 'to_pydatetime'):
                                        ea = ea.to_pydatetime()
                                    # Only overwrite if still poisoned; otherwise another
                                    # process already recovered or wrote a real item_id.
                                    if ea and ea > now and d.get("item_id") is None:
                                        txn.set(ref, new_data)
                                    else:
                                        raise _AlreadyExists("lock_recovered")

                                _txn = _get_booking_firestore().transaction()
                                _recover_poisoned_lock(_txn, _booking_lock_ref, _new_lock_data, _now)
                                # Fall through to mutation (not returning here)
                            except _AlreadyExists:
                                # Concurrent recovery winner exists; return skipped_duplicate
                                logger.info(json.dumps({
                                    "event": "create_inventory_booking",
                                    "phase": "skipped_duplicate",
                                    "service": "monday_server",
                                    "board_id": board_id,
                                    "existing_item_id": None,
                                    "item_name": item_name,
                                    "lock_key": _booking_lock_key,
                                    "reason": "poisoned_lock_recovery_collision",
                                }))
                                return json.dumps({
                                    "status": "skipped_duplicate",
                                    "item_id": None,
                                    "message": "Booking already exists (idempotency lock)",
                                    "source_message_id": source_message_id,
                                    "lock_key": _booking_lock_key,
                                })
                            except Exception as _txn_err:
                                logger.warning(json.dumps({
                                    "event": "create_inventory_booking",
                                    "phase": "poisoned_lock_recover_failed",
                                    "service": "monday_server",
                                    "lock_key": _booking_lock_key,
                                    "error": str(_txn_err),
                                }))
                                # Fall through to mutation attempt anyway
                        else:
                            # Lock is not poisoned; existing_id is set
                            logger.info(json.dumps({
                                "event": "create_inventory_booking",
                                "phase": "skipped_duplicate",
                                "service": "monday_server",
                                "board_id": board_id,
                                "existing_item_id": _existing_id,
                                "item_name": item_name,
                                "lock_key": _booking_lock_key,
                            }))
                            return json.dumps({
                                "status": "skipped_duplicate",
                                "item_id": _existing_id,
                                "message": "Booking already exists (idempotency lock)",
                                "source_message_id": source_message_id,
                                "lock_key": _booking_lock_key,
                            })
                    # Expired lock Ã¢â‚¬â€ overwrite with a fresh one
                    _booking_lock_ref.set({
                        "owner": "monday_server_booking",
                        "created_at": _now,
                        "expires_at": _exp,
                        "item_id": None,
                    })
        except _AlreadyExists:
            pass  # handled in inner try above
        except Exception as _le:
            _invalidate_booking_firestore()
            logger.warning(json.dumps({
                "event": "create_inventory_booking",
                "phase": "dedup_check_failed",
                "service": "monday_server",
                "lock_key": _booking_lock_key,
                "error": str(_le),
            }))
            _booking_lock_ref = None  # don't attempt update on a ref we couldn't create

    # ---- CROSS-DATE DUPLICATE CHECK (thread-scoped, date-independent) ----
    # The lock above only catches duplicates for the SAME date Ã¢â‚¬â€ its key is
    # booking:<thread_id>:<date>, so a reschedule to a different date is a miss
    # there. resolve_thread_item() is date-independent, so it still finds the
    # existing item and we skip creating a second one for this thread.
    if thread_id:
        _existing_thread_item = resolve_thread_item(thread_id)
        if _existing_thread_item and _existing_thread_item.get("item_id"):
            _existing_item_id = _existing_thread_item["item_id"]
            logger.info(json.dumps({
                "event": "create_inventory_booking",
                "phase": "skipped_duplicate",
                "service": "monday_server",
                "board_id": board_id,
                "existing_item_id": _existing_item_id,
                "item_name": item_name,
                "thread_id": thread_id,
                "reason": "cross_date_thread_resolver_hit",
            }))
            return json.dumps({
                "status": "skipped_duplicate",
                "item_id": _existing_item_id,
                "message": "Booking already exists for this thread (resolved via thread_bookings; date may differ)",
                "source_message_id": source_message_id,
                "lock_key": _booking_lock_key,
            })

    query = """
    mutation ($board_id: ID!, $item_name: String!, $group_id: String!, $column_values: JSON!) {
        create_item (board_id: $board_id, item_name: $item_name, group_id: $group_id, column_values: $column_values) {
            id
        }
    }
    """

    vars = {
        "board_id": board_id,
        "item_name": item_name,
        "group_id": resolved_group_id,
        "column_values": column_values_json,
    }

    try:
        data = await _monday_request(query, vars)
        latency_ms = int((time.time() - start_time) * 1000)

        if "errors" in data:
            error_msg = data["errors"][0]["message"]
            logger.error(json.dumps({
                "event": "create_inventory_booking",
                "agent": "system",
                "service": "monday_server",
                "tool": "create_inventory_booking",
                "status": "graphql_error",
                "error": error_msg,
                "board_id": board_id,
                "group_id": resolved_group_id,
                "column_values_sent": column_values_json,
                "latency_ms": latency_ms,
            }))
            if _booking_lock_ref is not None:
                try:
                    _booking_lock_ref.delete()
                    logger.warning(json.dumps({
                        "event": "create_inventory_booking",
                        "phase": "booking_lock_released_on_error",
                        "service": "monday_server",
                        "lock_key": _booking_lock_key,
                    }))
                except Exception as _le:
                    logger.warning(json.dumps({
                        "event": "create_inventory_booking",
                        "phase": "booking_lock_release_failed",
                        "service": "monday_server",
                        "lock_key": _booking_lock_key,
                        "error": str(_le),
                    }))
            return _error_response(tool="create_inventory_booking", error_code="graphql_error", message=error_msg, latency_ms=latency_ms)

        new_id = data["data"]["create_item"]["id"]

        # Write confirmed item_id back to the idempotency lock so future duplicates
        # return the real existing item id instead of None.
        if _booking_lock_ref is not None:
            for _wb_attempt in range(3):
                try:
                    _booking_lock_ref.update({"item_id": new_id})
                    break
                except Exception as _we:
                    if _wb_attempt == 2:
                        logger.critical(json.dumps({
                            "event": "create_inventory_booking",
                            "phase": "booking_lock_writeback_failed_all_retries",
                            "service": "monday_server",
                            "item_id": new_id,
                            "lock_key": _booking_lock_key,
                            "error": str(_we),
                        }))
                    else:
                        await asyncio.sleep(0.5)

        # Durable thread->item resolver: write/merge thread_bookings/{thread_id} so a
        # later reschedule to a DIFFERENT date can still be resolved back to this same
        # item (the task_locks lock above is keyed by date and blind to that case).
        # created_at reuses the same _now computed for the lock doc Ã¢â‚¬â€ one source of
        # truth for both, ready for the 2-hour-window feature building on this later.
        if thread_id:
            _tb_now = _now or datetime.now(timezone.utc)
            try:
                _fs_tb = _get_booking_firestore()
                _fs_tb.collection(_THREAD_BOOKINGS_COLLECTION).document(thread_id).set({
                    "item_id": new_id,
                    "board_id": board_id,
                    "booking_date": date_only,
                    "created_at": _tb_now,
                    "updated_at": _tb_now,
                }, merge=True)
            except Exception as _tbe:
                _invalidate_booking_firestore()
                logger.warning(json.dumps({
                    "event": "create_inventory_booking",
                    "phase": "thread_bookings_write_failed",
                    "service": "monday_server",
                    "thread_id": thread_id,
                    "item_id": new_id,
                    "error": str(_tbe),
                }))

        logger.info(json.dumps({
            "event": "create_inventory_booking",
            "agent": "system",
            "service": "monday_server",
            "tool": "create_inventory_booking",
            "status": "success",
            "board_id": board_id,
            "group_id": resolved_group_id,
            "item_id": new_id,
            "item_name": item_name,
            "booking_date": booking_date,
            "latency_ms": latency_ms,
        }))

        # 'customer' is a required column mis-mapped onto the item-Name column (or
        # unmapped) counts as a blank field even though a real (wrong) column id may
        # have been populated Ã¢â‚¬â€ surface it so the name gets backfilled and the config
        # gets fixed rather than the caller trusting a false "success".
        _missing_report = list(_missing)
        for _n in _unmapped_required:
            if _n not in _missing_report:
                _missing_report.append(_n)
        if _missing_report:
            return json.dumps({
                "status": "partial_success",
                "item_id": new_id,
                "group_id": resolved_group_id,
                "message": f"Created booking '{item_name}' (ID: {new_id}) but these fields are blank: {_missing_report}. Call update_multiple_columns with the item_id to fill them.",
                "missing_fields": _missing_report,
                "hour_interval": _hour_interval,
                "dropped_labels": _dropped_labels,
                "source_message_id": source_message_id,
            })

        return json.dumps({
            "status": "success",
            "item_id": new_id,
            "group_id": resolved_group_id,
            "message": f"Created inventory booking '{item_name}' (ID: {new_id}) in group {resolved_group_id}",
            "hour_interval": _hour_interval,
            "dropped_labels": _dropped_labels,
            "source_message_id": source_message_id,
        })
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "create_inventory_booking",
            "agent": "system",
            "service": "monday_server",
            "tool": "create_inventory_booking",
            "status": "error",
            "error": str(e),
            "board_id": board_id,
            "latency_ms": latency_ms,
        }))
        if _booking_lock_ref is not None:
            try:
                _booking_lock_ref.delete()
                logger.warning(json.dumps({
                    "event": "create_inventory_booking",
                    "phase": "booking_lock_released_on_error",
                    "service": "monday_server",
                    "lock_key": _booking_lock_key,
                }))
            except Exception as _le:
                logger.warning(json.dumps({
                    "event": "create_inventory_booking",
                    "phase": "booking_lock_release_failed",
                    "service": "monday_server",
                    "lock_key": _booking_lock_key,
                    "error": str(_le),
                }))
        return _error_response(tool="create_inventory_booking", error_code="api_error", message=str(e), latency_ms=latency_ms)


@mcp.tool()
async def amend_booking(
    board_id: int | str,
    thread_id: str,
    updates: dict | None = None,
    report_status: Optional[str] = None,
) -> dict:
    """Amends the existing Monday.com booking item for a Gmail thread instead of
    creating a duplicate (e.g. when a client reschedules to a different date).

    Resolves the item via resolve_thread_item(thread_id) Ã¢â‚¬â€ a durable, date-independent
    thread->item mapping. If no item is found for this thread, returns an error rather
    than falling through to create a new one (use create_inventory_booking for that).

    Args:
        board_id: Monday board ID (e.g. 18392817962 for inventories, 18397426458 for contractors)
        thread_id: Gmail thread ID used to resolve the existing booking item
        updates: Logical field name -> new value, e.g. {"date": "2026-07-08", "hour": "14:00"}.
            Resolved to real column ids via the same board config mapping create_inventory_booking
            uses. Any key that doesn't resolve to a configured column is skipped, not an error.
        report_status: Report status label to set, canonicalized against the board's live
            labels (e.g. "Future report", "Not confirmed").

    Returns:
        {"status": "success", "item_id": ..., "updated_fields": [...], "report_status": "..."}
        or {"status": "error", "message": "..."}.
    """
    start_time = time.time()

    if not thread_id:
        return {"status": "error", "message": "thread_id is required to resolve the existing booking item"}

    board_id = _ensure_id(board_id)

    resolved = resolve_thread_item(thread_id)
    if resolved is None:
        logger.warning(json.dumps({
            "event": "amend_booking",
            "service": "monday_server",
            "phase": "no_existing_item",
            "thread_id": thread_id,
            "board_id": board_id,
        }))
        return {
            "status": "error",
            "message": f"No existing booking found for thread {thread_id} Ã¢â‚¬â€ cannot amend a non-existent item.",
        }

    item_id = resolved.get("item_id")

    board_key = _board_key_for_id(board_id)
    if not board_key:
        logger.warning(json.dumps({
            "event": "amend_booking",
            "service": "monday_server",
            "phase": "board_key_unresolved",
            "board_id": board_id,
            "thread_id": thread_id,
        }))
        return {
            "status": "error",
            "message": f"Board {board_id} is not a recognised configured board (inventories/contractors).",
        }

    def _col(name: str) -> Optional[str]:
        return get_column_id(board_key, name)

    # Status/color columns need Monday's {"label": ...} value AND a label that exists
    # on the live board Ã¢â‚¬â€ writing a raw string (e.g. job_status="Booked") is rejected
    # with "This status label doesn't exist" and the WHOLE amend fails, taking any
    # access/notes in the same call down with it (observed on contractors confirmations).
    _STATUS_LOGICAL_FIELDS = {
        "report_status", "report_type", "payment_status", "payment_reminder",
        "Acme_confirmation", "job_status", "reason_delay",
    }
    column_values: dict[str, object] = {}
    _skipped_fields: list[str] = []
    for _field, _value in (updates or {}).items():
        _cid = _col(_field)
        if not _cid:
            _skipped_fields.append(_field)
            continue
        if _field == "date":
            _dval = str(_value)
            column_values[_cid] = {"date": _dval.split("T")[0] if "T" in _dval else _dval}
        elif _field == "hour":
            # Same 30-minute interval normalization as create_inventory_booking
            # ("09:00 - 09:30"). Fall back to the raw value if unparseable.
            _hi = _format_hour_interval(_value)
            column_values[_cid] = _hi if _hi else _value
        elif _field in _STATUS_LOGICAL_FIELDS:
            _lbl = await _canonicalize_status_label(board_id, _cid, str(_value))
            column_values[_cid] = {"label": _lbl}
        elif _field in ("notes", "access_info", "access_instructions"):
            # notes and access_info/access_instructions share the same physical
            # column on the inventories board (access___info) Ã¢â‚¬â€ merge instead of
            # clobbering when a single amend_booking call updates more than one
            # of these logical fields (see _merge_text_column).
            _merge_text_column(column_values, _cid, str(_value))
        else:
            column_values[_cid] = _value

    if _skipped_fields:
        logger.info(json.dumps({
            "event": "amend_booking",
            "service": "monday_server",
            "phase": "unmapped_fields_skipped",
            "board_id": board_id,
            "thread_id": thread_id,
            "skipped_fields": _skipped_fields,
        }))

    # Only touch the status column when the caller explicitly passed report_status.
    # Previously this defaulted to "Not confirmed" and wrote it on EVERY amend, so a
    # plain detail/access update silently reset a booking that had already been
    # confirmed ("Future report") back to provisional.
    _canonical_status = None
    _status_col = _col("report_status")
    if report_status is not None and _status_col:
        _canonical_status = await _canonicalize_status_label(board_id, _status_col, report_status)
        column_values[_status_col] = {"label": _canonical_status}

    try:
        result_json = await update_multiple_columns(board_id, item_id, json.dumps(column_values))
        result = json.loads(result_json)
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "amend_booking",
            "service": "monday_server",
            "tool": "amend_booking",
            "status": "error",
            "error": str(e),
            "board_id": board_id,
            "item_id": item_id,
            "latency_ms": latency_ms,
        }))
        return {"status": "error", "message": str(e)[:200]}

    if result.get("status") != "success":
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "amend_booking",
            "service": "monday_server",
            "tool": "amend_booking",
            "status": "update_failed",
            "board_id": board_id,
            "item_id": item_id,
            "monday_result": result,
            "latency_ms": latency_ms,
        }))
        return {"status": "error", "message": result.get("message", "Failed to update Monday item")}

    # Keep thread_bookings in sync Ã¢â‚¬â€ updated_at always; booking_date only when the
    # caller actually changed the date.
    try:
        db = _get_booking_firestore()
        _tb_update: dict[str, object] = {"updated_at": datetime.now(timezone.utc)}
        if updates and updates.get("date"):
            _dval = str(updates["date"])
            _tb_update["booking_date"] = _dval.split("T")[0] if "T" in _dval else _dval
        db.collection(_THREAD_BOOKINGS_COLLECTION).document(thread_id).set(_tb_update, merge=True)
    except Exception as _tbe:
        _invalidate_booking_firestore()
        logger.warning(json.dumps({
            "event": "amend_booking",
            "service": "monday_server",
            "phase": "thread_bookings_update_failed",
            "thread_id": thread_id,
            "item_id": item_id,
            "error": str(_tbe),
        }))

    latency_ms = int((time.time() - start_time) * 1000)
    logger.info(json.dumps({
        "event": "amend_booking",
        "agent": "system",
        "service": "monday_server",
        "tool": "amend_booking",
        "status": "success",
        "board_id": board_id,
        "item_id": item_id,
        "thread_id": thread_id,
        "updated_fields": list(column_values.keys()),
        "report_status": _canonical_status,
        "latency_ms": latency_ms,
    }))

    return {
        "status": "success",
        "item_id": item_id,
        "updated_fields": list(column_values.keys()),
        "report_status": _canonical_status,
    }


@mcp.tool()
async def get_working_schedule(limit: int = 50) -> str:
    """Gets items from the Working Schedule board for inventory clerks.

    Board: "UPDATED Clerk Availability" (id 9529643731). Resolved at runtime via
    get_board_id("working_schedule") Ã¢â‚¬â€ not hardcoded here.

    Structure: each item (clerk) has an "Employment Type" status column with labels
    Full Time, Part time (only 3 days), Freelance, 0hrs Ã¢â‚¬â€ plus one text column per
    day of the week (Monday through Sunday) holding that day's availability as free
    text (e.g. hours worked, or "off").

    Returns a JSON object with status and a list of items, each containing the clerk
    name and column values (id + text) for the Employment Type column and each day
    column. Default limit lowered to 50 (from 200) to cap token cost; pass higher if
    needed. Raw JSON 'value' field omitted Ã¢â‚¬â€ agents use 'text'.
    Use this tool when checking clerk availability or building the next-day schedule.
    """
    start_time = time.time()
    board_id = get_board_id("working_schedule")
    query = """
    query ($board_id: ID!, $limit: Int) {
        boards (ids: [$board_id]) {
            items_page (limit: $limit) {
                items {
                    id
                    name
                    column_values {
                        id
                        text
                    }
                }
            }
        }
    }
    """
    try:
        data = await _monday_request(query, {"board_id": board_id, "limit": limit})
        latency_ms = int((time.time() - start_time) * 1000)

        if "errors" in data:
            error_msg = data["errors"][0]["message"]
            logger.error(json.dumps({
                "event": "get_working_schedule",
                "agent": "system",
                "service": "monday_server",
                "tool": "get_working_schedule",
                "status": "graphql_error",
                "error": error_msg,
                "board_id": board_id,
                "latency_ms": latency_ms
            }))
            return _error_response(tool="get_working_schedule", error_code="graphql_error", message=error_msg, latency_ms=latency_ms)

        boards = data.get("data", {}).get("boards", [])
        if not boards:
            logger.info(json.dumps({
                "event": "get_working_schedule",
                "agent": "system",
                "service": "monday_server",
                "tool": "get_working_schedule",
                "status": "not_found",
                "board_id": board_id,
                "latency_ms": latency_ms
            }))
            return json.dumps({"status": "success", "items": [], "message": "Working schedule board not found"})

        items = boards[0].get("items_page", {}).get("items", [])

        logger.info(json.dumps({
            "event": "get_working_schedule",
            "agent": "system",
            "service": "monday_server",
            "tool": "get_working_schedule",
            "status": "success",
            "board_id": board_id,
            "count": len(items),
            "latency_ms": latency_ms
        }))

        return _cap_tool_result(json.dumps({"status": "success", "items": items}))

    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "get_working_schedule",
            "agent": "system",
            "service": "monday_server",
            "tool": "get_working_schedule",
            "status": "error",
            "error": str(e),
            "board_id": board_id,
            "latency_ms": latency_ms
        }))
        return _error_response(tool="get_working_schedule", error_code="api_error", message=str(e), latency_ms=latency_ms)


@mcp.tool()
async def check_availability(date: str, service_type: str = "") -> str:
    """Compute deterministic availability for a given date.

    Queries the inventories board for existing bookings on that date, cross-references
    the working schedule to determine which clerks work that day, then returns
    remaining capacity per clerk.

    ALWAYS use this tool for client availability questions â€” never manually query
    boards and attempt to count slots yourself.

    Args:
        date: Target date in YYYY-MM-DD format (required). Resolve relative dates
              ("tomorrow", "next Friday") to YYYY-MM-DD before calling.
        service_type: Optional service type hint ("inventory", "check-in", "checkout",
                      "epc", etc.) â€” currently unused but reserved for future routing.

    Returns:
        JSON with availability summary and per-clerk breakdown.
    """
    start_time = time.time()

    # Validate date
    try:
        target_date = datetime.strptime(date.strip(), "%Y-%m-%d").date()
    except ValueError:
        return _error_response(
            tool="check_availability", error_code="invalid_date",
            message=f"date must be YYYY-MM-DD, got: {date!r}"
        )

    today = datetime.now().date()
    if target_date < today:
        return _error_response(
            tool="check_availability", error_code="past_date",
            message=f"Cannot check availability for past date {date}"
        )
    if (target_date - today).days > 90:
        return _error_response(
            tool="check_availability", error_code="too_far_ahead",
            message=f"Date {date} is more than 90 days out â€” too far to reliably check"
        )

    max_jobs = int(os.getenv("CLERK_MAX_DAILY_JOBS", "4"))
    day_name = calendar.day_name[target_date.weekday()]  # e.g. "Monday"

    # --- Step 1: Query inventories board for bookings on this date ---
    inv_board_id = get_board_id("inventories")
    report_status_col = get_column_id("inventories", "report_status") or "report_status"
    date_col = get_column_id("inventories", "date") or "date"
    bookings_query = f"""
    query ($board_id: ID!, $columns: [ItemsPageByColumnValuesQuery!]!) {{
        items_page_by_column_values (board_id: $board_id, columns: $columns, limit: 200) {{
            items {{
                id
                name
                column_values (ids: ["inspector", "dup__of_hour0__1", "{report_status_col}"]) {{
                    id
                    text
                }}
            }}
        }}
    }}
    """
    try:
        bookings_data = await _monday_request(bookings_query, {
            "board_id": inv_board_id,
            "columns": [{"column_id": date_col, "column_values": [date]}],
        })
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        return _error_response(
            tool="check_availability", error_code="api_error",
            message=f"Bookings query failed: {str(e)[:150]}", latency_ms=latency_ms
        )

    booked_items = (
        bookings_data.get("data", {})
        .get("items_page_by_column_values", {})
        .get("items", [])
    )

    # Count bookings per clerk and collect time slots
    bookings_per_clerk: dict[str, int] = {}
    booked_times: list[str] = []
    for item in booked_items:
        col_map = {cv["id"]: (cv.get("text") or "") for cv in item.get("column_values", [])}
        # Skip cancelled bookings (report_status = "Cancelled")
        if col_map.get(report_status_col, "").strip().lower() == "cancelled":
            continue
        inspector = col_map.get("inspector", "").strip()
        if inspector:
            bookings_per_clerk[inspector] = bookings_per_clerk.get(inspector, 0) + 1
        hour_text = col_map.get("dup__of_hour0__1", "").strip()
        if hour_text:
            booked_times.append(hour_text)

    # --- Step 2: Query working schedule for clerk day-of-week availability ---
    ws_board_id = get_board_id("working_schedule")
    schedule_query = """
    query ($board_id: ID!) {
        boards (ids: [$board_id]) {
            items_page (limit: 50) {
                items {
                    id
                    name
                    column_values {
                        id
                        text
                        column {
                            title
                        }
                    }
                }
            }
        }
    }
    """
    try:
        schedule_data = await _monday_request(schedule_query, {"board_id": ws_board_id})
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        return _error_response(
            tool="check_availability", error_code="api_error",
            message=f"Schedule query failed: {str(e)[:150]}", latency_ms=latency_ms
        )

    schedule_items = (
        schedule_data.get("data", {})
        .get("boards", [{}])[0]
        .get("items_page", {})
        .get("items", [])
    )

    # Determine per-clerk availability for the target day
    clerk_results = []
    unavailable_texts = {"", "n/a", "0", "off", "none", "no", "unavailable", "need to request availability"}
    for item in schedule_items:
        clerk_name = item["name"].strip()
        day_col_text = ""
        for cv in item.get("column_values", []):
            col_title = (cv.get("column", {}).get("title") or cv.get("title") or "").strip().lower()
            if col_title == day_name.lower():
                day_col_text = (cv.get("text") or "").strip()
                break

        available = day_col_text.lower() not in unavailable_texts
        booked = bookings_per_clerk.get(clerk_name, 0)
        remaining = max(0, max_jobs - booked) if available else 0

        clerk_results.append({
            "name": clerk_name,
            "available": available,
            "hours": day_col_text if available else None,
            "booked_jobs": booked,
            "remaining_capacity": remaining,
        })

    available_clerks = [c for c in clerk_results if c["available"]]
    total_remaining = sum(c["remaining_capacity"] for c in available_clerks)
    has_availability = total_remaining > 0

    if not has_availability and schedule_items:
        logger.warning(json.dumps({
            "event": "check_availability_no_capacity",
            "date": date,
            "day_of_week": day_name,
            "schedule_items_count": len(schedule_items),
            "available_clerks_count": len(available_clerks),
            "clerk_breakdown": [
                {"name": c["name"], "available": c["available"], "booked": c["booked_jobs"], "remaining": c["remaining_capacity"]}
                for c in clerk_results
            ],
        }))

    latency_ms = int((time.time() - start_time) * 1000)
    logger.info(json.dumps({
        "event": "check_availability",
        "agent": "system",
        "service": "monday_server",
        "tool": "check_availability",
        "status": "success",
        "date": date,
        "day_of_week": day_name,
        "has_availability": has_availability,
        "total_remaining": total_remaining,
        "total_bookings": len(booked_items),
        "latency_ms": latency_ms,
    }))

    return json.dumps({
        "status": "success",
        "date": date,
        "day_of_week": day_name,
        "message": (
            f"{total_remaining} slot(s) available across {len(available_clerks)} clerk(s) on {day_name} {date}."
            if has_availability
            else f"No availability on {day_name} {date}. All {len(clerk_results)} clerks are fully booked or unavailable."
        ),
        "has_availability": has_availability,
        "total_remaining_capacity": total_remaining,
        "total_available_clerks": len(available_clerks),
        "total_booked_jobs": sum(c["booked_jobs"] for c in clerk_results),
        "booked_time_slots": sorted(set(booked_times)),
    })


@mcp.tool()
async def create_lead(
    board_id: int | str,
    item_name: str,
    email: Optional[str] = None,
    phone: Optional[str] = None,
    service_interest: Optional[str] = None,
    address: Optional[str] = None,
    notes: Optional[str] = None,
    company_name: Optional[str] = None,
    source_message_id: Optional[str] = None,
    customer_name: Optional[str] = None,
) -> str:
    """Creates a lead item for an incomplete or abandoned booking enquiry.

    Adds the contact to group_mm2q9amr (Leads group) on the given board
    with no status set, so human admin can follow up.

    Args:
        board_id: Monday board ID (use 18392817962 for inventories)
        item_name: Ignored Ã¢â‚¬â€ server always forces the item name to 'AI Assisted' (prevents
            customer/address names leaking into the item name column).
        email: Client email address
        phone: Client phone number
        service_interest: Service they enquired about (e.g. "Check-in", "Inventory")
        address: Property address mentioned (if any)
        notes: Any other relevant context from the conversation
        customer_name: Client full name Ã¢â‚¬â€ written to the Customer column
    """
    if not item_name or not item_name.strip():
        return json.dumps({"status": "error", "error": "Lead name is required"})

    board_id = _ensure_id(board_id)
    item_name = "AI Assisted"
    start_time = time.time()
    group_id = get_group_id("inventories", "leads")
    if not group_id:
        return json.dumps({
            "status": "error",
            "error": "Leads group not configured Ã¢â‚¬â€ set MONDAY_LEADS_GROUP_ID or configure in the dashboard",
        })

    column_values: dict[str, object] = {}
    _missing_cols: list[str] = []

    if email:
        _ecol = get_column_id("inventories", "email")
        if _ecol:
            column_values[_ecol] = {"email": email, "text": email}
        else:
            _missing_cols.append("email")
    if phone:
        _pcol = get_column_id("inventories", "phone")
        if _pcol:
            column_values[_pcol] = phone
        else:
            _missing_cols.append("phone")
    if address:
        _acol = get_column_id("inventories", "address")
        if _acol:
            column_values[_acol] = address
        else:
            _missing_cols.append("address")
    if notes:
        _ncol = get_column_id("inventories", "notes")
        if _ncol:
            column_values[_ncol] = notes
        else:
            _missing_cols.append("notes")
    if service_interest:
        _rtcol = get_column_id("inventories", "report_type")
        if _rtcol:
            column_values[_rtcol] = {"label": _resolve_service_type(service_interest) or service_interest}
        else:
            _missing_cols.append("report_type")
    if company_name:
        _ccol = get_column_id("inventories", "company_name")
        if _ccol:
            column_values[_ccol] = company_name
        else:
            _missing_cols.append("company_name")
    if customer_name:
        _custcol = get_column_id("inventories", "customer")
        if _custcol and _custcol != "name":
            column_values[_custcol] = customer_name
        else:
            _missing_cols.append("customer")

    if _missing_cols:
        logger.warning(json.dumps({
            "event": "create_lead",
            "service": SERVICE_NAME,
            "message": f"Column IDs not mapped, data dropped: {_missing_cols}",
            "missing_columns": _missing_cols,
        }))

    if not column_values and (email or phone or address or notes or service_interest):
        return json.dumps({
            "status": "error",
            "error": f"No column IDs mapped for lead fields {_missing_cols} Ã¢â‚¬â€ configure Monday column IDs in the dashboard",
        })

    column_values_json = json.dumps(column_values) if column_values else None

    # ---- IDEMPOTENCY PRE-CHECK ----
    # Check if a matching lead already exists to prevent duplicates
    # when the same enquiry email is re-processed (e.g., every 5 min polling)
    try:
        # Normalise for case-insensitive compare; Monday email columns match on display text
        email_norm = (email or "").strip().lower() if email else None
        phone_norm = "".join(ch for ch in (phone or "") if ch.isdigit()) if phone else None

        if email_norm:
            # Primary path: query by email
            email_col_id = get_column_id("inventories", "email")
            if email_col_id:
                dedup_query = """
                query ($board_id: ID!, $columns: [ItemsPageByColumnValuesQuery!]) {
                    items_page_by_column_values(limit: 50, board_id: $board_id, columns: $columns) {
                        items { id name group { id } }
                    }
                }
                """
                dedup_vars = {
                    "board_id": board_id,
                    "columns": [
                        {"column_id": email_col_id, "column_values": [email_norm]},
                    ]
                }
                data = await _monday_request(dedup_query, dedup_vars)
                if "errors" not in data:
                    items = data.get("data", {}).get("items_page_by_column_values", {}).get("items", [])
                    for item in items:
                        if not item.get("group"):
                            logger.warning(json.dumps({"event": "create_lead", "phase": "missing_group_field",
                                "service": "monday_server", "item_id": item.get("id")}))
                            continue
                        if item["group"].get("id") == group_id and item.get("name", "").strip().lower() == item_name.strip().lower():
                            existing_id = item["id"]
                            logger.info(json.dumps({
                                "event": "create_lead",
                                "phase": "skipped_duplicate",
                                "service": "monday_server",
                                "board_id": board_id,
                                "existing_item_id": existing_id,
                                "item_name": item_name,
                            }))
                            return json.dumps({
                                "status": "skipped_duplicate",
                                "item_id": existing_id,
                                "message": "Lead already exists",
                                "source_message_id": source_message_id,
                            })
        elif phone_norm:
            # Secondary path: query by phone (digits only)
            phone_col_id = get_column_id("inventories", "phone")
            if phone_col_id:
                dedup_query = """
                query ($board_id: ID!, $columns: [ItemsPageByColumnValuesQuery!]) {
                    items_page_by_column_values(limit: 50, board_id: $board_id, columns: $columns) {
                        items { id name group { id } }
                    }
                }
                """
                dedup_vars = {
                    "board_id": board_id,
                    "columns": [
                        {"column_id": phone_col_id, "column_values": [phone_norm]},
                    ]
                }
                data = await _monday_request(dedup_query, dedup_vars)
                if "errors" not in data:
                    items = data.get("data", {}).get("items_page_by_column_values", {}).get("items", [])
                    for item in items:
                        if not item.get("group"):
                            logger.warning(json.dumps({"event": "create_lead", "phase": "missing_group_field",
                                "service": "monday_server", "item_id": item.get("id")}))
                            continue
                        if item["group"].get("id") == group_id and item.get("name", "").strip().lower() == item_name.strip().lower():
                            existing_id = item["id"]
                            logger.info(json.dumps({
                                "event": "create_lead",
                                "phase": "skipped_duplicate",
                                "service": "monday_server",
                                "board_id": board_id,
                                "existing_item_id": existing_id,
                                "item_name": item_name,
                            }))
                            return json.dumps({
                                "status": "skipped_duplicate",
                                "item_id": existing_id,
                                "message": "Lead already exists",
                                "source_message_id": source_message_id,
                            })
        else:
            # Fallback path: query leads group directly by name only
            # Name-only fallback: limited to first 100; ensure email/phone provided upstream for large groups
            dedup_query = """
            query ($board_id: ID!, $group_id: String!) {
                boards(ids: [$board_id]) {
                    groups(ids: [$group_id]) {
                        items_page(limit: 100) {
                            items { id name }
                        }
                    }
                }
            }
            """
            dedup_vars = {
                "board_id": board_id,
                "group_id": group_id,
            }
            data = await _monday_request(dedup_query, dedup_vars)
            if "errors" not in data:
                items = data.get("data", {}).get("boards", [{}])[0].get("groups", [{}])[0].get("items_page", {}).get("items", [])
                for item in items:
                    if item.get("name", "").strip().lower() == item_name.strip().lower():
                        existing_id = item["id"]
                        logger.info(json.dumps({
                            "event": "create_lead",
                            "phase": "skipped_duplicate",
                            "service": "monday_server",
                            "board_id": board_id,
                            "existing_item_id": existing_id,
                            "item_name": item_name,
                        }))
                        return json.dumps({
                            "status": "skipped_duplicate",
                            "item_id": existing_id,
                            "message": "Lead already exists",
                            "source_message_id": source_message_id,
                        })
    except Exception as e:
        # Fail open: dedup check failed, proceed with lead creation
        logger.warning(json.dumps({
            "event": "create_lead",
            "phase": "dedup_check_failed",
            "service": "monday_server",
            "board_id": board_id,
            "error": str(e),
        }))

    query = """
    mutation ($board_id: ID!, $item_name: String!, $group_id: String!, $column_values: JSON) {
        create_item (board_id: $board_id, item_name: $item_name, group_id: $group_id, column_values: $column_values) {
            id
        }
    }
    """
    vars = {
        "board_id": board_id,
        "item_name": item_name,
        "group_id": group_id,
        "column_values": column_values_json,
    }

    try:
        data = await _monday_request(query, vars)
        latency_ms = int((time.time() - start_time) * 1000)

        if "errors" in data:
            error_msg = data["errors"][0]["message"]
            logger.error(json.dumps({
                "event": "create_lead",
                "agent": "system",
                "service": "monday_server",
                "tool": "create_lead",
                "status": "graphql_error",
                "error": error_msg,
                "board_id": board_id,
                "latency_ms": latency_ms,
            }))
            return _error_response(tool="create_lead", error_code="graphql_error", message=error_msg, latency_ms=latency_ms)

        new_id = data["data"]["create_item"]["id"]
        logger.info(json.dumps({
            "event": "create_lead",
            "agent": "system",
            "service": "monday_server",
            "tool": "create_lead",
            "status": "success",
            "board_id": board_id,
            "group_id": group_id,
            "item_id": new_id,
            "item_name": item_name,
            "latency_ms": latency_ms,
        }))
        return json.dumps({
            "status": "success",
            "item_id": new_id,
            "message": f"Created lead '{item_name}' (ID: {new_id}) for human follow-up",
            "source_message_id": source_message_id,
        })
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "create_lead",
            "agent": "system",
            "service": "monday_server",
            "tool": "create_lead",
            "status": "error",
            "error": str(e),
            "board_id": board_id,
            "latency_ms": latency_ms,
        }))
        return _error_response(tool="create_lead", error_code="api_error", message=str(e), latency_ms=latency_ms)

@mcp.tool()
async def reload_monday_config() -> str:
    """Invalidates the Monday.com config cache so the next call re-reads from Firestore.

    Call this after updating Monday settings in the dashboard to apply changes
    without restarting the service.
    """
    invalidate_cache()
    logger.info(json.dumps({
        "event": "reload_monday_config",
        "agent": "system",
        "service": "monday_server",
        "status": "cache_invalidated",
    }))
    return json.dumps({"status": "success", "message": "Monday config cache cleared Ã¢â‚¬â€ next request re-reads from Firestore"})


@mcp.tool()
async def record_pending_invoice(
    item_id: str,
    thread_id: str,
    amount: float,
    currency: str,
    vat_applies: bool,
    customer_email: str,
    customer_name: str,
    line_item_description: str = "",
    booking_date: str | None = None,
) -> dict:
    """Records a pending invoice in Firestore for later Xero creation.

    Validates amount, email, and customer name before writing to pending_invoices collection.
    Returns {"status": "success"} or {"status": "error", "reason": "..."} or
    {"status": "hold_review", "reason": "amount_exceeds_ceiling"}.
    """
    # Validate amount > 0
    if amount <= 0:
        return {"status": "error", "reason": "amount_must_be_positive"}

    # Validate amount <= ceiling
    max_amount = float(os.getenv("INVOICE_MAX_GBP", "2500"))
    if amount > max_amount:
        return {"status": "hold_review", "reason": "amount_exceeds_ceiling"}

    # Validate email with basic regex
    email_pattern = r'^[^@\s]+@[^@\s]+\.[^@\s]+$'
    if not re.match(email_pattern, customer_email):
        return {"status": "error", "reason": "invalid_email"}

    # Validate customer name
    if not customer_name or len(customer_name.strip()) == 0:
        return {"status": "error", "reason": "customer_name_required"}

    try:
        db = _get_booking_firestore()
        ref = db.collection("pending_invoices").document(str(item_id))
        ref.set({
            "item_id": item_id,
            "thread_id": thread_id,
            "amount": amount,
            "currency": currency,
            "vat_applies": vat_applies,
            "customer_email": customer_email,
            "customer_name": customer_name,
            "line_item_description": line_item_description,
            "booking_date": booking_date,
            "status": "pending",
            "created_at": time.time(),
        })
        return {"status": "success", "item_id": item_id}
    except Exception as e:
        _invalidate_booking_firestore()
        return {"status": "error", "reason": "firestore_error", "detail": str(e)}


@mcp.tool()
async def set_invoice_number(
    board_id: str,
    item_id: str,
    invoice_number: str,
    pay_link: Optional[str] = None,
    amount: Optional[float] = None,
    xero_status: str = "DRAFT",
    invoice_id: Optional[str] = None,
) -> dict:
    """Sets the invoice_number column and payment_status for a Monday item.

    xero_status: The Xero invoice status returned by create_invoice ("DRAFT" or "AUTHORISED").
        "DRAFT"      -> sets payment_status to "Invoice drafted" (admin will send later)
        "AUTHORISED" -> sets payment_status to "Invoice sent by xero" (already emailed to client)
        Defaults to "DRAFT" (safe default -- shows "Invoice drafted" if omitted).

    invoice_id: Xero invoice UUID (the `invoice_id` field returned by create_invoice).
        When provided alongside xero_status=â€DRAFTâ€, writes a Firestore mapping doc to
        xero_invoice_map/{invoice_id} so the /webhook/xero endpoint can find this Monday
        item and flip the status when admin later sends the invoice in Xero.

    pay_link: Optional Xero online-invoice URL (the `online_url` field returned by
        create_invoice) to write to the Pay Link text column. Best-effort Ã¢â‚¬â€ if the
        column isn't mapped for this board, it's silently skipped (invoice_number
        and payment_status still get written).

    amount: Optional ex-VAT amount to write to the price column. When provided, also
        zeros the Surcharge and Discount% companion columns on the inventories board
        so the Revenue formula evaluates correctly (same pattern as create_inventory_booking).

    Returns {â€œstatusâ€: â€œsuccessâ€, â€œitem_idâ€: item_id, â€œinvoice_numberâ€: invoice_number}
    or {â€œstatusâ€: â€œerrorâ€, â€œreasonâ€: â€œ...â€}.
    """
    try:
        board_id = _ensure_id(board_id)
        item_id = _ensure_id(item_id)

        # Resolve numeric board_id back to its config board_key Ã¢â‚¬" require_column_id/
        # get_column_id are keyed by "inventories"/"contractors", not the numeric id.
        board_key = _board_key_for_id(board_id)
        if not board_key:
            return {"status": "error", "reason": f"Unrecognized board_id {board_id} -- not mapped to a known board_key"}

        # Resolve column IDs
        inv_col_id = require_column_id(board_key, "inv_number")
        status_col_id = require_column_id(board_key, "payment_status")

        payment_label = "Invoice drafted" if xero_status == "DRAFT" else "Invoice sent by xero"

        # Build column values
        column_values = {
            inv_col_id: invoice_number,
            status_col_id: {"label": payment_label},
        }
        if pay_link:
            _pl_col = get_column_id(board_key, "pay_link")
            if _pl_col:
                column_values[_pl_col] = pay_link
        if amount is not None:
            price_col = get_column_id(board_key, "price")
            if price_col:
                column_values[price_col] = str(amount)
                if board_key == "inventories":
                    column_values["numeric_mm28j2pq"] = "0"  # Surcharge
                    column_values["numeric_mm28b3gs"] = "0"  # Discount%

        # Use update_multiple_columns mutation
        query = """
        mutation ($board_id: ID!, $item_id: ID!, $column_values: JSON!) {
            change_multiple_column_values (board_id: $board_id, item_id: $item_id, column_values: $column_values) {
                id
            }
        }
        """
        vars = {
            "board_id": board_id,
            "item_id": item_id,
            "column_values": json.dumps(column_values)
        }

        data = await _monday_request(query, vars)

        if "errors" in data:
            error_msg = data['errors'][0]['message']
            return {"status": "error", "reason": error_msg}

        # Write Xeroâ†’Monday mapping so the /webhook/xero endpoint can find this item
        # when admin later sends the drafted invoice in Xero.
        if xero_status == "DRAFT" and invoice_id:
            try:
                db = _get_booking_firestore()
                db.collection("xero_invoice_map").document(invoice_id).set({
                    "board_id": board_id,
                    "item_id": item_id,
                    "invoice_number": invoice_number,
                    "xero_status": "DRAFT",
                    "created_at": time.time(),
                })
            except Exception as _map_err:
                _invalidate_booking_firestore()
                import logging as _log
                _log.getLogger(__name__).warning(
                    f"xero_invoice_map write failed: invoice_id={invoice_id} err={_map_err}"
                )

        return {"status": "success", "item_id": item_id, "invoice_number": invoice_number}
    except Exception as e:
        return {"status": "error", "reason": str(e)}


@mcp.tool()
async def list_pending_invoices(limit: int = 50) -> str:
    """Query Firestore pending_invoices collection and return all docs with status='pending'."""
    try:
        db = _get_booking_firestore()
        docs = db.collection("pending_invoices").where("status", "==", "pending").limit(limit).stream()
        results = []
        for doc in docs:
            data = doc.to_dict()
            results.append(data)
        return json.dumps({"status": "success", "pending_invoices": results, "count": len(results)})
    except Exception as e:
        _invalidate_booking_firestore()
        return json.dumps({"status": "error", "reason": str(e), "pending_invoices": []})


if __name__ == "__main__":
    # Run startup health check before starting the server
    import asyncio as _asyncio
    
    async def _run_with_health_check():
        health_ok = await _startup_health_check()
        if not health_ok:
            logger.error(json.dumps({
                "event": "server_startup",
                "agent": "system",
                "service": "monday_server",
                "status": "CRITICAL",
                "message": "Tool Unavailable - API key validation failed",
                "action_required": "Admin must verify MONDAY_API_KEY and restart service",
                "alert_channel": "cloud_monitoring"
                # TODO: Integrate with reporting_server.py or whatsapp_server.py to send
                # admin alert when health check fails. For now, rely on Cloud Monitoring
                # to detect CRITICAL logs and trigger alerts.
            }))
            # Continue anyway to allow MCP discovery, but tools will fail gracefully
        
    # Run health check synchronously before starting MCP
    _asyncio.run(_run_with_health_check())
    
    mcp.run()
