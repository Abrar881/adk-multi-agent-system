"""System state helper — global spend cap and kill-switch stored in Firestore.

Single source of truth: collection `system_state`, document `spend`.
Shared between the agent service (CostGuardPlugin) and the dashboard service,
which both reach the same Firestore project.

All writes use atomic Increment / merge-set so concurrent calls from multiple
Gunicorn threads can't race on a read-modify-write cycle.

Fail-open policy: if Firestore is temporarily unavailable the in-memory cache
is used; if no cache exists the system is allowed through (fail-open) and the
error is logged loudly. The billing-budget backstop (Layer 2) catches anything
that slips through a transient Firestore blip.
"""
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

from google.cloud import firestore as _firestore
from google.api_core.exceptions import AlreadyExists as _AlreadyExists

logger = logging.getLogger(__name__)

_COLLECTION = "system_state"
_DOC_ID = "spend"
_CACHE_TTL = 15.0  # seconds — short enough to catch a reapproval within ~15 s

_db: _firestore.Client | None = None
_db_lock = threading.Lock()

# In-memory cache: {"state": dict, "expires": float}
_cache: dict = {}
_cache_lock = threading.Lock()


def _get_db() -> _firestore.Client | None:
    global _db
    with _db_lock:
        if _db is None:
            try:
                _db = _firestore.Client()
            except Exception as exc:
                logger.error(json.dumps({
                    "event": "system_state_db_init_failed",
                    "error": str(exc)[:200],
                }))
        return _db


def _defaults() -> dict:
    cap = float(os.environ.get("SPEND_CAP_GBP", "20.0"))
    return {
        "enabled": True,
        "spend_gbp": 0.0,
        "cap_gbp": cap,
        "disabled_reason": "",
        "disabled_at": None,
        "reset_at": None,
    }


def get_state(force_refresh: bool = False) -> dict:
    """Return current system state dict.

    Cached for _CACHE_TTL seconds. On Firestore error returns last cached
    value (fail-open). If no cache and Firestore fails, returns enabled=True
    defaults (fail-open) and logs loudly.
    """
    now = time.monotonic()
    with _cache_lock:
        if not force_refresh and _cache.get("expires", 0) > now:
            return _cache["state"]

    db = _get_db()
    if db is None:
        _warn_fallback("no_db_client")
        return _cache.get("state") or _defaults()

    try:
        ref = db.collection(_COLLECTION).document(_DOC_ID)
        snap = ref.get()
        if snap.exists:
            state = snap.to_dict()
        else:
            # First run — create doc with defaults
            state = _defaults()
            ref.set(state)
        with _cache_lock:
            _cache["state"] = state
            _cache["expires"] = time.monotonic() + _CACHE_TTL
        return state
    except Exception as exc:
        _warn_fallback(str(exc)[:200])
        return _cache.get("state") or _defaults()


def ping() -> tuple[bool, str]:
    """Quick Firestore connectivity check. Returns (reachable, error_msg).

    Skips the fail-open cache so callers get a live read result.
    """
    db = _get_db()
    if db is None:
        return False, "no_db_client"
    try:
        db.collection(_COLLECTION).document(_DOC_ID).get()
        return True, ""
    except Exception as exc:
        return False, str(exc)[:200]


def add_spend(gbp: float) -> float:
    """Atomically add `gbp` to cumulative spend. Latches system off if cap crossed.

    Returns the new cumulative spend_gbp total (best-effort; may be slightly
    stale if the Increment read-back fails). Silently skips on zero/negative.
    """
    if gbp <= 0:
        return 0.0
    db = _get_db()
    if db is None:
        logger.error(json.dumps({"event": "system_state_add_spend_no_db", "gbp": gbp}))
        return 0.0

    try:
        ref = db.collection(_COLLECTION).document(_DOC_ID)
        ref.set({"spend_gbp": _firestore.Increment(gbp)}, merge=True)
        snap = ref.get()
        state = snap.to_dict() if snap.exists else _defaults()

        # Refresh cache
        with _cache_lock:
            _cache["state"] = state
            _cache["expires"] = time.monotonic() + _CACHE_TTL

        new_total = state.get("spend_gbp", 0.0)
        cap = state.get("cap_gbp", float(os.environ.get("SPEND_CAP_GBP", "20.0")))

        if new_total >= cap and state.get("enabled", True):
            _latch_off(ref, "spend_cap_exceeded", new_total, cap)

        return new_total
    except Exception as exc:
        logger.error(json.dumps({
            "event": "system_state_add_spend_failed",
            "gbp": gbp,
            "error": str(exc)[:200],
        }))
        try:
            from servers.alerting import alert_admin, HIGH
            alert_admin(HIGH, "spend_tracking_failed", f"add_spend({gbp:.4f} GBP) failed: {str(exc)[:200]}")
        except Exception as _alert_exc:
            logger.critical(json.dumps({
                "event": "spend_tracking_double_failure",
                "original_error": str(exc)[:200],
                "alert_error": str(_alert_exc)[:200],
                "gbp": gbp,
            }))
        return 0.0


_DAILY_SPEND_DOC_PREFIX = "daily_spend_"


def add_daily_spend(gbp: float) -> float:
    """Atomically add to today's global daily spend. Returns new daily total.

    Uses the same atomic Increment pattern as add_spend. Fail-open: returns 0.0
    on Firestore error so caller falls back to session-state tracking.
    """
    if gbp <= 0:
        return 0.0
    db = _get_db()
    if db is None:
        logger.error(json.dumps({
            "event": "daily_spend_no_db",
            "gbp": gbp,
        }))
        try:
            from servers.alerting import alert_admin, HIGH
            alert_admin(HIGH, "daily_spend_tracking_no_db",
                        f"add_daily_spend({gbp:.4f} GBP) skipped — Firestore unavailable")
        except Exception:
            pass
        return 0.0
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    doc_id = f"{_DAILY_SPEND_DOC_PREFIX}{today}"
    try:
        ref = db.collection(_COLLECTION).document(doc_id)
        ref.set({"spend_gbp": _firestore.Increment(gbp), "date": today}, merge=True)
        snap = ref.get()
        data = snap.to_dict() if snap.exists else {}
        return float(data.get("spend_gbp", 0.0))
    except Exception as exc:
        logger.error(json.dumps({
            "event": "daily_spend_update_failed",
            "error": str(exc)[:200],
        }))
        return 0.0


def get_daily_spend() -> float:
    """Read today's global daily spend across all instances. Fail-open: returns 0.0."""
    db = _get_db()
    if db is None:
        return 0.0
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        snap = db.collection(_COLLECTION).document(f"{_DAILY_SPEND_DOC_PREFIX}{today}").get()
        return float(snap.to_dict().get("spend_gbp", 0.0)) if snap.exists else 0.0
    except Exception as exc:
        logger.error(json.dumps({
            "event": "get_daily_spend_failed",
            "error": str(exc)[:200],
        }))
        return 0.0


def disable(reason: str) -> None:
    """Latch system off with given reason. Used by billing-alert webhook."""
    db = _get_db()
    if db is None:
        logger.error(json.dumps({"event": "system_state_disable_failed", "reason": "no_db"}))
        return
    try:
        ref = db.collection(_COLLECTION).document(_DOC_ID)
        _latch_off(ref, reason)
    except Exception as exc:
        logger.error(json.dumps({"event": "system_state_disable_error", "error": str(exc)[:200]}))


def reapprove() -> None:
    """Re-enable system and zero the spend counter. Called from dashboard."""
    db = _get_db()
    if db is None:
        raise RuntimeError("Firestore unavailable — cannot reapprove")
    ref = db.collection(_COLLECTION).document(_DOC_ID)
    updates = {
        "enabled": True,
        "spend_gbp": 0.0,
        "disabled_reason": "",
        "disabled_at": None,
        "reset_at": datetime.now(timezone.utc),
    }
    ref.set(updates, merge=True)
    with _cache_lock:
        _cache.clear()
    logger.info(json.dumps({"event": "system_state_reapproved"}))


def set_cap(cap_gbp: float) -> None:
    """Update the spend cap. Called from dashboard PUT /api/system-state."""
    if not (0 < cap_gbp <= 10_000):
        raise ValueError(f"cap_gbp must be between 0 and 10000, got {cap_gbp}")
    db = _get_db()
    if db is None:
        raise RuntimeError("Firestore unavailable — cannot update cap")
    ref = db.collection(_COLLECTION).document(_DOC_ID)
    ref.set({"cap_gbp": cap_gbp}, merge=True)
    with _cache_lock:
        _cache.clear()
    logger.info(json.dumps({"event": "system_state_cap_updated", "cap_gbp": cap_gbp}))


# ---------------------------------------------------------------------------
# Gmail push historyId dedup — separate doc `gmail_dedup` in the same
# collection. Fail-open: Firestore errors return True/no-op rather than
# raising, so a transient Firestore blip never blocks Gmail push handling.
#
# The check-and-set is done as a single Firestore transaction (rather than a
# separate read then write) so that two concurrent Pub/Sub redeliveries of
# the same or overlapping historyIds can't both read a stale last_history_id,
# both pass the check, and both proceed — Pub/Sub is at-least-once and does
# redeliver, so this race is not hypothetical.
# ---------------------------------------------------------------------------

_GMAIL_DEDUP_DOC_ID = "gmail_dedup"


@_firestore.transactional
def _claim_history_id_txn(transaction, ref, history_id: int) -> bool:
    snap = ref.get(transaction=transaction)
    last = snap.to_dict().get("last_history_id") if snap.exists else None
    if last is not None and history_id <= last:
        return False
    transaction.set(ref, {
        "last_history_id": history_id,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }, merge=True)
    return True


def try_claim_gmail_history_id(history_id: int) -> bool:
    """Atomically check-and-claim a Gmail historyId. Returns True if this call
    is the first to claim it (caller should proceed), False if a duplicate/stale
    historyId (caller should skip). Fails OPEN (returns True) if Firestore is
    unavailable — matches this file's existing fail-open philosophy elsewhere.

    Note: the claim happens before the caller's downstream event handling
    runs/succeeds, so a downstream failure does not "unclaim" the historyId —
    acceptable given this file's fail-open design, since any missed mail from
    a failed run is caught by the next scheduled sweep or the next push.
    """
    db = _get_db()
    if db is None:
        return True
    try:
        ref = db.collection(_COLLECTION).document(_GMAIL_DEDUP_DOC_ID)
        transaction = db.transaction()
        return _claim_history_id_txn(transaction, ref, history_id)
    except Exception as exc:
        logger.error(json.dumps({
            "event": "system_state_claim_history_id_failed",
            "error": str(exc)[:200],
        }))
        return True  # fail open — don't block real mail on a Firestore hiccup


def get_last_gmail_history_id() -> int | None:
    """Read-only lookup of the previously claimed Gmail historyId — used by
    server.py's gmail_push_handler as the `startHistoryId` for the self-mutation
    check (distinguishing a real incoming email from a push caused by the agent's
    own AI_PROCESSED/AI_BOOKED label write on a prior turn).

    Read before the claim, not inside the same transaction — a concurrent claim
    racing this read only widens or narrows the delta the caller inspects, it
    never causes a real email to be skipped (the self-mutation check itself is
    fail-open). Returns None (fail open — caller proceeds with the turn) if
    Firestore is unavailable or no historyId has been claimed yet.
    """
    db = _get_db()
    if db is None:
        return None
    try:
        ref = db.collection(_COLLECTION).document(_GMAIL_DEDUP_DOC_ID)
        snap = ref.get()
        if not snap.exists:
            return None
        return snap.to_dict().get("last_history_id")
    except Exception as exc:
        logger.error(json.dumps({
            "event": "system_state_get_last_history_id_failed",
            "error": str(exc)[:200],
        }))
        return None  # fail open — caller treats None as "can't determine"


# ---------------------------------------------------------------------------
# Monday webhook per-item cooldown — prevents rapid repeated edits to the same
# item (e.g. quick status corrections) from each firing a full dispatcher LLM
# run. One Firestore document holds a map of item_id → last_seen ISO timestamp.
# The document grows at most to the number of distinct Monday items (~hundreds),
# bounded in practice — no TTL sweep needed.
# ---------------------------------------------------------------------------

_MONDAY_COOLDOWN_DOC_ID = "monday_webhook_cooldown"
_MONDAY_COOLDOWN_SECONDS = 300  # 5 minutes


@_firestore.transactional
def _claim_monday_webhook_txn(
    transaction, ref, item_id: str, now_iso: str, cooldown_seconds: int
) -> bool:
    snap = ref.get(transaction=transaction)
    data = snap.to_dict() if snap.exists else {}
    last_str = data.get(item_id)
    if last_str:
        last_dt = datetime.fromisoformat(last_str)
        now_dt = datetime.fromisoformat(now_iso)
        if (now_dt - last_dt).total_seconds() < cooldown_seconds:
            return False
    transaction.set(ref, {item_id: now_iso}, merge=True)
    return True


def try_claim_monday_webhook(
    item_id: str, cooldown_seconds: int = _MONDAY_COOLDOWN_SECONDS
) -> bool:
    """Return True if this item_id may proceed (first event or cooldown elapsed).

    Returns False if the same item_id triggered within the last cooldown_seconds.
    Fails OPEN (returns True) if Firestore is unavailable — a transient blip
    should not suppress a real status change.
    """
    db = _get_db()
    if db is None:
        return True
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        ref = db.collection(_COLLECTION).document(_MONDAY_COOLDOWN_DOC_ID)
        transaction = db.transaction()
        return _claim_monday_webhook_txn(transaction, ref, item_id, now_iso, cooldown_seconds)
    except Exception as exc:
        logger.error(json.dumps({
            "event": "system_state_monday_cooldown_failed",
            "item_id": item_id,
            "error": str(exc)[:200],
        }))
        return True  # fail open — don't suppress a real status change on a Firestore hiccup


# ---------------------------------------------------------------------------
# Gmail push dispatch cooldown — coalesces rapid burst pushes into a single
# LLM dispatch. Each dispatcher run scans the full unprocessed inbox, so the
# first push in a burst handles all queued mail; subsequent pushes within the
# cooldown window are redundant. Configurable via dashboard_config/settings.
# ---------------------------------------------------------------------------

_GMAIL_DISPATCH_DOC_ID = "gmail_dispatch_cooldown"


@_firestore.transactional
def _claim_gmail_dispatch_txn(
    transaction, ref, now_iso: str, cooldown_seconds: int
) -> tuple[bool, float]:
    snap = ref.get(transaction=transaction)
    data = snap.to_dict() if snap.exists else {}
    last_str = data.get("last_dispatched_at")
    if last_str:
        last_dt = datetime.fromisoformat(last_str)
        now_dt = datetime.fromisoformat(now_iso)
        elapsed = (now_dt - last_dt).total_seconds()
        if elapsed < cooldown_seconds:
            return False, cooldown_seconds - elapsed
    transaction.set(ref, {"last_dispatched_at": now_iso}, merge=True)
    return True, 0.0


def try_claim_gmail_dispatch(cooldown_seconds: int = 90) -> tuple[bool, float]:
    """Return (True, 0) if a push LLM dispatch may proceed; (False, remaining) if
    a prior dispatch was started within the last cooldown_seconds.  Fails OPEN
    (returns (True, 0)) if Firestore is unavailable — a transient blip should
    never silently block real mail.
    """
    db = _get_db()
    if db is None:
        return True, 0.0
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        ref = db.collection(_COLLECTION).document(_GMAIL_DISPATCH_DOC_ID)
        transaction = db.transaction()
        return _claim_gmail_dispatch_txn(transaction, ref, now_iso, cooldown_seconds)
    except Exception as exc:
        logger.error(json.dumps({
            "event": "system_state_gmail_dispatch_cooldown_failed",
            "error": str(exc)[:200],
        }))
        return True, 0.0  # fail open — don't suppress real mail on a Firestore hiccup


# ---------------------------------------------------------------------------
# Pending invoice count — used by invoice_sweep cron guard to skip the LLM
# when there are no pending invoices. Queries the same Firestore project as
# the rest of this module (GOOGLE_CLOUD_PROJECT). Returns -1 on error so
# callers can fail open (proceed with the LLM run) rather than silently skip.
# ---------------------------------------------------------------------------

def count_pending_invoices() -> int:
    """Count pending_invoices docs with status='pending'. Returns -1 on Firestore error."""
    db = _get_db()
    if db is None:
        return -1
    try:
        docs = db.collection("pending_invoices").where("status", "==", "pending").stream()
        return sum(1 for _ in docs)
    except Exception as exc:
        logger.error(json.dumps({
            "event": "count_pending_invoices_failed",
            "error": str(exc)[:200],
        }))
        return -1


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _latch_off(ref, reason: str, spend: float = 0.0, cap: float = 0.0) -> None:
    updates = {
        "enabled": False,
        "disabled_reason": reason,
        "disabled_at": datetime.now(timezone.utc),
    }
    ref.set(updates, merge=True)
    with _cache_lock:
        _cache.clear()
    logger.warning(json.dumps({
        "event": "system_spend_cap_latched_off",
        "disabled_reason": reason,
        "spend_gbp": round(spend, 4),
        "cap_gbp": cap,
        "action": "system_disabled_pending_reapproval",
    }))


# ---------------------------------------------------------------------------
# WhatsApp MessageSid dedup — one Firestore doc per Twilio MessageSid with a
# 24-hour TTL. Atomic create() prevents two concurrent Twilio retries from both
# dispatching an LLM run for the same inbound message.
# ---------------------------------------------------------------------------

_WA_MSG_DEDUP_COLLECTION = "whatsapp_message_dedup"
_WA_MSG_DEDUP_TTL = 86400  # 24 hours


def try_claim_whatsapp_message_sid(message_sid: str) -> bool:
    """Atomically claim a Twilio MessageSid. Returns True if first claim
    (caller should proceed), False if already claimed (duplicate).
    Fails OPEN on Firestore errors."""
    if not message_sid:
        return True
    db = _get_db()
    if db is None:
        return True
    try:
        now = time.time()
        ref = db.collection(_WA_MSG_DEDUP_COLLECTION).document(message_sid)
        ref.create({"claimed_at": now, "expires_at": now + _WA_MSG_DEDUP_TTL})
        return True
    except _AlreadyExists:
        logger.info(json.dumps({
            "event": "whatsapp_message_dedup",
            "message_sid": message_sid,
            "action": "duplicate_skipped",
        }))
        return False
    except Exception as exc:
        logger.error(json.dumps({
            "event": "system_state_wa_msg_dedup_failed",
            "error": str(exc)[:200],
        }))
        return True  # fail open


def _warn_fallback(reason: str) -> None:
    cached = _cache.get("state")
    logger.error(json.dumps({
        "event": "system_state_firestore_error",
        "reason": reason,
        "fallback": "cached_value" if cached else "fail_open_defaults",
    }))


# ---------------------------------------------------------------------------
# Scheduled task health tracking
# ---------------------------------------------------------------------------

_TASK_HEALTH_COLLECTION = "task_health"
_TASK_FAILURE_ALERT_THRESHOLD = 3


def record_task_success(task_name: str) -> None:
    """Reset consecutive failure counter for task. Fail-safe."""
    db = _get_db()
    if db is None:
        return
    try:
        db.collection(_TASK_HEALTH_COLLECTION).document(task_name).set(
            {"consecutive_failures": 0, "last_success_at": time.time()},
            merge=True,
        )
    except Exception as exc:
        logger.warning(json.dumps({
            "event": "task_health_success_write_failed",
            "task": task_name,
            "error": str(exc)[:200],
        }))


def record_task_failure(task_name: str, error: str) -> int:
    """Atomically increment consecutive failure counter. Alert admin at threshold. Returns new count."""
    db = _get_db()
    if db is None:
        return 0
    try:
        ref = db.collection(_TASK_HEALTH_COLLECTION).document(task_name)
        ref.set(
            {
                "consecutive_failures": _firestore.Increment(1),
                "last_failure_at": time.time(),
                "last_error": error[:500],
            },
            merge=True,
        )
        snap = ref.get()
        count = int((snap.to_dict() or {}).get("consecutive_failures", 1))
        if count >= _TASK_FAILURE_ALERT_THRESHOLD:
            from servers.alerting import alert_admin, CRITICAL
            alert_admin(
                CRITICAL,
                f"scheduled_task_repeated_failure:{task_name}",
                f"Task '{task_name}' failed {count} consecutive times. Last error: {error[:300]}",
            )
        return count
    except Exception as exc:
        logger.warning(json.dumps({
            "event": "task_health_failure_write_failed",
            "task": task_name,
            "error": str(exc)[:200],
        }))
        return 0
