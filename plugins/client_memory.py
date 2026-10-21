"""Client memory plugin — loads and saves per-client profiles from Firestore.

Wave 4: Tier-3 long-term memory keyed by normalised client email/phone.
Loads client context before each agent run; extracts facts after significant
tool calls (bookings, drafts, updates) and persists them asynchronously.
"""
import asyncio
import json
import logging
import re
import threading
from datetime import datetime, timezone
from typing import Optional
from google.adk.plugins import BasePlugin

logger = logging.getLogger(__name__)

_COLLECTION = "client_memory"
_SIGNIFICANT_TOOLS = frozenset({
    "create_inventory_booking", "draft_reply", "update_column_value",
    "update_multiple_columns", "create_lead",
})

_db = None
_db_lock = threading.Lock()


def _get_db():
    global _db
    with _db_lock:
        if _db is None:
            try:
                from google.cloud import firestore
                _db = firestore.Client()
            except Exception as exc:
                logger.warning(json.dumps({"event": "client_memory_db_init_failed", "error": str(exc)[:200]}))
        return _db


def _normalise_key(email: str = "", phone: str = "") -> Optional[str]:
    """Return a stable Firestore doc key from email or phone."""
    if email:
        return re.sub(r"[^a-z0-9._-]", "_", email.lower().strip())
    if phone:
        return re.sub(r"[^0-9]", "", phone)[:15] or None
    return None


def _load_profile_sync(key: str) -> dict:
    db = _get_db()
    if db is None:
        return {}
    try:
        snap = db.collection(_COLLECTION).document(key).get()
        return snap.to_dict() or {} if snap.exists else {}
    except Exception as exc:
        logger.warning(json.dumps({"event": "client_memory_load_failed", "error": str(exc)[:200]}))
        return {}


def _save_profile_sync(key: str, updates: dict) -> None:
    db = _get_db()
    if db is None:
        return
    try:
        updates["updated_at"] = datetime.now(timezone.utc).isoformat()
        db.collection(_COLLECTION).document(key).set(updates, merge=True)
    except Exception as exc:
        logger.warning(json.dumps({"event": "client_memory_save_failed", "error": str(exc)[:200]}))


class ClientMemoryPlugin(BasePlugin):
    """Injects client profile context before agent runs and saves facts after tool calls."""

    def __init__(self):
        super().__init__(name="client_memory")

    async def before_agent_callback(self, *, agent, callback_context):
        """Load client profile into session state before agent starts."""
        state = getattr(callback_context, "state", None)
        if state is None:
            return None

        email = state.get("client_email", "")
        phone = state.get("client_phone", "")
        key = _normalise_key(email=email, phone=phone)
        if not key:
            return None

        try:
            profile = await asyncio.to_thread(_load_profile_sync, key)
            if profile:
                state["client_context"] = json.dumps(profile)
                logger.info(json.dumps({"event": "client_memory_loaded", "key": key}))
        except Exception as exc:
            logger.warning(json.dumps({"event": "client_memory_load_error", "error": str(exc)[:200]}))

        return None

    async def after_tool_callback(self, *, tool, tool_args, tool_context, result):
        """Extract and persist client facts after significant tool calls."""
        if tool.name not in _SIGNIFICANT_TOOLS:
            return None

        state = getattr(tool_context, "state", None)
        if state is None:
            return None

        email = (tool_args or {}).get("email", "") or state.get("client_email", "")
        phone = (tool_args or {}).get("phone", "") or state.get("client_phone", "")
        key = _normalise_key(email=email, phone=phone)
        if not key:
            return None

        updates: dict = {}
        if email:
            updates["email"] = email
        if phone:
            updates["phone"] = phone

        for field in ("customer", "company_name", "address", "postcode", "spec", "notes"):
            val = (tool_args or {}).get(field)
            if val:
                updates[field] = val

        if updates:
            try:
                loop = asyncio.get_running_loop()
                task = loop.create_task(asyncio.to_thread(_save_profile_sync, key, updates))
                def _log_exc(t, _key=key):
                    if not t.cancelled() and t.exception() is not None:
                        logger.error(json.dumps({"event": "client_memory_save_exception", "key": _key, "error": str(t.exception())[:200]}))
                task.add_done_callback(_log_exc)
            except Exception as exc:
                logger.warning(json.dumps({"event": "client_memory_save_error", "error": str(exc)[:200]}))

        return None
