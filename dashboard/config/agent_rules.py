"""Agent custom rules — Firestore-backed with TTL cache.

Rules are plain-English operational instructions added by the client from the dashboard.
They are appended to each agent's system instruction at runtime.

Firestore document: agent_config/rules
Structure:
{
    "dispatcher": [{"rule": "...", "created_at": "ISO8601"}, ...],
    "finance": [...],
    "ops": [...],
    "updated_at": "ISO8601"
}
"""
import time
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_CACHE_TTL = 300  # 5 minutes
_cache: tuple[Optional[dict], float] = (None, 0.0)

COLLECTION = "agent_config"
DOCUMENT = "rules"
VALID_AGENTS = {"dispatcher", "finance", "ops"}
MAX_RULES_PER_AGENT = 20
RULE_MIN_LEN = 10
RULE_MAX_LEN = 500


def _get_raw() -> dict:
    """Fetch rules from Firestore with TTL cache. Returns raw doc dict or empty dict."""
    global _cache
    now = time.monotonic()
    data, expiry = _cache
    if data is not None and now < expiry:
        return data

    try:
        from google.cloud import firestore
        db = firestore.Client()
        doc = db.collection(COLLECTION).document(DOCUMENT).get()
        data = doc.to_dict() if doc.exists else {}
        _cache = (data, now + _CACHE_TTL)
        return data
    except Exception as exc:
        logger.warning("Firestore agent_config/rules read failed (%s) — no rules applied", exc)

    data = {}
    _cache = (data, now + _CACHE_TTL)
    return data


def get_rules(agent_name: str) -> list[str]:
    """Return list of rule strings for the given agent. Empty list if none or error."""
    if agent_name not in VALID_AGENTS:
        return []
    raw = _get_raw()
    entries = raw.get(agent_name, [])
    if not isinstance(entries, list):
        return []
    return [e["rule"] for e in entries if isinstance(e, dict) and e.get("rule")]


def get_all_rules() -> dict:
    """Return all rules grouped by agent as {agent: [rule_str, ...]}."""
    return {agent: get_rules(agent) for agent in VALID_AGENTS}


def invalidate_cache() -> None:
    global _cache
    _cache = (None, 0.0)
