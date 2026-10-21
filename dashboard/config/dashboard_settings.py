"""Shared dashboard configuration â€” Firestore-backed with TTL cache and hardcoded defaults fallback.

Mirrors the monday_config.py pattern. Used by both dashboard/app.py and tools/approval.py
so config changes in the dashboard are picked up by the agent service within one cache TTL.
"""
import os
import re
import time
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_TIME_RE = re.compile(r"^\d{2}:\d{2}$")

_CACHE_TTL = 300  # 5 minutes (agent service); dashboard overrides to 60s via _DASHBOARD_CACHE_TTL
_DASHBOARD_CACHE_TTL = 60  # 1 minute for dashboard process
_cache: tuple[Optional[dict], float] = (None, 0.0)

COLLECTION = "dashboard_config"
DOCUMENT = "settings"

_DEFAULTS: dict = {
    "approval_threshold_gbp": float(os.environ.get("APPROVAL_THRESHOLD_GBP", "500")),
    "agent_status_thresholds": {"green_minutes": 30, "amber_hours": 2},
    "activity_feed_limit": 50,
    "escalation_scan_limit": 200,
    "suggestion_chips": [
        "Chase outstanding invoices",
        "What's scheduled today?",
        "Check last week's bookings",
    ],
    "company_name": "Multi-Agent System",
    "company_tagline": "Operations Hub",
    "polling_interval_ms": 30000,
    "agent_labels": {
        "dispatcher": "Administrator",
        "finance": "Finance",
        "ops": "Operations",
        "system": "System",
    },
    "monthly_budget_gbp": 0.0,
    "summary_categories": {
        "bookings_confirmed": {"label": "Bookings Confirmed", "enabled": True},
        "invoices_chased": {"label": "Invoices Chased", "enabled": True},
        "emails_sent": {"label": "Emails Sent", "enabled": True},
        "escalations": {"label": "Escalations", "enabled": True},
        "tool_calls": {"label": "Tool Calls", "enabled": False},
        "scheduled_tasks": {"label": "Scheduled Tasks", "enabled": False},
    },
    "email_listening_enabled": True,
    "email_window_start": "07:00",   # HH:MM, Europe/London
    "email_window_end": "22:00",
    "admin_contacts": [],  # list of {name: str, phone: str}
}

_E164_RE = re.compile(r"^\+[1-9]\d{6,14}$")


def get_defaults() -> dict:
    """Return a deep copy of the hardcoded defaults."""
    import copy
    return copy.deepcopy(_DEFAULTS)


def get_config(cache_ttl: int = _CACHE_TTL) -> dict:
    """Fetch config from Firestore dashboard_config/settings with TTL cache. Falls back to defaults."""
    global _cache
    now = time.monotonic()
    config, expiry = _cache
    if config is not None and now < expiry:
        return config

    try:
        from google.cloud import firestore
        db = firestore.Client()
        doc = db.collection(COLLECTION).document(DOCUMENT).get()
        if doc.exists:
            stored = doc.to_dict() or {}
            # Deep merge: defaults fill any missing keys, stored values win on conflicts
            config = _deep_merge(get_defaults(), stored)
            _cache = (config, now + cache_ttl)
            return config
    except Exception as exc:
        logger.warning("Firestore dashboard_config read failed (%s) â€” using defaults", exc)

    config = get_defaults()
    _cache = (config, now + cache_ttl)
    return config


def invalidate_cache() -> None:
    global _cache
    _cache = (None, 0.0)


def get_approval_threshold() -> float:
    """Return the approval threshold in GBP. Used by tools/approval.py."""
    return float(get_config().get("approval_threshold_gbp", _DEFAULTS["approval_threshold_gbp"]))


def email_listening_active() -> tuple[bool, str]:
    """Return (should_process, reason) for whether incoming email should be processed now.

    Checks the master switch then the Europe/London time window. Fail-open on any
    config/parse error (returns True) â€” matches get_config()'s fall-back-to-defaults
    convention, so a bad config value never blocks email processing.
    """
    try:
        from datetime import datetime
        from zoneinfo import ZoneInfo

        config = get_config()
        if not config.get("email_listening_enabled", True):
            return False, "listening disabled"

        start_raw = config.get("email_window_start", _DEFAULTS["email_window_start"])
        end_raw = config.get("email_window_end", _DEFAULTS["email_window_end"])

        def _to_minutes(hhmm: str) -> int:
            h, m = hhmm.split(":")
            h, m = int(h), int(m)
            if not (0 <= h <= 23 and 0 <= m <= 59):
                raise ValueError(f"time out of range: {hhmm}")
            return h * 60 + m

        start_min = _to_minutes(start_raw)
        end_min = _to_minutes(end_raw)

        now = datetime.now(ZoneInfo("Europe/London"))
        now_min = now.hour * 60 + now.minute

        if start_min <= end_min:
            active = start_min <= now_min < end_min
        else:
            # Overnight window (e.g. 22:00 -> 07:00)
            active = now_min >= start_min or now_min < end_min

        return active, ("in window" if active else "outside window")
    except Exception as exc:
        logger.warning("email_listening_active check failed (%s) â€” fail-open (processing)", exc)
        return True, "fail-open"


def get_admin_phones() -> list[str]:
    """Return admin phone numbers (E.164). Firestore-first, ADMIN_PHONE env var fallback."""
    try:
        contacts = get_config().get("admin_contacts", [])
    except Exception:
        contacts = []
    phones = [c["phone"] for c in contacts if isinstance(c, dict) and c.get("phone")]
    if phones:
        return phones
    raw = os.environ.get("ADMIN_PHONE", "")
    return [p.strip() for p in raw.split(",") if p.strip()]


def get_admin_contacts() -> list[dict]:
    """Return admin contacts [{name, phone}]. Firestore-first, ADMIN_PHONE env var fallback."""
    try:
        contacts = get_config().get("admin_contacts", [])
    except Exception:
        contacts = []
    valid = [c for c in contacts if isinstance(c, dict) and c.get("phone")]
    if valid:
        return valid
    raw = os.environ.get("ADMIN_PHONE", "")
    return [{"name": "Admin", "phone": p.strip()} for p in raw.split(",") if p.strip()]


def _deep_merge(base: dict, override: dict) -> dict:
    """Merge override into base recursively. override values win. base fills missing keys."""
    result = base.copy()
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result


def validate_config_update(data: dict) -> tuple[dict, list[str]]:
    """Validate a partial config update dict. Returns (cleaned_data, errors)."""
    cleaned: dict = {}
    errors: list[str] = []

    if "approval_threshold_gbp" in data:
        try:
            v = float(data["approval_threshold_gbp"])
            if not 0 <= v <= 100_000:
                errors.append("approval_threshold_gbp must be between 0 and 100,000")
            else:
                cleaned["approval_threshold_gbp"] = v
        except (TypeError, ValueError):
            errors.append("approval_threshold_gbp must be a number")

    if "agent_status_thresholds" in data:
        t = data["agent_status_thresholds"]
        if not isinstance(t, dict):
            errors.append("agent_status_thresholds must be an object")
        else:
            thresholds: dict = {}
            if "green_minutes" in t:
                try:
                    gm = int(t["green_minutes"])
                    if not 1 <= gm <= 1440:
                        errors.append("green_minutes must be 1â€“1440")
                    else:
                        thresholds["green_minutes"] = gm
                except (TypeError, ValueError):
                    errors.append("green_minutes must be an integer")
            if "amber_hours" in t:
                try:
                    ah = int(t["amber_hours"])
                    if not 1 <= ah <= 48:
                        errors.append("amber_hours must be 1â€“48")
                    else:
                        thresholds["amber_hours"] = ah
                except (TypeError, ValueError):
                    errors.append("amber_hours must be an integer")
            if thresholds:
                cleaned["agent_status_thresholds"] = thresholds

    if "activity_feed_limit" in data:
        try:
            v = int(data["activity_feed_limit"])
            if not 10 <= v <= 500:
                errors.append("activity_feed_limit must be 10â€“500")
            else:
                cleaned["activity_feed_limit"] = v
        except (TypeError, ValueError):
            errors.append("activity_feed_limit must be an integer")

    if "escalation_scan_limit" in data:
        try:
            v = int(data["escalation_scan_limit"])
            if not 50 <= v <= 1000:
                errors.append("escalation_scan_limit must be 50â€“1000")
            else:
                cleaned["escalation_scan_limit"] = v
        except (TypeError, ValueError):
            errors.append("escalation_scan_limit must be an integer")

    if "suggestion_chips" in data:
        chips = data["suggestion_chips"]
        if not isinstance(chips, list):
            errors.append("suggestion_chips must be a list")
        elif len(chips) > 10:
            errors.append("suggestion_chips max 10 items")
        elif any(not isinstance(c, str) or len(c) > 100 for c in chips):
            errors.append("each chip must be a string of at most 100 characters")
        else:
            cleaned["suggestion_chips"] = [c.strip() for c in chips if c.strip()]

    if "company_name" in data:
        v = str(data["company_name"]).strip()
        if len(v) > 50:
            errors.append("company_name max 50 characters")
        elif v:
            cleaned["company_name"] = v

    if "company_tagline" in data:
        v = str(data["company_tagline"]).strip()
        if len(v) > 100:
            errors.append("company_tagline max 100 characters")
        else:
            cleaned["company_tagline"] = v

    if "polling_interval_ms" in data:
        try:
            v = int(data["polling_interval_ms"])
            if not 5000 <= v <= 300_000:
                errors.append("polling_interval_ms must be 5000â€“300000")
            else:
                cleaned["polling_interval_ms"] = v
        except (TypeError, ValueError):
            errors.append("polling_interval_ms must be an integer")

    if "agent_labels" in data:
        labels = data["agent_labels"]
        if not isinstance(labels, dict):
            errors.append("agent_labels must be an object")
        elif len(labels) > 4:
            errors.append("agent_labels max 4 entries")
        elif any(not isinstance(k, str) or not isinstance(v, str) or len(v) > 50 for k, v in labels.items()):
            errors.append("each agent label must be a string of at most 50 characters")
        else:
            cleaned["agent_labels"] = {k: v.strip() for k, v in labels.items() if v.strip()}

    if "monthly_budget_gbp" in data:
        try:
            v = float(data["monthly_budget_gbp"])
            if not 0 <= v <= 100_000:
                errors.append("monthly_budget_gbp must be between 0 and 100,000")
            else:
                cleaned["monthly_budget_gbp"] = v
        except (TypeError, ValueError):
            errors.append("monthly_budget_gbp must be a number")

    if "summary_categories" in data:
        cats = data["summary_categories"]
        if not isinstance(cats, dict):
            errors.append("summary_categories must be an object")
        else:
            valid_keys = set(_DEFAULTS["summary_categories"].keys())
            cleaned_cats: dict = {}
            for k, v in cats.items():
                if k not in valid_keys:
                    errors.append(f"unknown summary category: {k}")
                elif not isinstance(v, dict):
                    errors.append(f"summary_categories.{k} must be an object")
                else:
                    entry: dict = {}
                    if "enabled" in v:
                        entry["enabled"] = bool(v["enabled"])
                    if "label" in v:
                        lbl = str(v["label"]).strip()
                        if len(lbl) > 50:
                            errors.append(f"summary_categories.{k}.label max 50 characters")
                        elif lbl:
                            entry["label"] = lbl
                    if entry:
                        cleaned_cats[k] = entry
            if cleaned_cats:
                cleaned["summary_categories"] = cleaned_cats

    if "email_listening_enabled" in data:
        cleaned["email_listening_enabled"] = bool(data["email_listening_enabled"])

    for key in ("email_window_start", "email_window_end"):
        if key in data:
            v = str(data[key]).strip()
            if not _TIME_RE.match(v):
                errors.append(f"{key} must be in HH:MM format")
            else:
                hh, mm = v.split(":")
                if not (0 <= int(hh) <= 23 and 0 <= int(mm) <= 59):
                    errors.append(f"{key} must be a valid 24-hour time (00:00â€“23:59)")
                else:
                    cleaned[key] = v

    if "admin_contacts" in data:
        contacts = data["admin_contacts"]
        if not isinstance(contacts, list):
            errors.append("admin_contacts must be a list")
        elif len(contacts) > 10:
            errors.append("admin_contacts max 10 entries")
        else:
            valid_contacts = []
            contact_errors: list[str] = []
            for i, c in enumerate(contacts):
                if not isinstance(c, dict):
                    contact_errors.append(f"admin_contacts[{i}] must be an object")
                    continue
                name = str(c.get("name", "")).strip()
                phone = str(c.get("phone", "")).strip()
                item_ok = True
                if not name:
                    contact_errors.append(f"admin_contacts[{i}].name is required")
                    item_ok = False
                elif len(name) > 50:
                    contact_errors.append(f"admin_contacts[{i}].name max 50 characters")
                    item_ok = False
                if not phone:
                    contact_errors.append(f"admin_contacts[{i}].phone is required")
                    item_ok = False
                elif not _E164_RE.match(phone):
                    contact_errors.append(f"admin_contacts[{i}].phone must be E.164 format (e.g. +447444475452)")
                    item_ok = False
                if item_ok:
                    valid_contacts.append({"name": name, "phone": phone})
            errors.extend(contact_errors)
            if not contact_errors:
                cleaned["admin_contacts"] = valid_contacts

    return cleaned, errors
