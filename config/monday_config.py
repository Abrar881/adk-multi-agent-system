"""Shared Monday.com board/column configuration â€” Firestore-backed with env-var fallback."""
import os
import time
import logging
import threading
from typing import Optional

logger = logging.getLogger(__name__)

_CACHE_TTL = 300  # 5 minutes
_cache: tuple[Optional[dict], float] = (None, 0.0)
_db = None
_db_lock = threading.Lock()

_COLUMN_ALIASES: dict[str, list[str]] = {
    "report_status":    ["report status", "status", "report"],
    "inspector":        ["inspector", "clerk", "assignee"],
    "report_type":      ["report type", "type"],
    "reason_delay":     ["reason for delay", "delay reason", "reason delay"],
    "payment_status":   ["payment status", "payment"],
    "payment_reminder": ["payment reminder", "reminder"],
    "Acme_confirmation": ["Acme confirmation", "confirmation"],
    "postcode":         ["postcode", "post code", "zip code", "zip"],
    "phone":            ["phone", "telephone", "mobile", "contact phone"],
    "customer":         ["customer", "client", "client name"],  # "name" removed â€” matches item-Name column, not Customer
    "company_name":     ["company name", "company", "agency name", "agency"],
    "access_info":      ["access info", "access information", "access", "access instructions", "instructions"],
    "contact_number":   ["contact number", "contact", "phone number"],
    "notes":            ["notes", "note", "comments"],
    "inv_number":       ["inv #", "inv number", "invoice number", "invoice #", "inv"],
    "hour":             ["hour", "time", "appointment time", "booking time"],
    "email":            ["email", "e-mail", "email address"],
    "address":          ["address", "property address", "full address"],
    "spec":             ["spec", "specification"],
    "date":             ["date", "booking date", "appointment date"],
    "job_status":       ["job status", "job_status"],
    "price":            ["price", "base cost", "Acme price"],
    "pay_link":         ["pay link", "payment link", "invoice link"],
}

REQUIRED_COLUMNS = {"date", "address", "customer"}
EXPECTED_COLUMNS = {"phone", "email", "postcode", "hour", "spec", "company_name"}


def _build_env_fallback() -> dict:
    return {
        "api_token": os.environ.get("MONDAY_API_KEY", ""),
        "boards": {
            "inventories": {
                # Board ID: 18392817962 â€” "2026 Inventories Board" (Acmeinventories.monday.com)
                # Column IDs verified 2026-07-09 via Monday MCP get_board_info (live read-back).
                # These serve as env-var-backed defaults until the client connects the
                # board in the dashboard (Settings â†’ Monday.com), which writes to
                # Firestore and takes precedence over this fallback.
                "id": int(os.getenv("BOOKINGS_BOARD_ID", "18392817962") or "0"),
                "columns": {
                    "report_status":      {"id": os.getenv("MONDAY_COL_REPORT_STATUS",      "report_status")},
                    "inspector":          {"id": os.getenv("MONDAY_COL_INSPECTOR",           "inspector")},
                    "report_type":        {"id": os.getenv("MONDAY_COL_REPORT_TYPE",         "report___type")},
                    # reason_delay: intentionally unmapped â€” this column does not exist on the new inventories board.
                    "payment_status":     {"id": os.getenv("MONDAY_COL_PAYMENT_STATUS",      "payment___status")},
                    "payment_reminder":   {"id": os.getenv("MONDAY_COL_PAYMENT_REMINDER",    "status7")},
                    "Acme_confirmation": {"id": os.getenv("MONDAY_COL_Acme_CONFIRMATION",  "status2")},
                    "postcode":           {"id": os.getenv("MONDAY_COL_POSTCODE",            "location")},
                    "phone":              {"id": os.getenv("MONDAY_COL_PHONE",               "contact_number")},
                    "customer":           {"id": os.getenv("MONDAY_COL_CUSTOMER",            "customer")},
                    "company_name":       {"id": os.getenv("MONDAY_COL_COMPANY_NAME",        "text_mm58vrcb")},
                    "access_info":        {"id": os.getenv("MONDAY_COL_ACCESS_INFO",         "access___info")},
                    "contact_number":     {"id": os.getenv("MONDAY_COL_CONTACT_NUMBER",      "")},
                    "notes":              {"id": os.getenv("MONDAY_COL_NOTES",               "access___info")},
                    "inv_number":         {"id": os.getenv("MONDAY_COL_INV_NUMBER",          "inv__4")},
                    "access_instructions":{"id": os.getenv("MONDAY_COL_ACCESS_INSTRUCTIONS", "access___info")},
                    "hour":               {"id": os.getenv("MONDAY_COL_HOUR",               "dup__of_hour0__1")},
                    "email":              {"id": os.getenv("MONDAY_COL_EMAIL",               "email")},
                    "address":            {"id": os.getenv("MONDAY_COL_ADDRESS",             "address")},
                    "spec":               {"id": os.getenv("MONDAY_COL_SPEC",               "spec")},
                    "date":               {"id": os.getenv("MONDAY_COL_DATE",               "date")},
                    "price":              {"id": os.getenv("MONDAY_COL_PRICE",              "numeric_mm28fpan")},
                    "pay_link":           {"id": os.getenv("MONDAY_COL_PAY_LINK",            "text7")},
                },
                "groups": {
                    "leads": os.getenv("MONDAY_LEADS_GROUP_ID", "group_mm52zj1p"),
                },
            },
            "daily_tasks": {
                "id": int(os.getenv("DAILY_TASKS_BOARD_ID", "393394274") or "0"),
            },
            "working_schedule": {
                "id": int(os.getenv("WORKING_SCHEDULE_BOARD_ID", "9529643731") or "0"),
                "columns": {
                    "phone": {"id": os.getenv("MONDAY_WS_COL_PHONE", "phone")},
                },
            },
            "contractors": {
                # Board ID: 18397426458 â€” "2026 Landlord safety certificates & Cleaning" (Acmeinventories.monday.com)
                # Column IDs verified 2026-07-09 via Monday MCP get_board_info (live read-back).
                # These serve as env-var-backed defaults until the client connects the board
                # in the dashboard (Settings â†’ Monday.com), which writes to Firestore and
                # takes precedence over this fallback.
                "id": int(os.getenv("CONTRACTORS_BOARD_ID", "18397426458") or "0"),
                "columns": {
                    "report_type":       {"id": os.getenv("MONDAY_CON_COL_SERVICE_TYPE",     "report___type")},
                    "job_status":        {"id": os.getenv("MONDAY_CON_COL_JOB_STATUS",       "job_status_mkkr74pn")},
                    "inspector":         {"id": os.getenv("MONDAY_CON_COL_INSPECTOR",         "inspector")},
                    "date":              {"id": os.getenv("MONDAY_CON_COL_DATE",              "date")},
                    "hour":              {"id": os.getenv("MONDAY_CON_COL_HOUR",              "dup__of_hour0__1")},
                    "address":           {"id": os.getenv("MONDAY_CON_COL_ADDRESS",           "address")},
                    "postcode":          {"id": os.getenv("MONDAY_CON_COL_POSTCODE",          "location")},
                    "spec":              {"id": os.getenv("MONDAY_CON_COL_SPEC",              "spec")},
                    "email":             {"id": os.getenv("MONDAY_CON_COL_EMAIL",             "email")},
                    "phone":             {"id": os.getenv("MONDAY_CON_COL_PHONE",             "contact_number_mkkrrw4q")},
                    "payment_status":    {"id": os.getenv("MONDAY_CON_COL_PAYMENT_STATUS",    "payment___status")},
                    "payment_reminder":  {"id": os.getenv("MONDAY_CON_COL_PAYMENT_REMINDER",  "status7")},
                    "Acme_confirmation":{"id": os.getenv("MONDAY_CON_COL_Acme_CONFIRMATION","status2")},
                    "reason_delay":      {"id": os.getenv("MONDAY_CON_COL_REASON_DELAY",      "color_mksnadmq")},
                    "contact_number":    {"id": os.getenv("MONDAY_CON_COL_CONTACT_NUMBER",    "contact_number_mkkrrw4q")},
                    "inv_number":        {"id": os.getenv("MONDAY_CON_COL_INV_NUMBER",        "inv__4")},
                    "customer":          {"id": os.getenv("MONDAY_CON_COL_CUSTOMER",          "customer")},
                    "company_name":      {"id": os.getenv("MONDAY_CON_COL_COMPANY_NAME",     "text_mm58wdep")},
                    "access_info":       {"id": os.getenv("MONDAY_CON_COL_ACCESS_INFO",       "access___info")},
                    "notes":             {"id": os.getenv("MONDAY_CON_COL_NOTES",             "text5")},
                    "price":             {"id": os.getenv("MONDAY_CON_COL_PRICE",             "numeric_mm2jc0vj")},
                    "pay_link":          {"id": os.getenv("MONDAY_CON_COL_PAY_LINK",          "text7")},
                },
                "groups": {},
            },
        },
    }


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override onto base, returning a new dict. Does not mutate inputs."""
    out = dict(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _merge_board_config(base: dict, stored: dict) -> dict:
    """Merge Firestore-stored config onto env-fallback base with board-id awareness.

    Resolution rules per board:
    - stored has no boards key (e.g. token-only doc) â†’ env board+columns survive.
    - stored board id == env board id, partial columns â†’ env columns fill unmapped gaps.
    - stored board id differs â†’ env columns DROPPED entirely (they're board-specific;
      injecting them onto a different board would corrupt writes).
    """
    merged = _deep_merge(base, stored)
    base_boards = base.get("boards", {})
    stored_boards = (stored or {}).get("boards", {})
    for key, board in merged.get("boards", {}).items():
        env_id = int(base_boards.get(key, {}).get("id", 0) or 0)
        stored_id = int(stored_boards.get(key, {}).get("id", 0) or 0)
        if stored_id and env_id and stored_id != env_id:
            # Client switched to a different board â€” env column IDs are invalid for it.
            # Use only the columns Firestore saved (populated by the dashboard wizard).
            board["columns"] = stored_boards.get(key, {}).get("columns", {})
    return merged


def get_config() -> dict:
    """Fetch config from Firestore monday_config/boards with 300s TTL cache.

    Resolution:
    - Start from env-var fallback as base (hardcoded test-board defaults).
    - Deep-merge Firestore doc on top when present and readable.
    - Per board: if stored board id differs from env default, drop env columns
      (board-specific IDs would corrupt writes on a different board).
    - Go-live fail-safe: when MONDAY_ALLOW_TEST_FALLBACK=0 and no real board config
      was loaded from Firestore, blank the inventories board to force a clear
      ValueError("not configured") rather than silently writing to the test board.
      Set this env var to "0" in Cloud Run at go-live.
    """
    global _cache
    now = time.monotonic()
    config, expiry = _cache
    if config is not None and now < expiry:
        return config

    base = _build_env_fallback()
    firestore_loaded = False

    try:
        global _db
        if _db is None:
            with _db_lock:
                if _db is None:
                    from google.cloud import firestore
                    _db = firestore.Client()
        doc = _db.collection("monday_config").document("boards").get()
        if doc.exists:
            config = _merge_board_config(base, doc.to_dict() or {})
            firestore_loaded = True
        else:
            config = base
    except Exception as exc:
        logger.warning("Firestore monday_config read failed (%s) â€” using env-var fallback", exc)
        config = base

    # Go-live fail-safe: refuse to silently fall back to the test/template board when
    # MONDAY_ALLOW_TEST_FALLBACK=0 and Firestore has no real board config loaded.
    # Flip to "0" in Cloud Run environment variables at client go-live.
    allow_test_fallback = os.environ.get("MONDAY_ALLOW_TEST_FALLBACK", "1") == "1"
    if not allow_test_fallback and not firestore_loaded:
        # Require real Firestore-backed config at go-live â€” blank the env fallback
        # regardless of what board id it happens to default to, so a missing/unreadable
        # Firestore doc fails loudly instead of silently writing to whatever board is
        # hardcoded in _build_env_fallback.
        logger.error(
            "monday_config: MONDAY_ALLOW_TEST_FALLBACK=0 but no Firestore board "
            "config loaded â€” blanking env fallback to prevent wrong-board writes."
        )
        config = _deep_merge(config, {"boards": {"inventories": {"id": 0, "columns": {}}}})

    _cache = (config, now + _CACHE_TTL)
    return config


def invalidate_cache() -> None:
    global _cache
    _cache = (None, 0.0)


def get_api_token() -> str:
    return os.environ.get("MONDAY_API_KEY", "")


def get_board_id(board_key: str) -> int:
    config = get_config()
    board = config.get("boards", {}).get(board_key, {})
    return int(board.get("id", 0) or 0)


def get_column_id(board_key: str, logical_name: str) -> Optional[str]:
    config = get_config()
    col = config.get("boards", {}).get(board_key, {}).get("columns", {}).get(logical_name)
    if col is None:
        return None
    if isinstance(col, dict):
        return col.get("id") or None
    return col or None


def require_column_id(board_key: str, logical_name: str) -> str:
    col_id = get_column_id(board_key, logical_name)
    if not col_id:
        raise ValueError(f"Required column '{logical_name}' not mapped for board '{board_key}'. Configure Monday settings in the dashboard.")
    return col_id


def get_group_id(board_key: str, group_key: str) -> Optional[str]:
    config = get_config()
    groups = config.get("boards", {}).get(board_key, {}).get("groups", {})
    return groups.get(group_key) or None


def is_configured() -> bool:
    config = get_config()
    return bool(get_api_token()) and bool(
        config.get("boards", {}).get("inventories", {}).get("id")
    )


def detect_columns(board_columns: list) -> dict:
    """Auto-detect logicalâ†’column_id mappings from a board's column list.

    board_columns: list of {"id": "...", "title": "..."}
    Returns: {logical_name: {"id": col_id, "title": col_title, "confidence": float}}
    """
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
