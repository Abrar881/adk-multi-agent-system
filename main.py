"""Main Orchestrator - Acme Property Services Multi-Agent System (Google ADK).

Hub-and-Spoke Architecture:
- Hub: Routes tasks to appropriate sub-agent (Dispatcher, Finance, Ops)
- Spokes: Separate LlmAgent instances with dedicated system instructions
- All agents share the same McpToolset connections (13 MCP server subprocesses)

Execution Modes:
- CLI: Interactive testing (EXECUTION_MODE=cli)
- Event: Webhook handler (EXECUTION_MODE=event)
- Schedule: Cloud Scheduler tasks (EXECUTION_MODE=schedule)

ADK Feature Utilisation (post-transformation):
- Wave 0: App pattern (name validation, proper Runner wiring)
- Wave 1: CostGuardPlugin, ActivityLogPlugin, ReflectAndRetryToolPlugin,
          GlobalInstructionPlugin, EventsCompactionConfig (replaces 50-event cap)
- Wave 2: gemini-2.5-flash upgrade, deterministic pre-routing (_TASK_AGENT_MAP, _EVENT_AGENT_MAP)
- Wave 3: SequentialAgent reconciliation+chase pipelines on finance_agent
- Wave 4: ResumabilityConfig + ClientMemoryPlugin (wired via Acme_app)
- Wave 5: OpenTelemetry / Cloud Trace (optional, see telemetry.py)
"""
import asyncio
import contextvars
import random
import sys
import os
import json
import logging
import threading
import time
import base64
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from uuid import uuid4
from dotenv import load_dotenv
load_dotenv()
from google.genai import types
from google.adk.agents import LlmAgent
from google.adk.agents.readonly_context import ReadonlyContext
from google.adk.agents.run_config import RunConfig
from google.adk.apps.app import App, EventsCompactionConfig, ResumabilityConfig
from google.adk.models.llm_request import ContextCacheConfig
from google.adk.models.llm_response import LlmResponse
from google.adk.apps.compaction import LlmEventSummarizer
from google.adk.models.google_llm import Gemini
from google.adk.flows.llm_flows.base_llm_flow import StreamingMode
from google.adk.plugins import ReflectAndRetryToolPlugin
from google.adk.runners import Runner
from sessions.firestore_session import FirestoreSessionService
from google.adk.tools.mcp_tool.mcp_toolset import StdioConnectionParams
from google.adk.tools.agent_tool import AgentTool
from mcp import StdioServerParameters
from mcp.shared.exceptions import McpError
from anyio import BrokenResourceError
from google.cloud import firestore as _firestore
from google.api_core import exceptions as _gapi_exceptions
from mcp_runtime import SerializedMcpToolset

from agents.dispatcher import (
    get_system_instruction as get_dispatcher_instruction,
    get_static_instruction as get_dispatcher_static,
    get_dynamic_context as get_dispatcher_dynamic_ctx,
)
from agents.finance import (
    get_system_instruction as get_finance_instruction,
    get_static_instruction as get_finance_static,
    get_dynamic_context as get_finance_dynamic_ctx,
)
from security.tool_predicate import RoleAwareToolFilter, AgentScopedToolFilter
from security.tool_permissions import (
    DISPATCHER_MONDAY_TOOLS, FINANCE_MONDAY_TOOLS, OPS_MONDAY_TOOLS,
    AGENT_LOCKING_TOOLS, FINANCE_REPORTING_TOOLS,
    DISPATCHER_XERO_TOOLS, FINANCE_XERO_TOOLS,
    DISPATCHER_WHATSAPP_TOOLS, OPS_WHATSAPP_TOOLS,
)
from security.sanitize import sanitize_payload_field
from plugins.role_guard import RoleGuardPlugin
from agents.ops import (
    get_system_instruction as get_ops_instruction,
    get_static_instruction as get_ops_static,
    get_dynamic_context as get_ops_dynamic_ctx,
)
from agents.finance_workflows import create_finance_workflows
from agents.reflection import create_booking_confirmation_loop
from plugins.cost_guard import CostGuardPlugin, ERR_MAX_ITERATIONS, ERR_TRANSFER_LOOP, ERR_INPUT_CEILING, ERR_SPEND_CAP
from plugins.activity_log import ActivityLogPlugin
from plugins.client_memory import ClientMemoryPlugin
from tools.approval import approval_tool
from config.monday_config import get_board_id
from config.coverage import classify_coverage
from config.agent_rules import get_rules as get_agent_rules
import system_state as _system_state

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT') or os.environ.get('GCLOUD_PROJECT')
if not PROJECT_ID:
    raise RuntimeError("GOOGLE_CLOUD_PROJECT env var must be set")
LOCATION = os.environ.get('GOOGLE_CLOUD_LOCATION', 'global')
HUB_MODEL = "gemini-3.5-flash-lite"    # latest lite: routing + compaction
AGENT_MODEL = "gemini-3.8-flash"        # latest flash: dispatcher, finance, ops â€” Gemini 3 thinking_level supported
OPS_MODEL = os.environ.get("OPS_MODEL", AGENT_MODEL)  # defaults to gemini-3.7-flash; override via env var if needed


# Set to False in chat entry points (stream_chat/route_and_execute) so interactive requests use
# standard priority tier. Scheduled/event processing keeps the default True (Flex = 50% cheaper).
_flex_enabled: contextvars.ContextVar[bool] = contextvars.ContextVar('flex_enabled', default=True)


class FlexGemini(Gemini):
    """Gemini on Flex PayGo tier â€” identical model weights, 50% cheaper, latency-tolerant (2026-07-18)."""
    def _tracking_headers(self) -> dict[str, str]:
        headers = super()._tracking_headers()
        if _flex_enabled.get():
            headers["X-Vertex-AI-LLM-Request-Type"] = "shared"
            headers["X-Vertex-AI-LLM-Shared-Request-Type"] = "flex"
        return headers

# SSE streaming config â€” enables token-level partial events from run_async(). Interactive
# dashboard/CLI chat only, so the higher cap stays â€” a human is present to notice runaway loops.
_STREAM_CONFIG = RunConfig(streaming_mode=StreamingMode.SSE, max_llm_calls=30)

# Hard timeout for stream_chat(). Must be < frontend abort (375s) so the backend error message
# reaches the client before the browser gives up. The error SSE event arrives ~0-3s after this fires.
_CHAT_STREAM_TIMEOUT = 360

# Non-streaming cap â€” mirrors the streaming limit for interactive (non-_AUTO_PROCESS_ROLES) callers.
# Without this the ADK default (500 calls) applies; a single stuck request can burn millions of
# tokens before the hard stop triggers.
_NONSTREAM_CONFIG = RunConfig(max_llm_calls=50)

# Cap for autonomous/scheduled/event-driven roles (_AUTO_PROCESS_ROLES) â€” no human is
# watching these runs. CostGuard session-level caps (300 calls, daily GBP limit) provide the
# backstop against runaway loops. Kept at 50 to stay within Gemini API quota limits.
# Selected in route_and_execute/invoke_agent_directly based on source_role.
_NONSTREAM_CONFIG_LOW = RunConfig(max_llm_calls=50)

# Structured JSON logging
logging.basicConfig(
    format='%(message)s',
    level=logging.INFO,
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

# Surface ADK context-cache decisions â€” library defaults to WARNING, which hides
# "Cache created successfully" / "Failed to create cache" / "Previous request too small".
# Setting INFO makes these visible in Cloud Run without debug noise from other ADK internals.
logging.getLogger("google_adk").setLevel(logging.INFO)
# Suppress benign MCP session-teardown noise: ADK 1.30 closes MCP sessions in a
# dedicated task; the resulting "exit cancel scope in a different task" RuntimeError
# is already swallowed by ADK internally (session_context.py:151) but still logged
# at WARNING. No functional impact â€” suppress to ERROR to stop polluting Cloud Run logs.
logging.getLogger("google_adk.google.adk.tools.mcp_tool.session_context").setLevel(logging.ERROR)

# Lazy Firestore client â€” not constructed at import time so that
# `import main` works without GCP credentials (e.g. in pytest collection).
_activity_db: _firestore.Client | None = None
_activity_db_lock = threading.Lock()


def _get_activity_db() -> _firestore.Client | None:
    global _activity_db
    with _activity_db_lock:
        if _activity_db is None:
            try:
                _activity_db = _firestore.Client()
            except Exception as _e:
                try:
                    from servers.alerting import alert_admin, HIGH
                    alert_admin(HIGH, "activity_db_unavailable", f"Firestore client init failed: {str(_e)[:200]}")
                except Exception:
                    pass
                return None
        return _activity_db


def _safe_create_task(coro, name: str = "unnamed") -> "asyncio.Task":
    """asyncio.create_task wrapper that logs unhandled background task exceptions."""
    task = asyncio.create_task(coro, name=name)
    def _log_exc(t):
        if not t.cancelled() and t.exception() is not None:
            logger.error(json.dumps({"event": "background_task_exception", "task": name, "error": str(t.exception())[:300]}))
    task.add_done_callback(_log_exc)
    return task


def _firestore_add(log_entry: dict) -> None:
    """Blocking Firestore write â€” always called on a thread pool worker."""
    db = _get_activity_db()
    if db is None:
        return
    try:
        db.collection("agent_activity").add(log_entry)
    except Exception as e:
        logger.warning(json.dumps({"event": "firestore_write_failed", "error": str(e)[:200]}))


def _run_db_hygiene_sync() -> dict:
    """Delete agent_activity docs older than DB_HYGIENE_RETENTION_DAYS days (default 90).

    Runs on a thread pool worker.
    """
    from datetime import timedelta
    retention_days = int(os.environ.get("DB_HYGIENE_RETENTION_DAYS", "90"))
    db = _get_activity_db()
    if db is None:
        return {"status": "error", "message": "Firestore unavailable"}
    cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()
    try:
        docs = list(db.collection("agent_activity").where("timestamp", "<", cutoff).stream())
        deleted = 0
        batch = db.batch()
        count = 0
        for doc in docs:
            batch.delete(doc.reference)
            count += 1
            deleted += 1
            if count >= 500:
                batch.commit()
                batch = db.batch()
                count = 0
        if count > 0:
            batch.commit()
        # Sweep stale agent_sessions (>24h inactive)
        cutoff_sessions = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        stale = db.collection("agent_sessions").where("updated_at", "<", cutoff_sessions).limit(200).stream()
        deleted_sessions = 0
        for sdoc in stale:
            sdoc.reference.delete()
            deleted_sessions += 1
        if deleted_sessions:
            logger.info("db_hygiene: deleted %d stale sessions", deleted_sessions)
        # Sweep old pending_enroute_checks (keep 7 days)
        cutoff_enroute = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
        try:
            stale_enroute = list(db.collection("pending_enroute_checks")
                                 .where("date", "<", cutoff_enroute).limit(500).stream())
            for edoc in stale_enroute:
                edoc.reference.delete()
            if stale_enroute:
                logger.info("db_hygiene: deleted %d stale enroute_check docs", len(stale_enroute))
        except Exception as enroute_exc:
            logger.warning("db_hygiene: enroute_checks sweep failed: %s", str(enroute_exc)[:100])
        # Sweep old whatsapp_messages (same retention window)
        cutoff_wa = datetime.now(timezone.utc) - timedelta(days=retention_days)
        try:
            stale_wa = list(db.collection("whatsapp_messages")
                            .where("timestamp", "<", cutoff_wa)
                            .limit(500).stream())
            for wdoc in stale_wa:
                wdoc.reference.delete()
            if stale_wa:
                logger.info("db_hygiene: deleted %d stale whatsapp_message docs", len(stale_wa))
        except Exception as wa_exc:
            logger.warning("db_hygiene: whatsapp_messages sweep failed: %s", str(wa_exc)[:100])
        # Sweep whatsapp_rate_limits older than 24h
        cutoff_wa_rl = datetime.now(timezone.utc) - timedelta(hours=24)
        try:
            stale_wa_rl = list(db.collection("whatsapp_rate_limits")
                               .where("created_at", "<", cutoff_wa_rl).limit(500).stream())
            for doc in stale_wa_rl:
                doc.reference.delete()
            if stale_wa_rl:
                logger.info("db_hygiene: deleted %d stale whatsapp_rate_limits docs", len(stale_wa_rl))
        except Exception as exc_wa_rl:
            logger.warning("db_hygiene: whatsapp_rate_limits sweep failed: %s", str(exc_wa_rl)[:100])

        # Sweep _sched_dedup older than 24h (started_at is a float epoch)
        cutoff_sched = time.time() - 86400
        try:
            stale_sched = list(db.collection("_sched_dedup")
                               .where("started_at", "<", cutoff_sched).limit(500).stream())
            for doc in stale_sched:
                doc.reference.delete()
            if stale_sched:
                logger.info("db_hygiene: deleted %d stale _sched_dedup docs", len(stale_sched))
        except Exception as exc_sched:
            logger.warning("db_hygiene: _sched_dedup sweep failed: %s", str(exc_sched)[:100])

        # Sweep whatsapp_message_dedup older than 24h (expires_at is a float epoch)
        try:
            stale_wa_dedup = list(db.collection("whatsapp_message_dedup")
                                  .where("expires_at", "<", time.time()).limit(500).stream())
            for doc in stale_wa_dedup:
                doc.reference.delete()
            if stale_wa_dedup:
                logger.info("db_hygiene: deleted %d expired whatsapp_message_dedup docs", len(stale_wa_dedup))
        except Exception as exc_wa_dedup:
            logger.warning("db_hygiene: whatsapp_message_dedup sweep failed: %s", str(exc_wa_dedup)[:100])

        # Sweep clerk_timetable_sends and contractor_evening_reminders older than 2 days
        _cutoff_date = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
        for _coll in ("clerk_timetable_sends", "contractor_evening_reminders"):
            try:
                stale_coll = [d for d in db.collection(_coll).limit(500).stream()
                              if d.id[:10] < _cutoff_date]
                for doc in stale_coll:
                    doc.reference.delete()
                if stale_coll:
                    logger.info("db_hygiene: deleted %d stale %s docs", len(stale_coll), _coll)
            except Exception as exc_coll:
                logger.warning("db_hygiene: %s sweep failed: %s", _coll, str(exc_coll)[:100])

        # Trim monday_webhook_cooldown keys older than 24h
        try:
            _cooldown_ref = db.collection("system_state").document("monday_webhook_cooldown")
            _cooldown_snap = _cooldown_ref.get()
            if _cooldown_snap.exists:
                _cooldown_data = _cooldown_snap.to_dict() or {}
                _cutoff_iso = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
                _trimmed = {k: v for k, v in _cooldown_data.items()
                            if isinstance(v, str) and v >= _cutoff_iso}
                if len(_trimmed) < len(_cooldown_data):
                    _cooldown_ref.set(_trimmed)
                    logger.info("db_hygiene: trimmed %d stale monday_webhook_cooldown keys",
                                len(_cooldown_data) - len(_trimmed))
        except Exception as exc_mw:
            logger.warning("db_hygiene: monday_webhook_cooldown trim failed: %s", str(exc_mw)[:100])

        # Sweep xero_webhook_events older than 24h (ts is a float epoch)
        try:
            _xero_wh_cutoff = time.time() - 86400
            _stale_xero_wh = list(
                db.collection("xero_webhook_events")
                .where("ts", "<", _xero_wh_cutoff)
                .limit(500)
                .stream()
            )
            for _doc in _stale_xero_wh:
                _doc.reference.delete()
            if _stale_xero_wh:
                logger.info(json.dumps({
                    "event": "db_hygiene_xero_webhook_events",
                    "deleted": len(_stale_xero_wh),
                }))
        except Exception as _exc_xero_wh:
            logger.warning(json.dumps({
                "event": "db_hygiene_xero_webhook_events_failed",
                "error": str(_exc_xero_wh)[:100],
            }))

        return {"status": "success", "task": "db_hygiene", "deleted": deleted, "deleted_sessions": deleted_sessions}
    except Exception as exc:
        return {"status": "error", "message": str(exc)[:200]}


def _monday_api_request_sync(query: str, variables: dict, timeout: int = 15) -> dict:
    """Monday.com API POST with 3-attempt exponential backoff retry."""
    import requests as _req_m
    import time as _time_m
    monday_api_key = os.environ.get("MONDAY_API_KEY", "")
    last_exc: Exception = RuntimeError("no attempts made")
    for attempt in range(3):
        try:
            resp = _req_m.post(
                "https://api.monday.com/v2",
                json={"query": query, "variables": variables},
                headers={"Authorization": monday_api_key, "API-Version": "2026-01"},
                timeout=timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except (_req_m.exceptions.ConnectionError, _req_m.exceptions.Timeout, _req_m.exceptions.HTTPError) as exc:
            last_exc = exc
            if attempt < 2:
                _time_m.sleep(2 ** attempt)  # 1s, 2s
    raise last_exc


def _precompute_enroute_checks_sync() -> dict:
    """Read tomorrow's jobs and write pending_enroute_checks to Firestore.

    Called after send_clerk_timetables LLM task completes. No LLM involved.
    Doc ID format: {date}_{clerk_name}_{HHMM} â€” idempotent on retry.
    """
    from datetime import timedelta
    from zoneinfo import ZoneInfo

    monday_api_key = os.environ.get("MONDAY_API_KEY", "")
    if not monday_api_key:
        return {"status": "error", "message": "MONDAY_API_KEY not set"}

    db = _get_activity_db()
    if db is None:
        return {"status": "error", "message": "Firestore unavailable"}

    london = ZoneInfo("Europe/London")
    now_london = datetime.now(london)
    tomorrow = (now_london + timedelta(days=1)).date()
    tomorrow_str = tomorrow.strftime("%Y-%m-%d")

    # Query inventories board for tomorrow's jobs
    query = """
    query ($board_id: ID!, $date_str: String!) {
      boards(ids: [$board_id]) {
        items_page(limit: 100, query_params: {
          rules: [{column_id: "date", compare_value: ["EXACT", $date_str]}]
        }) {
          items {
            id
            name
            column_values(ids: ["inspector", "dup__of_hour0__1", "address", "customer", "access___info", "spec", "report___type", "contact_number"]) {
              id
              text
            }
          }
        }
      }
    }
    """
    try:
        data = _monday_api_request_sync(query, {"board_id": "18392817962", "date_str": tomorrow_str})
        items = (data.get("data", {}).get("boards", [{}])[0]
                 .get("items_page", {}).get("items", []))
    except Exception as exc:
        return {"status": "error", "message": f"Monday query failed: {str(exc)[:150]}"}

    # Query working_schedule board for clerk phones
    phone_query = """
    query ($board_id: ID!) {
      boards(ids: [$board_id]) {
        items_page(limit: 100) {
          items {
            name
            column_values(ids: ["phone_mm5htbwr"]) { id text }
          }
        }
      }
    }
    """
    try:
        phone_data = _monday_api_request_sync(phone_query, {"board_id": "9529643731"})
        clerk_items = (phone_data.get("data", {}).get("boards", [{}])[0]
                       .get("items_page", {}).get("items", []))
        clerk_phones: dict[str, str] = {
            item["name"].strip(): (
                next((cv["text"] for cv in item.get("column_values", [])
                      if cv.get("id") == "phone_mm5htbwr" and cv.get("text")), "")
            )
            for item in clerk_items
        }
    except Exception as exc:
        logger.warning("_precompute_enroute_checks_sync: phone lookup failed: %s", str(exc)[:100])
        clerk_phones = {}

    stored = 0
    batch = db.batch()
    batch_count = 0
    min_send_hour = 6
    min_send_minute = 30

    for item in items:
        col_map = {cv["id"]: cv["text"] for cv in item.get("column_values", [])}
        clerk_name = col_map.get("inspector", "").strip()
        job_time_str = col_map.get("dup__of_hour0__1", "").strip()
        job_address = col_map.get("address", "").strip()
        if not clerk_name or not job_address:
            continue
        clerk_phone = clerk_phones.get(clerk_name, "")

        # Parse job time (expected "HH:MM" or "HH:MM AM/PM" or "HH:MM - HH:MM" range)
        job_hour, job_minute = 9, 0
        try:
            start = job_time_str.split("-")[0].strip()
            parts = start.replace("am", "").replace("pm", "").replace("AM", "").replace("PM", "").strip()
            h, m = parts.split(":")[:2]
            job_hour, job_minute = int(h), int(m)
            if "pm" in job_time_str.lower() and job_hour < 12:
                job_hour += 12
        except Exception:
            pass  # fallback to 09:00

        job_dt = datetime(tomorrow.year, tomorrow.month, tomorrow.day,
                          job_hour, job_minute, tzinfo=london)
        send_dt = job_dt - timedelta(hours=1)

        # Clamp minimum send time to 06:30 London
        min_send_dt = datetime(tomorrow.year, tomorrow.month, tomorrow.day,
                               min_send_hour, min_send_minute, tzinfo=london)
        if send_dt < min_send_dt:
            send_dt = min_send_dt

        hhmm = f"{job_hour:02d}{job_minute:02d}"
        safe_clerk = clerk_name.replace(" ", "_").replace("/", "_")
        doc_id = f"{tomorrow_str}_{safe_clerk}_{hhmm}"

        doc_ref = db.collection("pending_enroute_checks").document(doc_id)
        batch.set(doc_ref, {
            "clerk_name": clerk_name,
            "clerk_phone": clerk_phone,
            "job_address": job_address,
            "job_time": job_dt.isoformat(),
            "send_at": send_dt.isoformat(),
            "sent": False,
            "responded": False,
            "response": None,
            "date": tomorrow_str,
            "customer_name": col_map.get("customer", ""),
            "service_type": col_map.get("report___type", ""),
        }, merge=True)
        batch_count += 1
        stored += 1
        if batch_count >= 500:
            batch.commit()
            batch = db.batch()
            batch_count = 0

    if batch_count > 0:
        batch.commit()

    # --- Contractor jobs (2026 Landlord safety certificates & Cleaning board) ---
    contractor_job_query = """
    query ($board_id: ID!, $date_str: String!) {
      boards(ids: [$board_id]) {
        items_page(limit: 200, query_params: {
          rules: [{column_id: "date", compare_value: ["EXACT", $date_str]}]
        }) {
          items {
            id
            name
            column_values(ids: ["inspector", "dup__of_hour0__1", "address", "customer", "report___type", "job_status_mkkr74pn"]) {
              id
              text
            }
          }
        }
      }
    }
    """
    try:
        contractor_data = _monday_api_request_sync(contractor_job_query, {"board_id": "18397426458", "date_str": tomorrow_str})
        contractor_items = (contractor_data.get("data", {}).get("boards", [{}])[0]
                            .get("items_page", {}).get("items", []))
    except Exception as exc:
        logger.warning("_precompute_enroute_checks_sync: contractor board query failed: %s", str(exc)[:100])
        contractor_items = []

    # Query Acme CONTRACTORS directory for phone numbers
    contractor_phone_query = """
    query ($board_id: ID!) {
      boards(ids: [$board_id]) {
        items_page(limit: 100) {
          items {
            name
            column_values(ids: ["phone_mm5jztsp"]) { id text }
          }
        }
      }
    }
    """
    try:
        cp_data = _monday_api_request_sync(contractor_phone_query, {"board_id": "18423594400"})
        cp_items = (cp_data.get("data", {}).get("boards", [{}])[0]
                    .get("items_page", {}).get("items", []))
        contractor_phones: dict[str, str] = {
            item["name"].strip(): (
                next((cv["text"] for cv in item.get("column_values", [])
                      if cv.get("id") == "phone_mm5jztsp" and cv.get("text")), "")
            )
            for item in cp_items
        }
    except Exception as exc:
        logger.warning("_precompute_enroute_checks_sync: contractor phone lookup failed: %s", str(exc)[:100])
        contractor_phones = {}

    batch = db.batch()
    batch_count = 0
    for item in contractor_items:
        col_map = {cv["id"]: cv["text"] for cv in item.get("column_values", [])}
        if col_map.get("job_status_mkkr74pn", "").strip() == "Cancelled":
            continue
        contractor_name = col_map.get("inspector", "").strip()
        job_time_str = col_map.get("dup__of_hour0__1", "").strip()
        job_address = col_map.get("address", "").strip()
        if not contractor_name or not job_address:
            continue
        contractor_phone = contractor_phones.get(contractor_name, "")

        job_hour, job_minute = 9, 0
        try:
            start = job_time_str.split("-")[0].strip()
            parts = start.replace("am", "").replace("pm", "").replace("AM", "").replace("PM", "").strip()
            h, m = parts.split(":")[:2]
            job_hour, job_minute = int(h), int(m)
            if "pm" in job_time_str.lower() and job_hour < 12:
                job_hour += 12
        except Exception:
            pass

        job_dt = datetime(tomorrow.year, tomorrow.month, tomorrow.day,
                          job_hour, job_minute, tzinfo=london)
        send_dt = job_dt - timedelta(hours=1)
        min_send_dt = datetime(tomorrow.year, tomorrow.month, tomorrow.day,
                               min_send_hour, min_send_minute, tzinfo=london)
        if send_dt < min_send_dt:
            send_dt = min_send_dt

        hhmm = f"{job_hour:02d}{job_minute:02d}"
        safe_name = contractor_name.replace(" ", "_").replace("/", "_")
        doc_id = f"{tomorrow_str}_{safe_name}_{hhmm}_c"  # _c suffix avoids clash with clerk doc IDs

        doc_ref = db.collection("pending_enroute_checks").document(doc_id)
        batch.set(doc_ref, {
            "clerk_name": contractor_name,
            "clerk_phone": contractor_phone,
            "job_address": job_address,
            "job_time": job_dt.isoformat(),
            "send_at": send_dt.isoformat(),
            "sent": False,
            "responded": False,
            "response": None,
            "date": tomorrow_str,
            "customer_name": col_map.get("customer", ""),
            "service_type": col_map.get("report___type", ""),
            "source": "contractor",
        }, merge=True)
        batch_count += 1
        stored += 1
        if batch_count >= 500:
            batch.commit()
            batch = db.batch()
            batch_count = 0

    if batch_count > 0:
        batch.commit()

    return {"status": "success", "stored": stored, "date": tomorrow_str}


def _send_clerk_timetables_sync() -> dict:
    """Send next-day timetables to clerks via WhatsApp template. No LLM involved.

    Runs at 20:00 London time via Cloud Scheduler. Queries inventories board for
    tomorrow's jobs, groups by inspector, builds timetable text, and sends via Twilio.
    Followed by _precompute_enroute_checks_sync() to set up next-morning polls.
    """
    from datetime import timedelta
    from zoneinfo import ZoneInfo
    from servers.whatsapp_server import (
        send_twilio_api_request,
        TWILIO_ACCOUNT_SID,
        TWILIO_WHATSAPP_NUMBER,
        TWILIO_TIMETABLE_TEMPLATE_SID,
        _normalize_phone,
    )
    from servers.whatsapp_logging import log_whatsapp_message

    monday_api_key = os.environ.get("MONDAY_API_KEY", "")
    if not monday_api_key:
        return {"status": "error", "message": "MONDAY_API_KEY not set"}

    if not TWILIO_TIMETABLE_TEMPLATE_SID:
        return {"status": "error", "message": "TWILIO_TIMETABLE_TEMPLATE_SID not set"}

    db = _get_activity_db()
    if db is None:
        return {"status": "error", "message": "Firestore unavailable"}

    london = ZoneInfo("Europe/London")
    now_london = datetime.now(london)
    tomorrow = (now_london + timedelta(days=1)).date()
    tomorrow_str = tomorrow.strftime("%Y-%m-%d")
    tomorrow_display = tomorrow.strftime("%A %d %B %Y")
    now_iso = now_london.isoformat()

    # Query inventories board for tomorrow's jobs
    job_query = """
    query ($board_id: ID!, $date_str: String!) {
      boards(ids: [$board_id]) {
        items_page(limit: 200, query_params: {
          rules: [{column_id: "date", compare_value: ["EXACT", $date_str]}]
        }) {
          items {
            id
            name
            column_values(ids: ["inspector", "dup__of_hour0__1", "report___type", "address", "customer", "access___info", "spec"]) {
              id
              text
            }
          }
        }
      }
    }
    """
    try:
        raw = _monday_api_request_sync(job_query, {"board_id": "18392817962", "date_str": tomorrow_str})
        items = (raw.get("data", {}).get("boards", [{}])[0]
                 .get("items_page", {}).get("items", []))
        logger.info(json.dumps({
            "event": "clerk_timetables_monday_query",
            "tomorrow_str": tomorrow_str,
            "items_returned": len(items),
            "sample_inspector": (
                next((
                    {cv["id"]: cv["text"] for cv in item.get("column_values", [])}.get("inspector", "")
                    for item in items[:1]
                ), None)
            ),
            "monday_errors": raw.get("errors"),
        }))
    except Exception as exc:
        return {"status": "error", "message": f"Monday jobs query failed: {str(exc)[:150]}"}

    # Query working schedule board for clerk phones (column: phone_mm5htbwr)
    phone_query = """
    query ($board_id: ID!) {
      boards(ids: [$board_id]) {
        items_page(limit: 100) {
          items {
            name
            column_values(ids: ["phone_mm5htbwr"]) { id text }
          }
        }
      }
    }
    """
    try:
        cl_items = (_monday_api_request_sync(phone_query, {"board_id": "9529643731"})
                    .get("data", {}).get("boards", [{}])[0]
                    .get("items_page", {}).get("items", []))
        clerk_phones: dict[str, str] = {
            item["name"].strip(): (
                next((cv["text"] for cv in item.get("column_values", [])
                      if cv.get("id") == "phone_mm5htbwr" and cv.get("text")), "")
            )
            for item in cl_items
        }
    except Exception as exc:
        logger.warning("_send_clerk_timetables_sync: phone lookup failed: %s", str(exc)[:100])
        clerk_phones = {}

    # Group jobs by clerk (inspector label)
    clerk_jobs: dict[str, list[dict]] = {}
    items_no_inspector = 0
    for item in items:
        col_map = {cv["id"]: (cv["text"] or "") for cv in item.get("column_values", [])}
        clerk_name = col_map.get("inspector", "").strip()
        if not clerk_name:
            items_no_inspector += 1
            continue
        clerk_jobs.setdefault(clerk_name, []).append(col_map)
    logger.info(json.dumps({
        "event": "clerk_timetables_grouped",
        "total_items": len(items),
        "items_no_inspector": items_no_inspector,
        "clerks_found": list(clerk_jobs.keys()),
        "clerk_phones_loaded": len(clerk_phones),
    }))

    from_formatted = (TWILIO_WHATSAPP_NUMBER if TWILIO_WHATSAPP_NUMBER.startswith("whatsapp:")
                      else f"whatsapp:{TWILIO_WHATSAPP_NUMBER}")
    endpoint = f"Accounts/{TWILIO_ACCOUNT_SID}/Messages.json"

    sent_count = 0
    skipped: list[str] = []

    for clerk_name, jobs in clerk_jobs.items():
        phone = clerk_phones.get(clerk_name, "").strip()
        if not phone:
            logger.warning(json.dumps({
                "event": "clerk_timetable_skipped",
                "reason": "no_phone",
                "clerk_name": clerk_name,
            }))
            skipped.append(f"{clerk_name} (no phone)")
            continue

        # Cross-invocation dedup: one timetable per clerk per day
        safe_name = clerk_name.replace(" ", "_").replace("/", "_")
        dedup_doc_id = f"{tomorrow_str}_{safe_name}"
        try:
            dedup_ref = db.collection("clerk_timetable_sends").document(dedup_doc_id)
            _dedup_doc = dedup_ref.get()
            _dedup_data = _dedup_doc.to_dict() if _dedup_doc.exists else None
            if _dedup_data and _dedup_data.get("sent"):
                logger.info(json.dumps({
                    "event": "clerk_timetable_skipped",
                    "reason": "already_sent",
                    "clerk_name": clerk_name,
                }))
                continue
        except Exception:
            pass

        # Parse hour for each job, sort chronologically
        def _parse_hour(col_map: dict) -> tuple[int, int]:
            raw = col_map.get("dup__of_hour0__1", "").strip()
            if not raw:
                return 9, 0
            try:
                # Monday.com returns time ranges like "09:00 - 09:30"; take start time only
                start = raw.split("-")[0].strip()
                parts = start.replace("am", "").replace("pm", "").replace("AM", "").replace("PM", "").strip()
                h, m = parts.split(":")[:2]
                hh, mm = int(h), int(m)
                if "pm" in raw.lower() and hh < 12:
                    hh += 12
                return hh, mm
            except Exception:
                return 9, 0

        jobs_sorted = sorted(jobs, key=lambda j: _parse_hour(j))

        # Build timetable text â€” no newlines (Twilio ContentVariables rejects them, error 21656)
        job_parts = []
        for i, job in enumerate(jobs_sorted, 1):
            hh, mm = _parse_hour(job)
            time_str = f"{hh:02d}:{mm:02d}"
            service = job.get("report___type", "").strip()
            address = job.get("address", "").strip()
            customer = job.get("customer", "").strip()
            part = f"{i}) {time_str}"
            if service:
                part += f" {service}"
            if address:
                part += f" - {address}"
            if customer:
                part += f" ({customer})"
            job_parts.append(part)
        timetable_text = " | ".join(job_parts)

        to_formatted = f"whatsapp:{_normalize_phone(phone)}"
        payload = {
            "From": from_formatted,
            "To": to_formatted,
            "ContentSid": TWILIO_TIMETABLE_TEMPLATE_SID,
            "ContentVariables": json.dumps({"1": clerk_name, "2": tomorrow_display, "3": timetable_text}),
        }

        try:
            result = send_twilio_api_request(endpoint, payload)
            dedup_ref.set({
                "sent": True,
                "sent_at": now_iso,
                "clerk_name": clerk_name,
                "job_count": len(jobs),
                "date": tomorrow_str,
                "message_sid": result.get("sid", ""),
            })
            log_whatsapp_message(
                direction="outbound",
                message_id=result.get("sid", ""),
                from_phone=TWILIO_WHATSAPP_NUMBER,
                to_phone=phone,
                body=f"Timetable for {clerk_name} ({tomorrow_display}): {timetable_text}",
                message_type="timetable",
                status="sent",
                contact_name=clerk_name,
                metadata={"timetable_date": tomorrow_display, "job_count": len(jobs), "date": tomorrow_str},
            )
            sent_count += 1
            logger.info(json.dumps({
                "event": "send_clerk_timetable",
                "clerk_name": clerk_name,
                "job_count": len(jobs),
                "sid": result.get("sid", ""),
                "status": "success",
                "date": tomorrow_str,
            }))
        except Exception as exc:
            logger.error(json.dumps({
                "event": "send_clerk_timetable",
                "clerk_name": clerk_name,
                "status": "error",
                "error": str(exc)[:300],
                "content_sid": TWILIO_TIMETABLE_TEMPLATE_SID,
                "content_variables_preview": json.dumps({"1": clerk_name, "2": tomorrow_display, "3": timetable_text[:100]}),
            }))
            skipped.append(f"{clerk_name} (send error)")
            try:
                from servers.alerting import alert_admin, HIGH
                alert_admin(HIGH, "clerk_timetable_send_failed", f"{clerk_name}: {str(exc)[:200]}")
            except Exception:
                pass

    return {"status": "success", "sent": sent_count, "skipped": skipped, "date": tomorrow_str}


def _send_contractor_evening_reminders_sync() -> dict:
    """Send evening-before WhatsApp confirmations to contractors with tomorrow's jobs.

    Runs at 18:00 London time via Cloud Scheduler. No LLM involved.
    Uses Twilio API directly. Sends a confirmation request to each contractor
    assigned a job tomorrow on the contractor jobs board.
    """
    from datetime import timedelta
    from zoneinfo import ZoneInfo
    from servers.whatsapp_server import (
        send_twilio_api_request,
        TWILIO_ACCOUNT_SID,
        TWILIO_WHATSAPP_NUMBER,
        _normalize_phone,
    )
    from servers.whatsapp_logging import log_whatsapp_message

    monday_api_key = os.environ.get("MONDAY_API_KEY", "")
    if not monday_api_key:
        return {"status": "error", "message": "MONDAY_API_KEY not set"}

    db = _get_activity_db()
    if db is None:
        return {"status": "error", "message": "Firestore unavailable"}

    london = ZoneInfo("Europe/London")
    now_london = datetime.now(london)
    tomorrow = (now_london + timedelta(days=1)).date()
    tomorrow_str = tomorrow.strftime("%Y-%m-%d")
    now_iso = now_london.isoformat()  # used for Firestore sent_at timestamp

    # Query contractor jobs board for tomorrow's jobs
    job_query = """
    query ($board_id: ID!, $date_str: String!) {
      boards(ids: [$board_id]) {
        items_page(limit: 200, query_params: {
          rules: [{column_id: "date", compare_value: ["EXACT", $date_str]}]
        }) {
          items {
            id
            name
            column_values(ids: ["inspector", "dup__of_hour0__1", "address", "report___type", "job_status_mkkr74pn"]) {
              id
              text
            }
          }
        }
      }
    }
    """
    try:
        items = (_monday_api_request_sync(job_query, {"board_id": "18397426458", "date_str": tomorrow_str})
                 .get("data", {}).get("boards", [{}])[0]
                 .get("items_page", {}).get("items", []))
    except Exception as exc:
        return {"status": "error", "message": f"Monday query failed: {str(exc)[:150]}"}

    # Query Acme CONTRACTORS directory for phone numbers
    phone_query = """
    query ($board_id: ID!) {
      boards(ids: [$board_id]) {
        items_page(limit: 100) {
          items {
            name
            column_values(ids: ["phone_mm5jztsp"]) { id text }
          }
        }
      }
    }
    """
    try:
        cp_items = (_monday_api_request_sync(phone_query, {"board_id": "18423594400"})
                    .get("data", {}).get("boards", [{}])[0]
                    .get("items_page", {}).get("items", []))
        contractor_phones: dict[str, str] = {
            item["name"].strip(): (
                next((cv["text"] for cv in item.get("column_values", [])
                      if cv.get("id") == "phone_mm5jztsp" and cv.get("text")), "")
            )
            for item in cp_items
        }
    except Exception as exc:
        logger.warning("_send_contractor_evening_reminders_sync: phone lookup failed: %s", str(exc)[:100])
        contractor_phones = {}

    evening_template_sid = os.environ.get("TWILIO_CONTRACTOR_EVENING_TEMPLATE_SID", "")
    if not evening_template_sid:
        return {"status": "error", "message": "TWILIO_CONTRACTOR_EVENING_TEMPLATE_SID not set â€” required for business-initiated WhatsApp"}

    from_formatted = (TWILIO_WHATSAPP_NUMBER if TWILIO_WHATSAPP_NUMBER.startswith("whatsapp:")
                      else f"whatsapp:{TWILIO_WHATSAPP_NUMBER}")
    endpoint = f"Accounts/{TWILIO_ACCOUNT_SID}/Messages.json"

    sent_count = 0
    skipped = 0
    seen: set[str] = set()  # deduplicate per contractor per day

    for item in items:
        col_map = {cv["id"]: cv["text"] for cv in item.get("column_values", [])}
        if col_map.get("job_status_mkkr74pn", "").strip() == "Cancelled":
            continue
        contractor_name = col_map.get("inspector", "").strip()
        job_time = col_map.get("dup__of_hour0__1", "").strip()
        job_address = col_map.get("address", "").strip()
        service_type = col_map.get("report___type", "").strip()
        if not contractor_name or not job_address:
            continue

        dedup_key = f"{tomorrow_str}_{contractor_name}"
        if dedup_key in seen:
            logger.warning(json.dumps({
                "event": "evening_reminder_multi_job_skipped",
                "contractor_name": contractor_name,
                "skipped_address": job_address,
                "skipped_time": job_time,
            }))
            continue
        seen.add(dedup_key)

        phone = contractor_phones.get(contractor_name, "").strip()
        if not phone:
            logger.warning(json.dumps({
                "event": "evening_reminder_skipped",
                "reason": "no_phone",
                "contractor_name": contractor_name,
            }))
            skipped += 1
            continue

        # Cross-invocation dedup: skip if already sent this run or a prior retry
        safe_name = contractor_name.replace(" ", "_").replace("/", "_")
        reminder_doc_id = f"{tomorrow_str}_{safe_name}"
        reminder_ref = db.collection("contractor_evening_reminders").document(reminder_doc_id)
        try:
            _snap = reminder_ref.get()
            if _snap.exists and (_snap.to_dict() or {}).get("sent"):
                continue
        except Exception:
            pass  # Firestore error â€” fail open, allow send

        to_formatted = f"whatsapp:{_normalize_phone(phone)}"
        job_label = f"{service_type} at {job_time}" if service_type and job_time else job_time or service_type

        payload = {
            "From": from_formatted,
            "To": to_formatted,
            "ContentSid": evening_template_sid,
            "ContentVariables": json.dumps({"1": contractor_name, "2": job_label, "3": job_address}),
        }

        try:
            result = send_twilio_api_request(endpoint, payload)
            reminder_ref.set({
                "sent": True,
                "sent_at": now_iso,
                "contractor_name": contractor_name,
                "job_address": job_address,
                "message_sid": result.get("sid", ""),
                "date": tomorrow_str,
            })
            log_whatsapp_message(
                direction="outbound",
                message_id=result.get("sid", ""),
                from_phone=TWILIO_WHATSAPP_NUMBER,
                to_phone=phone,
                body=f"Evening reminder for {contractor_name}: {job_label} at {job_address}",
                message_type="evening_reminder",
                status="sent",
                contact_name=contractor_name,
                metadata={"job_address": job_address, "job_time": job_time, "date": tomorrow_str},
            )
            sent_count += 1
            logger.info(json.dumps({
                "event": "evening_reminder_sent",
                "contractor_name": contractor_name,
                "sid": result.get("sid", ""),
            }))
        except Exception as exc:
            logger.error(json.dumps({
                "event": "evening_reminder_send_failed",
                "contractor_name": contractor_name,
                "error": str(exc)[:150],
            }))
            try:
                from servers.alerting import alert_admin, HIGH
                alert_admin(HIGH, "contractor_reminder_send_failed", f"{contractor_name}: {str(exc)[:200]}")
            except Exception:
                pass

    return {"status": "success", "sent": sent_count, "skipped": skipped, "date": tomorrow_str}


def _poll_enroute_checks_sync() -> dict:
    """Check pending_enroute_checks and send due en-route WhatsApp checks.

    System-level task (no LLM). Runs every 30 min 07:00-18:00 via Cloud Scheduler.
    Uses Twilio API directly â€” no MCP round-trip needed.
    """
    from zoneinfo import ZoneInfo
    from servers.whatsapp_server import (
        send_twilio_api_request,
        TWILIO_ACCOUNT_SID,
        TWILIO_WHATSAPP_NUMBER,
        TWILIO_STATUS_CHECK_TEMPLATE_SID,
        _normalize_phone,
    )
    from servers.whatsapp_logging import log_whatsapp_message

    if not TWILIO_STATUS_CHECK_TEMPLATE_SID:
        return {"status": "error", "message": "TWILIO_STATUS_CHECK_TEMPLATE_SID not set â€” required for business-initiated WhatsApp"}

    db = _get_activity_db()
    if db is None:
        return {"status": "error", "message": "Firestore unavailable"}

    london = ZoneInfo("Europe/London")
    now = datetime.now(london)
    today_str = now.strftime("%Y-%m-%d")
    now_iso = now.isoformat()

    try:
        docs = list(
            db.collection("pending_enroute_checks")
            .where("date", "==", today_str)
            .where("sent", "==", False)
            .stream()
        )
    except Exception as exc:
        return {"status": "error", "message": f"Firestore query failed: {str(exc)[:150]}"}

    checked = 0
    sent_count = 0
    for doc in docs:
        data = doc.to_dict()
        send_at_str = data.get("send_at", "")
        try:
            from datetime import datetime as _dt
            send_at = _dt.fromisoformat(send_at_str)
            if send_at.tzinfo is None:
                send_at = send_at.replace(tzinfo=london)
        except Exception as exc:
            logger.error(json.dumps({
                "event": "deferred_send_bad_timestamp",
                "doc_id": doc.id,
                "send_at_raw": (send_at_str or "")[:50],
                "error": str(exc)[:200],
            }))
            try:
                from servers.alerting import alert_admin, HIGH
                alert_admin(HIGH, "deferred_send_parse_failed",
                            f"doc {doc.id}: bad send_at '{(send_at_str or '')[:30]}' â€” message lost")
            except Exception:
                pass
            continue

        if send_at > now:
            continue  # not due yet

        checked += 1
        clerk_phone = data.get("clerk_phone", "").strip()
        clerk_name = data.get("clerk_name", "Clerk")
        job_address = data.get("job_address", "your job location")

        if not clerk_phone:
            logger.warning(json.dumps({
                "event": "enroute_check_skipped",
                "reason": "no_phone",
                "clerk_name": clerk_name,
                "doc_id": doc.id,
            }))
            doc.reference.update({"sent": True, "sent_at": now_iso, "skip_reason": "no_phone"})
            continue

        to_formatted = f"whatsapp:{_normalize_phone(clerk_phone)}"
        from_formatted = TWILIO_WHATSAPP_NUMBER if TWILIO_WHATSAPP_NUMBER.startswith("whatsapp:") else f"whatsapp:{TWILIO_WHATSAPP_NUMBER}"
        endpoint = f"Accounts/{TWILIO_ACCOUNT_SID}/Messages.json"

        payload = {
            "From": from_formatted,
            "To": to_formatted,
            "ContentSid": TWILIO_STATUS_CHECK_TEMPLATE_SID,
            "ContentVariables": json.dumps({"1": clerk_name, "2": job_address}),
        }

        try:
            result = send_twilio_api_request(endpoint, payload)
            doc.reference.update({
                "sent": True,
                "sent_at": now_iso,
                "message_sid": result.get("sid", ""),
            })
            sent_count += 1
            logger.info(json.dumps({
                "event": "enroute_check_sent",
                "clerk_name": clerk_name,
                "doc_id": doc.id,
                "sid": result.get("sid", ""),
            }))
            log_whatsapp_message(
                direction="outbound",
                message_id=result.get("sid", ""),
                from_phone=TWILIO_WHATSAPP_NUMBER,
                to_phone=clerk_phone,
                body=f"En-route check for {clerk_name}: Are you on your way to {job_address}?",
                message_type="enroute_check",
                status="sent",
                contact_name=clerk_name,
                metadata={"job_address": job_address, "job_time": data.get("job_time", "")},
            )
        except Exception as exc:
            logger.error(json.dumps({
                "event": "enroute_check_send_failed",
                "clerk_name": clerk_name,
                "doc_id": doc.id,
                "error": str(exc)[:150],
            }))

    return {"status": "success", "checked": checked, "sent": sent_count, "date": today_str}


# Bound concurrent Firestore log writes to avoid exhausting the thread pool
# when many tool calls fire log_event simultaneously (e.g. 25-call sessions).
_log_semaphore: asyncio.Semaphore | None = None


def _get_log_semaphore() -> asyncio.Semaphore:
    global _log_semaphore
    if _log_semaphore is None:
        sem = asyncio.Semaphore(10)
        _log_semaphore = sem
    return _log_semaphore


# Per-toolset respawn locks (keyed by global_name) â€” independent subprocess
# deaths must not serialize behind each other's close+respawn+re-warm cycle.
_respawn_locks: dict[str, asyncio.Lock] = {}


def _get_respawn_lock(global_name: str) -> asyncio.Lock:
    lock = _respawn_locks.get(global_name)
    if lock is None:
        lock = asyncio.Lock()
        _respawn_locks[global_name] = lock
    return lock


async def _firestore_add_bounded(log_entry: dict) -> None:
    async with _get_log_semaphore():
        try:
            await asyncio.to_thread(_firestore_add, log_entry)
        except RuntimeError:
            # Thread pool shut down during container teardown â€” log already on stderr, drop Firestore write silently
            pass


_REDACT_SUBSTRINGS = frozenset({"email", "phone", "api_key", "password", "secret", "body", "content"})
_REDACT_EXACT = frozenset({"token"})


_ACTIVITY_LOG_MODE = os.environ.get("ACTIVITY_LOG_FIRESTORE", "all")
_TOOL_EVENTS = frozenset({"tool_call", "tool_result"})


def log_event(agent: str, event: str, **kwargs):
    """Structured logging for operational events.

    stderr write is always synchronous. Firestore write is offloaded to the
    default thread pool when called from inside an async context (ADK runner,
    plugin callbacks) so it never blocks the event loop.

    Set ACTIVITY_LOG_FIRESTORE=llm_only to skip Firestore writes for tool_call
    and tool_result events (they still go to Cloud Logging via stderr). Cuts
    ~84% of agent_activity Firestore writes with no loss of cost-analysis data.
    """
    filtered = {
        k: "[REDACTED]" if (k.lower() in _REDACT_EXACT or any(s in k.lower() for s in _REDACT_SUBSTRINGS)) else v
        for k, v in kwargs.items()
    }
    log_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "agent": agent,
        "event": event,
        **filtered,
    }
    logger.info(json.dumps(log_entry))

    if _ACTIVITY_LOG_MODE == "llm_only" and event in _TOOL_EVENTS:
        return

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is not None:
        _safe_create_task(_firestore_add_bounded(log_entry), "_firestore_add")
    else:
        _firestore_add(log_entry)


# ---------------------------------------------------------------------------
# MCP server environment and paths
# ---------------------------------------------------------------------------

# Absolute path to servers/ â€” CWD-independent, works under any Dockerfile WORKDIR
_SERVERS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "servers")

# Environment variables shared by every MCP subprocess regardless of server.
_MCP_COMMON_VARS = {
    "GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT",
    "PATH", "HOME", "PYTHONPATH", "PYTHONUNBUFFERED", "PYTHONUTF8",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "K_SERVICE", "K_REVISION", "K_CONFIGURATION",
    "GOOGLE_CLOUD_LOCATION", "GOOGLE_GENAI_USE_VERTEXAI",
}

# Per-server allowlists â€” each server receives only what it needs.
# A missing key falls back to an empty set (server gets common vars only).
_MCP_SERVER_VARS: dict[str, set[str]] = {
    "gmail_server.py":     {"GMAIL_TOKEN_JSON", "GMAIL_CRED_JSON"},
    "monday_server.py":    {"MONDAY_API_KEY"},
    "whatsapp_server.py":  {"TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN",
                            "TWILIO_WHATSAPP_NUMBER", "ADMIN_PHONE",
                            "TWILIO_STATUS_CHECK_TEMPLATE_SID",
                            "TWILIO_TIMETABLE_TEMPLATE_SID",
                            "TWILIO_ADMIN_ALERT_TEMPLATE_SID"},
                            # TWILIO_CONTRACTOR_EVENING_TEMPLATE_SID is NOT forwarded here â€”
                            # it is read directly from os.environ in _send_contractor_evening_reminders_sync
                            # (main process only, never used inside the MCP subprocess).
    "xero_server.py":      {"XERO_CLIENT_ID", "XERO_CLIENT_SECRET",
                            "XERO_TOKEN_JSON", "XERO_TOKEN_FILE",
                            "INVOICE_CREATION_ENABLED", "XERO_INVOICE_STATUS",
                            "INVOICE_MAX_GBP", "XERO_VAT_TAXTYPE",
                            "XERO_PAYMENT_TERMS_DAYS", "XERO_TENANT_ID"},
    "ops_server.py":       {"GOOGLE_MAPS_API_KEY"},
    "reporting_server.py": set(),
    "locking_server.py":   set(),
    "rag_server.py":       set(),
}


def _build_env(script_name: str) -> dict[str, str]:
    """Return a filtered env dict for an MCP subprocess.

    Each server gets the common baseline plus its own allowlisted vars.
    Keys absent from os.environ are silently omitted.
    """
    allowed_keys = _MCP_COMMON_VARS | _MCP_SERVER_VARS.get(script_name, set())
    return {k: v for k, v in os.environ.items() if k in allowed_keys}


_role_filter = RoleAwareToolFilter()

# Per-toolset respawn attempt counters for exponential backoff.
# Reset to 0 on successful respawn. Key = global_name.
_respawn_attempts: dict[str, int] = {}
_MAX_RESPAWN_ATTEMPTS = 5
_RESPAWN_BACKOFF_BASE = 10  # seconds; delays: 10, 20, 40, 80, 160
_RESPAWN_RECOVERY_INTERVAL = 300  # seconds between slow retry attempts after exhaustion


def _schedule_respawn(global_name: str, old_ts=None) -> None:
    """Fire-and-forget: schedule an immediate or delayed respawn.

    Called synchronously from inside SerializedMcpToolset's wrapped run_async
    the moment a call fails in a way that suggests a dead/hung subprocess â€”
    NOT from the outer route-handler except blocks, because
    ReflectAndRetryToolPlugin often absorbs the first tool failure per turn
    and never lets the exception propagate that far (see mcp_runtime.py's
    module docstring). Scheduling a task here means recovery starts
    immediately regardless of whether the plugin re-raises.

    Exponential backoff: attempt N is delayed _RESPAWN_BACKOFF_BASE * 2^N seconds.
    After _MAX_RESPAWN_ATTEMPTS, stops retrying and logs CRITICAL.
    """
    attempt = _respawn_attempts.get(global_name, 0)
    if attempt >= _MAX_RESPAWN_ATTEMPTS:
        import json as _json, sys as _sys
        msg = {"severity": "CRITICAL", "event": "toolset_respawn_exhausted",
               "toolset": global_name, "attempts": attempt}
        print(_json.dumps(msg), file=_sys.stderr, flush=True)
        log_event("system", "toolset_respawn_exhausted", toolset=global_name,
                  attempts=attempt, severity="CRITICAL")
        try:
            _safe_create_task(_send_mcp_down_alert(global_name), "mcp_down_alert")
        except RuntimeError:
            pass
        # Slow periodic recovery: one attempt every _RESPAWN_RECOVERY_INTERVAL seconds.
        # Resets counter to MAX-1 so _respawn_ts gets exactly one attempt; if that
        # attempt fails, _respawn_ts -> _schedule_respawn loops back here for another
        # wait. This prevents permanent death without spamming attempts or alerts.
        try:
            _loop = asyncio.get_running_loop()
            def _recovery_retry(gn=global_name):
                _respawn_attempts[gn] = _MAX_RESPAWN_ATTEMPTS - 1
                try:
                    _safe_create_task(_respawn_ts(gn), "respawn_ts_recovery")
                except RuntimeError:
                    pass
            _loop.call_later(_RESPAWN_RECOVERY_INTERVAL, _recovery_retry)
        except RuntimeError:
            pass
        return
    _respawn_attempts[global_name] = attempt + 1
    delay = _RESPAWN_BACKOFF_BASE * (2 ** attempt) if attempt > 0 else 0

    loop = asyncio.get_running_loop()
    if delay > 0:
        loop.call_later(delay, lambda: _safe_create_task(_respawn_ts(global_name, old_ts), "respawn_ts"))
    else:
        _safe_create_task(_respawn_ts(global_name, old_ts), "respawn_ts")


def _toolset(script_name: str, global_name: str) -> SerializedMcpToolset:
    """Create a SerializedMcpToolset for a single MCP stdio server.

    SerializedMcpToolset (mcp_runtime.py) serializes concurrent calls into the
    subprocess and fails fast on a hung/dead one â€” see that module's docstring
    for why: production crashes correlated with concurrent tool calls hitting
    a shared subprocess simultaneously. global_name is this toolset's module-
    level variable name (e.g. "rag_ts"), used to trigger its own respawn.
    """
    return SerializedMcpToolset(
        connection_params=StdioConnectionParams(
            server_params=StdioServerParameters(
                command=sys.executable,
                args=[os.path.join(_SERVERS_DIR, script_name)],
                env=_build_env(script_name),
            ),
            timeout=60,  # ADK default is 5s; rag_server Vertex warmup alone takes ~5-7s
        ),
        tool_filter=_role_filter,
        on_dead=lambda ts: _schedule_respawn(global_name, ts),
    )


def _toolset_scoped(script_name: str, allowed_tools: frozenset, global_name: str) -> SerializedMcpToolset:
    """Create a SerializedMcpToolset with per-agent tool scoping on top of role-based filtering.

    Unrestricted roles are additionally restricted to allowed_tools, cutting the
    Monday schema from 20 tools down to 5-11 per agent.
    Restricted roles remain governed by CUSTOMER_ALLOWED_TOOLS as before.
    """
    return SerializedMcpToolset(
        connection_params=StdioConnectionParams(
            server_params=StdioServerParameters(
                command=sys.executable,
                args=[os.path.join(_SERVERS_DIR, script_name)],
                env=_build_env(script_name),
            ),
            timeout=60,
        ),
        tool_filter=AgentScopedToolFilter(allowed_tools),
        on_dead=lambda ts: _schedule_respawn(global_name, ts),
    )


# Shared toolsets â€” one subprocess per server, reused across all agents.
# Exception: Monday/Xero/WhatsApp use per-agent instances scoped to each agent's tool subset.
rag_ts       = _toolset("rag_server.py", "rag_ts")
# Locking scoped to exclude cleanup_expired_locks â€” admin maintenance tool,
# agents must not call it autonomously.
locking_ts   = _toolset_scoped("locking_server.py", AGENT_LOCKING_TOOLS, "locking_ts")
gmail_ts     = _toolset("gmail_server.py", "gmail_ts")
# Per-agent Monday toolsets â€” each scoped to the tools that agent actually calls.
# Reduces Monday schema from 20 tools to 5-11 per agent for unrestricted roles.
dispatcher_monday_ts = _toolset_scoped("monday_server.py", DISPATCHER_MONDAY_TOOLS, "dispatcher_monday_ts")
finance_monday_ts    = _toolset_scoped("monday_server.py", FINANCE_MONDAY_TOOLS, "finance_monday_ts")
ops_monday_ts        = _toolset_scoped("monday_server.py", OPS_MONDAY_TOOLS, "ops_monday_ts")
# Per-agent Xero toolsets â€” dispatcher gets create_invoice only; finance gets full set.
dispatcher_xero_ts   = _toolset_scoped("xero_server.py", DISPATCHER_XERO_TOOLS, "dispatcher_xero_ts")
finance_xero_ts      = _toolset_scoped("xero_server.py", FINANCE_XERO_TOOLS, "finance_xero_ts")
reporting_ts = _toolset("reporting_server.py", "reporting_ts")
# Finance only needs invoice-specific reporting tools (3/7); dispatcher gets full set.
finance_reporting_ts = _toolset_scoped("reporting_server.py", FINANCE_REPORTING_TOOLS, "finance_reporting_ts")
ops_ts       = _toolset("ops_server.py", "ops_ts")
# Per-agent WhatsApp toolsets â€” dispatcher handles customer messaging; ops handles contractor/admin.
dispatcher_whatsapp_ts = _toolset_scoped("whatsapp_server.py", DISPATCHER_WHATSAPP_TOOLS, "dispatcher_whatsapp_ts")
ops_whatsapp_ts        = _toolset_scoped("whatsapp_server.py", OPS_WHATSAPP_TOOLS, "ops_whatsapp_ts")

# Registry mapping each shared toolset's module-level variable name to how to
# rebuild it, used by _respawn_ts to recreate a crashed subprocess in place.
# allowed_tools=None -> built via _toolset() (role-filtered only);
# a frozenset -> built via _toolset_scoped() (role + per-agent scoping).
_TOOLSET_REGISTRY: tuple[tuple[str, str, frozenset | None], ...] = (
    ("rag_ts", "rag_server.py", None),
    ("locking_ts", "locking_server.py", AGENT_LOCKING_TOOLS),
    ("gmail_ts", "gmail_server.py", None),
    ("dispatcher_monday_ts", "monday_server.py", DISPATCHER_MONDAY_TOOLS),
    ("finance_monday_ts", "monday_server.py", FINANCE_MONDAY_TOOLS),
    ("ops_monday_ts", "monday_server.py", OPS_MONDAY_TOOLS),
    ("dispatcher_xero_ts", "xero_server.py", DISPATCHER_XERO_TOOLS),
    ("finance_xero_ts", "xero_server.py", FINANCE_XERO_TOOLS),
    ("reporting_ts", "reporting_server.py", None),
    ("finance_reporting_ts", "reporting_server.py", FINANCE_REPORTING_TOOLS),
    ("ops_ts", "ops_server.py", None),
    ("dispatcher_whatsapp_ts", "whatsapp_server.py", DISPATCHER_WHATSAPP_TOOLS),
    ("ops_whatsapp_ts", "whatsapp_server.py", OPS_WHATSAPP_TOOLS),
)


def _all_toolsets() -> list[SerializedMcpToolset]:
    """Current toolset instances for every registry entry.

    Reads through globals() rather than a fixed tuple so it always reflects
    the latest instance after a respawn (_respawn_ts rebinds the module-level
    global in place) â€” used by startup warmup and shutdown cleanup.
    """
    return [globals()[name] for name, _script, _allowed in _TOOLSET_REGISTRY]


# Exceptions that indicate the MCP child process has died, its stdio pipe broke,
# or a tool call hit SerializedMcpToolset's fail-fast timeout.
_PIPE_ERRORS = (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, EOFError,
                McpError, BrokenResourceError)


def _iter_agents(root):
    """Yield root and every agent reachable via sub_agents or AgentTool-wrapped tools.

    Walks the whole hub -> dispatcher/finance/ops -> finance workflow
    sub-agents -> booking-confirmation loop tree, so a respawned toolset gets
    patched into every holder, not just the top-level agents.
    """
    seen: set[int] = set()
    stack = [root]
    while stack:
        agent = stack.pop()
        if id(agent) in seen:
            continue
        seen.add(id(agent))
        yield agent
        for sub in getattr(agent, "sub_agents", None) or []:
            stack.append(sub)
        for tool in getattr(agent, "tools", None) or []:
            wrapped_agent = getattr(tool, "agent", None)  # AgentTool wraps a sub-agent
            if wrapped_agent is not None:
                stack.append(wrapped_agent)


def _patch_toolset_everywhere(old_ts, new_ts) -> int:
    """Replace old_ts with new_ts in every agent's .tools list under hub_agent.

    Returns the number of references patched (for logging).
    """
    patched = 0
    for agent in _iter_agents(hub_agent):
        tools = getattr(agent, "tools", None)
        if not tools:
            continue
        for i, tool in enumerate(tools):
            if tool is old_ts:
                tools[i] = new_ts
                patched += 1
    return patched


async def _respawn_ts(global_name: str, old_ts=None) -> None:
    """Close a dead MCP subprocess toolset and spawn a fresh one in its place.

    Generalizes the old gmail-only respawn to all 13 shared toolsets: closes
    the old subprocess, rebuilds a replacement with the same factory used at
    startup, rebinds the module-level global, and patches every agent in the
    tree that held the old reference (dispatcher/finance/ops, the finance
    workflow sub-agents, and the booking-confirmation loop â€” see _iter_agents).
    Locked per-toolset (not globally) so an unrelated subprocess dying around
    the same time doesn't wait behind this one's close+respawn+re-warm cycle.

    old_ts: the instance that triggered the respawn. When provided, the identity
    check bails out if the global has already been replaced by a concurrent
    respawn â€” preventing a second on_dead from killing the healthy replacement.
    Callers that omit old_ts (backup sweep, retry) skip the identity check.
    """
    async with _get_respawn_lock(global_name):
        current = globals().get(global_name)
        if old_ts is not None and current is not old_ts:
            # A concurrent respawn already replaced the subprocess â€” bail out.
            log_event("system", "toolset_respawn_skipped",
                      toolset=global_name, reason="already_replaced")
            return
        spec = next((s for s in _TOOLSET_REGISTRY if s[0] == global_name), None)
        if spec is None or current is None:
            return
        _, script, allowed_tools = spec

        log_event("system", "toolset_respawn", toolset=global_name, status="starting")
        try:
            await current.close()
        except Exception as exc:
            log_event("system", "toolset_respawn", toolset=global_name,
                      status="close_warning", error=str(exc)[:200])

        new_ts = (
            _toolset(script, global_name) if allowed_tools is None
            else _toolset_scoped(script, allowed_tools, global_name)
        )
        try:
            await new_ts.get_tools()
        except Exception as exc:
            log_event("system", "toolset_respawn", toolset=global_name,
                      status="failed", error=str(exc)[:400], severity="ERROR")
            import json as _json, sys as _sys
            print(_json.dumps({"severity": "ERROR", "event": "toolset_respawn_failed",
                               "toolset": global_name, "error": str(exc)[:400]}),
                  file=_sys.stderr, flush=True)
            try:
                await new_ts.close()
            except Exception:
                pass
            # current is already closed (_closed=True); on_dead won't fire from
            # future tool calls. Schedule a retry so recovery isn't stuck. Pass
            # current so the retry's identity check stays coherent; close() is
            # idempotent so the retry re-closing this dead toolset is a no-op.
            _schedule_respawn(global_name, current)
            # Re-alert if still failing after max attempts (alert_admin dedup prevents spam)
            if _respawn_attempts.get(global_name, 0) >= _MAX_RESPAWN_ATTEMPTS:
                try:
                    _safe_create_task(_send_mcp_down_alert(global_name), "mcp_down_reawakeup")
                except RuntimeError:
                    pass
            return
        globals()[global_name] = new_ts
        _respawn_attempts[global_name] = 0  # reset backoff counter on success
        patched = _patch_toolset_everywhere(current, new_ts)
        current._respawn_ready.set()
        log_event("system", "toolset_respawn", toolset=global_name,
                  status="success", patched_refs=patched)


async def _respawn_dead_toolsets() -> None:
    """Sweep every shared toolset and respawn whichever subprocess session is dead.

    This is a backup path â€” the primary recovery trigger is
    SerializedMcpToolset's on_dead callback, fired immediately from inside a
    failing tool call (see mcp_runtime.py and _schedule_respawn). This sweep
    additionally runs from the route handlers on any MCP-adjacent error
    (broken pipe, McpError, BrokenResourceError, timeout) in case the failure
    surfaced there without on_dead having fired (e.g. an error outside a
    wrapped tool call). A generic exception at that layer doesn't identify
    which of the 13 subprocesses died, so this checks each one's live session
    state and only respawns the ones that are actually disconnected â€”
    checked and respawned concurrently so one dead subprocess doesn't delay
    detection of another.
    """
    async def _check_and_respawn(global_name: str) -> None:
        ts = globals().get(global_name)
        manager = getattr(ts, "_mcp_session_manager", None) if ts is not None else None
        if manager is None:
            return
        session_entry = (getattr(manager, "_sessions", None) or {}).get("stdio_session")
        if session_entry is None:
            return  # never connected yet â€” nothing to respawn
        session = session_entry[0]
        try:
            is_dead = manager._is_session_disconnected(session)
        except Exception:
            is_dead = False
        if is_dead:
            await _respawn_ts(global_name, ts)

    await asyncio.gather(
        *(_check_and_respawn(name) for name, _script, _allowed in _TOOLSET_REGISTRY),
        return_exceptions=True,
    )


# ---------------------------------------------------------------------------
# InstructionProviders â€” wraps zero-arg callables so ADK fires them per-request
# static_instruction: stable body fingerprinted and cached by ADK ContextCacheConfig.
# instruction (InstructionProvider): volatile datetime + per-session client rules,
# redirected by ADK into a user content message so the cache fingerprint stays stable.
# Static bodies computed once at startup â€” board IDs and env vars don't change at runtime.
# ---------------------------------------------------------------------------
_dispatcher_static_inst = get_dispatcher_static()
_finance_static_inst = get_finance_static()
_ops_static_inst = get_ops_static()

# Roles that indicate autonomous / scheduled processing â€” no confirmation gate needed.
# Manual dashboard/CLI requests have _source_role="dashboard_admin"/"system" (absent from set).
_AUTO_PROCESS_ROLES = {"email_customer", "whatsapp_customer", "monday_webhook", "scheduled_task"}


def _is_interactive(ctx) -> bool:
    """True when the request is operator-initiated (manual chat/CLI); False for auto-process paths.

    Scheduled tasks and event-driven paths legitimately need autonomous writes (SLA booking
    creation, roll-call WhatsApp, reconciliation, etc.) â€” they must NOT be gated.
    Uses _source_role from session state set at session creation (route_and_execute /
    stream_chat / invoke_agent_directly).  Null-safe: missing state â†’ interactive (safe default).
    """
    return (getattr(ctx, "state", None) or {}).get("_source_role", "") not in _AUTO_PROCESS_ROLES


# Per-agent interactive-mode gate directives.  Injected into the dynamic instruction (user content,
# off the cache fingerprint) only on manual chat/CLI requests â€” static_instruction unchanged.
# Each gate is intent-scoped so explicit act requests ("reply toâ€¦", "chase invoices") still fire.
_INTERACTIVE_GATE = (
    "## INTERACTIVE MODE â€” operator-initiated request\n"
    "Act ONLY on what the operator's message explicitly asks.\n"
    "A read/triage request ('check emails', 'any new emails', 'what's in the inbox', 'list emails') "
    "is READ-ONLY: call list_unread_emails(include_processed=True, limit=15) â€” this shows ALL recent "
    "inbox mail including already-handled emails. Window inferred from request phrasing (default 1d, "
    "'this week'â†’window='7d', etc.; ask if ambiguous). Each email has unread (bool) and processed (bool) "
    "flags â€” annotate already-handled items as '(already handled)' in your summary. Do NOT draft replies "
    "to processed emails unless the operator explicitly asks. Then call read_email_body for emails needing detail.\n"
    "If a booking-request email is present, SURFACE it (sender, service, address, date) and ASK "
    "whether to create the booking â€” do NOT call create_inventory_booking, update Monday columns, "
    "draft_reply, create_lead, or transfer_to_agent(booking_confirmation_loop) without explicit "
    "operator go-ahead.\n"
    "Take a write action ONLY when the operator's message explicitly requests it "
    "(e.g. 'reply toâ€¦', 'book this', 'create the booking', 'send the confirmation').\n"
    "When unsure whether an action was requested, ask first."
)

_FINANCE_GATE = (
    "## INTERACTIVE MODE â€” operator-initiated request\n"
    "Act ONLY on what the operator's message explicitly asks.\n"
    "A read/lookup request ('check invoices', 'what's owed', 'reconciliation status', 'show P&L', "
    "'any outstanding payments') is READ-ONLY: call list_unpaid_invoices / get_invoice_details / "
    "reporting tools, then summarise what you found.\n"
    "Do NOT chase invoices, call draft_reply, update Monday columns, or delegate to "
    "reconciliation_pipeline / chase_pipeline without an explicit operator request "
    "(e.g. 'chase the overdue invoices', 'run reconciliation', 'draft a reminder for X').\n"
    "When unsure whether an action was requested, ask first."
)

_OPS_GATE = (
    "## INTERACTIVE MODE â€” operator-initiated request\n"
    "Act ONLY on what the operator's message explicitly asks.\n"
    "A read/lookup request ('check the schedule', 'what's on Friday', 'open tasks', 'route for X', "
    "'feasibility check') is READ-ONLY: call get_ops_status / check_job_feasibility / "
    "optimize_route / calculate_travel_time / get_board_items, then summarise.\n"
    "Do NOT call create_inventory_booking, update Monday columns, duplicate_board, or send ANY "
    "WhatsApp (send_whatsapp_message, send_contractor_status_check, send_admin_alert) without an "
    "explicit operator request. WhatsApp messages are irreversible â€” ALWAYS confirm before sending.\n"
    "When unsure whether an action was requested, ask first."
)


def _dispatcher_instruction(_ctx: ReadonlyContext) -> str:
    """Volatile context only (datetime + client rules) â€” redirected to user content by ADK."""
    parts = [get_dispatcher_dynamic_ctx()]
    rules = get_agent_rules("dispatcher")
    if rules:
        parts.append("## CLIENT RULES (follow these exactly):\n" + "\n".join(f"- {r}" for r in rules))
    # Interactive-mode gate: read intents are read-only; no writes without explicit request.
    # Scheduled/event roles bypass â€” SLA auto-processing unchanged.  See _is_interactive().
    if _is_interactive(_ctx):
        parts.append(_INTERACTIVE_GATE)
    return "\n\n".join(parts)


def _finance_instruction(_ctx: ReadonlyContext) -> str:
    """Volatile context only (datetime + client rules) â€” redirected to user content by ADK."""
    parts = [get_finance_dynamic_ctx()]
    rules = get_agent_rules("finance")
    if rules:
        parts.append("## CLIENT RULES (follow these exactly):\n" + "\n".join(f"- {r}" for r in rules))
    if _is_interactive(_ctx):
        parts.append(_FINANCE_GATE)
    return "\n\n".join(parts)


def _ops_instruction(_ctx: ReadonlyContext) -> str:
    """Volatile context only (datetime + client rules) â€” redirected to user content by ADK."""
    parts = [get_ops_dynamic_ctx()]
    rules = get_agent_rules("ops")
    if rules:
        parts.append("## CLIENT RULES (follow these exactly):\n" + "\n".join(f"- {r}" for r in rules))
    if _is_interactive(_ctx):
        parts.append(_OPS_GATE)
    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Agent GenerateContentConfig definitions
# ---------------------------------------------------------------------------
_hub_gen_cfg = types.GenerateContentConfig(
    max_output_tokens=512,
    thinking_config=types.ThinkingConfig(
        thinking_budget=0,      # hub just routes â€” no thinking needed
    ),
)

_dispatcher_gen_cfg = types.GenerateContentConfig(
    max_output_tokens=4096,
    thinking_config=types.ThinkingConfig(
        thinking_level=types.ThinkingLevel.HIGH,  # Gemini 3 dynamic reasoning; deep on multi-booking, light on simple queries
    ),
)

_finance_ops_gen_cfg = types.GenerateContentConfig(
    max_output_tokens=4096,
    thinking_config=types.ThinkingConfig(
        thinking_level=types.ThinkingLevel.HIGH,  # Gemini 3 dynamic reasoning; deep on reconciliation, light on simple lookups
    ),
)

# Ops spoke on gemini-3.7-flash â€” MEDIUM thinking balances scheduling reasoning with the 240s stream timeout.
# HIGH was causing 4+ timeouts/day on complex fleet tasks (get_working_schedule â†’ NÃ—send_contractor_status_check
# chains exhausted the 15-LLM-call stream budget before the 240s wall).
_ops_gen_cfg = types.GenerateContentConfig(
    max_output_tokens=4096,
    thinking_config=types.ThinkingConfig(
        thinking_level=types.ThinkingLevel.MEDIUM,
    ),
)

# ---------------------------------------------------------------------------
# Wave 3: SequentialAgent pipelines for Finance
# (defined before finance_agent so they can be added as sub_agents)
# ---------------------------------------------------------------------------
reconciliation_pipeline, chase_pipeline, invoice_sweep = create_finance_workflows(
    locking_ts=locking_ts,
    xero_ts=finance_xero_ts,
    monday_ts=finance_monday_ts,
    gmail_ts=gmail_ts,
    rag_ts=rag_ts,
    reporting_ts=finance_reporting_ts,
    agent_model=FlexGemini(model=AGENT_MODEL),
    lite_model=FlexGemini(model=HUB_MODEL),   # lock/fetch steps are mechanical â€” flash-lite + no thinking
)

# Wave 3J: LoopAgent for quality-gated booking confirmation drafts
booking_confirmation_loop = create_booking_confirmation_loop(
    rag_ts=rag_ts,
    agent_model=FlexGemini(model=AGENT_MODEL),
    lite_model=FlexGemini(model=HUB_MODEL),
)

# ---------------------------------------------------------------------------
# Agent definitions
# ---------------------------------------------------------------------------



dispatcher_agent = LlmAgent(
    model=FlexGemini(model=AGENT_MODEL),
    name="dispatcher",
    description="Default triage agent: handles general inbound (email, WhatsApp, Monday "
                "webhooks), new bookings, client queries, and RAG knowledge lookups.",
    static_instruction=_dispatcher_static_inst,
    instruction=_dispatcher_instruction,
    tools=[rag_ts, locking_ts, gmail_ts, dispatcher_monday_ts, dispatcher_whatsapp_ts, reporting_ts, dispatcher_xero_ts,
           classify_coverage,  # deterministic postcode coverage gate (audit H1) â€” auto-wrapped as FunctionTool
           AgentTool(agent=booking_confirmation_loop)],
    generate_content_config=_dispatcher_gen_cfg,
)

finance_agent = LlmAgent(
    model=FlexGemini(model=AGENT_MODEL),
    name="finance",
    description="Finance Sentinel: handles invoices, payments, chasing, reconciliation, "
                "and all Xero operations.",
    static_instruction=_finance_static_inst,
    instruction=_finance_instruction,
    tools=[locking_ts, finance_xero_ts, finance_monday_ts, gmail_ts, rag_ts, finance_reporting_ts, approval_tool],
    generate_content_config=_finance_ops_gen_cfg,
    sub_agents=[reconciliation_pipeline, chase_pipeline, invoice_sweep],
)

ops_agent = LlmAgent(
    model=FlexGemini(model=OPS_MODEL),  # gemini-3.7-flash default; override via OPS_MODEL env var
    name="ops",
    description="Ops Controller: handles logistics, routing, contractor status, "
                "travel time calculations, and job feasibility checks.",
    static_instruction=_ops_static_inst,
    instruction=_ops_instruction,
    tools=[locking_ts, ops_ts, ops_monday_ts, rag_ts, ops_whatsapp_ts],
    generate_content_config=_ops_gen_cfg,
)

# Hub instruction as InstructionProvider â€” injects direct routing hint when
# invoke_agent_directly() pre-sets _skip_hub_to in session state.
_HUB_INSTRUCTION_BASE = """You are Acme's dispatcher hub. You do not answer user requests yourself.
Your only job is to route each incoming message to exactly one sub-agent via transfer_to_agent.

CLASSIFICATION â€” determine intent first:
1. DATA QUERY (lookup/status/search): "check last booking", "what's scheduled tomorrow",
   "show me unpaid invoices", "who is inspector for X", "how many bookings this week"
2. ACTION (create/update/send/chase): "book an inventory", "send confirmation",
   "chase invoice 123", "update contractor status"

Routing rules:
- DATA QUERIES about bookings, schedules, board items, general info â†’ dispatcher
- ACTION on invoices, payments, chasing debt, reconciliation, Xero-related work â†’ finance
- ACTION on logistics, routing, contractor status, travel time, job feasibility, WhatsApp alerts to contractors â†’ ops
- Everything else â€” general triage, inbound email/WhatsApp replies, new bookings, client queries, RAG lookups, Monday webhook events â†’ dispatcher

Always transfer. Never answer yourself.

LANGUAGE: Use British English spelling, grammar, and conventions in all responses (e.g. organise, colour, whilst, centre, recognise, travelling, licence/license distinction). TIMEZONE: All times are UTC internally. Display in Europe/London timezone for all human-facing messages. All API calls are made via MCP tool servers â€” you contain decision logic only."""


def _hub_instruction(ctx: ReadonlyContext) -> str:
    """Volatile routing hint only â€” injected as user content (off the cache fingerprint).

    _HUB_INSTRUCTION_BASE is in static_instruction (cached). This function emits ONLY
    the per-request _skip_hub_to override when invoke_agent_directly() pre-sets it.
    Returns empty string on normal paths so the cache fingerprint stays stable.
    """
    skip_to = ctx.state.get("_skip_hub_to") if ctx.state else None
    if skip_to:
        return (
            f"Transfer immediately to the '{skip_to}' agent via transfer_to_agent. "
            f"No analysis or classification needed."
        )
    return ""  # static_instruction covers everything; no volatile content on normal paths


def _hub_skip_callback(callback_context, llm_request):
    """Skip hub LLM when routing target is pre-determined by invoke_agent_directly()."""
    skip_to = callback_context.state.get("_skip_hub_to") if callback_context.state else None
    if not skip_to:
        return None
    return LlmResponse(
        content=types.Content(
            role="model",
            parts=[types.Part(function_call=types.FunctionCall(
                name="transfer_to_agent",
                args={"agent_name": skip_to},
            ))],
        ),
    )


hub_agent = LlmAgent(
    model=FlexGemini(model=HUB_MODEL),
    name="hub",
    description="Root dispatcher hub â€” transfers every request to the correct sub-agent.",
    static_instruction=_HUB_INSTRUCTION_BASE,   # cacheable routing rules
    instruction=_hub_instruction,                # volatile override hint only (empty on normal paths)
    before_model_callback=_hub_skip_callback,
    sub_agents=[dispatcher_agent, finance_agent, ops_agent],
    generate_content_config=_hub_gen_cfg,
)

# ---------------------------------------------------------------------------
# App (Waves 0, 1, 4)
# ---------------------------------------------------------------------------
Acme_app = App(
    name="multi_agent_system",           # must be a valid Python identifier (no hyphens)
    root_agent=hub_agent,
    plugins=[
        RoleGuardPlugin(firestore_log_func=log_event),  # FIRST: blocks before cost counter
        CostGuardPlugin(max_tool_calls=200, max_transfers=5),
        ActivityLogPlugin(firestore_log_func=log_event),
        ReflectAndRetryToolPlugin(max_retries=1),  # 1 retry keeps transient-error resilience; 3Ã— per-item loops was primary cost amplifier
        ClientMemoryPlugin(),          # load/save per-client Firestore profiles
    ],
    events_compaction_config=EventsCompactionConfig(
        summarizer=LlmEventSummarizer(llm=FlexGemini(model=AGENT_MODEL)),  # flash for compaction: better fidelity on booking details vs flash-lite
        compaction_interval=10,        # compact every 10 invocations
        overlap_size=2,               # keep 2-invocation overlap across compactions
        token_threshold=60000,        # raised from 25k â€” prevents premature compaction on short chats
        event_retention_size=30,      # raised from 8 â€” keep raw events for full recall within a session
    ),
    resumability_config=ResumabilityConfig(is_resumable=True),
    context_cache_config=ContextCacheConfig(
        cache_intervals=1,    # lowered from 3 (2026-07-29) â€” scheduled tasks are single-invocation so
                              # cache_intervals=3 means the cache is never created for them; at 1, cache
                              # is created after the first turn and reused by subsequent runs within TTL
                              # (e.g. enroute_check_poll every 30min, invoice_sweep every 4h)
        ttl_seconds=21600,    # 6-hour TTL (raised from 60min) â€” static_instruction is fixed at startup
                              # (board IDs/env vars only), never goes stale, so a longer TTL has no
                              # correctness risk; helps bursty event-driven (Gmail push) traffic reuse
                              # the cache across gaps longer than the old 60-min window
        min_tokens=4096,      # Gemini-3.x implicit cache floor is 4096 tokens; below this Vertex never honors the cache
    ),
)

# ---------------------------------------------------------------------------
# Deterministic pre-routing (Wave 2D) â€” skips hub LLM for ~70% of requests
# ---------------------------------------------------------------------------
_TASK_AGENT_MAP: dict[str, str] = {
    "morning_roll_call":     "ops",
    "send_clerk_timetables": "ops",
    "preflight_check":       "finance",
    "chase_morning":         "finance",
    "reconciliation":        "finance",
    "verification_emails":   "dispatcher",
    "chase_afternoon":       "finance",
    "logistics_first_check": "ops",
    "finance_sweep":         "finance",
    "invoice_sweep":         "finance",
    "logistics_final_check": "ops",
    "inbox_sweep":           "dispatcher",
    "capacity_planning":     "ops",
    "financial_close":       "finance",
    "route_verification":    "ops",
    "weekly_board_dup":      "ops",
    "weekly_prep":           "ops",
    "inbox_check":           "dispatcher",
}

_EVENT_AGENT_MAP: dict[str, str] = {
    "email_received":    "dispatcher",
    "whatsapp_received": "dispatcher",
    "monday_webhook":    "dispatcher",
}

# Source roles that arrive from external events rather than scheduled tasks.
# These invocations are self-contained (all context in the task string) and
# don't benefit from cross-invocation session history, so they use ephemeral
# sessions to prevent sched- Firestore docs from growing unbounded.
_EVENT_SOURCE_ROLES: frozenset[str] = frozenset({
    "email_customer",
    "whatsapp_customer",
    "monday_webhook",
})

# Scheduled tasks that require Monday.com MCP tools to function. Pre-flight
# check skips these (with a clear log) when all Monday toolsets are down,
# instead of letting the LLM run without its core tools and compose a
# confusing "tool not found" admin alert.
_MONDAY_DEPENDENT_TASKS: frozenset[str] = frozenset({
    "logistics_final_check",
    "logistics_first_check",
    "route_verification",
    "preflight_check",
    "verification_emails",
    "capacity_planning",
    "financial_close",
    "weekly_prep",
    "weekly_board_dup",
    "inbox_check",       # dispatcher calls create_inventory_booking
    "inbox_sweep",       # dispatcher calls create_inventory_booking
    "chase_morning",     # finance uses monday_ts for NEEDS_REVIEW tasks
    "chase_afternoon",   # finance uses monday_ts for NEEDS_REVIEW tasks
    "reconciliation",    # finance uses monday_ts to update payment status columns
    "invoice_sweep",     # finance uses monday_ts for set_invoice_number
})


def _monday_mcp_healthy() -> bool:
    """Return True if at least one Monday toolset has a populated tool cache."""
    for name, _, _ in _TOOLSET_REGISTRY:
        if "monday" not in name:
            continue
        ts = globals().get(name)
        if ts is not None and getattr(ts, "_raw_tools_cache", None):
            return True
    return False


def get_mcp_health() -> dict[str, bool]:
    """Return health of all 8 MCP server categories for the /health and /ready endpoints."""
    result: dict[str, bool] = {
        "monday": False, "gmail": False, "xero": False,
        "rag": False, "locking": False, "ops": False,
        "reporting": False, "whatsapp": False,
    }
    for name, _, _ in _TOOLSET_REGISTRY:
        ts = globals().get(name)
        healthy = ts is not None and bool(getattr(ts, "_raw_tools_cache", None))
        if "monday" in name:
            result["monday"] = result["monday"] or healthy
        elif "gmail" in name:
            result["gmail"] = result["gmail"] or healthy
        elif "xero" in name:
            result["xero"] = result["xero"] or healthy
        elif "rag" in name:
            result["rag"] = result["rag"] or healthy
        elif "locking" in name:
            result["locking"] = result["locking"] or healthy
        elif "ops" in name:
            result["ops"] = result["ops"] or healthy
        elif "reporting" in name:
            result["reporting"] = result["reporting"] or healthy
        elif "whatsapp" in name:
            result["whatsapp"] = result["whatsapp"] or healthy
    return result


# Dedup tracker so repeated MCP crash alerts don't spam every 5 minutes.
_mcp_alert_last_sent: dict[str, float] = {}
_MCP_ALERT_DEDUP_SECS = 1800  # 30 minutes


async def _send_mcp_down_alert(toolset_name: str) -> None:
    """Alert admin via WhatsApp when a toolset's respawn attempts are exhausted.

    Calls Twilio REST API directly (not through MCP â€” MCP may be what's down).
    Falls back to log-only if credentials are missing or the API call fails.
    """
    log_event("system", "mcp_toolset_unrecoverable", toolset=toolset_name,
              max_attempts=_MAX_RESPAWN_ATTEMPTS)

    now = time.monotonic()
    if now - _mcp_alert_last_sent.get(toolset_name, 0.0) < _MCP_ALERT_DEDUP_SECS:
        return
    _mcp_alert_last_sent[toolset_name] = now

    try:
        from config.dashboard_settings import get_admin_phones
        admin_phones = get_admin_phones()
        account_sid = os.environ.get("TWILIO_ACCOUNT_SID", "")
        auth_token = os.environ.get("TWILIO_AUTH_TOKEN", "")
        from_number = os.environ.get("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")
        if not all([account_sid, auth_token]) or not admin_phones:
            return
        msg = (
            f"[Multi-Agent System] MCP server '{toolset_name}' unrecoverable after "
            f"{_MAX_RESPAWN_ATTEMPTS} respawn attempts. Manual intervention needed."
        )
        import httpx as _httpx
        async with _httpx.AsyncClient(timeout=10.0) as client:
            for admin_phone in admin_phones:
                to_number = admin_phone if admin_phone.startswith("whatsapp:") else f"whatsapp:{admin_phone}"
                await client.post(
                    f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
                    auth=(account_sid, auth_token),
                    data={"From": from_number, "To": to_number, "Body": msg},
                )
        logger.info(json.dumps({"event": "mcp_alert_sent", "toolset": toolset_name, "count": len(admin_phones)}))
    except Exception as exc:
        logger.warning(json.dumps({
            "event": "mcp_alert_send_failed",
            "toolset": toolset_name,
            "error": str(exc)[:200],
        }))


# ---------------------------------------------------------------------------
# Orchestrator â€” same public API as before; server.py / dashboard/app.py unchanged
# ---------------------------------------------------------------------------
class AcmeOrchestrator:
    """Manages ADK Runner and MCP toolset lifecycle."""

    USER_ID = "system"

    def __init__(self):
        self.session_service: FirestoreSessionService | None = None
        self.runner: Runner | None = None

    async def initialize(self, stack: AsyncExitStack) -> None:
        """Initialize the Runner and warm up all MCP toolsets."""
        log_event("system", "startup", status="initializing")

        self.session_service = FirestoreSessionService(project=PROJECT_ID)
        self.runner = Runner(
            app=Acme_app,
            session_service=self.session_service,
        )

        # Warm up all toolsets in parallel so cold-start cost is absorbed inside
        # initialize() (within the 60s dashboard readiness window) rather
        # than during the first user request. return_exceptions=True means one
        # slow/failing server does not block the others.
        log_event("system", "tools_loading", servers=13)
        toolsets = _all_toolsets()
        try:
            async with asyncio.timeout(55):
                warmup_results = await asyncio.gather(
                    *(ts.get_tools() for ts in toolsets),
                    return_exceptions=True,
                )
        except asyncio.TimeoutError:
            log_event("system", "tools_warmup_timeout", severity="WARNING")
            warmup_results = [TimeoutError("warmup exceeded 55s")] * len(toolsets)
        failed_pairs = [
            (ts, result) for ts, result in zip(toolsets, warmup_results)
            if isinstance(result, BaseException)
        ]
        if failed_pairs:
            failed_names = [
                os.path.basename(ts._connection_params.server_params.args[0])
                for ts, _ in failed_pairs
            ]
            for ts, exc in failed_pairs:
                name = os.path.basename(ts._connection_params.server_params.args[0])
                log_event("system", "tools_warmup_error", server=name,
                          error=str(exc)[:400], error_type=type(exc).__name__, severity="ERROR")
            log_event("system", "tools_partial_failure", failed=failed_names, severity="CRITICAL")
        log_event("system", "tools_loaded", servers=13 - len(failed_pairs))

        # Register clean shutdown on the caller's AsyncExitStack
        stack.push_async_callback(self._aclose_toolsets)

        log_event("system", "ready", status="operational")

    async def _aclose_toolsets(self) -> None:
        for ts in _all_toolsets():
            try:
                await ts.close()
            except (Exception, asyncio.CancelledError) as exc:
                log_event("system", "toolset_close_warning", error=str(exc)[:200])

    async def route_and_execute(
        self, user_input: str, session_id: str | None = None, user_id: str | None = None,
        source_role: str = "system", persist: bool = False, timeout: float = 240,
        images: list | None = None,
    ) -> tuple[str, str, str]:
        """Route input to the appropriate agent and return (agent_name, result, session_id).

        If session_id is provided, the existing session is reused (conversation memory).
        If session_id is None and persist=True, a persistent chat session is created and kept
        after the request so the caller can resume it with the returned session_id.
        If session_id is None and persist=False (default), an ephemeral session is created and
        deleted after the request (used for one-shot event/schedule invocations).
        """
        _flex_enabled.set(False)  # interactive â€” use standard priority tier
        uid = user_id or self.USER_ID
        ephemeral = session_id is None
        if ephemeral:
            session_id = f"chat-{uuid4()}" if persist else f"req-{uuid4()}"

        existing = None
        if not ephemeral:
            existing = await self.session_service.get_session(
                app_name=Acme_app.name, user_id=uid, session_id=session_id,
            )
        if existing is None:
            initial_state: dict = {"_source_role": source_role}
            if ephemeral and persist:
                initial_state["_chat_title"] = user_input.strip()[:60]
            await self.session_service.create_session(
                app_name=Acme_app.name, user_id=uid, session_id=session_id,
                state=initial_state,
            )
        elif "_source_role" not in (existing.state or {}):
            existing.state["_source_role"] = source_role
            await self.session_service.patch_state(
                app_name=Acme_app.name, user_id=uid, session_id=session_id,
                state=existing.state,
            )

        _parts = [types.Part(text=user_input)] if user_input else []
        for _img in (images or []):
            _parts.append(types.Part(inline_data=types.Blob(
                mime_type=_img["mime_type"],
                data=base64.b64decode(_img["data"]),
            )))
        new_message = types.Content(role="user", parts=_parts)

        final_text = ""
        target_agent = "dispatcher"
        _run_cfg = _NONSTREAM_CONFIG_LOW if source_role in _AUTO_PROCESS_ROLES else _NONSTREAM_CONFIG
        try:
            try:
                _t0 = time.monotonic()
                async with asyncio.timeout(timeout):
                    async for event in self.runner.run_async(
                        user_id=uid,
                        session_id=session_id,
                        new_message=new_message,
                        run_config=_run_cfg,
                    ):
                        author = getattr(event, "author", None)
                        if author in ("dispatcher", "finance", "ops"):
                            target_agent = author
                        if event.is_final_response() and event.content and event.content.parts:
                            for part in event.content.parts:
                                text = getattr(part, "text", None)
                                if text:
                                    final_text = text
                                    break
            except asyncio.TimeoutError:
                log_event("system", "route_timeout", target_agent=target_agent, elapsed_s=round(time.monotonic() - _t0, 1))
                await _respawn_dead_toolsets()
                return (target_agent, "The request timed out. Please try again with a simpler question.", session_id)
            except _PIPE_ERRORS as exc:
                log_event("system", "mcp_pipe_error", error=str(exc)[:200], severity="WARNING")
                await _respawn_dead_toolsets()
                return (target_agent, "I had a connection issue with one of my tools. Please try again.", session_id)
        finally:
            if ephemeral and not persist:
                try:
                    await self.session_service.delete_session(
                        app_name=Acme_app.name, user_id=uid, session_id=session_id,
                    )
                except Exception as exc:
                    log_event("system", "session_cleanup_failed",
                              session_id=session_id, error=str(exc)[:200])

        log_event("system", "route_complete", target_agent=target_agent, elapsed_s=round(time.monotonic() - _t0, 1))
        log_event("system", "response", target_agent=target_agent, response_length=len(final_text))
        return (target_agent, final_text, session_id)

    async def invoke_agent_directly(
        self,
        user_input: str,
        target_agent: str,
        user_id: str | None = None,
        source_role: str = "system",
        timeout: float = 240,
    ) -> tuple[str, str, str]:
        """Bypass hub LLM by pre-routing to a specific agent via session state.

        Sets _skip_hub_to in the session state so hub's InstructionProvider
        injects a direct transfer instruction, skipping the routing LLM call.
        Used for scheduled tasks and known event types (~70% of all requests).
        """
        uid = user_id or self.USER_ID
        session_id = (
            f"req-{uuid4()}" if source_role in _EVENT_SOURCE_ROLES
            else f"sched-{target_agent}-{datetime.now(timezone.utc).date().isoformat()}"
        )

        existing = await self.session_service.get_session(
            app_name=Acme_app.name, user_id=uid, session_id=session_id
        )
        if existing is None:
            await self.session_service.create_session(
                app_name=Acme_app.name,
                user_id=uid,
                session_id=session_id,
                state={"_skip_hub_to": target_agent, "_source_role": source_role},
            )
        elif "_source_role" not in (existing.state or {}):
            existing.state["_source_role"] = source_role
            await self.session_service.patch_state(
                app_name=Acme_app.name, user_id=uid, session_id=session_id,
                state=existing.state,
            )

        new_message = types.Content(
            role="user",
            parts=[types.Part(text=user_input)],
        )

        final_text = ""
        actual_agent = target_agent
        _run_cfg = _NONSTREAM_CONFIG_LOW if source_role in _AUTO_PROCESS_ROLES else _NONSTREAM_CONFIG
        _t0 = time.monotonic()
        try:
            async with asyncio.timeout(timeout):
                async for event in self.runner.run_async(
                    user_id=uid,
                    session_id=session_id,
                    new_message=new_message,
                    run_config=_run_cfg,
                ):
                    author = getattr(event, "author", None)
                    if author in ("dispatcher", "finance", "ops"):
                        actual_agent = author
                    if event.is_final_response() and event.content and event.content.parts:
                        for part in event.content.parts:
                            text = getattr(part, "text", None)
                            if text:
                                final_text = text
                                break
        except asyncio.TimeoutError:
            log_event("system", "route_timeout", target_agent=actual_agent, elapsed_s=round(time.monotonic() - _t0, 1))
            await _respawn_dead_toolsets()
            return (actual_agent, "The request timed out. Please try again with a simpler question.", session_id)
        except _PIPE_ERRORS as exc:
            log_event("system", "mcp_pipe_error", error=str(exc)[:200], severity="WARNING")
            await _respawn_dead_toolsets()
            return (actual_agent, "I had a connection issue with one of my tools. Please try again.", session_id)
        finally:
            if not session_id.startswith("sched-"):
                try:
                    await self.session_service.delete_session(
                        app_name=Acme_app.name, user_id=uid, session_id=session_id,
                    )
                except Exception as exc:
                    log_event("system", "session_cleanup_failed",
                              session_id=session_id, error=str(exc)[:200])

        log_event("system", "route_complete", target_agent=actual_agent, elapsed_s=round(time.monotonic() - _t0, 1))
        log_event("system", "response", target_agent=actual_agent, response_length=len(final_text))
        return (actual_agent, final_text, session_id)

    async def stream_chat(
        self, user_input: str, session_id: str | None = None, user_id: str | None = None,
        source_role: str = "system", persist: bool = False,
        images: list | None = None,
    ) -> AsyncGenerator[dict, None]:
        """Like route_and_execute but yields partial text chunks for SSE streaming.

        Yields dicts with keys: type ("chunk"|"final"|"error"), text, agent, session_id.
        "chunk" events are partial tokens (event.partial=True).
        "final" is the complete aggregated response.
        persist=True keeps the session after the request so the caller can resume it
        with the returned session_id (used by dashboard chat).
        """
        _flex_enabled.set(False)  # interactive â€” use standard priority tier
        _chat_start = time.monotonic()
        _timed_out = False
        uid = user_id or self.USER_ID
        ephemeral = session_id is None
        if ephemeral:
            session_id = f"chat-{uuid4()}" if persist else f"req-{uuid4()}"

        existing = None
        if not ephemeral:
            existing = await self.session_service.get_session(
                app_name=Acme_app.name, user_id=uid, session_id=session_id,
            )
        if existing is None:
            initial_state: dict = {"_source_role": source_role}
            if ephemeral and persist:
                initial_state["_chat_title"] = user_input.strip()[:60]
            await self.session_service.create_session(
                app_name=Acme_app.name, user_id=uid, session_id=session_id,
                state=initial_state,
            )
        elif "_source_role" not in (existing.state or {}):
            existing.state["_source_role"] = source_role
            await self.session_service.patch_state(
                app_name=Acme_app.name, user_id=uid, session_id=session_id,
                state=existing.state,
            )

        _parts = [types.Part(text=user_input)] if user_input else []
        for _img in (images or []):
            _parts.append(types.Part(inline_data=types.Blob(
                mime_type=_img["mime_type"],
                data=base64.b64decode(_img["data"]),
            )))
        new_message = types.Content(role="user", parts=_parts)
        target_agent = "dispatcher"
        _MAX_CHAT_QUOTA_RETRIES = 2
        _chunks_yielded = 0
        try:
            for _chat_attempt in range(_MAX_CHAT_QUOTA_RETRIES + 1):
                _quota_retry = False
                try:
                    async with asyncio.timeout(_CHAT_STREAM_TIMEOUT):
                        async for event in self.runner.run_async(
                            user_id=uid, session_id=session_id,
                            new_message=new_message, run_config=_STREAM_CONFIG,
                        ):
                            author = getattr(event, "author", None)
                            if author in ("dispatcher", "finance", "ops"):
                                target_agent = author
                            if not (event.content and event.content.parts):
                                continue
                            for part in event.content.parts:
                                text = getattr(part, "text", None)
                                if text:
                                    if event.partial:
                                        _chunks_yielded += 1
                                        yield {"type": "chunk", "text": text, "agent": target_agent, "session_id": session_id}
                                    elif event.is_final_response():
                                        _chunks_yielded += 1
                                        yield {"type": "final", "text": text, "agent": target_agent, "session_id": session_id}
                                        break
                                elif getattr(part, "function_call", None):
                                    tool_name = part.function_call.name
                                    yield {"type": "progress", "text": f"Using {tool_name}â€¦", "agent": target_agent, "session_id": session_id}
                except asyncio.CancelledError:
                    log_event("system", "chat_cancelled", session_id=session_id)
                    raise  # must re-raise â€” CancelledError is BaseException and must propagate
                except asyncio.TimeoutError:
                    _timed_out = True
                    await _respawn_dead_toolsets()
                    yield {"type": "error", "text": "The request timed out. Please try again with a simpler question."}
                except RuntimeError as exc:
                    msg = str(exc)
                    if ERR_MAX_ITERATIONS in msg or "Max number of llm calls limit" in msg:
                        log_event("system", "max_iterations_exceeded", session_id=session_id)
                        yield {"type": "error", "text": "I've made too many tool calls in this session. Please start a new conversation to continue."}
                    elif ERR_TRANSFER_LOOP in msg:
                        log_event("system", "transfer_loop_abort", session_id=session_id)
                        yield {"type": "error", "text": "I got stuck in a routing loop. Please try rephrasing your request."}
                    elif ERR_INPUT_CEILING in msg:
                        log_event("system", "input_token_ceiling_exceeded", session_id=session_id)
                        yield {"type": "error", "text": "This request used too much context. Please start a new conversation or simplify your question."}
                    else:
                        raise
                except _gapi_exceptions.InvalidArgument as exc:
                    log_event("system", "session_size_exceeded", session_id=session_id, error=str(exc)[:300])
                    if not ephemeral:
                        try:
                            await self.session_service.delete_session(
                                app_name=Acme_app.name, user_id=uid, session_id=session_id,
                            )
                        except Exception:
                            pass
                    yield {"type": "error", "text": "This conversation grew too large and has been reset. You can continue in this session â€” previous context won't be available."}
                except _PIPE_ERRORS as exc:
                    log_event("system", "mcp_pipe_error", error=str(exc)[:200], severity="WARNING")
                    await _respawn_dead_toolsets()
                    yield {"type": "error", "text": "I had a connection issue with one of my tools. Please try again."}
                except Exception as exc:
                    err = str(exc)
                    if (("RESOURCE_EXHAUSTED" in err or "429" in err
                             or "UNAVAILABLE" in err or "503" in err)
                            and _chat_attempt < _MAX_CHAT_QUOTA_RETRIES
                            and _chunks_yielded == 0):
                        _delay = (2 ** (_chat_attempt + 2)) + random.uniform(0, 2)
                        log_event("system", "chat_quota_retry", session_id=session_id,
                                  attempt=_chat_attempt + 1, delay_s=round(_delay, 2))
                        await asyncio.sleep(_delay)
                        _quota_retry = True
                    elif "RESOURCE_EXHAUSTED" in err or "429" in err:
                        log_event("system", "chat_quota_exhausted", session_id=session_id)
                        yield {"type": "error", "text": "The AI service is temporarily busy. Please try again in a moment."}
                    else:
                        raise
                if not _quota_retry:
                    break
        finally:
            log_event("system", "chat_complete", session_id=session_id,
                      elapsed_s=round(time.monotonic() - _chat_start, 1),
                      timed_out=_timed_out)
            if ephemeral and not persist:
                try:
                    await self.session_service.delete_session(
                        app_name=Acme_app.name, user_id=uid, session_id=session_id,
                    )
                except Exception as exc:
                    log_event("system", "session_cleanup_failed",
                              session_id=session_id, error=str(exc)[:200])


# =============================================================================
# EXECUTION MODES
# =============================================================================

async def run_cli_mode(orchestrator: AcmeOrchestrator):
    """Interactive CLI mode for testing."""
    log_event("system", "mode", mode="cli")
    session_id = f"cli-{uuid4()}"

    while True:
        try:
            user_input = input("\nUser: ")
            if user_input.lower() in ["quit", "exit"]:
                log_event("system", "shutdown", reason="user_request")
                break

            target_agent, result, session_id = await orchestrator.route_and_execute(
                user_input, session_id=session_id,
            )

            # CLI output for human readability
            sys.stdout.write(f"\n{target_agent.title()}: {result}\n")
            sys.stdout.flush()

        except KeyboardInterrupt:
            log_event("system", "shutdown", reason="keyboard_interrupt")
            break
        except Exception as e:
            err = str(e)
            if ERR_SPEND_CAP in err:
                sys.stdout.write(
                    "\n[System paused] Spend cap reached. Reapprove in the dashboard to resume.\n"
                )
                sys.stdout.flush()
            else:
                log_event("system", "error", error=err, error_type=type(e).__name__)


async def handle_event(orchestrator: AcmeOrchestrator, event_type: str, payload: dict) -> dict:
    """Handle incoming events (webhooks).

    Event types:
    - email_received: New email in monitored inbox
    - whatsapp_received: New WhatsApp message
    - monday_webhook: Monday.com status change or new item
    - cli_test: Interactive CLI testing
    """
    log_event("system", "event_received", event_type=event_type, payload_keys=list(payload.keys()))

    _VALID_EVENT_TYPES: frozenset[str] = frozenset({
        "email_received", "whatsapp_received", "monday_webhook", "cli_test",
    })
    if event_type not in _VALID_EVENT_TYPES:
        log_event("system", "invalid_event_type", event_type=event_type[:50])
        return {"status": "error", "event_type": event_type[:50], "message": "unknown event type"}

    _EVENT_SOURCE_ROLES = {
        "email_received":    "email_customer",
        "whatsapp_received": "whatsapp_customer",
        "monday_webhook":    "monday_webhook",
        "cli_test":          "dashboard_admin",
    }
    source_role = _EVENT_SOURCE_ROLES.get(event_type, "system")

    try:
        if event_type == "email_received":
            subject = sanitize_payload_field(payload.get("subject", "No subject"), max_len=200)
            sender  = sanitize_payload_field(payload.get("sender", "Unknown"), max_len=100)
            task = f"Process incoming email: {subject} from {sender}"
        elif event_type == "whatsapp_received":
            from_   = sanitize_payload_field(payload.get("from", "Unknown"), max_len=100)
            message = sanitize_payload_field(payload.get("message", ""), max_len=500)
            # WhatsApp carries no native thread/message id the booking tool understands â€”
            # derive a stable thread_id from Twilio's WaId (fallback to the from number)
            # and source_message_id from Twilio's MessageSid, so a WhatsApp booking routes
            # through the same Firestore idempotency lock / resolve_thread_item email
            # bookings already get (fixes duplicate bookings across channels, e.g. the
            # Julie Zhuang double-booking).
            source_message_id = sanitize_payload_field(payload.get("message_sid", ""), max_len=100)
            wa_id = sanitize_payload_field(payload.get("wa_id", ""), max_len=100)
            # If both wa_id and from_ are empty (malformed/replayed request), fall back to
            # the Twilio MessageSid so thread_id never degenerates to the bare constant
            # "whatsapp:" â€” which would collide across unrelated senders in the Firestore
            # booking lock. If source_message_id is ALSO empty, this is a genuinely
            # malformed request; thread_id ends up None, which downstream lock-key
            # resolution already falls back from (source_message_id / address+postcode+date).
            if wa_id:
                thread_id = f"whatsapp:{wa_id}"
            elif from_:
                thread_id = f"whatsapp:{from_}"
            elif source_message_id:
                thread_id = f"whatsapp:msg:{source_message_id}"
            else:
                thread_id = None
            task = f"Process WhatsApp message from {from_}: {message}"
            if source_message_id or wa_id:
                task += (
                    f"\n\n[BOOKING DEDUP CONTEXT] If you call create_inventory_booking for this "
                    f"message, pass thread_id=\"{thread_id}\" and source_message_id=\"{source_message_id}\" "
                    f"verbatim â€” this enables idempotency dedup for WhatsApp bookings the same way "
                    f"email bookings already get."
                )
        elif event_type == "monday_webhook":
            event_name = sanitize_payload_field(payload.get("event", "status_change"), max_len=50)
            item_id    = sanitize_payload_field(payload.get("item_id", "Unknown"), max_len=50)
            task = f"Process Monday.com webhook: {event_name} on item {item_id}"
        elif event_type == "cli_test":
            task = sanitize_payload_field(payload.get("message", "No message provided"), max_len=2000)
    except Exception as exc:
        log_event("system", "event_dispatch_error", event_type=event_type, error=str(exc))
        return {"status": "error", "event_type": event_type, "message": "malformed payload"}

    if event_type == "monday_webhook":
        # Layer 1: event type filter â€” only status changes and new items need the dispatcher LLM.
        # Text/date/number column edits carry no actionable signal for the agent.
        _MONDAY_STATUS_EVENTS = frozenset({"status_change", "create_item"})
        _is_relevant = (event_name in _MONDAY_STATUS_EVENTS or "status" in event_name)
        if not _is_relevant:
            log_event("system", "monday_webhook_filtered",
                      reason="non_status_event", event_name=event_name, item_id=item_id)
            return {"status": "success", "event_type": event_type, "skipped": "non_status_event"}

        # Layer 2: per-item cooldown (5 min) â€” rapid repeated changes to the same item
        # (e.g. quick status corrections) would each fire a full dispatcher LLM run.
        if not _system_state.try_claim_monday_webhook(item_id):
            log_event("system", "monday_webhook_filtered",
                      reason="item_cooldown", event_name=event_name, item_id=item_id)
            return {"status": "success", "event_type": event_type, "skipped": "item_cooldown"}

    _MAX_QUOTA_RETRIES = 3
    # A 429 can surface on ANY LLM call within a multi-step tool-using turn â€”
    # including one AFTER an earlier step already had a side effect. A whole-turn
    # replay would then re-apply that side effect. We only replay for events whose
    # mutation tools are idempotent by id: email (draft_reply dedups by
    # reply_to_message_id, create_inventory_booking dedups by thread_id) and monday
    # webhooks (column writes are set-idempotent). whatsapp_received is EXCLUDED â€”
    # send_whatsapp_message is not idempotent, so a replay would send a live customer
    # a duplicate reply. (Backlog: make send_whatsapp_message idempotent by message
    # hash, then whatsapp can rejoin the retry-safe set.)
    _RETRY_SAFE_EVENTS = frozenset({"email_received", "monday_webhook"})
    target_agent = result = None
    for _attempt in range(_MAX_QUOTA_RETRIES + 1):
        try:
            # Deterministic routing for known event types â€” skips hub LLM
            direct_agent = _EVENT_AGENT_MAP.get(event_type)
            if direct_agent:
                _event_timeout = 1800 if event_type == "email_received" else 240
                target_agent, result, _ = await orchestrator.invoke_agent_directly(
                    task, direct_agent, source_role=source_role, timeout=_event_timeout,
                )
            else:
                target_agent, result, _ = await orchestrator.route_and_execute(
                    task, source_role=source_role,
                )
            break  # success â€” exit retry loop
        except Exception as exc:
            err = str(exc)
            # 429 RESOURCE_EXHAUSTED: transient quota error â€” retry with backoff, but
            # ONLY for retry-safe events (idempotent mutations, see above) so a replay
            # cannot double-apply a side effect. cost_guard before_run_callback re-fires
            # each attempt (resets per-turn counters + checks the spend cap), so retries
            # cannot circumvent spending limits.
            if (("RESOURCE_EXHAUSTED" in err or "429" in err
                     or "UNAVAILABLE" in err or "503" in err)
                    and _attempt < _MAX_QUOTA_RETRIES
                    and event_type in _RETRY_SAFE_EVENTS):
                _delay = (2 ** (_attempt + 2)) + random.uniform(0, 2)
                log_event("system", "quota_retry", event_type=event_type,
                          attempt=_attempt + 1, delay_s=round(_delay, 2))
                await asyncio.sleep(_delay)
                continue
            # Terminal control-flow errors â€” intentional, not genuine faults. Re-raise so
            # server.py can return a non-retry 2xx response instead of a 500.
            _TERMINAL = (ERR_SPEND_CAP, ERR_MAX_ITERATIONS, ERR_TRANSFER_LOOP, ERR_INPUT_CEILING)
            if any(s in err for s in _TERMINAL) or "Max number of llm calls limit" in err:
                log_event("system", "event_terminal", event_type=event_type, reason=err[:200])
                raise
            log_event("system", "handle_event_error", event_type=event_type, error=err[:300])
            return {"status": "error", "event_type": event_type, "message": "Internal error processing event."}

    return {
        "status": "success",
        "agent": target_agent,
        "result": result
    }


async def run_scheduled_task(orchestrator: AcmeOrchestrator, task_name: str) -> dict:
    """Handle Cloud Scheduler tasks.

    Task names mapped from knowledge/daily_tasks.csv:
    - morning_roll_call: 08:00 daily (Ops)
    - preflight_check: 09:00 daily (Finance)
    - chase_morning: 09:30 daily (Finance)
    - reconciliation: 10:30 daily (Finance)
    - verification_emails: 11:30 daily (Dispatcher)
    - chase_afternoon: 15:00 daily (Finance)
    - logistics_first_check: 15:30 daily (Ops)
    - finance_sweep: 17:00 daily (Finance)
    - logistics_final_check: 17:30 daily (Ops)
    - inbox_sweep: 17:35 daily (Dispatcher)
    - capacity_planning: 18:30 daily (Ops)
    - financial_close: 19:00 daily (Finance)
    - route_verification: 20:00 daily (Ops)
    - weekly_board_dup: 20:30 Sunday only / board duplication (Ops)
    - weekly_prep: 20:30 Mon-Sat / next day prep (Ops)
    - inbox_check: Every 12 hours (Dispatcher) â€” fallback; primary via /webhook/gmail-push
    - lock_cleanup: 03:00 daily (System)
    - db_hygiene: 03:30 daily (System) â€” deletes agent_activity docs older than 30 days
    """
    log_event("system", "scheduled_task", task=task_name)

    # System tasks that bypass the LLM and run directly
    if task_name == "db_hygiene":
        result = await asyncio.to_thread(_run_db_hygiene_sync)
        if result.get("status") == "success":
            log_event("system", "db_hygiene_complete", deleted=result.get("deleted", 0))
        else:
            log_event("system", "db_hygiene_error", error=result.get("message", ""))
        return result

    if task_name == "lock_cleanup":
        from servers.locking_server import _cleanup_expired_locks_sync
        result_str = await asyncio.to_thread(_cleanup_expired_locks_sync)
        result = json.loads(result_str)
        if result.get("status") == "success":
            log_event("system", "lock_cleanup_complete", deleted=result.get("deleted_count", 0))
        else:
            log_event("system", "lock_cleanup_error", error=result.get("message", ""))
        return result

    if task_name == "renew_gmail_watch":
        from servers.gmail_server import _register_gmail_watch_sync
        result = json.loads(await asyncio.to_thread(_register_gmail_watch_sync))
        if result.get("status") == "success":
            log_event("system", "gmail_watch_renewed", expiration=result.get("expiration"))
        else:
            _gmail_watch_err = result.get("message", "unknown")
            log_event("system", "gmail_watch_error", error=_gmail_watch_err)
            from servers.alerting import alert_admin, CRITICAL
            alert_admin(
                CRITICAL,
                "gmail_watch_renewal_failed",
                f"Gmail push notifications will expire within 7 days. Re-deploy or manual renewal required. Error: {_gmail_watch_err[:300]}",
            )
        return result

    if task_name == "enroute_check_poll":
        result = await asyncio.to_thread(_poll_enroute_checks_sync)
        if result.get("status") == "success":
            log_event("system", "enroute_poll_complete",
                      sent=result.get("sent", 0), checked=result.get("checked", 0))
        else:
            log_event("system", "enroute_poll_error", error=result.get("message", ""))
        return result

    if task_name == "contractor_evening_reminder":
        result = await asyncio.to_thread(_send_contractor_evening_reminders_sync)
        if result.get("status") == "success":
            log_event("system", "contractor_evening_reminder_complete",
                      sent=result.get("sent", 0), skipped=result.get("skipped", 0))
        else:
            log_event("system", "contractor_evening_reminder_error", error=result.get("message", ""))
        return result

    if task_name == "send_clerk_timetables":
        result = await asyncio.to_thread(_send_clerk_timetables_sync)
        if result.get("status") == "success":
            log_event("system", "clerk_timetables_complete",
                      sent=result.get("sent", 0), skipped=len(result.get("skipped", [])))
        else:
            log_event("system", "clerk_timetables_error", error=result.get("message", ""))
        # Always run precompute after timetables (writes pending_enroute_checks for next morning)
        precompute_result = await asyncio.to_thread(_precompute_enroute_checks_sync)
        log_event("system", "enroute_precompute_complete",
                  stored=precompute_result.get("stored", 0), date=precompute_result.get("date", ""))
        return result

    # Pre-compute London-time dates for prompts that reference "yesterday"/"today"
    try:
        from zoneinfo import ZoneInfo as _ZI
    except ImportError:
        from backports.zoneinfo import ZoneInfo as _ZI
    _london_now = datetime.now(_ZI("Europe/London"))
    _london_yesterday = (_london_now - __import__('datetime').timedelta(days=1)).strftime('%Y-%m-%d')
    _london_today = _london_now.strftime('%Y-%m-%d')
    _london_tomorrow = _london_now + __import__('datetime').timedelta(days=1)
    _london_tomorrow_str = _london_tomorrow.strftime('%Y-%m-%d')
    _london_tomorrow_display = _london_tomorrow.strftime('%A %d %B %Y')

    task_prompts = {
        # 08:00 - Ops Controller (safety-net sweep â€” en-route checks are automated per job)
        "morning_roll_call": (
            "Morning Roll Call: En-route checks are sent automatically 1 hour before each job. "
            "STEP 1 â€” Call get_pending_enroute_checks (no arguments needed â€” defaults to today). "
            "STEP 2 â€” If there are ANY non-responders, compile a single summary listing each one "
            "(clerk name + job address), then call send_admin_alert ONCE with "
            "alert_type='task_summary' and details formatted as: "
            "'MORNING ROLL CALL: N non-responder(s):\\n- ClerkName at JobAddress\\n- ...' "
            "(list up to 10, then '... and N more' if there are additional). "
            "Do NOT call send_admin_alert multiple times â€” one summary message only. "
            "If all clerks have responded, do NOT send any alert. "
            "STEP 3 â€” Report how many were non-responders and list any clerks skipped (no phone number). "
            "Do NOT use send_contractor_status_check here â€” the automated polling system handles that."
        ),

        # 09:00 - Finance Sentinel
        "preflight_check": (
            f"Pre-flight Check: "
            f"STEP 1 â€” Call search_items with board_id={get_board_id('inventories') or '18392817962'}, "
            f"column_id='date', value=tomorrow's date (YYYY-MM-DD). "
            f"STEP 2 â€” From the results, identify any jobs where the payment status column shows unpaid or no payment recorded. "
            f"STEP 3 â€” If ANY unpaid jobs are found, compile a single summary listing each one "
            f"(customer name + job address + scheduled time), then call send_admin_alert ONCE with "
            f"alert_type='task_summary' and details formatted as: "
            f"'PREFLIGHT CHECK: N unpaid job(s) starting tomorrow:\\n- CustomerName at Address (TIME)\\n- ...' "
            f"(list up to 10, then '... and N more' if there are additional). "
            f"Do NOT call send_admin_alert multiple times â€” one summary message only. "
            f"If all jobs are paid, do NOT send any alert. "
            f"STEP 4 â€” Report total jobs checked, how many are unpaid, and how many alerts were sent."
        ),

        # 09:30 - Finance Sentinel
        "chase_morning": "Morning Wave Invoice Chasing: List unpaid invoices and process up to 5, prioritised by days overdue (oldest first). Draft appropriate chase emails based on urgency level (drafts saved for human review).",

        # 10:30 - Finance Sentinel
        "reconciliation": f"Reconcile payments for {_london_yesterday} (yesterday) against Xero. Update Monday board and Xero for matched payments.",

        # 11:30 - Dispatcher
        "verification_emails": (
            f"Booking Confirmation Check: "
            f"STEP 1 â€” Call search_items with board_id={get_board_id('inventories') or '18392817962'}, "
            f"column_id='date', value={_london_today} to get today's bookings. "
            f"STEP 2 â€” Call list_sent_emails to check which confirmation emails were sent today. "
            f"STEP 3 â€” Cross-reference: for each booking in STEP 1, check if a confirmation email exists in STEP 2 "
            f"(match by customer name or email address). "
            f"STEP 4 â€” If ANY bookings are missing confirmation emails, compile a single summary listing each one "
            f"(customer name + job address), then call send_admin_alert ONCE with "
            f"alert_type='task_summary' and details formatted as: "
            f"'CONFIRMATION CHECK: N booking(s) missing confirmation email:\\n- CustomerName at Address\\n- ...' "
            f"(list up to 10, then '... and N more' if there are additional). "
            f"Do NOT call send_admin_alert multiple times â€” one summary message only. "
            f"If all bookings have confirmations, do NOT send any alert. "
            f"Report total bookings checked and how many had missing confirmations."
        ),

        # 15:00 - Finance Sentinel
        "chase_afternoon": "Afternoon Wave Invoice Chasing: List unpaid invoices and process up to 5 landlord and outstanding invoices, prioritised by days overdue (oldest first). Draft chase emails (drafts saved for human review).",

        # 15:30 - Ops Controller
        "logistics_first_check": (
            f"Logistics First Check: "
            f"STEP 1 â€” Call search_items with board_id={get_board_id('inventories') or '18392817962'}, "
            f"column_id='date', value=tomorrow's date (YYYY-MM-DD). Do NOT call get_board_items. "
            f"STEP 2 â€” Call get_working_schedule to get available clerks. "
            f"STEP 3 â€” For each job in STEP 1, check: (a) is an inspector assigned? (b) is access info complete? "
            f"(c) do travel times between consecutive jobs allow 15-min buffer? Use check_job_feasibility if needed. "
            f"STEP 4 â€” If ANY issues are found (unassigned jobs or infeasible routes), compile a single summary "
            f"listing each issue (type + job address + what is missing/wrong), then call send_admin_alert ONCE with "
            f"alert_type='task_summary' and details formatted as: "
            f"'LOGISTICS FIRST CHECK: N issue(s) found for tomorrow:\\n- Issue type: Address (detail)\\n- ...' "
            f"(list up to 10, then '... and N more' if there are additional). "
            f"Do NOT call send_admin_alert multiple times â€” one summary message only. "
            f"If all jobs are fully assigned and feasible, do NOT send any alert. "
            f"Report total jobs, issues found, and any unresolved gaps."
        ),

        # 17:00 - Finance Sentinel
        "finance_sweep": (
            "Finance Sweep: Check accounts email for payment queries. "
            "For each email, BEFORE drafting any reply, call claim_email_for_processing(message_id); "
            "if the response has \"claimed\": false, skip that email entirely â€” another run already owns it. "
            "Only proceed when \"claimed\": true. Route to appropriate handler or respond directly."
        ),

        # 17:00 (every 4 hours) - Finance Sentinel
        "invoice_sweep": "Run the invoice sweep: for each pending invoice in Firestore, create a Xero invoice (AUTHORISED), update the Monday item with the invoice number, and log results.",

        # 17:30 - Ops Controller
        "logistics_final_check": (
            f"Logistics Final Check: "
            f"STEP 1 â€” Call search_items with board_id={get_board_id('inventories') or '18392817962'}, "
            f"column_id='date', value=tomorrow's date (YYYY-MM-DD). Do NOT call get_board_items. "
            f"STEP 2 â€” Call get_working_schedule to confirm all assigned clerks are still on the schedule. "
            f"STEP 3 â€” Verify every job has: inspector assigned, full address, access instructions, and hour set. "
            f"STEP 4 â€” If ANY jobs are missing required info, compile a single summary listing each one "
            f"(job address + what is missing), then call send_admin_alert ONCE with "
            f"alert_type='task_summary' and details formatted as: "
            f"'LOGISTICS FINAL CHECK: N job(s) missing required info:\\n- Address (missing: X, Y)\\n- ...' "
            f"(list up to 10, then '... and N more' if there are additional). "
            f"Do NOT call send_admin_alert multiple times â€” one summary message only. "
            f"If all jobs are complete, do NOT send any alert. "
            f"Report: total jobs verified, any outstanding issues."
        ),

        # 17:30 - Dispatcher
        "inbox_sweep": (
            "End of Day Inbox Sweep: Call list_unread_emails(include_read=True, limit=25, window='2d') to surface all "
            "recent emails (read and unread) from the last 2 days that have not yet been AI_PROCESSED. "
            "For each email: call read_email_body to get full context. "
            "BEFORE drafting any reply or creating any booking/lead, call claim_email_for_processing(message_id). "
            "If the response has \"claimed\": false, SKIP this email entirely â€” another concurrent run already owns it. "
            "Only proceed when \"claimed\": true. "
            "If admin_replied is True in the response, an admin already handled this thread â€” call "
            "mark_email_processed(message_id) and move on; do NOT draft a reply. "
            "Otherwise draft replies to remaining unhandled emails. "
            "Process AT MOST 3 emails per run (prioritised by newest first). "
            "If more unprocessed emails remain after 3, report the count so the next run handles them. "
            "Check internal logs for unresolved bot conversations. Flag NEEDS_REVIEW items."
        ),

        # 18:30 - Ops Controller
        "capacity_planning": (
            f"Capacity Planning (next 7 days): "
            f"For each of the next 7 days starting tomorrow, call search_items with "
            f"board_id={get_board_id('inventories') or '18392817962'} and column_id='date'. "
            f"Identify days with fewer than 2 bookings or with unassigned inspector slots. "
            f"Also call get_working_schedule to see available clerks. "
            f"For each gap day, suggest which available clerk could cover additional jobs. "
            f"Do NOT call get_board_items. Report gaps found and recommended cover."
        ),

        # 19:00 - Finance Sentinel
        "financial_close": (
            f"Financial Close: "
            f"STEP 1 â€” Call list_pending_invoices to see if any Firestore pending invoices still need processing. "
            f"For each pending invoice, call create_invoice to generate it in Xero (AUTHORISED status). "
            f"STEP 2 â€” Call search_items with board_id={get_board_id('inventories') or '18392817962'}, "
            f"column_id='date', value={_london_today} to get today's completed jobs. "
            f"STEP 3 â€” For any completed job with no invoice number recorded, log a warning. "
            f"Report: invoices created in STEP 1, jobs with missing invoices in STEP 3."
        ),

        # 19:45 - Ops Controller (runs before send_clerk_timetables so issues are caught first)
        "route_verification": (
            f"Route Verification: "
            f"STEP 1 â€” Call search_items with board_id={get_board_id('inventories') or '18392817962'}, "
            f"column_id='date', value=tomorrow's date (YYYY-MM-DD). Do NOT call get_board_items. "
            f"STEP 2 â€” For each job, check that these fields are populated: "
            f"access instructions, key arrangement, full address, scheduled time (hour column). "
            f"STEP 3 â€” If ANY jobs are missing access or key info, compile a single summary listing each one "
            f"(customer name + job address + what is missing), then call send_admin_alert ONCE with "
            f"alert_type='task_summary' and details formatted as: "
            f"'ROUTE VERIFICATION: N job(s) missing access/key info:\\n- CustomerName at Address (missing: X)\\n- ...' "
            f"(list up to 10, then '... and N more' if there are additional). "
            f"Do NOT call send_admin_alert multiple times â€” one summary message only. "
            f"If all jobs have complete access info, do NOT send any alert. "
            f"STEP 4 â€” Report total jobs checked, how many had complete info, and how many alerts were sent."
        ),

        # 20:30 - Ops Controller (Sunday only)
        "weekly_board_dup": (
            "Weekly Board Duplication: Duplicate the Daily Tasks board "
            f"(board_id={get_board_id('daily_tasks') or os.getenv('DAILY_TASKS_BOARD_ID', '')}) "
            "using duplicate_board with duplicate_type='duplicate_board_with_pulses'. "
            "This creates next week's task tracking board with all task items copied."
        ),

        # 20:30 - Ops Controller (Mon-Sat)
        "weekly_prep": (
            f"Weekly Prep: "
            f"STEP 1 â€” Call search_items with board_id={get_board_id('inventories') or '18392817962'}, "
            f"column_id='date', value=tomorrow's date (YYYY-MM-DD). "
            f"STEP 2 â€” Call get_working_schedule to see all available clerks. "
            f"STEP 3 â€” For each job in STEP 1, check if an inspector is assigned (status column = clerk name). "
            f"STEP 4 â€” If ANY jobs have no inspector assigned, compile a single summary listing each one "
            f"(customer name + job address + scheduled time), then call send_admin_alert ONCE with "
            f"alert_type='task_summary' and details formatted as: "
            f"'WEEKLY PREP: N unassigned job(s) tomorrow:\\n- CustomerName at Address (TIME)\\n- ...' "
            f"(list up to 10, then '... and N more' if there are additional). "
            f"Do NOT call send_admin_alert multiple times â€” one summary message only. "
            f"If all jobs are assigned, do NOT send any alert. "
            f"Report total jobs reviewed, how many are assigned, and how many need attention."
        ),

        # Every 30 minutes - Dispatcher (fallback; primary delivery via Gmail push webhook)
        "inbox_check": (
            "Periodic Inbox Check: Call list_unread_emails(include_read=True, limit=25, window='1d') to check all monitored "
            "inboxes (Acme, Ace, Contractors, EPC) for recent emails (read and unread, last 24 hours) "
            "that have not yet been AI_PROCESSED. "
            "For each email: call read_email_body to get full context. "
            "BEFORE drafting any reply or creating any booking/lead, call claim_email_for_processing(message_id). "
            "If the response has \"claimed\": false, SKIP this email entirely â€” another concurrent run already owns it. "
            "Only proceed when \"claimed\": true. "
            "If admin_replied is True in the response, an admin already handled this thread â€” call "
            "mark_email_processed(message_id) and move on; do NOT draft a reply. "
            "Otherwise: triage, route bookings to Ops, route payment queries to Finance, "
            "draft auto-replies for amendments (all drafts saved for human review), and log all actions. "
            "IMPORTANT: When replying to any inbound email, always pass reply_to_message_id (the message id from list_unread_emails) "
            "and thread_id so the system can deduplicate replies across runs. "
            "If draft_reply returns status 'skipped', the draft was already created â€” move on. "
            "Process AT MOST 1 email per run (prioritised by newest first). "
            "If more unprocessed emails remain, report the count â€” the next periodic check will handle them. "
            "Also check WhatsApp for new direct messages and respond or escalate appropriately."
        ),

        # send_clerk_timetables is now handled by _send_clerk_timetables_sync() above â€” no LLM needed.

    }

    prompt = task_prompts.get(task_name)
    if not prompt:
        safe_name = task_name[:50] if task_name else ""
        log_event("system", "unknown_scheduled_task", task=safe_name)
        return {"status": "error", "message": f"Unknown scheduled task: {safe_name}"}

    # empty-inbox early-exit: skip LLM if no actionable emails (inbox_check + inbox_sweep).
    # Window MUST match each prompt's own scan window (1d / 2d) or a sweep could skip a
    # 2-day-old email it would otherwise have found.
    _early_exit_windows = {"inbox_check": "1d", "inbox_sweep": "2d", "finance_sweep": "1d"}
    if task_name in _early_exit_windows:
        try:
            from servers.gmail_server import _list_unread_emails_sync
            _inbox_data = json.loads(await asyncio.to_thread(
                _list_unread_emails_sync,
                limit=25, include_read=True, window=_early_exit_windows[task_name], include_processed=False
            ))
            if len(_inbox_data.get("emails", [])) == 0:
                log_event("system", "inbox_check_skipped", reason="empty_inbox", task=task_name)
                return {"status": "success", "task": task_name, "skipped": "empty_inbox"}
        except Exception as _e:
            logger.warning(json.dumps({"event": "inbox_early_exit_failed", "error": str(_e)}))
            # On failure, proceed with normal LLM execution

    # invoice_sweep: skip LLM if Firestore shows 0 pending invoices
    if task_name == "invoice_sweep":
        try:
            _pending_count = _system_state.count_pending_invoices()
            if _pending_count == 0:
                log_event("system", "invoice_sweep_skipped", reason="no_pending_invoices")
                return {"status": "success", "task": task_name, "skipped": "no_pending_invoices"}
        except Exception as _e:
            logger.warning(json.dumps({"event": "invoice_sweep_guard_failed", "error": str(_e)}))
            # On failure, proceed with normal LLM execution

    # reconciliation: deterministic 30-min cooldown â€” prevents duplicate triggers (amplification
    # bug observed 2026-07-23 where 13 runs fired instead of 1 due to duplicate event delivery).
    # Uses a separate lock key so it doesn't interfere with the pipeline-level recon_lock (300s).
    if task_name == "reconciliation":
        try:
            from servers.locking_server import _lock_task_sync
            try:
                from zoneinfo import ZoneInfo
            except ImportError:
                from backports.zoneinfo import ZoneInfo
            _today = datetime.now(ZoneInfo("Europe/London")).strftime('%Y-%m-%d')
            _cooldown_result = _lock_task_sync(
                f"sched_recon_{_today}",
                owner="scheduler",
                ttl_seconds=1800,
            )
            if _cooldown_result != "LOCKED":
                log_event("system", "reconciliation_cooldown", reason="already_ran_recently")
                return {"status": "success", "task": task_name, "skipped": "cooldown_active"}
        except Exception as _e:
            logger.warning(json.dumps({"event": "recon_cooldown_guard_failed", "error": str(_e)}))
            # On failure, proceed with normal LLM execution

    # weekly_board_dup: guard against empty board ID (Firestore failure + missing env var)
    if task_name == "weekly_board_dup":
        _dup_board_id = get_board_id('daily_tasks') or os.getenv('DAILY_TASKS_BOARD_ID', '')
        if not _dup_board_id:
            log_event("system", "weekly_board_dup_skipped", reason="board_id_unavailable")
            return {"status": "error", "task": task_name, "message": "daily_tasks board ID unavailable â€” check Firestore config or set DAILY_TASKS_BOARD_ID env var"}

    # Pre-flight: Monday-dependent tasks cannot proceed without Monday MCP.
    # Return a structured skip instead of letting the LLM run without its
    # core tools and produce a confusing "tool not found" admin alert.
    if task_name in _MONDAY_DEPENDENT_TASKS and not _monday_mcp_healthy():
        log_event("system", "task_skipped_mcp_down", task=task_name,
                  reason="monday_mcp_unavailable", severity="ERROR")
        return {
            "status": "skipped",
            "task": task_name,
            "reason": "Monday.com MCP unavailable â€” no Monday tools loaded. Task deferred pending MCP recovery.",
        }

    # Deterministic pre-routing â€” all known tasks bypass hub LLM
    direct_agent = _TASK_AGENT_MAP.get(task_name)
    if direct_agent:
        # Finance tasks (Xero + Monday API chains), timetable, and inbox tasks need more than 240s default
        _LONG_TIMEOUT_TASKS = {
            "send_clerk_timetables",
            "preflight_check", "chase_morning", "chase_afternoon",
            "reconciliation", "finance_sweep", "invoice_sweep", "financial_close",
            "inbox_check", "inbox_sweep",
        }
        _task_timeout = 420 if task_name in _LONG_TIMEOUT_TASKS else 240
        target_agent, result, _ = await orchestrator.invoke_agent_directly(
            prompt, direct_agent, source_role="scheduled_task", timeout=_task_timeout,
        )
    else:
        target_agent, result, _ = await orchestrator.route_and_execute(
            prompt, source_role="scheduled_task",
        )

    return {
        "status": "success",
        "task": task_name,
        "agent": target_agent,
        "result": result
    }


async def main():
    """Main entry point - determines execution mode."""
    mode = os.getenv("EXECUTION_MODE", "cli")

    try:
        async with AsyncExitStack() as stack:
            orchestrator = AcmeOrchestrator()
            await orchestrator.initialize(stack)

            if mode == "cli":
                await run_cli_mode(orchestrator)

            elif mode == "event":
                # Event mode expects payload via environment
                payload_json = os.getenv("EVENT_PAYLOAD", "{}")
                event_type = os.getenv("EVENT_TYPE", "unknown")
                try:
                    payload = json.loads(payload_json)
                except json.JSONDecodeError:
                    payload = {}

                result = await handle_event(orchestrator, event_type, payload)
                sys.stdout.write(json.dumps(result))
                sys.stdout.flush()

            elif mode == "schedule":
                task_name = os.getenv("SCHEDULED_TASK", "")
                if not task_name:
                    log_event("system", "error", message="SCHEDULED_TASK not set")
                    sys.exit(1)

                result = await run_scheduled_task(orchestrator, task_name)
                sys.stdout.write(json.dumps(result))
                sys.stdout.flush()

            else:
                log_event("system", "error", message=f"Unknown execution mode: {mode}")
                sys.exit(1)

    except Exception as e:
        log_event("system", "fatal_error", error=str(e), error_type=type(e).__name__)
        raise


if __name__ == "__main__":
    asyncio.run(main())
