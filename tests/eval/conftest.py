"""Eval-suite conftest â€” mocked MCP toolsets + real Vertex LLM.

Loads .env first (so real GCP credentials are picked up), then
patches SerializedMcpToolset.get_tools to return lightweight FunctionTool stubs.
The stubs record every call in RECORDED_CALLS so assertions can inspect
the trajectory without spawning real MCP subprocesses.

Scope notes:
  _patch_mcp_module â€” module scope, must be active before the
      eval_orchestrator fixture calls initialize() so that get_tools()
      returns fakes during the warm-up pass.
  eval_orchestrator â€” module scope, re-used across tests in the same
      file to avoid re-booting Vertex sessions.
  _reset_calls â€” function scope, clears RECORDED_CALLS before each test.
"""

import os
import json
import unittest.mock as mock
from contextlib import AsyncExitStack

import pytest
from dotenv import load_dotenv

# Load .env before importing anything that reads env vars.
# override=True so that real .env values WIN over the test defaults that
# tests/conftest.py sets via setdefault() (which runs first during collection).
# Without override=True, the parent conftest's "test-project" sentinel would
# silently prevent the skip-guard from releasing, making all eval tests skip
# even when the user has a real GOOGLE_CLOUD_PROJECT in .env.
load_dotenv(override=True)

# Mirror the parent conftest defaults so collecting tests/eval/ standalone
# doesn't raise RuntimeError on missing project/password vars.
os.environ.setdefault("GOOGLE_CLOUD_PROJECT", "test-project")
os.environ.setdefault("GOOGLE_CLOUD_LOCATION", "global")
os.environ.setdefault("DASHBOARD_PASSWORD", "test-password-secure-123")
os.environ.setdefault("DASHBOARD_SECRET_KEY", "test-secret-key-secure-abc-xyz")

# Import ADK types needed for the fake toolset.
from mcp_runtime import SerializedMcpToolset
from google.adk.tools.function_tool import FunctionTool

import main  # noqa: E402 â€” env vars must be set first

# ---------------------------------------------------------------------------
# Recorded-calls registry â€” tests can inspect this to verify trajectory.
# ---------------------------------------------------------------------------
RECORDED_CALLS: list[dict] = []

# ---------------------------------------------------------------------------
# Canned responses per tool name â€” realistic but minimal.
# Keys must match what the real MCP server scripts export via @mcp.tool().
# ---------------------------------------------------------------------------
_CANNED = {
    # locking_server
    "lock_task":                '{"status": "locked", "task_id": "test-task-001"}',
    "acquire_lock":             '{"status": "acquired", "resource_id": "test-resource"}',
    "release_lock":             '{"status": "released", "resource_id": "test-resource"}',
    "cleanup_expired_locks":    '{"status": "success", "cleaned": 0}',
    # xero_server
    "list_unpaid_invoices": json.dumps({"invoices": [
        {"number": "INV-001", "amount": 500.00, "due_date": "2026-05-01",
         "contact": "Test Client Ltd", "status": "AUTHORISED"}
    ], "total": 1, "currency": "GBP"}),
    "get_invoice_details": json.dumps({"invoice": {
        "number": "INV-001", "status": "AUTHORISED", "amount": 500.00,
        "contact": "Test Client Ltd"
    }}),
    # rag_server
    "consult_knowledge":        '{"answer": "Use the standard process per the SOP.", "confidence": 0.9}',
    # reporting_server
    "generate_board_health_report": '{"report": "10 items â€” 8 complete, 2 in progress.", "score": 0.8}',
    "generate_daily_summary":       '{"summary": "3 jobs completed today."}',
    "generate_weekly_summary":      '{"summary": "15 jobs completed this week."}',
    "detect_missing_fields":        '{"missing": [], "checked": 5}',
    "generate_invoice_audit_report":'{"audit": "All invoices accounted for.", "gaps": 0}',
    "format_report_for_email":      '{"email_body": "Daily summary report is ready."}',
    "calculate_completion_rate":    '{"rate": 0.87, "period_days": 7}',
    # whatsapp_server
    "send_whatsapp_message":        '{"status": "sent", "message_sid": "SM_test_abc123"}',
    "send_whatsapp_template":       '{"status": "sent", "message_sid": "SM_test_def456"}',
    "get_whatsapp_status":          '{"status": "operational", "balance": "Â£5.00"}',
    "send_contractor_status_check": '{"status": "sent", "recipients": 1}',
    "send_admin_alert":             '{"status": "sent", "alert_id": "ALT_001"}',
    # gmail_server
    "list_unread_emails": json.dumps({"emails": [
        {"id": "msg001", "subject": "Booking Request", "from": "client@example.com",
         "snippet": "Hi, I would like to book an inspection..."}
    ], "count": 1}),
    "read_email_body":  '{"body": "Hi, I would like to book an inspection for next Monday.", "subject": "Booking Request"}',
    "draft_reply":      '{"status": "draft_created", "draft_id": "draft001"}',
    "send_email":       '{"status": "sent", "message_id": "msg_sent_001"}',
    "list_sent_emails": '{"emails": [], "count": 0}',
    # ops_server
    "calculate_travel_time": '{"duration": "45 minutes", "distance": "12.3 km", "mode": "transit", "feasible": true}',
    "check_job_feasibility": '{"feasible": true, "recommendation": "Job is feasible within the timeframe."}',
    "optimize_route":        '{"route": ["Depot", "Job A", "Job B"], "total_time": "2h 15m"}',
    "get_ops_status":        '{"status": "operational", "google_maps": "connected", "version": "1.0"}',
    # monday_server
    "list_boards":              '{"boards": [{"id": 393394274, "name": "Daily Tasks"}], "count": 1}',
    "create_task_item":         '{"item": {"id": 9990001, "name": "Test Task Item"}}',
    "get_board_items":          '{"items": [{"id": 1, "name": "Test Job", "status": "In Progress"}], "count": 1}',
    "search_items":             '{"items": [], "count": 0}',
    "update_column_value":      '{"status": "updated", "item_id": 1, "column_id": "status"}',
    "update_multiple_columns":  '{"status": "updated", "item_id": 1}',
    "update_status":            '{"status": "updated", "item_id": 1, "new_status": "Done"}',
    "create_inventory_task":    '{"item": {"id": 9990002, "name": "New Inventory Task"}}',
    "get_board_schema":         '{"schema": {"columns": [{"id": "status", "type": "status"}]}}',
    "get_item":                 '{"item": {"id": 1, "name": "Test Job", "group": "This Week"}}',
    "create_item_update":       '{"update": {"id": 500, "body": "Update posted."}}',
    "search_items_paginated":   '{"items": [], "count": 0, "cursor": null}',
    "archive_item":             '{"status": "archived", "item_id": 1}',
    "move_item_to_group":       '{"status": "moved", "item_id": 1, "group_id": "done"}',
    "duplicate_board":          '{"board": {"id": 9990003, "name": "Daily Tasks (copy)"}}',
    "create_inventory_booking": '{"booking": {"id": 9990004, "status": "created"}}',
    "get_working_schedule":     '{"schedule": [], "count": 0}',
    "create_lead":              '{"lead": {"id": 9990005, "name": "New Lead"}}',
    "reload_monday_config":     '{"status": "success", "message": "Cache cleared"}',
}


def _make_fake_tool(tool_name: str) -> FunctionTool:
    response = _CANNED.get(tool_name, '{"status": "ok"}')

    async def _impl(**kwargs):
        RECORDED_CALLS.append({"tool": tool_name, "args": dict(kwargs)})
        return response

    _impl.__name__ = tool_name
    _impl.__doc__ = f"Fake {tool_name} â€” eval suite stub."
    return FunctionTool(func=_impl)


# Build the per-toolset catalog keyed by id(toolset_instance).
# Keying by id works because the module-level toolset objects in main.py
# live for the process lifetime.
FAKE_TOOLS: dict[int, list] = {
    id(main.locking_ts):   [_make_fake_tool(n) for n in
                            ("lock_task", "acquire_lock", "release_lock", "cleanup_expired_locks")],
    id(main.rag_ts):       [_make_fake_tool("consult_knowledge")],
    id(main.reporting_ts): [_make_fake_tool(n) for n in (
                            "generate_board_health_report", "generate_daily_summary",
                            "generate_weekly_summary", "detect_missing_fields",
                            "generate_invoice_audit_report", "format_report_for_email",
                            "calculate_completion_rate")],
    id(main.gmail_ts):     [_make_fake_tool(n) for n in (
                            "list_unread_emails", "read_email_body",
                            "draft_reply", "send_email", "list_sent_emails")],
    id(main.ops_ts):       [_make_fake_tool(n) for n in (
                            "calculate_travel_time", "check_job_feasibility",
                            "optimize_route", "get_ops_status")],
    **{
        id(ts): [_make_fake_tool(n) for n in
                 ("list_unpaid_invoices", "get_invoice_details")]
        for ts in (main.dispatcher_xero_ts, main.finance_xero_ts)
    },
    **{
        id(ts): [_make_fake_tool(n) for n in (
                 "send_whatsapp_message", "send_whatsapp_template",
                 "get_whatsapp_status", "send_contractor_status_check",
                 "send_admin_alert")]
        for ts in (main.dispatcher_whatsapp_ts, main.ops_whatsapp_ts)
    },
    **{
        id(ts): [_make_fake_tool(n) for n in (
                 "list_boards", "create_task_item", "get_board_items",
                 "search_items", "update_column_value", "update_multiple_columns",
                 "update_status", "create_inventory_task", "get_board_schema",
                 "get_item", "create_item_update", "search_items_paginated",
                 "archive_item", "move_item_to_group", "duplicate_board",
                 "create_inventory_booking", "get_working_schedule", "create_lead",
                 "reload_monday_config")]
        for ts in (main.dispatcher_monday_ts, main.finance_monday_ts, main.ops_monday_ts)
    },
}


async def _fake_get_tools(self, readonly_context=None):
    """Replacement for SerializedMcpToolset.get_tools â€” returns stubs, no subprocess."""
    return FAKE_TOOLS.get(id(self), [])


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module", autouse=True)
def _patch_mcp_module():
    """Patch SerializedMcpToolset.get_tools at module scope.

    Module scope is required so the patch is active when the
    eval_orchestrator fixture calls initialize() (which calls get_tools()
    on every toolset to warm them up).
    """
    with mock.patch.object(SerializedMcpToolset, "get_tools", _fake_get_tools):
        yield


@pytest.fixture(autouse=True)
def _reset_calls():
    """Clear RECORDED_CALLS before each test â€” prevents cross-test leakage."""
    RECORDED_CALLS.clear()
    yield


@pytest.fixture(autouse=True)
def _mock_monday_config():
    """Patch config.monday_config.get_config to return a test config, preventing Firestore reads in eval tests."""
    test_config = {
        "api_token": "test-monday-key",
        "boards": {
            "inventories": {
                "id": 18410040105,
                "columns": {
                    "report_status": {"id": "report_status"},
                    "inspector": {"id": "inspector"},
                    "report_type": {"id": "report___type"},
                    "reason_delay": {"id": "color_mksmbkb8"},
                    "payment_status": {"id": "color_mm2qx88z"},
                    "payment_reminder": {"id": "color_mm2q46rh"},
                    "Acme_confirmation": {"id": "color_mm2q1r4r"},
                    "postcode": {"id": "text_mm2qp73p"},
                    "phone": {"id": "text_mm2q758g"},
                    "customer": {"id": "text_mm2qsstc"},
                    "access_info": {"id": "text_mm2qcrtb"},
                    "contact_number": {"id": "text_mm2q6vgs"},
                    "notes": {"id": "text_mm2q379g"},
                    "inv_number": {"id": "text_mm2q4acb"},
                    "access_instructions": {"id": "text_mm2qqer2"},
                    "hour": {"id": "dup__of_hour0__1"},
                    "email": {"id": "email_mm2qp164"},
                    "address": {"id": "address"},
                    "spec": {"id": "spec"},
                    "date": {"id": "date"},
                },
                "groups": {"leads": "group_mm2q9amr"},
            },
            "daily_tasks": {"id": 393394274},
            "working_schedule": {"id": 5310376333},
            "contractors": {"id": 18328214271},
        },
    }
    import config.monday_config as mc
    with mock.patch.object(mc, "get_config", new=lambda: test_config):
        yield


@pytest.fixture(scope="module")
async def eval_orchestrator(_patch_mcp_module):
    """Real AcmeOrchestrator with real Vertex LLM + mocked MCP toolsets.

    Skips automatically when GOOGLE_CLOUD_PROJECT is set to the CI test
    sentinel value "test-project" (which has no real Vertex quota).
    Set it to a real GCP project before running the eval suite.
    """
    project = os.environ.get("GOOGLE_CLOUD_PROJECT", "")
    if not project or project == "test-project":
        pytest.skip(
            "Eval tests require a real GOOGLE_CLOUD_PROJECT with Vertex AI quota. "
            "Run: GOOGLE_CLOUD_PROJECT=<your-project> pytest tests/eval/ -v -m eval"
        )

    async with AsyncExitStack() as stack:
        orc = main.AcmeOrchestrator()
        await orc.initialize(stack)
        yield orc
