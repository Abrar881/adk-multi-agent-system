"""Layer 2 â€” Webhook-decode unit tests.

Verifies that `handle_event` correctly transforms each webhook payload
shape into the right `user_input` string before calling `route_and_execute`.

These tests are FAST (no LLM, no MCP, no Vertex) â€” `route_and_execute` is
mocked to an AsyncMock that captures the string passed to it. All assertions
are about payload-to-string transformation, not agent behaviour.

Run:
    pytest tests/test_handle_event.py -v
"""

import pytest
from unittest.mock import AsyncMock, MagicMock

import main


@pytest.fixture
def stub_orchestrator():
    """Stub orchestrator that records route_and_execute and invoke_agent_directly calls."""
    orc = MagicMock(spec=main.AcmeOrchestrator)
    orc.route_and_execute = AsyncMock(return_value=("dispatcher", "Task completed.", "sess-test-123"))
    orc.invoke_agent_directly = AsyncMock(return_value=("dispatcher", "Task completed.", "sess-test-123"))
    return orc


# ---------------------------------------------------------------------------
# Happy-path â€” one test per supported event_type
# ---------------------------------------------------------------------------

async def test_email_received_builds_correct_task(stub_orchestrator):
    """email_received should include subject and sender in the task string."""
    result = await main.handle_event(stub_orchestrator, "email_received", {
        "subject": "Booking for Monday 21 April",
        "sender": "client@example.com",
    })
    assert result["status"] == "success"
    assert result["agent"] == "dispatcher"

    task: str = stub_orchestrator.invoke_agent_directly.call_args[0][0]
    assert "Booking for Monday 21 April" in task
    assert "client@example.com" in task


async def test_whatsapp_received_builds_correct_task(stub_orchestrator):
    """whatsapp_received should include sender and message body in the task."""
    result = await main.handle_event(stub_orchestrator, "whatsapp_received", {
        "from": "+447911123456",
        "message": "Hi, is the job still on for tomorrow?",
    })
    assert result["status"] == "success"

    task: str = stub_orchestrator.invoke_agent_directly.call_args[0][0]
    assert "+447911123456" in task
    assert "Hi, is the job still on for tomorrow?" in task


async def test_monday_webhook_builds_correct_task(stub_orchestrator):
    """monday_webhook should include event type and item_id in the task."""
    result = await main.handle_event(stub_orchestrator, "monday_webhook", {
        "event": "status_change",
        "item_id": "9990001",
    })
    assert result["status"] == "success"

    task: str = stub_orchestrator.route_and_execute.call_args[0][0]
    assert "status_change" in task
    assert "9990001" in task


async def test_cli_test_passes_message_verbatim(stub_orchestrator):
    """cli_test should pass the message field directly to route_and_execute."""
    msg = "List all unpaid Xero invoices"
    result = await main.handle_event(stub_orchestrator, "cli_test", {"message": msg})
    assert result["status"] == "success"

    task: str = stub_orchestrator.route_and_execute.call_args[0][0]
    assert task == msg


async def test_unknown_event_type_falls_back_gracefully(stub_orchestrator):
    """Unrecognised event_type should not crash â€” it falls back to a generic task."""
    result = await main.handle_event(stub_orchestrator, "unknown_future_event", {
        "data": "something"
    })
    # handle_event should still call route_and_execute (fallback branch)
    # and return a success dict.
    assert result["status"] == "success"
    task: str = stub_orchestrator.route_and_execute.call_args[0][0]
    assert "unknown_future_event" in task


# ---------------------------------------------------------------------------
# Edge cases â€” malformed payloads
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Edge cases â€” malformed payloads
# ---------------------------------------------------------------------------

async def test_email_received_missing_fields_uses_defaults(stub_orchestrator):
    """email_received with empty payload should not raise â€” uses defaults."""
    result = await main.handle_event(stub_orchestrator, "email_received", {})
    assert result["status"] == "success"
    task: str = stub_orchestrator.invoke_agent_directly.call_args[0][0]
    assert "No subject" in task or "Unknown" in task


async def test_whatsapp_missing_message_uses_default(stub_orchestrator):
    """whatsapp_received with no message key should not raise."""
    result = await main.handle_event(stub_orchestrator, "whatsapp_received", {
        "from": "+447900000000"
    })
    assert result["status"] == "success"
    task: str = stub_orchestrator.invoke_agent_directly.call_args[0][0]
    assert "+447900000000" in task


# ---------------------------------------------------------------------------
# Additional edge cases (from TEST_CREATION_PLAN.md Part A2)
# ---------------------------------------------------------------------------

async def test_schedule_event_calls_run_scheduled_task(stub_orchestrator):
    """event_type='schedule' falls through to generic route_and_execute (schedule
    dispatching is handled upstream in server.py, not in handle_event)."""
    result = await main.handle_event(stub_orchestrator, "schedule", {"task_name": "test_task"})
    assert result["status"] == "success"
    stub_orchestrator.route_and_execute.assert_called_once()


async def test_empty_payload_does_not_crash(stub_orchestrator):
    """Empty dict payload handled gracefully across all event types."""
    for event_type in ("email_received", "whatsapp_received", "monday_webhook", "cli_test"):
        result = await main.handle_event(stub_orchestrator, event_type, {})
        assert result["status"] == "success"


async def test_payload_with_extra_fields_ignored(stub_orchestrator):
    """Unknown fields in payload don't cause errors or appear in task string."""
    result = await main.handle_event(stub_orchestrator, "email_received", {
        "subject": "Test",
        "sender": "test@example.com",
        "extra_field": "should be ignored",
        "another_extra": 12345,
    })
    assert result["status"] == "success"
    task: str = stub_orchestrator.invoke_agent_directly.call_args[0][0]
    assert "should be ignored" not in task
    assert "12345" not in task


async def test_email_with_html_body(stub_orchestrator):
    """email_received task includes subject and sender (body not added to task string)."""
    result = await main.handle_event(stub_orchestrator, "email_received", {
        "subject": "Booking Confirmation",
        "sender": "client@example.com",
        "body": "<html><body><p>Dear Client</p></body></html>",
    })
    assert result["status"] == "success"
    task: str = stub_orchestrator.invoke_agent_directly.call_args[0][0]
    assert "Booking Confirmation" in task
    assert "client@example.com" in task


async def test_monday_webhook_with_missing_item_id(stub_orchestrator):
    """monday_webhook without item_id uses 'Unknown' as fallback."""
    result = await main.handle_event(stub_orchestrator, "monday_webhook", {
        "event": "column_changed"
    })
    assert result["status"] == "success"
    task: str = stub_orchestrator.route_and_execute.call_args[0][0]
    assert "column_changed" in task
    assert "Unknown" in task


async def test_multiple_events_sequential(stub_orchestrator):
    """Two events processed sequentially both succeed."""
    result1 = await main.handle_event(stub_orchestrator, "email_received", {
        "subject": "First",
        "sender": "a@example.com",
    })
    assert result1["status"] == "success"

    result2 = await main.handle_event(stub_orchestrator, "whatsapp_received", {
        "from": "+447900000001",
        "message": "Second message",
    })
    assert result2["status"] == "success"

    assert stub_orchestrator.invoke_agent_directly.call_count == 2
