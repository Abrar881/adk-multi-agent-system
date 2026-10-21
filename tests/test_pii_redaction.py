"""Unit tests for PII redaction in ActivityLogPlugin.before_tool_callback().

Expands coverage for the plugin that replaced _before_tool. All tests verify
that only arg key names are logged, never raw values.

Layer 0 â€” no LLM, no credentials. Uses mock_log_event fixture from conftest.

Run: pytest tests/test_pii_redaction.py -v
"""
import pytest
from unittest.mock import MagicMock

import main
from plugins.activity_log import ActivityLogPlugin


@pytest.fixture
def plugin(mock_log_event):
    """ActivityLogPlugin wired to the monkeypatched log_event."""
    return ActivityLogPlugin(firestore_log_func=main.log_event)


async def test_email_addresses_not_logged(plugin, mock_log_event):
    """Tool args with email values only log key names, not the email address."""
    stub_tool = MagicMock()
    stub_tool.name = "send_email"

    args = {
        "to": "client@example.com",
        "cc": "admin@Acmeinventories.co.uk",
        "subject": "Booking Confirmation",
    }

    result = await plugin.before_tool_callback(tool=stub_tool, tool_args=args, tool_context=None)
    assert result is None
    assert len(mock_log_event) == 1

    logged_kwargs = mock_log_event[0]["kwargs"]
    arg_data = logged_kwargs["arguments"]

    assert "client@example.com" not in str(mock_log_event)
    assert "admin@Acmeinventories.co.uk" not in str(mock_log_event)
    assert arg_data["arg_count"] == 3
    assert "to" in arg_data["keys"]
    assert "cc" in arg_data["keys"]


async def test_phone_numbers_not_logged(plugin, mock_log_event):
    """Tool args with phone numbers only log key names, not the number."""
    stub_tool = MagicMock()
    stub_tool.name = "send_whatsapp_message"

    args = {
        "to": "+447911123456",
        "message": "Your appointment is at 10am",
    }

    result = await plugin.before_tool_callback(tool=stub_tool, tool_args=args, tool_context=None)
    assert result is None
    assert len(mock_log_event) == 1

    assert "+447911123456" not in str(mock_log_event)
    assert "to" in mock_log_event[0]["kwargs"]["arguments"]["keys"]


async def test_message_bodies_not_logged(plugin, mock_log_event):
    """Large text args (email body, WhatsApp message) not logged."""
    stub_tool = MagicMock()
    stub_tool.name = "send_email"

    large_text = "Dear Client, this is a very long email body " * 50

    args = {
        "to": "client@example.com",
        "body": large_text,
        "subject": "Booking",
    }

    result = await plugin.before_tool_callback(tool=stub_tool, tool_args=args, tool_context=None)
    assert result is None

    log_str = str(mock_log_event)
    assert large_text[:50] not in log_str
    assert "Dear Client" not in log_str


async def test_empty_args_logged_correctly(plugin, mock_log_event):
    """Tool with no args logs arg_count=0 and empty keys list."""
    stub_tool = MagicMock()
    stub_tool.name = "get_ops_status"

    result = await plugin.before_tool_callback(tool=stub_tool, tool_args={}, tool_context=None)
    assert result is None

    arg_data = mock_log_event[0]["kwargs"]["arguments"]
    assert arg_data["arg_count"] == 0
    assert arg_data["keys"] == []


async def test_nested_dict_args_only_log_top_keys(plugin, mock_log_event):
    """Nested dicts don't leak inner values â€” only top-level key names are logged."""
    stub_tool = MagicMock()
    stub_tool.name = "create_inventory_booking"

    args = {
        "booking": {
            "client_name": "John Smith",
            "address": {
                "line1": "10 High Street",
                "postcode": "SW1A 1AA",
            },
            "amount": 150.00,
        },
        "priority": "high",
    }

    result = await plugin.before_tool_callback(tool=stub_tool, tool_args=args, tool_context=None)
    assert result is None

    logged_keys = mock_log_event[0]["kwargs"]["arguments"]["keys"]
    assert "booking" in logged_keys
    assert "priority" in logged_keys
    assert "client_name" not in logged_keys
    assert "line1" not in logged_keys
    assert "postcode" not in logged_keys

    log_str = str(mock_log_event)
    assert "John Smith" not in log_str
    assert "10 High Street" not in log_str
    assert "SW1A 1AA" not in log_str


async def test_before_tool_returns_none(plugin, mock_log_event):
    """before_tool_callback always returns None â€” it never blocks tool execution."""
    stub_tool = MagicMock()
    stub_tool.name = "test_tool"

    for args in [{}, {"a": 1}, {"long": "text" * 100}]:
        mock_log_event.clear()
        result = await plugin.before_tool_callback(tool=stub_tool, tool_args=args, tool_context=None)
        assert result is None


async def test_pii_values_absent_from_log_str(plugin, mock_log_event):
    """Verify no PII values appear anywhere in the logged payload."""
    stub_tool = MagicMock()
    stub_tool.name = "send_email"

    args = {
        "to": "secretive@client.com",
        "subject": "Confidential: Invoice",
        "body": "Account number: 12345678 sort code: 00-12-34",
    }

    await plugin.before_tool_callback(tool=stub_tool, tool_args=args, tool_context=None)

    log_str = str(mock_log_event)
    assert "secretive@client.com" not in log_str
    assert "Confidential:" not in log_str
    assert "12345678" not in log_str
    assert "00-12-34" not in log_str

    arg_data = mock_log_event[0]["kwargs"]["arguments"]
    assert arg_data["arg_count"] == 3
    assert sorted(arg_data["keys"]) == ["body", "subject", "to"]
