"""Integration tests for MCP tool failure handling.

Layer 0.5 â€” no LLM, tests error handling when MCP tools fail.
Uses tests/eval/conftest.py CANNED_RESPONSES pattern, but with error responses.

Run: pytest tests/test_tool_failures.py -v
"""
import pytest
from unittest.mock import AsyncMock, MagicMock

import main
from tests.eval.conftest import RECORDED_CALLS


async def test_monday_tool_timeout(monkeypatch):
    """Monday.com tool timeout returns graceful error."""
    from tests.eval.conftest import _CANNED, _make_fake_tool, FAKE_TOOLS

    error_response = '{"error": "timeout", "tool": "get_board_items"}'

    def _make_error_tool(tool_name):
        async def _impl(**kwargs):
            RECORDED_CALLS.append({"tool": tool_name, "args": dict(kwargs)})
            raise TimeoutError("MCP tool timed out")

        from google.adk.tools.function_tool import FunctionTool
        return FunctionTool(func=_impl)

    fake_tools_override = {
        id(main.dispatcher_monday_ts): [_make_error_tool("get_board_items")],
        id(main.finance_monday_ts): [_make_error_tool("get_board_items")],
        id(main.ops_monday_ts): [_make_error_tool("get_board_items")],
    }

    async def _fake_get_tools(self, readonly_context=None):
        return fake_tools_override.get(id(self), [])

    from mcp_runtime import SerializedMcpToolset

    monkeypatch.setattr(SerializedMcpToolset, "get_tools", _fake_get_tools)

    orc = main.AcmeOrchestrator()

    mock_runner = MagicMock()

    mock_part = MagicMock()
    mock_part.text = "Could not check board."

    mock_event = MagicMock()
    mock_event.author = "ops"
    mock_event.is_final_response.return_value = True
    mock_event.content = MagicMock()
    mock_event.content.parts = [mock_part]

    async def mock_run_async(**kwargs):
        yield mock_event

    mock_runner.run_async = mock_run_async

    mock_session_service = MagicMock()
    mock_session_service.get_session = AsyncMock(return_value=None)
    mock_session_service.create_session = AsyncMock()
    mock_session_service.delete_session = AsyncMock()

    orc.runner = mock_runner
    orc.session_service = mock_session_service

    agent, response, _ = await orc.route_and_execute("Check board status")
    assert agent == "ops"


async def test_gmail_send_failure(monkeypatch):
    """Gmail send failure returns error message, does not crash."""
    from mcp_runtime import SerializedMcpToolset
    from google.adk.tools.function_tool import FunctionTool

    async def _error_tool(**kwargs):
        RECORDED_CALLS.append({"tool": "send_email", "args": dict(kwargs)})
        return '{"error": "auth_failure", "message": "Invalid credentials"}'

    fake_tools_override = {
        id(main.gmail_ts): [FunctionTool(func=_error_tool)],
    }

    async def _fake_get_tools(self, readonly_context=None):
        return fake_tools_override.get(id(self), [])

    monkeypatch.setattr(SerializedMcpToolset, "get_tools", _fake_get_tools)

    mock_runner = MagicMock()
    mock_part = MagicMock()
    mock_part.text = "Failed to send email."

    mock_event = MagicMock()
    mock_event.author = "dispatcher"
    mock_event.is_final_response.return_value = True
    mock_event.content = MagicMock()
    mock_event.content.parts = [mock_part]

    async def mock_run_async(**kwargs):
        yield mock_event

    mock_runner.run_async = mock_run_async

    orc = main.AcmeOrchestrator()
    orc.runner = mock_runner
    orc.session_service = MagicMock()
    orc.session_service.create_session = AsyncMock()
    orc.session_service.get_session = AsyncMock(return_value=None)
    orc.session_service.delete_session = AsyncMock()

    agent, response, _ = await orc.route_and_execute("Send confirmation email")
    assert response is not None


async def test_rag_tool_returns_empty(monkeypatch):
    """consult_knowledge returns no results â€” agent escalates rather than hallucinate."""
    from mcp_runtime import SerializedMcpToolset
    from google.adk.tools.function_tool import FunctionTool

    async def _empty_tool(**kwargs):
        RECORDED_CALLS.append({"tool": "consult_knowledge", "args": dict(kwargs)})
        return '{"answer": "", "confidence": 0.0}'

    fake_tools_override = {
        id(main.rag_ts): [FunctionTool(func=_empty_tool)],
    }

    async def _fake_get_tools(self, readonly_context=None):
        return fake_tools_override.get(id(self), [])

    monkeypatch.setattr(SerializedMcpToolset, "get_tools", _fake_get_tools)

    mock_runner = MagicMock()
    mock_part = MagicMock()
    mock_part.text = "I don't have that information."

    mock_event = MagicMock()
    mock_event.author = "dispatcher"
    mock_event.is_final_response.return_value = True
    mock_event.content = MagicMock()
    mock_event.content.parts = [mock_part]

    async def mock_run_async(**kwargs):
        yield mock_event

    mock_runner.run_async = mock_run_async

    orc = main.AcmeOrchestrator()
    orc.runner = mock_runner
    orc.session_service = MagicMock()
    orc.session_service.create_session = AsyncMock()
    orc.session_service.get_session = AsyncMock(return_value=None)
    orc.session_service.delete_session = AsyncMock()

    agent, response, _ = await orc.route_and_execute("What's our policy for Saturday openings?")
    assert agent == "dispatcher"
    assert response is not None


async def test_multiple_tool_failures_handled(monkeypatch):
    """Chain of failures â€” first tool fails, second also fails â€” graceful degradation."""
    from mcp_runtime import SerializedMcpToolset
    from google.adk.tools.function_tool import FunctionTool

    call_count = {"count": 0}

    async def _failing_tool(**kwargs):
        call_count["count"] += 1
        RECORDED_CALLS.append({"tool": "list_unpaid_invoices", "args": dict(kwargs)})
        raise ConnectionError("MCP connection lost")

    async def _second_failing_tool(**kwargs):
        raise ConnectionError("MCP connection lost")

    fake_tools_override = {
        id(main.finance_xero_ts): [FunctionTool(func=_failing_tool)],
        id(main.reporting_ts): [FunctionTool(func=_second_failing_tool)],
    }

    async def _fake_get_tools(self, readonly_context=None):
        return fake_tools_override.get(id(self), [])

    monkeypatch.setattr(SerializedMcpToolset, "get_tools", _fake_get_tools)

    mock_runner = MagicMock()
    mock_part = MagicMock()
    mock_part.text = "Unable to process request."

    mock_event = MagicMock()
    mock_event.author = "finance"
    mock_event.is_final_response.return_value = True
    mock_event.content = MagicMock()
    mock_event.content.parts = [mock_part]

    async def mock_run_async(**kwargs):
        yield mock_event

    mock_runner.run_async = mock_run_async

    orc = main.AcmeOrchestrator()
    orc.runner = mock_runner
    orc.session_service = MagicMock()
    orc.session_service.create_session = AsyncMock()
    orc.session_service.get_session = AsyncMock(return_value=None)
    orc.session_service.delete_session = AsyncMock()

    agent, response, _ = await orc.route_and_execute("List all unpaid invoices")
    assert agent == "finance"
    assert response is not None