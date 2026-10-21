"""Smoke tests for Multi-Agent System â€” all mocked, no credentials or network required.

Test 1: Agent wiring â€” verify hub_agent is constructed with the 3 sub-agents.
Test 2: Routing â€” route_and_execute returns the correct agent when runner emits an ops event.
Test 3: PII redaction â€” _before_tool logs arg keys only, never raw values (pins C3).
"""
import pytest
from unittest.mock import AsyncMock, MagicMock


# ---------------------------------------------------------------------------
# Test 1 â€” orchestrator instantiation + agent wiring
# ---------------------------------------------------------------------------
def test_orchestrator_init():
    """AcmeOrchestrator can be instantiated and hub_agent sub-agents are wired."""
    import main
    from main import AcmeOrchestrator, hub_agent

    orch = AcmeOrchestrator()
    assert orch.runner is None
    assert orch.session_service is None
    assert main.Acme_app.name == "multi_agent_system"

    # Verify the hub has all 3 spokes
    sub_agent_names = [a.name for a in hub_agent.sub_agents]
    assert "dispatcher" in sub_agent_names
    assert "finance" in sub_agent_names
    assert "ops" in sub_agent_names


# ---------------------------------------------------------------------------
# Test 2 â€” route_and_execute routes to ops when runner emits an ops event
# ---------------------------------------------------------------------------
async def test_hub_routes_to_ops():
    """route_and_execute correctly identifies ops as the responding agent."""
    from main import AcmeOrchestrator

    # Build a synthetic final response event from the ops agent
    mock_part = MagicMock()
    mock_part.text = "Job is feasible."

    mock_event = MagicMock()
    mock_event.author = "ops"
    mock_event.is_final_response.return_value = True
    mock_event.content = MagicMock()
    mock_event.content.parts = [mock_part]

    async def mock_run_async(**kwargs):
        yield mock_event

    mock_runner = MagicMock()
    mock_runner.run_async = mock_run_async

    mock_session_service = AsyncMock()
    mock_session_service.create_session = AsyncMock()

    orch = AcmeOrchestrator()
    orch.runner = mock_runner
    orch.session_service = mock_session_service

    agent, result, _ = await orch.route_and_execute("Check if tomorrow's jobs are feasible")

    assert agent == "ops"
    assert result == "Job is feasible."


# ---------------------------------------------------------------------------
# Test 3 â€” ActivityLogPlugin logs key summary only (pins C3 PII redaction)
# ---------------------------------------------------------------------------
async def test_tool_invocation_logged(mock_log_event):
    """ActivityLogPlugin records arg count + key names, never raw argument values."""
    import main
    from plugins.activity_log import ActivityLogPlugin

    stub_tool = MagicMock()
    stub_tool.name = "send_email"

    args = {
        "to": "client@example.com",
        "subject": "Booking confirmation",
        "body": "Dear Client, your booking is confirmed.",
    }

    plugin = ActivityLogPlugin(firestore_log_func=main.log_event)
    result = await plugin.before_tool_callback(tool=stub_tool, tool_args=args, tool_context=None)

    assert result is None, "before_tool_callback must return None to proceed normally"
    assert len(mock_log_event) == 1

    logged_kwargs = mock_log_event[0]["kwargs"]
    assert "arguments" in logged_kwargs

    arg_data = logged_kwargs["arguments"]
    assert arg_data["arg_count"] == 3
    assert sorted(arg_data["keys"]) == ["body", "subject", "to"]

    # Verify no PII values appear anywhere in the logged payload
    log_str = str(mock_log_event)
    assert "client@example.com" not in log_str
    assert "Booking confirmation" not in log_str
    assert "Dear Client" not in log_str
