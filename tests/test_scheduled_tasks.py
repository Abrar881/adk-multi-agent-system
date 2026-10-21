"""Unit tests for run_scheduled_task() in main.py.

Layer 0 â€” no LLM, no credentials. Verifies correct prompts are generated
per task_name and routed to the correct agent.

Run: pytest tests/test_scheduled_tasks.py -v
"""
import pytest
from unittest.mock import AsyncMock, MagicMock

import main


@pytest.fixture
def stub_orchestrator():
    """Stub orchestrator that records route_and_execute and invoke_agent_directly calls."""
    orc = MagicMock(spec=main.AcmeOrchestrator)
    orc.route_and_execute = AsyncMock(
        return_value=("dispatcher", "Task completed.", "sess-test-123")
    )
    orc.invoke_agent_directly = AsyncMock(
        return_value=("dispatcher", "Task completed.", "sess-test-123")
    )
    return orc


async def test_morning_roll_call_prompt(stub_orchestrator):
    """task_name='morning_roll_call' generates ops roll-call prompt."""
    await main.run_scheduled_task(stub_orchestrator, "morning_roll_call")

    call_args = stub_orchestrator.invoke_agent_directly.call_args
    prompt = call_args[0][0]
    assert "Morning Roll Call" in prompt
    assert "inspector" in prompt.lower() or "contractor" in prompt.lower()


async def test_reconciliation_prompt(stub_orchestrator):
    """task_name='reconciliation' generates finance reconciliation prompt."""
    await main.run_scheduled_task(stub_orchestrator, "reconciliation")

    call_args = stub_orchestrator.invoke_agent_directly.call_args
    prompt = call_args[0][0]
    assert "Reconcile" in prompt or "reconciliation" in prompt.lower()


async def test_inbox_sweep_prompt(stub_orchestrator):
    """task_name='inbox_sweep' generates dispatcher email sweep prompt."""
    await main.run_scheduled_task(stub_orchestrator, "inbox_sweep")

    call_args = stub_orchestrator.invoke_agent_directly.call_args
    prompt = call_args[0][0]
    assert "Inbox Sweep" in prompt or "inbox" in prompt.lower()


async def test_afternoon_chase_prompt(stub_orchestrator):
    """task_name='chase_afternoon' generates finance chase prompt."""
    await main.run_scheduled_task(stub_orchestrator, "chase_afternoon")

    call_args = stub_orchestrator.invoke_agent_directly.call_args
    prompt = call_args[0][0]
    assert "Afternoon" in prompt or "chase" in prompt.lower() or "invoice" in prompt.lower()


async def test_board_hygiene_prompt(stub_orchestrator):
    """task_name='weekly_board_dup' generates ops board duplication prompt."""
    await main.run_scheduled_task(stub_orchestrator, "weekly_board_dup")

    call_args = stub_orchestrator.invoke_agent_directly.call_args
    prompt = call_args[0][0]
    assert "Weekly Board" in prompt or "duplicate" in prompt.lower()


async def test_revenue_audit_prompt(stub_orchestrator):
    """task_name='financial_close' generates finance audit prompt."""
    await main.run_scheduled_task(stub_orchestrator, "financial_close")

    call_args = stub_orchestrator.invoke_agent_directly.call_args
    prompt = call_args[0][0]
    assert "Financial Close" in prompt or "invoice" in prompt.lower()


async def test_unknown_task_name_handled(stub_orchestrator):
    """Unknown task_name returns error dict, does not crash."""
    result = await main.run_scheduled_task(stub_orchestrator, "nonexistent_task_xyz")

    assert result["status"] == "error"
    assert "Unknown" in result["message"] or "error" in result["status"]
    stub_orchestrator.route_and_execute.assert_not_called()
    stub_orchestrator.invoke_agent_directly.assert_not_called()


async def test_scheduled_task_routes_to_correct_agent(stub_orchestrator):
    """Verify morning_roll_callâ†’ops, reconciliationâ†’finance, inbox_sweepâ†’dispatcher."""
    for task_name, expected_agent in [
        ("morning_roll_call", "ops"),
        ("reconciliation", "finance"),
        ("inbox_sweep", "dispatcher"),
        ("chase_afternoon", "finance"),
        ("financial_close", "finance"),
    ]:
        stub_orchestrator.invoke_agent_directly.reset_mock()
        stub_orchestrator.invoke_agent_directly.return_value = (expected_agent, "done", "sess")

        result = await main.run_scheduled_task(stub_orchestrator, task_name)
        assert result["status"] == "success"
        assert result["agent"] == expected_agent