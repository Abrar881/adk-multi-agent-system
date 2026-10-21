"""Unit tests for AcmeOrchestrator lifecycle and configuration.

Layer 0 â€” no LLM, no credentials, no network. All mocks.
Run: pytest tests/test_orchestrator.py -v
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from contextlib import AsyncExitStack

import main


# ---------------------------------------------------------------------------
# Test 1 â€” APP_NAME constant
# ---------------------------------------------------------------------------
def test_app_name_is_multi_agent_system():
    """App name constant is 'multi_agent_system'."""
    import main
    assert main.Acme_app.name == "multi_agent_system"


# ---------------------------------------------------------------------------
# Test 2 â€” hub_agent has no tools (it only routes, never executes)
# ---------------------------------------------------------------------------
def test_hub_agent_has_no_tools():
    """Hub agent has empty tools list â€” routing only."""
    from main import hub_agent
    assert hub_agent.tools == []


# ---------------------------------------------------------------------------
# Test 3 â€” exactly 3 sub-agents with correct names
# ---------------------------------------------------------------------------
def test_sub_agents_have_correct_names():
    """Hub has exactly 3 sub-agents: dispatcher, finance, ops."""
    from main import hub_agent
    names = [a.name for a in hub_agent.sub_agents]
    assert set(names) == {"dispatcher", "finance", "ops"}


# ---------------------------------------------------------------------------
# Test 4 â€” each sub-agent has a model configured
# ---------------------------------------------------------------------------
def test_sub_agent_models_are_set():
    """Each sub-agent has a non-None model string."""
    from main import dispatcher_agent, finance_agent, ops_agent
    for agent in (dispatcher_agent, finance_agent, ops_agent):
        assert agent.model is not None
        assert isinstance(agent.model, str)
        assert len(agent.model) > 0


# ---------------------------------------------------------------------------
# Test 5 â€” initialize() creates FirestoreSessionService
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_initialize_creates_session_service():
    """After initialize(), session_service is a FirestoreSessionService instance."""
    with patch.object(main, 'FirestoreSessionService') as mock_fss_class:
        mock_fss_instance = MagicMock()
        mock_fss_class.return_value = mock_fss_instance

        mock_runner = MagicMock()
        mock_stack = AsyncExitStack()
        mock_stack.push_async_callback = MagicMock()

        orc = main.AcmeOrchestrator()

        with patch.object(main, 'Runner', return_value=mock_runner):
            with patch.object(main, '_all_toolsets', return_value=[]):
                await orc.initialize(mock_stack)

        assert orc.session_service is mock_fss_instance
        mock_fss_class.assert_called_once()


# ---------------------------------------------------------------------------
# Test 6 â€” initialize() creates Runner
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_initialize_creates_runner():
    """After initialize(), runner is an ADK Runner instance."""
    mock_session_service = MagicMock()
    mock_runner = MagicMock()
    mock_stack = AsyncExitStack()
    mock_stack.push_async_callback = MagicMock()

    orc = main.AcmeOrchestrator()
    orc.session_service = mock_session_service

    with patch.object(main, 'Runner', return_value=mock_runner) as mock_runner_cls:
        with patch.object(main, '_all_toolsets', return_value=[]):
            await orc.initialize(mock_stack)

    assert orc.runner is mock_runner
    mock_runner_cls.assert_called_once()


# ---------------------------------------------------------------------------
# Test 7 â€” initialize() calls get_tools() on all 13 toolsets
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_initialize_loads_mcp_tools():
    """initialize() calls SerializedMcpToolset.get_tools() for all 13 toolsets."""
    mock_stack = AsyncExitStack()
    mock_stack.push_async_callback = MagicMock()

    fake_toolset = MagicMock()
    fake_toolset.get_tools = AsyncMock(return_value=[])

    with patch.object(main, '_all_toolsets', return_value=[fake_toolset] * 13):
        with patch.object(main, 'FirestoreSessionService', return_value=MagicMock()):
            with patch.object(main, 'Runner', return_value=MagicMock()):
                orc = main.AcmeOrchestrator()
                await orc.initialize(mock_stack)

    assert fake_toolset.get_tools.call_count == 13


# ---------------------------------------------------------------------------
# Test 8 â€” route_and_execute returns a 3-tuple
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_route_and_execute_returns_triple():
    """route_and_execute returns (agent_name, response_text, session_id)."""
    mock_part = MagicMock()
    mock_part.text = "Test response"

    mock_event = MagicMock()
    mock_event.author = "dispatcher"
    mock_event.is_final_response.return_value = True
    mock_event.content = MagicMock()
    mock_event.content.parts = [mock_part]

    async def mock_run_async(**kwargs):
        yield mock_event

    mock_runner = MagicMock()
    mock_runner.run_async = mock_run_async

    mock_session_service = AsyncMock()
    mock_session_service.create_session = AsyncMock()
    mock_session_service.get_session = AsyncMock(return_value=None)

    orc = main.AcmeOrchestrator()
    orc.runner = mock_runner
    orc.session_service = mock_session_service

    result = await orc.route_and_execute("Test input")

    assert isinstance(result, tuple)
    assert len(result) == 3
    agent, response, session_id = result
    assert isinstance(agent, str)
    assert isinstance(response, str)
    assert isinstance(session_id, str)


# ---------------------------------------------------------------------------
# Test 9 â€” route_and_execute creates new session when session_id is None
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_route_and_execute_creates_session_when_none():
    """When session_id=None, a new session is created with a uuid-based ID."""
    mock_part = MagicMock()
    mock_part.text = "Response"

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
    mock_session_service.get_session = AsyncMock(return_value=None)
    mock_session_service.delete_session = AsyncMock()

    orc = main.AcmeOrchestrator()
    orc.runner = mock_runner
    orc.session_service = mock_session_service

    agent, response, session_id = await orc.route_and_execute("Test input", session_id=None)

    assert session_id.startswith("req-")
    mock_session_service.create_session.assert_called_once()
    mock_session_service.delete_session.assert_called_once()


# ---------------------------------------------------------------------------
# Test 10 â€” route_and_execute reuses existing session when session_id provided
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_route_and_execute_reuses_existing_session():
    """When session_id is provided, it reuses the existing session."""
    mock_part = MagicMock()
    mock_part.text = "Response"

    mock_event = MagicMock()
    mock_event.author = "finance"
    mock_event.is_final_response.return_value = True
    mock_event.content = MagicMock()
    mock_event.content.parts = [mock_part]

    async def mock_run_async(**kwargs):
        yield mock_event

    mock_runner = MagicMock()
    mock_runner.run_async = mock_run_async

    existing_session = MagicMock()
    mock_session_service = AsyncMock()
    mock_session_service.get_session = AsyncMock(return_value=existing_session)
    mock_session_service.create_session = AsyncMock()

    orc = main.AcmeOrchestrator()
    orc.runner = mock_runner
    orc.session_service = mock_session_service

    agent, response, session_id = await orc.route_and_execute(
        "Test input", session_id="existing-session-123"
    )

    assert session_id == "existing-session-123"
    mock_session_service.create_session.assert_not_called()
    mock_session_service.delete_session.assert_not_called()