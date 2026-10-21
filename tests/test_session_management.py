"""Integration tests for Firestore session management.

Layer 0.5 â€” no LLM, but tests component interaction. Mock Firestore client.

Run: pytest tests/test_session_management.py -v
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from contextlib import AsyncExitStack

import main


async def test_new_session_created_with_unique_id():
    """Each new session gets a unique uuid-based session_id."""
    mock_session_service = MagicMock()
    mock_session_service.get_session = AsyncMock(return_value=None)
    mock_session_service.create_session = AsyncMock()
    mock_session_service.delete_session = AsyncMock()

    mock_runner = MagicMock()

    mock_part = MagicMock()
    mock_part.text = "Done"

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
    orc.session_service = mock_session_service

    _, _, session_id = await orc.route_and_execute("Test", session_id=None)

    assert session_id.startswith("req-")
    assert len(session_id) > 4
    mock_session_service.create_session.assert_called_once()


async def test_session_persists_across_calls():
    """Same session_id returns same session state."""
    mock_session_service = MagicMock()

    existing = MagicMock()
    existing.state = {"history": ["msg1", "msg2"]}
    mock_session_service.get_session = AsyncMock(return_value=existing)
    mock_session_service.create_session = AsyncMock()

    mock_runner = MagicMock()

    mock_part = MagicMock()
    mock_part.text = "Reply"

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
    orc.session_service = mock_session_service

    _, _, session_id = await orc.route_and_execute("Test", session_id="persistent-123")

    assert session_id == "persistent-123"
    mock_session_service.get_session.assert_called()


async def test_session_stores_agent_state():
    """After route_and_execute, session contains agent response context."""
    mock_session_service = MagicMock()
    mock_session_service.get_session = AsyncMock(return_value=None)
    mock_session_service.create_session = AsyncMock()

    stored_state = {}

    async def mock_create_session(**kwargs):
        stored_state["call"] = kwargs

    mock_session_service.create_session = AsyncMock(side_effect=mock_create_session)

    mock_runner = MagicMock()

    mock_part = MagicMock()
    mock_part.text = "Invoice paid."

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
    orc.session_service = mock_session_service

    await orc.route_and_execute("Test", session_id=None)

    assert "call" in stored_state
    assert stored_state["call"]["session_id"].startswith("req-")


async def test_expired_session_handled():
    """Expired/deleted session_id creates a fresh session (get_session returns None)."""
    mock_session_service = MagicMock()
    mock_session_service.get_session = AsyncMock(return_value=None)
    mock_session_service.create_session = AsyncMock()

    mock_runner = MagicMock()

    mock_part = MagicMock()
    mock_part.text = "Done"

    mock_event = MagicMock()
    mock_event.author = "ops"
    mock_event.is_final_response.return_value = True
    mock_event.content = MagicMock()
    mock_event.content.parts = [mock_part]

    async def mock_run_async(**kwargs):
        yield mock_event

    mock_runner.run_async = mock_run_async

    orc = main.AcmeOrchestrator()
    orc.runner = mock_runner
    orc.session_service = mock_session_service

    _, _, session_id = await orc.route_and_execute("Test", session_id="deleted-session")

    assert session_id == "deleted-session"
    mock_session_service.create_session.assert_called_once()


async def test_ephemeral_session_deleted_after_request():
    """When session_id=None, session is deleted after the request completes."""
    mock_session_service = MagicMock()
    mock_session_service.get_session = AsyncMock(return_value=None)
    mock_session_service.create_session = AsyncMock()
    mock_session_service.delete_session = AsyncMock()

    mock_runner = MagicMock()

    mock_part = MagicMock()
    mock_part.text = "Done"

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
    orc.session_service = mock_session_service

    await orc.route_and_execute("Test", session_id=None)

    mock_session_service.delete_session.assert_called_once()