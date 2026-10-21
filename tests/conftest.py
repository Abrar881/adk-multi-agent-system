"""Shared pytest fixtures for the Multi-Agent System test suite.

Sets required environment variables before any test module imports main.py,
so that the PROJECT_ID guard and lazy Firestore client don't fail in CI.
"""
import os

# Must be set before `import main` anywhere in the test session.
os.environ.setdefault("GOOGLE_CLOUD_PROJECT", "test-project")
os.environ.setdefault("GOOGLE_CLOUD_LOCATION", "global")
os.environ.setdefault("DASHBOARD_PASSWORD", "test-password-secure-123")
os.environ.setdefault("DASHBOARD_SECRET_KEY", "test-secret-key-secure-abc-xyz")

import pytest
from unittest.mock import MagicMock


@pytest.fixture
def mock_log_event(monkeypatch):
    """Capture log_event calls without hitting Firestore or stderr."""
    import main
    calls = []
    monkeypatch.setattr(main, "log_event", lambda *a, **kw: calls.append({"args": a, "kwargs": kw}))
    return calls
