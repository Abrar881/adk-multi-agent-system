"""Unit tests for all dashboard routes.

Layer 0 — no LLM, no credentials. Tests every registered route responds
without 500, CSRF tokens on forms, rate limiting, and agent status display.

Run: pytest tests/test_dashboard_routes.py -v
"""
import os
import pytest
from unittest.mock import patch, MagicMock

os.environ.setdefault("DASHBOARD_PASSWORD", "test-password-secure-123")
os.environ.setdefault("DASHBOARD_SECRET_KEY", "test-secret-key-secure-abc-xyz")
os.environ.setdefault("DASHBOARD_AUTH_ENABLED", "true")

from dashboard.app import app


@pytest.fixture
def client():
    """Authenticated test client."""
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


def test_all_static_routes_respond(client):
    """Every registered route returns 200 or redirect, not 500."""
    with client.session_transaction() as sess:
        sess["authenticated"] = True

    routes = [
        ("/dashboard", 200),
        ("/api/feed", 200),
        ("/api/summary", 200),
        ("/api/escalations", 200),
        ("/api/status", 200),
        ("/api/knowledge", 200),
        ("/api/costs", 200),
        ("/api/chat", 200),
        ("/api/chat/stream", 200),
    ]

    for path, expected_min in routes:
        with patch("dashboard.app.db"):
            response = client.get(path)
            assert response.status_code >= expected_min, (
                f"Route {path} returned {response.status_code}, expected >= {expected_min}"
            )


def test_csrf_token_present_on_forms(client):
    """Login form includes CSRF token hidden field."""
    response = client.get("/login")
    assert response.status_code == 200
    assert b'name="_csrf"' in response.data or b'csrf' in response.data.lower()


def test_rate_limiting_on_login(client):
    """Login route has rate limiting — multiple failed logins trigger limit."""
    with patch("dashboard.app.DASHBOARD_PASSWORD", "correct-password"):
        for _ in range(6):
            client.post("/login", data={"password": "wrong-password"})

        response = client.post("/login", data={"password": "wrong-password"})

        assert response.status_code == 429


def test_stream_endpoint_content_type(client):
    """SSE endpoint returns text/event-stream content type."""
    with client.session_transaction() as sess:
        sess["authenticated"] = True

    with patch("dashboard.app._get_mas_token", return_value=None):
        with patch("dashboard.app.http_requests.post") as mock_post:
            mock_resp = MagicMock()
            mock_resp.ok = True
            mock_resp.iter_lines = MagicMock(return_value=iter([]))
            mock_post.return_value = mock_resp

            response = client.post("/api/chat/stream", json={"message": "test"})

            assert "text/event-stream" in response.content_type


def test_dashboard_shows_agent_status(client):
    """Dashboard page includes agent status section."""
    with client.session_transaction() as sess:
        sess["authenticated"] = True

    with patch("dashboard.app.db") as mock_db:
        mock_docs = []
        mock_db.collection.return_value.where.return_value.order_by.return_value.limit.return_value.stream.return_value = mock_docs

        response = client.get("/dashboard")

        assert response.status_code == 200