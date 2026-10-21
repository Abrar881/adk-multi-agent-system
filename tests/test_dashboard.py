"""Unit tests for Flask dashboard app (dashboard/app.py).

Layer 0 — no LLM, no credentials, no network.
Uses Flask test client with DASHBOARD_PASSWORD and DASHBOARD_SECRET_KEY set.

Run: pytest tests/test_dashboard.py -v
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
    """Flask test client with testing config."""
    app.config["TESTING"] = True
    app.config["WTF_CSRF_ENABLED"] = False
    with app.test_client() as client:
        yield client


def test_login_page_renders(client):
    """GET /login returns 200."""
    response = client.get("/login")
    assert response.status_code == 200


def test_login_with_correct_password(client):
    """POST /login with correct password redirects to /dashboard."""
    response = client.post("/login", data={"password": "test-password-secure-123"})
    assert response.status_code in (302, 303)


def test_login_with_wrong_password(client):
    """POST /login with wrong password shows error."""
    response = client.post("/login", data={"password": "wrong-password"})
    assert response.status_code == 200
    assert b"error" in response.data.lower() or b"incorrect" in response.data.lower()


def test_dashboard_requires_auth(client):
    """GET /dashboard without login redirects to /login."""
    response = client.get("/dashboard", follow_redirects=False)
    assert response.status_code in (302, 303)
    assert "/login" in response.location


def test_dashboard_accessible_after_login(client):
    """GET /dashboard after login returns 200."""
    with client.session_transaction() as sess:
        sess["authenticated"] = True
    response = client.get("/dashboard")
    assert response.status_code == 200


def test_health_endpoint(client):
    """GET /health returns 200 without auth."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json["status"] == "healthy"


def test_logout(client):
    """GET /logout clears session, redirects to /login."""
    with client.session_transaction() as sess:
        sess["authenticated"] = True
    response = client.get("/logout", follow_redirects=False)
    assert response.status_code in (302, 303)
    assert "/login" in response.location

    with client.session_transaction() as sess:
        assert sess.get("authenticated") is not True


def test_stream_chat_requires_auth(client):
    """POST /api/chat/stream without login returns 401 or redirect."""
    with patch("dashboard.app._get_mas_token", return_value=None):
        response = client.post(
            "/api/chat/stream",
            json={"message": "Hello"},
            follow_redirects=False,
        )
        assert response.status_code in (401, 302)


def test_stream_chat_returns_sse(client):
    """POST /api/chat/stream with auth returns text/event-stream content type."""
    with client.session_transaction() as sess:
        sess["authenticated"] = True

    with patch("dashboard.app._get_mas_token", return_value=None):
        with patch("dashboard.app.http_requests.post") as mock_post:
            mock_resp = MagicMock()
            mock_resp.ok = True
            mock_resp.iter_lines = MagicMock(return_value=iter([]))
            mock_post.return_value = mock_resp

            response = client.post(
                "/api/chat/stream",
                json={"message": "Hello"},
            )

            assert response.content_type.startswith("text/event-stream")