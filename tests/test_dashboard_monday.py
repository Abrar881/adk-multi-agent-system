"""Tests for Monday.com settings dashboard endpoints."""
import json
import unittest.mock as mock

import pytest

# Must set env vars before importing app
import os
os.environ.setdefault("GOOGLE_CLOUD_PROJECT", "test-project")
os.environ.setdefault("DASHBOARD_PASSWORD", "test-password-secure-123")
os.environ.setdefault("DASHBOARD_SECRET_KEY", "test-secret-key-secure-abc-xyz")
os.environ.setdefault("Acme_MAS_URL", "http://localhost:8080")

from dashboard.app import app


@pytest.fixture
def client():
    app.config["TESTING"] = True
    app.config["WTF_CSRF_ENABLED"] = False
    with app.test_client() as c:
        with c.session_transaction() as sess:
            sess["authenticated"] = True
        yield c


def _mock_firestore_doc(data=None, exists=True):
    doc = mock.MagicMock()
    doc.exists = exists
    doc.to_dict.return_value = data or {}
    return doc


def test_token_validation_success(client):
    mock_doc = _mock_firestore_doc(exists=False)
    mock_db = mock.MagicMock()
    mock_db.collection.return_value.document.return_value.get.return_value = mock_doc
    mock_db.collection.return_value.document.return_value.set = mock.MagicMock()

    with mock.patch("dashboard.app.db", mock_db), \
         mock.patch("dashboard.app._monday_api_request", return_value={"data": {"me": {"name": "Test User"}}}):
        r = client.post("/api/settings/monday/token",
                        json={"token": "test-token"},
                        content_type="application/json")
    assert r.status_code == 200
    assert r.get_json()["account_name"] == "Test User"


def test_token_validation_failure(client):
    with mock.patch("dashboard.app._monday_api_request", return_value={"errors": [{"message": "Bad token"}]}):
        r = client.post("/api/settings/monday/token",
                        json={"token": "bad-token"},
                        content_type="application/json")
    assert r.status_code == 400
    assert "error" in r.get_json()


def test_boards_list_requires_token(client):
    mock_doc = _mock_firestore_doc(data={}, exists=True)
    mock_db = mock.MagicMock()
    mock_db.collection.return_value.document.return_value.get.return_value = mock_doc

    with mock.patch("dashboard.app.db", mock_db):
        r = client.get("/api/settings/monday/boards")
    assert r.status_code == 400
    assert "error" in r.get_json()


def test_detect_columns_endpoint(client):
    mock_doc = _mock_firestore_doc(data={"api_token": "tok"}, exists=True)
    mock_db = mock.MagicMock()
    mock_db.collection.return_value.document.return_value.get.return_value = mock_doc

    mock_board_response = {
        "data": {
            "boards": [{"columns": [{"id": "col1", "title": "Postcode"}], "groups": [{"id": "grp1", "title": "Leads"}]}]
        }
    }
    with mock.patch("dashboard.app.db", mock_db), \
         mock.patch("dashboard.app._monday_api_request", return_value=mock_board_response):
        r = client.post("/api/settings/monday/detect-columns",
                        json={"board_id": "12345"},
                        content_type="application/json")
    assert r.status_code == 200
    data = r.get_json()
    assert "detected" in data
    assert "postcode" in data["detected"]


def test_save_config_validates_inventories_id(client):
    r = client.post("/api/settings/monday/save",
                    json={"boards": {"inventories": {"id": 0}}},
                    content_type="application/json")
    assert r.status_code == 400
