"""Unit tests for config.monday_config — runs with env-var fallback, no Firestore."""
import os
import time
import unittest.mock as mock

import pytest

os.environ.setdefault("BOOKINGS_BOARD_ID", "111")
os.environ.setdefault("DAILY_TASKS_BOARD_ID", "222")
os.environ.setdefault("WORKING_SCHEDULE_BOARD_ID", "333")
os.environ.setdefault("MONDAY_API_KEY", "test-key")
os.environ.setdefault("MONDAY_LEADS_GROUP_ID", "grp_leads")
os.environ.setdefault("MONDAY_COL_POSTCODE", "col_postcode")
os.environ.setdefault("MONDAY_COL_EMAIL", "col_email")
os.environ.setdefault("MONDAY_COL_ADDRESS", "col_address")
os.environ.setdefault("MONDAY_COL_DATE", "col_date")
os.environ.setdefault("MONDAY_COL_CUSTOMER", "col_customer")

import config.monday_config as mc


def _invalidate():
    mc._cache = (None, 0.0)


@pytest.fixture(autouse=True)
def reset_cache():
    _invalidate()
    yield
    _invalidate()


def test_env_fallback_when_no_firestore():
    with mock.patch("google.cloud.firestore.Client", side_effect=Exception("no firestore")):
        with mock.patch.dict(os.environ, {"MONDAY_API_KEY": "test-key"}):
            cfg = mc.get_config()
            assert cfg["boards"]["inventories"]["id"] == 111
            assert cfg["boards"]["daily_tasks"]["id"] == 222
            assert mc.get_api_token() == "test-key"


def test_cache_ttl_respected():
    with mock.patch("google.cloud.firestore.Client", side_effect=Exception("no firestore")):
        cfg1 = mc.get_config()
        cfg2 = mc.get_config()
    assert cfg1 is cfg2  # same object from cache


def test_cache_invalidation():
    with mock.patch("google.cloud.firestore.Client", side_effect=Exception("no firestore")):
        cfg1 = mc.get_config()
    mc.invalidate_cache()
    with mock.patch("google.cloud.firestore.Client", side_effect=Exception("no firestore")):
        cfg2 = mc.get_config()
    assert cfg1 is not cfg2


def test_get_column_id_returns_none_for_unmapped():
    with mock.patch("google.cloud.firestore.Client", side_effect=Exception("no firestore")):
        mc.get_config()
    result = mc.get_column_id("inventories", "nonexistent_column")
    assert result is None


def test_detect_columns_exact_match():
    cols = [
        {"id": "col_abc", "title": "Postcode"},
        {"id": "col_def", "title": "Email"},
        {"id": "col_xyz", "title": "Date"},
    ]
    detected = mc.detect_columns(cols)
    assert detected["postcode"]["id"] == "col_abc"
    assert detected["postcode"]["confidence"] == 1.0
    assert detected["email"]["id"] == "col_def"


def test_detect_columns_substring_match():
    cols = [
        {"id": "col_abc", "title": "Customer Post Code Area"},
    ]
    detected = mc.detect_columns(cols)
    assert "postcode" in detected
    assert detected["postcode"]["confidence"] < 1.0


def test_require_column_id_raises_when_unmapped():
    with mock.patch("google.cloud.firestore.Client", side_effect=Exception("no firestore")):
        mc.get_config()
    os.environ["MONDAY_COL_INSPECTOR"] = ""
    _invalidate()
    with mock.patch("google.cloud.firestore.Client", side_effect=Exception("no firestore")):
        mc.get_config()
    with pytest.raises(ValueError, match="inspector"):
        mc.require_column_id("inventories", "inspector")
