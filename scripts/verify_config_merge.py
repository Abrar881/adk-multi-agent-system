"""
Unit test: board-id-aware config merge in config/monday_config.py.
No network — monkeypatches the Firestore call.
All cases should PASS.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Clear any prior cache before each case
import config.monday_config as mc

# Mirrors config/monday_config.py's BOOKINGS_BOARD_ID env-fallback default, which is
# now the live inventories board (18392817962) after the account remap. This test
# exercises the merge logic against that fallback constant.
ENV_BOARD = 18392817962  # env-fallback default (config/monday_config.py)

CASES = []


def run_case(label, firestore_doc, env_overrides=None, expect_board=None,
             expect_cols=None, expect_none_cols=None, expect_board_zero=False):
    """expect_cols: {logical: col_id, ...}, expect_none_cols: [logical, ...]"""
    CASES.append((label, firestore_doc, env_overrides, expect_board,
                  expect_cols, expect_none_cols, expect_board_zero))


# ── Case 1: no Firestore doc -> pure env fallback ─────────────────────────────
run_case(
    "doc missing -> pure env fallback",
    firestore_doc=None,
    expect_board=ENV_BOARD,
    expect_cols={"customer": "customer", "report_type": "report___type"},
)

# ── Case 2: token-only doc (no boards key) -> env board+columns survive ────────
run_case(
    "token-only doc (no boards) -> env fallback wins (brick fixed)",
    firestore_doc={"updated_at": "2026-06-28T00:00:00Z"},
    expect_board=ENV_BOARD,
    expect_cols={"customer": "customer", "report_type": "report___type"},
)

# ── Case 3: different board id -> env columns dropped ─────────────────────────
run_case(
    "different board id -> env columns dropped (no stale leak)",
    firestore_doc={"boards": {"inventories": {
        "id": 99999,
        "columns": {"customer": {"id": "text_x"}, "date": {"id": "date_y"}},
    }}},
    expect_board=99999,
    expect_cols={"customer": "text_x", "date": "date_y"},
    expect_none_cols=["report_type"],  # NOT present in Firestore for this board
)

# ── Case 4: same board id, partial columns -> env fills gaps ──────────────────
run_case(
    "same board id, partial columns -> env fills unmapped gaps",
    firestore_doc={"boards": {"inventories": {
        "id": ENV_BOARD,
        "columns": {"customer": {"id": "c2"}},
    }}},
    expect_board=ENV_BOARD,
    expect_cols={"customer": "c2", "report_type": "report___type"},  # env gap filled
)

# ── Case 5: MONDAY_ALLOW_TEST_FALLBACK=0, doc missing -> board blanked ─────────
run_case(
    "MONDAY_ALLOW_TEST_FALLBACK=0, doc missing -> board id blanked",
    firestore_doc=None,
    env_overrides={"MONDAY_ALLOW_TEST_FALLBACK": "0"},
    expect_board_zero=True,
)

# ── Case 6: MONDAY_ALLOW_TEST_FALLBACK=0 but Firestore loaded real board ─────
run_case(
    "MONDAY_ALLOW_TEST_FALLBACK=0 but real board loaded -> no blanking",
    firestore_doc={"boards": {"inventories": {
        "id": 77777,
        "columns": {"customer": {"id": "cust_real"}},
    }}},
    env_overrides={"MONDAY_ALLOW_TEST_FALLBACK": "0"},
    expect_board=77777,
    expect_cols={"customer": "cust_real"},
    expect_none_cols=["report_type"],  # different board -> env cols dropped
)


# ─── Runner ──────────────────────────────────────────────────────────────────

class _FakeDoc:
    def __init__(self, data):
        self._data = data
        self.exists = data is not None
    def to_dict(self):
        return self._data


def _patch_firestore(doc_data, monkeypatch_db):
    """Patch get_config to intercept the Firestore call via monkeypatching."""
    import unittest.mock as mock

    fake_doc = _FakeDoc(doc_data)
    fake_collection = mock.MagicMock()
    fake_collection.document.return_value.get.return_value = fake_doc
    fake_db = mock.MagicMock()
    fake_db.collection.return_value = fake_collection
    monkeypatch_db[0] = fake_db


def run_all():
    import unittest.mock as mock

    all_pass = True
    print("=== verify_config_merge.py ===\n")

    for (label, firestore_doc, env_overrides, expect_board,
         expect_cols, expect_none_cols, expect_board_zero) in CASES:

        mc.invalidate_cache()

        # Patch env vars
        saved_env = {}
        if env_overrides:
            for k, v in env_overrides.items():
                saved_env[k] = os.environ.get(k)
                os.environ[k] = v

        # Patch Firestore
        fake_doc = _FakeDoc(firestore_doc)
        fake_col = mock.MagicMock()
        fake_col.document.return_value.get.return_value = fake_doc
        fake_db = mock.MagicMock()
        fake_db.collection.return_value = fake_col

        with mock.patch("google.cloud.firestore.Client", return_value=fake_db):
            got_board = mc.get_board_id("inventories")
            got_cols = {}
            if expect_cols:
                for logical in expect_cols:
                    got_cols[logical] = mc.get_column_id("inventories", logical)
            got_none_cols = {}
            if expect_none_cols:
                for logical in expect_none_cols:
                    got_none_cols[logical] = mc.get_column_id("inventories", logical)

        # Restore env
        for k, orig in saved_env.items():
            if orig is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = orig

        # Evaluate
        errors = []
        if expect_board is not None and got_board != expect_board:
            errors.append(f"board: got {got_board!r}, want {expect_board!r}")
        if expect_board_zero and got_board != 0:
            errors.append(f"board: got {got_board!r}, want 0 (blanked)")
        for logical, want_id in (expect_cols or {}).items():
            got_id = got_cols.get(logical)
            if got_id != want_id:
                errors.append(f"col[{logical}]: got {got_id!r}, want {want_id!r}")
        for logical in (expect_none_cols or []):
            got_id = got_none_cols.get(logical)
            if got_id is not None:
                errors.append(f"col[{logical}]: got {got_id!r}, want None (env leak!)")

        status = "PASS" if not errors else "FAIL"
        print(f"{status}  {label}")
        for e in errors:
            print(f"       FAIL: {e}")
        if errors:
            all_pass = False

        mc.invalidate_cache()

    print()
    if all_pass:
        print("All cases pass.")
    else:
        print("Some cases FAILED.")
        sys.exit(1)


if __name__ == "__main__":
    run_all()
