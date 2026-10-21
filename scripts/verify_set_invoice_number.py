"""
Offline test: set_invoice_number's board_id -> board_key resolution and pay_link write.
Pure-Python, no network — mocks Firestore (forces env-fallback config) and _monday_request
(captures the mutation payload instead of calling the real Monday API).

Regression coverage for a bug found while wiring up Pay Link: set_invoice_number was
calling require_column_id(board_id, ...) / get_column_id(board_id, ...) with the numeric
Monday board_id, but those config.monday_config functions are keyed by the string
board_key ("inventories"/"contractors") — every real call would have raised ValueError.
"""
import sys, os, asyncio, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest.mock as mock

import config.monday_config as mc


def _force_env_fallback():
    """Mock Firestore so get_config() falls through to pure env-fallback (deterministic)."""
    mc.invalidate_cache()
    fake_doc = mock.MagicMock()
    fake_doc.exists = False
    fake_col = mock.MagicMock()
    fake_col.document.return_value.get.return_value = fake_doc
    fake_db = mock.MagicMock()
    fake_db.collection.return_value = fake_col
    return mock.patch("google.cloud.firestore.Client", return_value=fake_db)


async def run():
    print("=== verify_set_invoice_number.py ===\n")
    all_pass = True

    with _force_env_fallback():
        import servers.monday_server as ms

        captured = {}

        async def fake_monday_request(query, variables=None):
            captured["query"] = query
            captured["variables"] = variables
            item_id = json.loads(variables["column_values"]) and variables["item_id"]
            return {"data": {"change_multiple_column_values": {"id": item_id}}}

        # ── Case 1: real inventories board_id -> resolves board_key, writes pay_link ──
        with mock.patch.object(ms, "_monday_request", fake_monday_request):
            result = await ms.set_invoice_number(
                board_id="18392817962",
                item_id="999",
                invoice_number="INV-OFFLINE-001",
                pay_link="https://in.xero.com/offline-test",
            )
        errors = []
        if result.get("status") != "success":
            errors.append(f"expected status=success, got {result!r}")
        cv = json.loads(captured["variables"]["column_values"])
        if cv.get("inv__4") != "INV-OFFLINE-001":
            errors.append(f"inv_number col: got {cv.get('inv__4')!r}, want 'INV-OFFLINE-001'")
        if cv.get("payment___status") != {"label": "Invoice sent by xero"}:
            errors.append(f"payment_status col (AUTHORISED): got {cv.get('payment___status')!r}, want 'Invoice sent by xero'")
        if cv.get("text7") != "https://in.xero.com/offline-test":
            errors.append(f"pay_link col (text7): got {cv.get('text7')!r}, want the test URL")
        status = "PASS" if not errors else "FAIL"
        print(f"{status}  inventories board_id -> board_key resolves; inv_number + payment_status + pay_link written")
        for e in errors:
            print(f"       FAIL: {e}")
        all_pass = all_pass and not errors

        # ── Case 2: real contractors board_id -> resolves board_key, pay_link column exists too ──
        captured.clear()
        with mock.patch.object(ms, "_monday_request", fake_monday_request):
            result = await ms.set_invoice_number(
                board_id="18397426458",
                item_id="888",
                invoice_number="INV-OFFLINE-002",
                pay_link="https://in.xero.com/offline-test-2",
            )
        errors = []
        if result.get("status") != "success":
            errors.append(f"expected status=success, got {result!r}")
        cv = json.loads(captured["variables"]["column_values"])
        if cv.get("text7") != "https://in.xero.com/offline-test-2":
            errors.append(f"pay_link col (text7): got {cv.get('text7')!r}")
        status = "PASS" if not errors else "FAIL"
        print(f"{status}  contractors board_id -> board_key resolves; pay_link written")
        for e in errors:
            print(f"       FAIL: {e}")
        all_pass = all_pass and not errors

        # ── Case 3: pay_link omitted -> no pay_link column written, no crash ──────────
        captured.clear()
        with mock.patch.object(ms, "_monday_request", fake_monday_request):
            result = await ms.set_invoice_number(
                board_id="18392817962",
                item_id="777",
                invoice_number="INV-OFFLINE-003",
            )
        errors = []
        if result.get("status") != "success":
            errors.append(f"expected status=success, got {result!r}")
        cv = json.loads(captured["variables"]["column_values"])
        if "text7" in cv:
            errors.append(f"pay_link col should be absent when pay_link=None, got {cv.get('text7')!r}")
        status = "PASS" if not errors else "FAIL"
        print(f"{status}  pay_link omitted -> column not written, no crash")
        for e in errors:
            print(f"       FAIL: {e}")
        all_pass = all_pass and not errors

        # ── Case 4: unrecognized board_id -> clean error, not a crash/wrong-board write ──
        captured.clear()
        with mock.patch.object(ms, "_monday_request", fake_monday_request):
            result = await ms.set_invoice_number(
                board_id="99999999999",
                item_id="666",
                invoice_number="INV-OFFLINE-004",
            )
        errors = []
        if result.get("status") != "error":
            errors.append(f"expected status=error for unrecognized board_id, got {result!r}")
        if "query" in captured:
            errors.append("_monday_request was called for an unrecognized board_id — should have short-circuited")
        status = "PASS" if not errors else "FAIL"
        print(f"{status}  unrecognized board_id -> clean error, no mutation attempted")
        for e in errors:
            print(f"       FAIL: {e}")
        all_pass = all_pass and not errors

        # ── Case 5: amount provided (inventories) -> price col + companion columns zeroed ──
        captured.clear()
        with mock.patch.object(ms, "_monday_request", fake_monday_request):
            result = await ms.set_invoice_number(
                board_id="18392817962",
                item_id="555",
                invoice_number="INV-OFFLINE-005",
                amount=150.0,
            )
        errors = []
        if result.get("status") != "success":
            errors.append(f"expected status=success, got {result!r}")
        cv = json.loads(captured["variables"]["column_values"])
        if cv.get("numeric_mm28fpan") != "150.0":
            errors.append(f"price col (numeric_mm28fpan): got {cv.get('numeric_mm28fpan')!r}, want '150.0'")
        if cv.get("numeric_mm28j2pq") != "0":
            errors.append(f"surcharge col: got {cv.get('numeric_mm28j2pq')!r}, want '0'")
        if cv.get("numeric_mm28b3gs") != "0":
            errors.append(f"discount col: got {cv.get('numeric_mm28b3gs')!r}, want '0'")
        status = "PASS" if not errors else "FAIL"
        print(f"{status}  amount provided (inventories) -> price col + surcharge + discount% zeroed")
        for e in errors:
            print(f"       FAIL: {e}")
        all_pass = all_pass and not errors

        # ── Case 6: amount=None (default) -> price col not written (backward compat) ──
        captured.clear()
        with mock.patch.object(ms, "_monday_request", fake_monday_request):
            result = await ms.set_invoice_number(
                board_id="18392817962",
                item_id="444",
                invoice_number="INV-OFFLINE-006",
            )
        errors = []
        if result.get("status") != "success":
            errors.append(f"expected status=success, got {result!r}")
        cv = json.loads(captured["variables"]["column_values"])
        for price_col in ("numeric_mm28fpan", "numeric_mm28j2pq", "numeric_mm28b3gs"):
            if price_col in cv:
                errors.append(f"price/companion col {price_col} should be absent when amount=None, got {cv.get(price_col)!r}")
        status = "PASS" if not errors else "FAIL"
        print(f"{status}  amount=None -> price and companion columns not written (backward compat)")
        for e in errors:
            print(f"       FAIL: {e}")
        all_pass = all_pass and not errors

        # ── Case 7: xero_status="DRAFT" -> payment_status label is "Invoice drafted" ──
        captured.clear()
        with mock.patch.object(ms, "_monday_request", fake_monday_request):
            result = await ms.set_invoice_number(
                board_id="18392817962",
                item_id="333",
                invoice_number="INV-OFFLINE-007",
                xero_status="DRAFT",
            )
        errors = []
        if result.get("status") != "success":
            errors.append(f"expected status=success, got {result!r}")
        cv = json.loads(captured["variables"]["column_values"])
        if cv.get("payment___status") != {"label": "Invoice drafted"}:
            errors.append(f"payment_status col (DRAFT): got {cv.get('payment___status')!r}, want 'Invoice drafted'")
        status = "PASS" if not errors else "FAIL"
        print(f"{status}  xero_status=DRAFT -> payment_status set to 'Invoice drafted'")
        for e in errors:
            print(f"       FAIL: {e}")
        all_pass = all_pass and not errors

    print()
    if all_pass:
        print("All cases pass.")
    else:
        print("Some cases FAILED.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(run())
