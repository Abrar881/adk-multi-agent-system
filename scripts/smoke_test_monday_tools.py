#!/usr/bin/env python3
"""Smoke test: verify Monday write-tools populate all columns correctly.

Creates test items in the 'cancelled_jobs' group and archives them on exit.
Usage:
    python scripts/smoke_test_monday_tools.py

DEPRECATED: written against the OLD Monday account (inventories board
18410040105, TEST_GROUP="group_mm2qp72e") and its old per-field column ids
(text_mm2qp73p, text_mm2qsstc, text_mm2qcrtb, text_mm2q6vgs, text_mm2q379g,
text_mm2q4acb, text_mm2qqer2, text_mm2q758g, email_mm2qp164, color_mm2qx88z,
color_mm2q46rh, color_mm2q1r4r). None of these exist on the new account's
inventories board (18392817962).

Not force-repointed because it's not a mechanical id swap: on the new board
several of these previously-distinct columns collapse onto shared column ids
(e.g. both "notes" and "access_info"/"access_instructions" now write to the
single "access___info" column, and "phone"/"contact_number" both write to
the single "contact_number" column), and "postcode" moved from a plain text
column to a native Monday "location" column (different value shape, not a
bare string). A straight id substitution here would silently overwrite one
logical field with another rather than fail loudly, so this needs a real
rewrite against the new schema, not a find/replace. Kept for reference only.
"""
import asyncio
import json
import os
import sys
from datetime import date
import httpx
from dotenv import load_dotenv

load_dotenv()

KEY = os.environ.get("MONDAY_API_KEY", "")
BOARD = 18410040105
TEST_GROUP = "group_mm2qp72e"
TODAY = date.today().isoformat()

_results: list[tuple[str, str, str]] = []  # (test, column/field, pass/fail/value)


def ok(test, msg):
    print(f"  PASS  {msg}")
    _results.append((test, msg, "PASS"))


def fail(test, msg):
    print(f"  FAIL  {msg}")
    _results.append((test, msg, "FAIL"))


async def gql(query, variables=None):
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(
            "https://api.monday.com/v2",
            headers={"Authorization": KEY, "API-Version": "2023-10"},
            json={"query": query, "variables": variables or {}},
        )
    data = r.json()
    if "errors" in data:
        return None, data["errors"][0]["message"]
    return data, None


async def create_item(item_name, column_values_dict, group_id=TEST_GROUP):
    col_json = json.dumps(column_values_dict)
    q = """
    mutation($board: ID!, $name: String!, $group: String!, $cols: JSON!) {
        create_item(board_id: $board, item_name: $name, group_id: $group, column_values: $cols) { id }
    }"""
    data, err = await gql(q, {"board": str(BOARD), "name": item_name, "group": group_id, "cols": col_json})
    if err:
        return None, err
    return data["data"]["create_item"]["id"], None


async def get_item(item_id):
    q = """
    query($ids: [ID!]) {
        items(ids: $ids) {
            id name
            column_values { id text value }
        }
    }"""
    data, err = await gql(q, {"ids": [str(item_id)]})
    if err:
        return None, err
    items = data["data"]["items"]
    return items[0] if items else None, None


async def archive_item(item_id):
    q = """mutation($id: ID!) { archive_item(item_id: $id) { id } }"""
    _, err = await gql(q, {"id": str(item_id)})
    return err


async def update_columns(item_id, column_values_dict):
    col_json = json.dumps(column_values_dict)
    q = """
    mutation($board: ID!, $item: ID!, $cols: JSON!) {
        change_multiple_column_values(board_id: $board, item_id: $item, column_values: $cols) { id }
    }"""
    data, err = await gql(q, {"board": str(BOARD), "item": str(item_id), "cols": col_json})
    return err


def check_columns(test_name, item, expected: dict):
    """Assert each expected column has a non-blank value."""
    col_map = {cv["id"]: cv for cv in item["column_values"]}
    for col_id, description in expected.items():
        cv = col_map.get(col_id)
        if not cv:
            fail(test_name, f"{col_id} ({description}) â€” column not found in item response")
            continue
        val = (cv.get("text") or "").strip() or (cv.get("value") or "").strip()
        if val and val not in ("null", "{}", ""):
            ok(test_name, f"{col_id} ({description}) = {val!r}")
        else:
            fail(test_name, f"{col_id} ({description}) â€” blank or null after write")


# ---------------------------------------------------------------------------
# Test 1: create_inventory_booking â€” all columns
# ---------------------------------------------------------------------------
async def test_create_inventory_booking():
    print("\n=== create_inventory_booking ===")
    # Build the column_values exactly as monday_server.py would
    col = {
        "report_status":    {"label": "Completed"},
        "inspector":        {"label": "Assign clerk"},
        "report___type":    {"label": "Midterm"},
        "color_mm2qx88z":   {"label": "Invoice sent by xero"},
        "color_mm2q46rh":   {"label": "Stuck"},
        "color_mm2q1r4r":   {"label": "Please be careful"},
        "text_mm2qp73p":    "SW1A 1AA",
        "spec":             "Check-in",
        "address":          "1 Smoke Test Road London",
        "text_mm2qsstc":    "Smoke Test Client",
        "text_mm2qcrtb":    "Key safe 1234",
        "text_mm2q6vgs":    "07111111111",
        "text_mm2q379g":    "Smoke test notes â€” delete",
        "text_mm2q4acb":    "INV-TEST-001",
        "text_mm2qqer2":    "Ring bell",
        "text_mm2q758g":    "07000000000",
        "dup__of_hour0__1": "14:30",
        "email_mm2qp164":   {"email": "smoketest@example.com", "text": "smoketest@example.com"},
        "date":             {"date": TODAY},
    }

    item_id, err = await create_item("SMOKETEST-BOOKING â€” delete", col)
    if err:
        print(f"  FAIL  create_item error: {err}")
        # Try to get schema labels if it's an invalid value error
        if "invalid value" in err.lower():
            print("  INFO  Fetching status column labels to diagnose...")
            await print_status_labels()
        _results.append(("create_inventory_booking", "create_item", f"FAIL: {err}"))
        return None

    ok("create_inventory_booking", f"Item created: {item_id}")

    item, err = await get_item(item_id)
    if err or not item:
        fail("create_inventory_booking", f"get_item failed: {err}")
        return item_id

    expected = {
        "report_status":    "Report status",
        "inspector":        "Inspector (clerk)",
        "report___type":    "Report type",
        "color_mm2qx88z":   "Payment status",
        "color_mm2q46rh":   "Payment Reminder",
        "color_mm2q1r4r":   "Acme Confirmation",
        "text_mm2qp73p":    "Postcode",
        "spec":             "Specification",
        "address":          "Address",
        "text_mm2qsstc":    "Customer",
        "text_mm2qcrtb":    "Access information",
        "text_mm2q6vgs":    "Contact Number",
        "text_mm2q379g":    "Notes",
        "text_mm2q4acb":    "INV #",
        "text_mm2qqer2":    "Access instructions",
        "text_mm2q758g":    "Phone",
        "dup__of_hour0__1": "Hour",
        "email_mm2qp164":   "Email",
        "date":             "Date",
    }
    check_columns("create_inventory_booking", item, expected)
    return item_id


# ---------------------------------------------------------------------------
# Test 2: update_multiple_columns
# ---------------------------------------------------------------------------
async def test_update_multiple_columns(item_id):
    print("\n=== update_multiple_columns ===")
    updates = {
        "spec":  "Check-out",
        "text_mm2q379g": "Updated by smoke test",
    }
    err = await update_columns(item_id, updates)
    if err:
        fail("update_multiple_columns", f"mutation error: {err}")
        return

    item, err = await get_item(item_id)
    if err or not item:
        fail("update_multiple_columns", f"get_item failed: {err}")
        return

    expected = {"spec": "Specification (updated to Check-out)", "text_mm2q379g": "Notes (updated)"}
    check_columns("update_multiple_columns", item, expected)


# ---------------------------------------------------------------------------
# Test 3: create_lead
# ---------------------------------------------------------------------------
async def test_create_lead():
    print("\n=== create_lead ===")
    col = {
        "email_mm2qp164":  {"email": "lead@example.com", "text": "lead@example.com"},
        "text_mm2q758g":   "07999000000",
        "address":        "99 Lead Street London",
        "text_mm2q379g":  "Lead smoke test note",
        "spec":           "Inventory",
    }
    item_id, err = await create_item("SMOKETEST-LEAD â€” delete", col, group_id="group_mm2q9amr")
    if err:
        fail("create_lead", f"create_item error: {err}")
        return None

    ok("create_lead", f"Item created: {item_id}")

    item, err = await get_item(item_id)
    if err or not item:
        fail("create_lead", f"get_item failed: {err}")
        return item_id

    expected = {
        "email_mm2qp164": "Email",
        "text_mm2q758g":  "Phone",
        "address":        "Address",
        "text_mm2q379g":  "Notes",
        "spec":           "Specification",
    }
    check_columns("create_lead", item, expected)
    return item_id


# ---------------------------------------------------------------------------
# Helper: print available labels for status columns on GraphQL error
# ---------------------------------------------------------------------------
async def print_status_labels():
    q = """query { boards(ids: [18410040105]) { columns { id settings_str } } }"""
    data, _ = await gql(q)
    if not data:
        return
    cols = {c["id"]: c for c in data["data"]["boards"][0]["columns"]}
    for cid in ["report_status", "inspector", "report___type", "color_mm2qx88z",
                "color_mksmbkb8", "color_mm2q1r4r", "color_mm2q46rh"]:
        if cid not in cols:
            continue
        try:
            settings = json.loads(cols[cid]["settings_str"])
            labels = [v for v in settings.get("labels", {}).values() if v]
            print(f"    {cid}: {labels[:6]}")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main():
    if not KEY:
        print("ERROR: MONDAY_API_KEY not set")
        sys.exit(1)

    print(f"Board: {BOARD}  |  Test group: {TEST_GROUP}  |  Date: {TODAY}")
    items_to_archive = []

    booking_id = await test_create_inventory_booking()
    if booking_id:
        items_to_archive.append(("booking", booking_id))
        await test_update_multiple_columns(booking_id)

    lead_id = await test_create_lead()
    if lead_id:
        items_to_archive.append(("lead", lead_id))

    # Archive test items
    print("\n=== Cleanup ===")
    for label, item_id in items_to_archive:
        err = await archive_item(item_id)
        if err:
            print(f"  WARNING: could not archive {label} item {item_id}: {err}")
        else:
            print(f"  Archived {label} item {item_id}")

    # Summary
    passes = sum(1 for _, _, s in _results if s == "PASS")
    fails  = sum(1 for _, _, s in _results if s.startswith("FAIL"))
    print(f"\n=== Summary: {passes} passed, {fails} failed ===")
    if fails:
        print("\nFailed checks:")
        for test, msg, status in _results:
            if status.startswith("FAIL"):
                print(f"  [{test}] {msg}")
        sys.exit(1)
    else:
        print("All checks passed.")


if __name__ == "__main__":
    asyncio.run(main())
