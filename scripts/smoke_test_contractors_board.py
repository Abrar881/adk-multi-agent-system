#!/usr/bin/env python3
"""Smoke test: verify all columns on the Contractors board (18397426458) can be filled.

Creates a test item in the TEST_GROUP group, reads back every column, asserts
non-blank values, tests an update, then archives. No production code is changed.

Usage:
    python scripts/smoke_test_contractors_board.py

Repointed to the new Monday account's contractors board (was 18328214271, now
18397426458). Column ids are NOT hardcoded here — the schema (columns + types)
is fetched live from the board each run, so no column-id changes were needed.
TEST_GROUP="cancelled_jobs" is carried over from the old board; it has not been
re-verified to exist on the new board — if it doesn't, create_item will fail
loudly with a clear "invalid group" error rather than silently writing wrong data.
"""
import asyncio
import json
import os
import sys
from datetime import date
import httpx
from dotenv import load_dotenv

load_dotenv()

KEY   = os.environ.get("MONDAY_API_KEY", "")
BOARD = 18397426458
TEST_GROUP = "cancelled_jobs"  # unverified on new board — see module docstring
TODAY = date.today().isoformat()

# Column types that cannot be written via column_values (skip them)
SKIP_TYPES = {
    "auto_number", "creation_log", "last_updated", "pulse_id", "formula",
    "mirror", "board_relation", "dependency", "file", "subtasks",
    "button", "doc", "lookup", "tags", "vote", "time_tracking",
    "world_clock", "country", "location", "timeline", "week",
    "checkbox",  # can be written but not useful for this test
    "rating",
    "color",     # Monday sometimes calls status "color" in older schemas
    "name",      # item name — always populated, not a column_values entry
}

_results: list[tuple[str, str]] = []  # (column_id_description, pass/fail)


def ok(msg: str):
    print(f"  PASS  {msg}")
    _results.append((msg, "PASS"))


def fail(msg: str):
    print(f"  FAIL  {msg}")
    _results.append((msg, "FAIL"))


async def gql(query: str, variables: dict | None = None):
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


async def fetch_schema():
    """Return (columns, status_labels) where status_labels[col_id] = list[str]."""
    q = """
    query($board: ID!) {
        boards(ids: [$board]) {
            columns { id title type settings_str }
            groups   { id title }
        }
    }"""
    data, err = await gql(q, {"board": str(BOARD)})
    if err:
        return None, None, err
    board = data["data"]["boards"][0]

    print(f"\nBoard {BOARD} groups:")
    for g in board["groups"]:
        print(f"  {g['id']:35s}  {g['title']}")

    cols = board["columns"]
    status_labels: dict[str, list[str]] = {}
    for c in cols:
        if c["type"] == "status":
            try:
                settings = json.loads(c["settings_str"] or "{}")
                labels = [v for v in settings.get("labels", {}).values() if v]
                status_labels[c["id"]] = labels
            except Exception:
                status_labels[c["id"]] = []
    return cols, status_labels, None


def build_column_values(cols: list[dict], status_labels: dict[str, list[str]]) -> dict:
    """Build a column_values dict for every writable column on the board."""
    cv: dict = {}
    for c in cols:
        cid   = c["id"]
        ctype = c["type"]
        title = c["title"]

        if ctype in SKIP_TYPES or cid == "name":
            continue

        if ctype == "text":
            cv[cid] = f"Smoke test {title}"
        elif ctype == "long_text":
            cv[cid] = f"Smoke test notes for {title} — delete me"
        elif ctype == "status":
            labels = status_labels.get(cid, [])
            if labels:
                cv[cid] = {"label": labels[0]}
            else:
                print(f"  WARN  {cid} ({title}) has no labels — skipping")
        elif ctype == "email":
            cv[cid] = {"email": "smoketest@example.com", "text": "smoketest@example.com"}
        elif ctype == "date":
            cv[cid] = {"date": TODAY}
        elif ctype == "numbers":
            cv[cid] = "99.00"
        elif ctype == "hour":
            # true hour type uses {"hour": int, "minute": int}
            cv[cid] = {"hour": 14, "minute": 30}
        elif ctype == "phone":
            cv[cid] = {"phone": "07000000000", "countryShortName": "GB"}
        else:
            # Unknown writable type — try a plain string, worst case Monday ignores it
            print(f"  INFO  {cid} ({title}) type={ctype} — sending plain string")
            cv[cid] = f"Smoke test {title}"
    return cv


async def create_item(name: str, col: dict, group: str = TEST_GROUP):
    col_json = json.dumps(col)
    q = """
    mutation($board: ID!, $name: String!, $group: String!, $cols: JSON!) {
        create_item(board_id: $board, item_name: $name, group_id: $group, column_values: $cols) { id }
    }"""
    data, err = await gql(q, {"board": str(BOARD), "name": name, "group": group, "cols": col_json})
    if err:
        return None, err
    return data["data"]["create_item"]["id"], None


async def get_item(item_id: str):
    q = """
    query($ids: [ID!]) {
        items(ids: $ids) { id name column_values { id text value } }
    }"""
    data, err = await gql(q, {"ids": [str(item_id)]})
    if err:
        return None, err
    items = data["data"]["items"]
    return items[0] if items else None, None


async def update_columns(item_id: str, col: dict):
    col_json = json.dumps(col)
    q = """
    mutation($board: ID!, $item: ID!, $cols: JSON!) {
        change_multiple_column_values(board_id: $board, item_id: $item, column_values: $cols) { id }
    }"""
    _, err = await gql(q, {"board": str(BOARD), "item": str(item_id), "cols": col_json})
    return err


async def archive_item(item_id: str):
    q = """mutation($id: ID!) { archive_item(item_id: $id) { id } }"""
    _, err = await gql(q, {"id": str(item_id)})
    return err


def check_columns(item: dict, attempted: dict, cols_meta: list[dict]):
    """Assert every attempted column has a non-blank value after read-back."""
    col_map = {cv["id"]: cv for cv in item["column_values"]}
    title_map = {c["id"]: c["title"] for c in cols_meta}
    for cid in attempted:
        cv  = col_map.get(cid)
        ttl = title_map.get(cid, cid)
        if not cv:
            fail(f"{cid} ({ttl}) — column not present in read-back")
            continue
        val = (cv.get("text") or "").strip() or (cv.get("value") or "").strip()
        if val and val not in ("null", "{}", ""):
            ok(f"{cid} ({ttl}) = {val!r}")
        else:
            fail(f"{cid} ({ttl}) — blank or null after write")


async def main():
    if not KEY:
        print("ERROR: MONDAY_API_KEY not set")
        sys.exit(1)

    print(f"Board: {BOARD}  |  Group: {TEST_GROUP}  |  Date: {TODAY}")

    # ------------------------------------------------------------------
    # Phase 1: fetch schema
    # ------------------------------------------------------------------
    print("\n=== Fetching board schema ===")
    cols, status_labels, err = await fetch_schema()
    if err:
        print(f"ERROR: schema fetch failed: {err}")
        sys.exit(1)

    print(f"\nColumns ({len(cols)} total):")
    for c in sorted(cols, key=lambda x: x["id"]):
        slabel = f"  labels={status_labels.get(c['id'], [])[:3]}" if c["type"] == "status" else ""
        title = c["title"].encode("ascii", "replace").decode()
        print(f"  {c['id']:35s} {c['type']:15s} {title}{slabel}")

    # ------------------------------------------------------------------
    # Phase 2: build payload
    # ------------------------------------------------------------------
    print("\n=== Building column_values payload ===")
    col_values = build_column_values(cols, status_labels)
    print(f"  Attempting to fill {len(col_values)} columns:")
    for cid, v in col_values.items():
        print(f"    {cid:35s} -> {json.dumps(v)[:60]}")

    # ------------------------------------------------------------------
    # Phase 3: create item
    # ------------------------------------------------------------------
    print("\n=== create_item ===")
    item_id, err = await create_item("SMOKETEST-CONTRACTOR — delete", col_values)
    if err:
        print(f"  FAIL  create_item error: {err}")
        if "invalid value" in err.lower():
            print("  INFO  Status labels available per column:")
            for cid, labels in status_labels.items():
                if labels:
                    print(f"    {cid}: {labels[:6]}")
        sys.exit(1)
    ok(f"Item created: {item_id}")

    # ------------------------------------------------------------------
    # Phase 4: read back and check columns
    # ------------------------------------------------------------------
    print("\n=== Read-back column assertions ===")
    item, err = await get_item(item_id)
    if err or not item:
        fail(f"get_item failed: {err}")
    else:
        check_columns(item, col_values, cols)

    # ------------------------------------------------------------------
    # Phase 5: test update
    # ------------------------------------------------------------------
    print("\n=== update_multiple_columns ===")
    # pick one text column and one status column to update
    text_col  = next((c["id"] for c in cols if c["type"] == "text"  and c["id"] in col_values), None)
    stat_col  = next((c["id"] for c in cols if c["type"] == "status" and c["id"] in col_values and
                       len(status_labels.get(c["id"], [])) > 1), None)

    update = {}
    if text_col:
        update[text_col] = "Updated by smoke test"
    if stat_col:
        update[stat_col] = {"label": status_labels[stat_col][1]}  # second label

    if update:
        err = await update_columns(item_id, update)
        if err:
            fail(f"update_multiple_columns error: {err}")
        else:
            item2, err2 = await get_item(item_id)
            if err2 or not item2:
                fail(f"get_item (post-update) failed: {err2}")
            else:
                check_columns(item2, update, cols)
    else:
        print("  SKIP  no suitable columns found for update test")

    # ------------------------------------------------------------------
    # Phase 6: archive
    # ------------------------------------------------------------------
    print("\n=== Cleanup ===")
    err = await archive_item(item_id)
    if err:
        print(f"  WARNING: could not archive item {item_id}: {err}")
    else:
        print(f"  Archived item {item_id}")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    passes = sum(1 for _, s in _results if s == "PASS")
    fails  = sum(1 for _, s in _results if s == "FAIL")
    print(f"\n=== Summary: {passes} passed, {fails} failed ===")
    if fails:
        print("\nFailed checks:")
        for msg, status in _results:
            if status == "FAIL":
                print(f"  {msg}")
        sys.exit(1)
    else:
        print("All checks passed.")


if __name__ == "__main__":
    asyncio.run(main())
