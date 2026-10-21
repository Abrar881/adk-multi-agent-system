#!/usr/bin/env python3
"""One-off: fetch Monday board column types to verify payload formats used in monday_server.py.

Usage:
    python scripts/check_board_column_types.py

Repointed to the new Monday account's inventories board (was 18410040105, now
18392817962), with COLUMNS_OF_INTEREST/EXPECTED_TYPES updated to the new column
ids per the approved mapping (see scripts/repair_monday_column_config.py). This
script is read-only (schema fetch only, no writes), so a stale/incorrect entry
here just shows up as a MISSING or TYPE MISMATCH line rather than corrupting
data — EXPECTED_TYPES below are carried over from the old board's verified types
for equivalent fields and have not been independently re-verified against the
new board's live schema; treat mismatches as informational, not authoritative.
"""
import asyncio
import json
import os
import sys
import httpx
from dotenv import load_dotenv

load_dotenv()

MONDAY_API_KEY = os.environ.get("MONDAY_API_KEY", "")
BOARD_ID = 18392817962

# Columns used in create_inventory_booking (must stay in sync with monday_server.py)
COLUMNS_OF_INTEREST = {
    # status columns
    "report_status", "inspector", "report___type",
    "payment___status", "status7", "status2",
    # text / location columns
    "spec", "contact_number", "location", "access___info", "dup__of_hour0__1",
    "inv__4", "address", "customer",
    # email / date / numeric
    "email", "date", "numeric_mm28fpan",
}


async def main():
    if not MONDAY_API_KEY:
        print("ERROR: MONDAY_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    query = """
    query($board_id: ID!) {
        boards(ids: [$board_id]) {
            columns {
                id
                title
                type
            }
            groups {
                id
                title
            }
        }
    }
    """
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://api.monday.com/v2",
            headers={"Authorization": MONDAY_API_KEY, "API-Version": "2023-10"},
            json={"query": query, "variables": {"board_id": str(BOARD_ID)}},
        )
        data = resp.json()

    if "errors" in data:
        print("GraphQL errors:", data["errors"])
        sys.exit(1)

    board = data["data"]["boards"][0]

    print(f"\n=== Board {BOARD_ID} Groups ===")
    for g in board["groups"]:
        print(f"  {g['id']:30s}  {g['title']}")

    print(f"\n=== Columns (relevant to monday_server.py) ===")
    print(f"{'ID':35s} {'Type':20s} {'Title'}")
    print("-" * 80)
    for col in sorted(board["columns"], key=lambda c: c["id"]):
        marker = " *** RELEVANT ***" if col["id"] in COLUMNS_OF_INTEREST else ""
        title = col["title"].encode("ascii", "replace").decode()
        print(f"{col['id']:35s} {col['type']:20s} {title}{marker}")

    # Best-effort expectations carried over from the old board's verified types for
    # the equivalent logical field — not independently re-verified on the new board.
    # "location" is expected to be Monday's native location column type (the old
    # board's postcode was a plain "text" column); flag a mismatch here as informational.
    EXPECTED_TYPES = {
        "report_status": "status", "inspector": "status", "report___type": "status",
        "payment___status": "status", "status7": "status", "status2": "status",
        "spec": "text", "contact_number": "text", "location": "location",
        "access___info": "text", "dup__of_hour0__1": "text",
        "inv__4": "text", "address": "text", "customer": "text",
        "email": "email", "date": "date", "numeric_mm28fpan": "numbers",
    }

    print("\n=== Type issues to check ===")
    col_map = {c["id"]: c for c in board["columns"]}
    all_ok = True
    for col_id in sorted(COLUMNS_OF_INTEREST):
        col = col_map.get(col_id)
        if not col:
            print(f"  MISSING: {col_id} not found on board!")
            all_ok = False
        else:
            t = col["type"]
            expected = EXPECTED_TYPES.get(col_id)
            if expected and t != expected:
                print(f"  TYPE MISMATCH: {col_id} — expected '{expected}', got '{t}'")
                all_ok = False
            else:
                title = col["title"].encode("ascii", "replace").decode()
                print(f"  OK  {col_id:35s} type={t}  ({title})")
    if all_ok:
        print("\n  All columns present and types match.")


if __name__ == "__main__":
    asyncio.run(main())
