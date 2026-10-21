"""
Live test: create_inventory_booking with address-embedded postcode (no separate postcode arg).
Verifies:
  1. report___type column populated ("Inventory & Check In")
  2. location (postcode) column populated from address derivation
Then archives the item (reversible, not deleted).
Uses January 2025 group (past/dormant) on the main inventories board.

Repointed to the new Monday account's inventories board (was 18410040105, now
18392817962). GROUP_ID uses the board's dedicated "Test Group" (verified live via
Monday MCP get_board_info — group_mm3t4aev exists on this board).
"""
import sys, os, asyncio, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from servers.monday_server import create_inventory_booking, get_item, archive_item

BOARD_ID = 18392817962
GROUP_ID = "group_mm3t4aev"  # "Test Group" — verified live


async def run():
    print("=== Live test: create_inventory_booking with embedded postcode ===\n")

    # --- Step 1: Create ---
    print("STEP 1: Creating test booking (no postcode arg, postcode embedded in address)...")
    result_raw = await create_inventory_booking(
        board_id=BOARD_ID,
        item_name="TEST_ONLY_DELETE - Postcode Derivation Test",
        booking_date="2025-01-01",
        spec="Inventory & Check In",
        customer="TEST ONLY DELETE",
        address="99 Test Street, London, E1 6RF",  # postcode embedded here
        postcode=None,                              # deliberately omitted
        notes="Automated live test — safe to archive/delete",
        group_id=GROUP_ID,
        thread_id=None,
        source_message_id=None,
    )
    result = json.loads(result_raw)
    print(f"  status       : {result.get('status')}")
    print(f"  item_id      : {result.get('item_id')}")
    print(f"  missing_fields: {result.get('missing_fields', [])}")
    print(f"  dropped_labels: {result.get('dropped_labels', [])}")
    print()

    if result.get("status") not in ("success", "partial_success"):
        print(f"FAIL: booking creation returned status={result.get('status')}")
        print(f"  {result_raw}")
        return

    item_id = result["item_id"]

    # --- Step 2: Verify columns ---
    print(f"STEP 2: Verifying columns on item {item_id}...")
    item_raw = await get_item(item_id)
    item = json.loads(item_raw)
    col_map = {}
    if item.get("status") == "success":
        for cv in item.get("item", {}).get("column_values", []):
            col_map[cv["id"]] = cv.get("text", "")

    rt = col_map.get("report___type", "")
    pc = col_map.get("location", "")
    print(f"  report___type : {rt!r}")
    print(f"  location      : {pc!r}")

    rt_ok = "Inventory & Check In" in rt or rt
    pc_ok = "E1 6RF" in pc or pc
    print()
    print(f"  report_type check : {'PASS' if rt_ok else 'FAIL'} (got {rt!r})")
    print(f"  postcode check    : {'PASS' if pc_ok else 'FAIL'} (got {pc!r})")
    print()

    # --- Step 3: Archive ---
    print(f"STEP 3: Archiving test item {item_id} (reversible)...")
    arch_raw = await archive_item(item_id)
    arch = json.loads(arch_raw)
    arch_ok = arch.get("status") == "success"
    print(f"  archive status: {arch.get('status')} {'OK' if arch_ok else 'FAILED'}")

    print()
    overall = rt_ok and pc_ok and arch_ok
    print("=== OVERALL:", "PASS" if overall else "FAIL", "===")
    if not overall:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(run())
