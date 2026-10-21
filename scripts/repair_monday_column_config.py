"""
One-shot pusher: write the full, live-verified board/column configuration for the
NEW Monday.com account to Firestore monday_config/boards.

This is the authoritative override read by config/monday_config.py at runtime â€”
Firestore wins over the env-var fallback (see _build_env_fallback / get_config in
that module), so the payload below is what actually takes effect in production.

Covers both boards in full (board id + columns + groups), plus the board ids for
working_schedule and daily_tasks. Column IDs verified live against the new Monday
account (boards created 2026-07-08/09).

Safe to re-run â€” idempotent (read-modify-write against the existing Firestore doc,
so unrelated top-level keys such as "api_token" are preserved).
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

INVENTORIES_BOARD_ID = 18392817962
CONTRACTORS_BOARD_ID = 18397426458
WORKING_SCHEDULE_BOARD_ID = 9529643731
DAILY_TASKS_BOARD_ID = 393394274

# NOTE: no "reason_delay" entry â€” that column does not exist on the inventories board.
INVENTORIES_COLUMNS = {
    "report_status":      {"id": "report_status"},
    "inspector":          {"id": "inspector"},
    "report_type":        {"id": "report___type"},
    "payment_status":     {"id": "payment___status"},
    "payment_reminder":   {"id": "status7"},
    "Acme_confirmation": {"id": "status2"},
    "postcode":           {"id": "location"},
    "phone":              {"id": "contact_number"},
    "customer":           {"id": "customer"},
    "email":              {"id": "email"},
    "address":            {"id": "address"},
    "spec":               {"id": "spec"},
    "hour":               {"id": "dup__of_hour0__1"},
    "date":               {"id": "date"},
    "access_info":        {"id": "access___info"},
    "notes":              {"id": "access___info"},
    "inv_number":         {"id": "inv__4"},
    "price":              {"id": "numeric_mm28fpan"},
    "pay_link":           {"id": "text7"},
}

INVENTORIES_GROUPS = {
    "leads": "group_mm52zj1p",
}

CONTRACTORS_COLUMNS = {
    "report_type":        {"id": "report___type"},
    "job_status":         {"id": "job_status_mkkr74pn"},
    "inspector":          {"id": "inspector"},
    "date":               {"id": "date"},
    "hour":               {"id": "dup__of_hour0__1"},
    "address":            {"id": "address"},
    "postcode":           {"id": "location"},
    "spec":               {"id": "spec"},
    "customer":           {"id": "customer"},
    "email":              {"id": "email"},
    "phone":              {"id": "contact_number_mkkrrw4q"},
    "contact_number":     {"id": "contact_number_mkkrrw4q"},
    "payment_status":     {"id": "payment___status"},
    "payment_reminder":   {"id": "status7"},
    "Acme_confirmation": {"id": "status2"},
    "reason_delay":       {"id": "color_mksnadmq"},
    "inv_number":         {"id": "inv__4"},
    "access_info":        {"id": "access___info"},
    "notes":              {"id": "text5"},
    "price":              {"id": "numeric_mm2jc0vj"},
    "pay_link":           {"id": "text7"},
}

CONTRACTORS_GROUPS: dict = {}


def main():
    try:
        from google.cloud import firestore
        db = firestore.Client()
    except Exception as e:
        print(f"FAIL: Could not connect to Firestore: {e}")
        sys.exit(1)

    ref = db.collection("monday_config").document("boards")
    doc = ref.get()

    if not doc.exists:
        print("Firestore monday_config/boards does not exist â€” creating fresh config.")
        current = {}
    else:
        current = doc.to_dict() or {}
        print(f"Current config loaded. Keys: {list(current.keys())}")

    boards = current.get("boards", {})

    inv = boards.get("inventories", {})
    old_inv_id = inv.get("id")
    inv["id"] = INVENTORIES_BOARD_ID
    inv["columns"] = INVENTORIES_COLUMNS
    inv["groups"] = INVENTORIES_GROUPS
    boards["inventories"] = inv

    con = boards.get("contractors", {})
    old_con_id = con.get("id")
    con["id"] = CONTRACTORS_BOARD_ID
    con["columns"] = CONTRACTORS_COLUMNS
    con["groups"] = CONTRACTORS_GROUPS
    boards["contractors"] = con

    ws = boards.get("working_schedule", {})
    ws["id"] = WORKING_SCHEDULE_BOARD_ID
    boards["working_schedule"] = ws

    dt = boards.get("daily_tasks", {})
    dt["id"] = DAILY_TASKS_BOARD_ID
    boards["daily_tasks"] = dt

    current["boards"] = boards

    ref.set(current)
    print("\nFirestore monday_config/boards updated successfully.")
    print(f"  inventories.id: {old_inv_id} -> {INVENTORIES_BOARD_ID}")
    print(f"  contractors.id: {old_con_id} -> {CONTRACTORS_BOARD_ID}")
    print(f"  working_schedule.id -> {WORKING_SCHEDULE_BOARD_ID}")
    print(f"  daily_tasks.id -> {DAILY_TASKS_BOARD_ID}")
    print(f"  inventories.groups.leads -> {INVENTORIES_GROUPS['leads']}")


if __name__ == "__main__":
    main()
