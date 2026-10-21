# One-time repair: backfill blank columns on item 12365396471 (Victor Oyewole).
# DEPRECATED: written against the OLD Monday account (inventories board 18410040105).
# That item/board no longer exists under the new account (inventories board is now
# 18392817962). This was a one-off repair tied to a specific historical booking item,
# not a reusable tool — kept for reference only, not repointed.
import httpx
import asyncio
import json
import os


async def main():
    """Repair Monday.com booking item by backfilling blank columns."""

    # Configuration
    BOARD_ID = "18410040105"  # Inventories board
    ITEM_ID = "12365396471"   # Victor Oyewole's booking
    API_URL = "https://api.monday.com/v2"

    # Get API token from environment
    token = os.getenv("MONDAY_API_KEY")
    if not token:
        raise ValueError("MONDAY_API_KEY environment variable is not set")

    headers = {"Authorization": token, "Content-Type": "application/json"}

    async with httpx.AsyncClient() as client:
        # Step 1: Fetch current item values
        print("Step 1: Fetching current item values...")
        get_item_query = """
        query {
            items(ids: [%s]) {
                id
                column_values {
                    id
                    title
                    text
                    value
                }
            }
        }
        """ % ITEM_ID

        response = await client.post(
            API_URL,
            headers=headers,
            json={"query": get_item_query}
        )
        response.raise_for_status()
        data = response.json()

        if "errors" in data:
            print(f"GraphQL Error: {data['errors']}")
            return

        current_columns = data["data"]["items"][0]["column_values"]
        print(f"Current columns: {json.dumps(current_columns, indent=2)}\n")

        # Step 2: Fetch board schema to find column IDs
        print("Step 2: Fetching board schema...")
        get_board_schema_query = """
        query {
            boards(ids: [%s]) {
                columns {
                    id
                    title
                    type
                }
            }
        }
        """ % BOARD_ID

        response = await client.post(
            API_URL,
            headers=headers,
            json={"query": get_board_schema_query}
        )
        response.raise_for_status()
        data = response.json()

        if "errors" in data:
            print(f"GraphQL Error: {data['errors']}")
            return

        board_columns = data["data"]["boards"][0]["columns"]

        # Find column IDs we need
        report_type_col = None
        postcode_col = None
        notes_col = None

        for col in board_columns:
            col_id = col["id"]
            col_title = col["title"]
            col_type = col["type"]

            if col_id == "report___type":
                report_type_col = col_id
            elif col_title.lower() in ["postcode", "post code"]:
                postcode_col = col_id
            elif col_title.lower() == "notes":
                notes_col = col_id

        print(f"Column IDs found:")
        print(f"  - Report type (report___type): {report_type_col}")
        print(f"  - Postcode: {postcode_col}")
        print(f"  - Notes: {notes_col}\n")

        # Step 3: Determine which columns to update (only if currently empty)
        current_values = {}
        for col in current_columns:
            current_values[col["id"]] = col.get("text") or col.get("value") or ""

        updates = {}

        # Report type: check if empty
        if not current_values.get(report_type_col, "").strip():
            updates[report_type_col] = json.dumps({"label": "Inventory & Schedule of condition"})
            print(f"✓ Will update Report type to 'Inventory & Schedule of condition'")
        else:
            print(f"✗ Report type already set: {current_values.get(report_type_col)}")

        # Postcode: check if empty
        if not current_values.get(postcode_col, "").strip():
            updates[postcode_col] = "RM10 7HT"
            print(f"✓ Will update Postcode to 'RM10 7HT'")
        else:
            print(f"✗ Postcode already set: {current_values.get(postcode_col)}")

        # Notes: check if empty
        if not current_values.get(notes_col, "").strip():
            updates[notes_col] = "3 bed, Furnished (Victor Oyewole — 100 Crescent Road)"
            print(f"✓ Will update Notes")
        else:
            print(f"✗ Notes already set: {current_values.get(notes_col)}")

        print(f"\nTotal updates: {len(updates)}\n")

        if not updates:
            print("No updates needed. All columns are already populated.")
            return

        # Step 4: Ask for confirmation
        proceed = input("Proceed with updates? [y/N]: ").strip().lower()
        if proceed != "y":
            print("Cancelled.")
            return

        # Step 5: Execute mutation
        print("\nExecuting mutation...")
        column_values_json = json.dumps(updates)

        mutation = """
        mutation {
            change_multiple_column_values(
                board_id: %s
                item_id: %s
                column_values: %s
            ) {
                id
            }
        }
        """ % (BOARD_ID, ITEM_ID, json.dumps(column_values_json))

        response = await client.post(
            API_URL,
            headers=headers,
            json={"query": mutation}
        )
        response.raise_for_status()
        result = response.json()

        if "errors" in result:
            print(f"GraphQL Error: {result['errors']}")
            return

        print(f"✓ Mutation successful!")
        print(f"Result: {json.dumps(result['data'], indent=2)}")


if __name__ == "__main__":
    asyncio.run(main())
