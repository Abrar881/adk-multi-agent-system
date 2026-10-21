"""One-off script: reset tool_call_count on any bricked agent_sessions docs.

Run once after deploying the cost_guard before_run_callback fix:

    python scripts/reset_stuck_sessions.py [--dry-run]

A session is "bricked" when its persisted tool_call_count >= max_tool_calls (50).
The before_run_callback fix prevents future bricking, but existing Firestore docs
with high counts will keep failing until their 2-hour TTL expires.

This script resets tool_call_count (and transfer_count) to 0 non-destructively â€”
conversation history is preserved, only the iteration counters are cleared.
"""
import argparse
import json
import os
import sys

from google.cloud import firestore

_COLLECTION = "agent_sessions"
_STUCK_THRESHOLD = 50  # matches CostGuardPlugin(max_tool_calls=50)


def main(dry_run: bool = False) -> None:
    project = os.environ.get("GOOGLE_CLOUD_PROJECT", "YOUR_GCP_PROJECT_ID")
    db = firestore.Client(project=project)

    docs = list(db.collection(_COLLECTION).stream())
    print(f"Found {len(docs)} session docs in '{_COLLECTION}'")

    reset_count = 0
    for doc in docs:
        data = doc.to_dict() or {}
        try:
            state = json.loads(data.get("state", "{}"))
        except (json.JSONDecodeError, TypeError):
            state = {}

        tool_count = state.get("tool_call_count", 0)
        transfer_count = state.get("transfer_count", 0)

        if tool_count >= _STUCK_THRESHOLD:
            print(
                f"  {'[DRY-RUN] would reset' if dry_run else 'Resetting'} "
                f"{doc.id!r}  tool_call_count={tool_count}  transfer_count={transfer_count}"
            )
            if not dry_run:
                state["tool_call_count"] = 0
                state["transfer_count"] = 0
                doc.reference.update({"state": json.dumps(state)})
            reset_count += 1

    print(
        f"\n{'Would reset' if dry_run else 'Reset'} {reset_count} "
        f"of {len(docs)} session(s)."
    )
    if dry_run:
        print("Re-run without --dry-run to apply.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reset bricked agent_sessions counters")
    parser.add_argument(
        "--dry-run", action="store_true", help="Print what would change without writing"
    )
    args = parser.parse_args()
    main(dry_run=args.dry_run)
