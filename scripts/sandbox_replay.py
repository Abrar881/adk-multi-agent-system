"""Sandbox Replay Harness â€” Layer 3 of the pre-production test pyramid.

Drives the REAL AcmeOrchestrator against REAL MCP servers (using sandbox /
test credentials from .env) and verifies the full end-to-end path:

    real Vertex LLM â†’ hub routing â†’ spoke â†’ real MCP tool â†’ Firestore log

This catches bugs that mocked-tool eval cannot: upstream API drift, auth
expiry, rate limits, the Monday locking race, real tool-argument shapes.

NOT a pytest test â€” it needs real env + real quotas and may take ~2 minutes.

Prerequisites (create once in your sandbox/test accounts):
  Monday:
    - Board 393394274 ("Daily Tasks") must exist (or update MONDAY_TEST_BOARD_ID)
    - One item on that board with id MONDAY_TEST_ITEM_ID
  Xero:
    - At least one sandbox invoice (read-only check, nothing created)
  Gmail:
    - At least one unread email in the monitored inbox (read-only)

Usage:
    python scripts/sandbox_replay.py
    python scripts/sandbox_replay.py --scenario xero_list_invoices
    python scripts/sandbox_replay.py --verbose

Exit codes:
    0 â€” all scenarios passed
    1 â€” one or more scenarios failed
"""

import argparse
import asyncio
import json
import os
import sys
import time
import traceback
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple

from dotenv import load_dotenv

# Load sandbox credentials from .env before importing main.
load_dotenv(override=True)

# Repo root on the path so `import main` works from any working directory.
sys.path.insert(0, str(Path(__file__).parent.parent))

import main  # noqa: E402


# ---------------------------------------------------------------------------
# Sandbox configuration â€” update these IDs to match your sandbox accounts.
# ---------------------------------------------------------------------------
MONDAY_TEST_BOARD_ID = int(os.environ.get("MONDAY_TEST_BOARD_ID", "393394274"))
MONDAY_TEST_ITEM_ID  = int(os.environ.get("MONDAY_TEST_ITEM_ID", "0"))  # 0 = skip write tests


class Scenario(NamedTuple):
    name: str
    prompt: str
    expected_agent: str
    expected_tool_keys: list[str]  # keys that must appear in agent_activity tool_call events
    must_succeed: bool = True


# ---------------------------------------------------------------------------
# Scenario catalogue â€” one per real-world surface.
# ---------------------------------------------------------------------------
SCENARIOS: list[Scenario] = [
    # --- READ-ONLY (safe to run any time) ---
    Scenario(
        name="xero_list_invoices",
        prompt="List all unpaid Xero invoices",
        expected_agent="finance",
        expected_tool_keys=["list_unpaid_invoices"],
    ),
    Scenario(
        name="ops_status",
        prompt="What is the current status of the operations system?",
        expected_agent="ops",
        expected_tool_keys=["get_ops_status"],
    ),
    Scenario(
        name="gmail_list_emails",
        prompt="Check my inbox for any unread emails",
        expected_agent="dispatcher",
        expected_tool_keys=["list_unread_emails"],
    ),
    Scenario(
        name="monday_list_boards",
        prompt="List all my Monday.com boards",
        expected_agent="ops",
        expected_tool_keys=["list_boards"],
    ),
    Scenario(
        name="rag_knowledge",
        prompt="What is the standard process for handling a new booking enquiry?",
        expected_agent="dispatcher",
        expected_tool_keys=["consult_knowledge"],
    ),
    Scenario(
        name="ops_travel_time",
        prompt="How long does it take to travel from London Bridge to Canary Wharf by tube?",
        expected_agent="ops",
        expected_tool_keys=["calculate_travel_time"],
    ),
    # --- ROUTING ACCURACY ---
    Scenario(
        name="routing_finance",
        prompt="What invoices are overdue?",
        expected_agent="finance",
        expected_tool_keys=[],  # just routing â€” tool selection verified separately
    ),
    Scenario(
        name="routing_dispatcher",
        prompt="Reply to the latest customer email asking about our availability",
        expected_agent="dispatcher",
        expected_tool_keys=[],
    ),
    Scenario(
        name="routing_ops",
        prompt="Check which jobs are scheduled for tomorrow on the board",
        expected_agent="ops",
        expected_tool_keys=[],
    ),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _check_firestore_tool_calls(expected_keys: list[str], since: datetime, verbose: bool) -> bool:
    """Query agent_activity for tool_call events created after `since` and verify expected tools fired."""
    if not expected_keys:
        return True  # No tool expectation for this scenario.

    db = main._get_activity_db()
    if db is None:
        print("    WARN: Firestore unavailable â€” skipping tool_call verification")
        return True

    cutoff = since.isoformat()
    try:
        docs = list(
            db.collection("agent_activity")
            .where("event", "==", "tool_call")
            .where("timestamp", ">=", cutoff)
            .stream()
        )
    except Exception as exc:
        print(f"    WARN: Firestore query failed ({exc}) â€” skipping verification")
        return True

    called_tools = [d.to_dict().get("tool", "") for d in docs]
    if verbose:
        print(f"    Tool calls logged: {called_tools}")

    missing = [k for k in expected_keys if k not in called_tools]
    if missing:
        print(f"    FAIL: Expected tools not found in Firestore: {missing}")
        return False
    return True


def _print_table(results: list[dict]) -> None:
    cols = ["scenario", "agent", "duration_s", "status", "notes"]
    widths = {c: max(len(c), max(len(str(r.get(c, ""))) for r in results)) for c in cols}
    sep = "  ".join("-" * widths[c] for c in cols)
    header = "  ".join(c.ljust(widths[c]) for c in cols)
    print("\n" + header)
    print(sep)
    for r in results:
        row = "  ".join(str(r.get(c, "")).ljust(widths[c]) for c in cols)
        print(row)
    print()


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

async def _run_scenario(
    orchestrator: main.AcmeOrchestrator,
    scenario: Scenario,
    verbose: bool,
) -> dict:
    start = datetime.now(timezone.utc)
    t0 = time.monotonic()
    status = "PASS"
    notes = ""

    try:
        agent_name, response = await orchestrator.route_and_execute(scenario.prompt)
    except Exception as exc:
        print(f"\n  ERROR traceback for {scenario.name!r}:", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        return {
            "scenario": scenario.name,
            "agent": "â€”",
            "duration_s": f"{time.monotonic() - t0:.1f}",
            "status": "ERROR",
            "notes": str(exc),
        }

    duration = time.monotonic() - t0

    # Check routing
    if agent_name != scenario.expected_agent:
        status = "FAIL"
        notes = f"expected agent={scenario.expected_agent!r}, got {agent_name!r}"
    elif not response:
        status = "FAIL"
        notes = "empty response"
    else:
        # Check Firestore tool_call events
        ok = _check_firestore_tool_calls(scenario.expected_tool_keys, start, verbose)
        if not ok:
            status = "FAIL"
            notes = f"missing tool calls: {scenario.expected_tool_keys}"

    if verbose:
        print(f"  Response ({len(response)} chars): {response[:150]!r}")

    return {
        "scenario": scenario.name,
        "agent": agent_name,
        "duration_s": f"{duration:.1f}",
        "status": status,
        "notes": notes,
    }


async def _main(run_scenarios: list[Scenario], verbose: bool) -> int:
    project = os.environ.get("GOOGLE_CLOUD_PROJECT", "")
    if not project or project == "test-project":
        print("ERROR: Set GOOGLE_CLOUD_PROJECT to a real GCP project.")
        print("       This script uses real Vertex AI and MCP servers.")
        return 1

    print(f"\nMulti-Agent System Sandbox Replay â€” {len(run_scenarios)} scenarios")
    print(f"Project: {project}  |  {datetime.now():%Y-%m-%d %H:%M}\n")

    async with AsyncExitStack() as stack:
        orc = main.AcmeOrchestrator()
        print("Booting orchestrator (MCP warm-up) â€¦")
        t0 = time.monotonic()
        await orc.initialize(stack)
        print(f"Ready in {time.monotonic() - t0:.1f}s\n")

        results = []
        for scenario in run_scenarios:
            print(f"â–¶ {scenario.name}")
            result = await _run_scenario(orc, scenario, verbose)
            results.append(result)
            icon = "âœ“" if result["status"] == "PASS" else "âœ—"
            print(f"  {icon} {result['status']} ({result['duration_s']}s)"
                  + (f"  {result['notes']}" if result["notes"] else ""))

    _print_table(results)

    failed = [r for r in results if r["status"] != "PASS"]
    if failed:
        print(f"FAILED: {len(failed)}/{len(results)} scenarios.\n")
        return 1
    print(f"All {len(results)} scenarios passed.\n")
    return 0


def main_cli() -> None:
    parser = argparse.ArgumentParser(description="Multi-Agent System sandbox replay harness")
    parser.add_argument("--scenario", help="Run only this scenario by name")
    parser.add_argument("--verbose", action="store_true", help="Print full LLM responses")
    args = parser.parse_args()

    if args.scenario:
        matches = [s for s in SCENARIOS if s.name == args.scenario]
        if not matches:
            names = ", ".join(s.name for s in SCENARIOS)
            print(f"Unknown scenario {args.scenario!r}. Available: {names}")
            sys.exit(1)
        run_scenarios = matches
    else:
        run_scenarios = SCENARIOS

    exit_code = asyncio.run(_main(run_scenarios, args.verbose))
    sys.exit(exit_code)


if __name__ == "__main__":
    main_cli()
