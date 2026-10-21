"""Layer 1a — Routing correctness tests.

Drives the REAL hub_agent (real gemini-3.1-pro-preview via Vertex) with
mocked MCP toolsets and asserts that `route_and_execute` returns the
correct spoke agent for each prompt category.

Cost: ~6 Vertex LLM calls per run (one per test case). Runs against real
Vertex quota — set GOOGLE_CLOUD_PROJECT to a real project before running.

Run:
    pytest tests/eval/test_routing.py -v -m eval
"""

import pytest

# RECORDED_CALLS and eval_orchestrator are provided by tests/eval/conftest.py

# (prompt, expected_spoke) — routing golden cases from TEST_CREATION_PLAN.md C1
ROUTING_CASES = [
    # Finance spoke — explicit Xero
    ("Pull up all the open Xero invoices and tell me the totals", "finance"),
    # Finance spoke — domain language, no tool name
    ("How much revenue did we bring in last quarter?", "finance"),
    # Ops spoke — job feasibility (explicitly listed in hub instruction under ops)
    ("Check if it's feasible for a contractor to do a job in Brixton at 9am and then travel to Canary Wharf for a 10:30am start", "ops"),
    # Ops spoke — contractor logistics
    ("Add a new contractor called Dave to the sub-contractors board", "ops"),
    # Dispatcher spoke — email
    ("Reply to the latest customer email asking about availability", "dispatcher"),
    # Dispatcher spoke — WhatsApp to a customer (not a contractor; should not go to ops)
    ("Send a WhatsApp to our customer John to confirm his property inspection appointment is at 9am tomorrow", "dispatcher"),
    # Additional cases from TEST_CREATION_PLAN.md Part C1
    ("What bookings are scheduled for Friday?", "dispatcher"),
    ("Book an inventory for 10 High Street London, 3-bed, Tuesday 10am", "dispatcher"),
    ("How many jobs by status on the board?", "dispatcher"),
    ("A new lead came in from the website — John Smith, 07700900000", "dispatcher"),
    ("What's the invoice status for the Johnson job?", "finance"),
    ("Mark invoice INV-0042 as paid", "finance"),
    ("Chase all invoices overdue by 14+ days", "finance"),
    ("How long to drive from Brixton to Croydon?", "ops"),
    ("Optimise tomorrow's route for 6 jobs", "ops"),
    ("Which contractors haven't checked in today?", "ops"),
    ("Run the board hygiene check", "ops"),
    ("Has the booking for Smith been paid?", "finance"),
    ("What do we charge for a 4-bed inventory?", "dispatcher"),
]


@pytest.mark.eval
@pytest.mark.parametrize("prompt,expected_agent", ROUTING_CASES)
async def test_hub_routes_correctly(eval_orchestrator, prompt, expected_agent):
    """Hub should transfer each prompt to the correct spoke agent."""
    agent_name, response, _ = await eval_orchestrator.route_and_execute(prompt)
    assert agent_name == expected_agent, (
        f"Expected routing to '{expected_agent}' but got '{agent_name}'.\n"
        f"Prompt: {prompt!r}\n"
        f"Response: {response[:200]!r}"
    )
    assert response, "Response should not be empty"
