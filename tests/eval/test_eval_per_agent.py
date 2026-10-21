"""Layer 1b — Per-agent tool-selection eval using ADK AgentEvaluator.

Uses the ADK evaluation framework to score tool trajectory and response
quality against hand-authored golden datasets in tests/eval/datasets/.

The AgentEvaluator drives the REAL hub_agent (real Vertex LLM) with
mocked MCP toolsets. It compares:
  - tool_trajectory_avg_score: did the spoke call the expected tool(s)?
  - response_match_score: ROUGE-1 similarity to the reference response.

Thresholds are configured in tests/eval/datasets/test_config.json.

Cost: ~9 Vertex LLM calls per run (3 per spoke dataset). Runs against
real Vertex quota.

Run:
    pytest tests/eval/test_eval_per_agent.py -v -m eval

If AgentEvaluator raises an ImportError or schema error, check the
installed google-adk version: `pip show google-adk`.
The parameter name may be `eval_dataset_file_path` in older versions.

Note: The .test.json datasets are also usable directly via the CLI:
    adk eval eval_agent tests/eval/datasets/ops_trajectory.test.json --print_detailed_results
And via the web UI:
    adk web eval_agent
"""

import pathlib
import pytest

try:
    from google.adk.evaluation.agent_evaluator import AgentEvaluator
    _HAS_EVALUATOR = True
except ImportError:
    _HAS_EVALUATOR = False

_DATASETS_DIR = pathlib.Path(__file__).parent / "datasets"


def _skip_if_no_evaluator():
    if not _HAS_EVALUATOR:
        pytest.skip(
            "google.adk.evaluation.agent_evaluator not available in this ADK version. "
            "Upgrade: pip install 'google-adk>=1.2.0'"
        )


@pytest.mark.eval
async def test_ops_agent_trajectory(eval_orchestrator):  # noqa: ARG001 — fixture boots the real runner
    """Ops agent should call Monday / ops tools for logistics prompts."""
    _skip_if_no_evaluator()
    await AgentEvaluator.evaluate(
        agent_module="eval_agent",
        eval_dataset_file_path_or_dir=str(_DATASETS_DIR / "ops_trajectory.test.json"),
    )


@pytest.mark.eval
async def test_finance_agent_trajectory(eval_orchestrator):  # noqa: ARG001
    """Finance agent should call Xero tools for invoice/payment prompts."""
    _skip_if_no_evaluator()
    await AgentEvaluator.evaluate(
        agent_module="eval_agent",
        eval_dataset_file_path_or_dir=str(_DATASETS_DIR / "finance_trajectory.test.json"),
    )


@pytest.mark.eval
async def test_dispatcher_agent_trajectory(eval_orchestrator):  # noqa: ARG001
    """Dispatcher agent should call Gmail or WhatsApp tools for comms prompts."""
    _skip_if_no_evaluator()
    await AgentEvaluator.evaluate(
        agent_module="eval_agent",
        eval_dataset_file_path_or_dir=str(_DATASETS_DIR / "dispatcher_trajectory.test.json"),
    )


@pytest.mark.eval
async def test_dispatcher_booking_trajectory(eval_orchestrator):  # noqa: ARG001
    """Dispatcher follows correct booking creation tool sequence: search→create_booking→send_email."""
    _skip_if_no_evaluator()
    await AgentEvaluator.evaluate(
        agent_module="eval_agent",
        eval_dataset_file_path_or_dir=str(_DATASETS_DIR / "dispatcher_booking.test.json"),
    )


@pytest.mark.eval
async def test_finance_reconciliation_trajectory(eval_orchestrator):  # noqa: ARG001
    """Finance follows lock→fetch→match→update sequence for reconciliation."""
    _skip_if_no_evaluator()
    await AgentEvaluator.evaluate(
        agent_module="eval_agent",
        eval_dataset_file_path_or_dir=str(_DATASETS_DIR / "finance_reconciliation.test.json"),
    )


@pytest.mark.eval
async def test_finance_chase_trajectory(eval_orchestrator):  # noqa: ARG001
    """Finance follows lock→details→email chase sequence for invoice chasing."""
    _skip_if_no_evaluator()
    await AgentEvaluator.evaluate(
        agent_module="eval_agent",
        eval_dataset_file_path_or_dir=str(_DATASETS_DIR / "finance_chase.test.json"),
    )


@pytest.mark.eval
async def test_ops_feasibility_trajectory(eval_orchestrator):  # noqa: ARG001
    """Ops uses correct feasibility tools based on job count (travel time for 2, optimize for 5+)."""
    _skip_if_no_evaluator()
    await AgentEvaluator.evaluate(
        agent_module="eval_agent",
        eval_dataset_file_path_or_dir=str(_DATASETS_DIR / "ops_feasibility.test.json"),
    )


@pytest.mark.eval
async def test_ops_roll_call_trajectory(eval_orchestrator):  # noqa: ARG001
    """Ops sends status check to all contractors on the board."""
    _skip_if_no_evaluator()
    await AgentEvaluator.evaluate(
        agent_module="eval_agent",
        eval_dataset_file_path_or_dir=str(_DATASETS_DIR / "ops_roll_call.test.json"),
    )
