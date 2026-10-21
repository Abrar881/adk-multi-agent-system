"""
Static guard: detect unescaped ADK template tokens in agent instruction strings.

ADK's inject_session_state treats every single-brace {token} in an agent
instruction as a required session-state key. Example text like
'Inventory service - {booking_date}' silently crashes the whole agent at
runtime if that key is absent (commit aa108ab fixed one such case).

This test parses agent source files with the AST module — no live API calls,
no ADK/MCP imports — and raises on any {token} that is NOT in the allowlist
of known legitimately-injectable keys.

Safe to run in CI: zero network, zero cost, zero side-effects.

To fix a failure:
  - If the token is example text:  escape it as {{token}}  (double braces)
  - If the token IS a real ADK-injected session key: add it to ALLOWLIST below
"""
import ast
import re
import pathlib

# ---------------------------------------------------------------------------
# Allowlist: all single-brace {token} patterns that are intentionally
# injected by ADK from session state (output_key values, plugin-provided keys).
# ---------------------------------------------------------------------------
ALLOWLIST: frozenset[str] = frozenset({
    # SequentialAgent output_key values (finance_workflows.py pipelines)
    "pending_invoices_list",    # pending_invoice_reader → invoice_creator
    "lock_result",              # match_lock → match_draft
    "match_result",             # match_draft → (downstream)
    "chase_lock_result",        # chase_lock → chase_draft
    # reflection.py pipeline output_key
    "booking_draft",            # drafter → pricer / reviewer steps
})

# Matches a single-brace token: {word} but NOT {{word}} or {word}}
_TOKEN_RE = re.compile(r"(?<!\{)\{([a-z_][a-z0-9_]*)}\B*")
# More precisely: not preceded by { and not followed by }
_TOKEN_RE = re.compile(r"(?<!\{)\{([a-z_][a-z0-9_]*)\}(?!\})")


def _extract_plain_string_constants(source: str) -> list[tuple[str, int]]:
    """Return (value, lineno) for every plain-string Constant in the AST.

    Python's ast module parses f-string variables as FormattedValue nodes
    (not Constant), so only regular string literals — the ones whose {token}
    patterns survive into ADK's inject_session_state — appear here.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    results = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            results.append((node.value, getattr(node, "lineno", 0)))
    return results


def test_no_unescaped_instruction_tokens() -> None:
    agents_dir = pathlib.Path(__file__).parent.parent / "agents"
    assert agents_dir.is_dir(), f"agents/ directory not found at {agents_dir}"

    violations: list[tuple[str, str, int, str]] = []  # (filename, token, lineno, snippet)

    for py_file in sorted(agents_dir.glob("*.py")):
        source = py_file.read_text(encoding="utf-8")
        for value, lineno in _extract_plain_string_constants(source):
            for match in _TOKEN_RE.finditer(value):
                token = match.group(1)
                if token not in ALLOWLIST:
                    snippet = value[max(0, match.start() - 30): match.end() + 30].replace("\n", "↵")
                    violations.append((py_file.name, token, lineno, snippet))

    if violations:
        lines = [
            f"  {fname}:{lineno}  {{{token}}}  …{snippet!r}…"
            for fname, token, lineno, snippet in violations
        ]
        raise AssertionError(
            "Unescaped {token} found in agent instruction string literals.\n"
            "ADK treats these as required session-state keys and crashes at runtime.\n\n"
            "Fix: escape example text as {{token}}, OR add to ALLOWLIST if it IS a real ADK key.\n\n"
            + "\n".join(lines)
        )
