"""Deterministic postcode coverage classification.

Audit H1 (2026-07-11): the coverage gate lived only as prose in the dispatcher
system instruction, relying on the LLM to self-apply an enumerated postcode list.
An LLM misread either wrongly auto-confirms an out-of-area job (logistics failure â€”
no clerk can attend) or wrongly stalls an in-area job (lost revenue). Coverage is a
hard business guarantee, so it must be decided by code, not inference.

`classify_coverage` is a pure, deterministic function wired as a dispatcher tool.
Authoritative source lists: dispatcher Critical Rule 7 + knowledge/coverage_areas.txt.

Safe default: anything unrecognised or outside the standard set escalates to admin â€”
we NEVER auto-confirm on uncertainty, and we NEVER decline outright.
"""
import re

# --- Standard coverage (auto-confirm) ------------------------------------------
# Greater London Zone 1 (central/inner) postal areas, per dispatcher Rule 7.
_LONDON_ZONE1 = frozenset({"E", "W", "N", "NW", "SE", "SW", "EC", "WC"})
# Greater London Zone 2 (suburban belt) â€” knowledge/coverage_areas.txt lists these as
# "proceed with standard booking". Omitting them (pre-2026-07-11) wrongly escalated ~a third
# of the service area (Kingston/Twickenham/Croydon/Bromley/Sutton/Harrow/Enfield/Ilford/
# Romford/Uxbridge/Dartford/Watford) as out_of_area. See audit round 2, finding Q1.
_LONDON_ZONE2 = frozenset({
    "KT", "TW", "CR", "BR", "SM", "HA", "EN", "IG", "RM", "UB", "DA", "WD",
})
# Greater Manchester areas, per knowledge/coverage_areas.txt (M plus BL/OL/SK/WN).
_MANCHESTER_AREAS = frozenset({"M", "BL", "OL", "SK", "WN"})
_STANDARD_AREAS = _LONDON_ZONE1 | _LONDON_ZONE2 | _MANCHESTER_AREAS

# --- Edge (manual review â€” never confirm, escalate to admin) --------------------
# Specific outward codes that straddle the service boundary.
_EDGE_OUTWARD = frozenset({
    "TN14", "TN16", "RH1", "RH2", "RH8", "RH9",
    "CM13", "CM14", "SL0", "SL3",
})
# Whole postal areas flagged edge (Reading, Hertfordshire, Stevenage).
_EDGE_AREAS = frozenset({"RG", "AL", "SG"})

_ESCALATE_LINE = (
    "One of our team members is checking the travel logistics for this area and "
    "will confirm shortly."
)

# Full UK postcode: area(1-2) + district(digit + optional letter/digit) + inward(digit + 2 letters).
_FULL_POSTCODE_RE = re.compile(r"^[A-Z]{1,2}\d[A-Z\d]?\d[A-Z]{2}$")
_OUTWARD_RE = re.compile(r"^([A-Z]{1,2})(\d[A-Z\d]?)?$")


def _outward_and_area(postcode: str) -> tuple[str, str]:
    """Return (outward_code, area) from a full or partial UK postcode.

    Handles full postcodes (strips the 3-char inward code) and bare outward codes.
    Returns ("", "") if the input does not parse as a postcode.
    """
    s = re.sub(r"\s+", "", (postcode or "").upper())
    if not s:
        return "", ""
    outward = s[:-3] if _FULL_POSTCODE_RE.match(s) else s
    m = _OUTWARD_RE.match(outward)
    if not m:
        return "", ""
    return outward, m.group(1)


def classify_coverage(postcode: str) -> dict:
    """Classify a UK postcode against Acme Property Services' service coverage.

    Call this BEFORE confirming availability or proceeding with any booking whenever
    a postcode is provided. Act on the returned "action":
      - "confirm": postcode is in standard coverage â€” proceed with normal booking.
      - "escalate_admin": edge / out-of-area / unrecognised â€” do NOT confirm and do
        NOT decline; reply with the returned "customer_reply" line and create a
        NEEDS_REVIEW task for admin.

    Args:
        postcode: The property postcode (full e.g. "SW1A 1AA" or outward e.g. "SW1A").

    Returns:
        dict with keys: postcode, outward, coverage
        ("standard" | "edge" | "out_of_area" | "unknown"), action
        ("confirm" | "escalate_admin"), message (guidance for the agent), and
        customer_reply (the exact line to send the customer when escalating).
    """
    outward, area = _outward_and_area(postcode)

    if not outward or not area:
        return {
            "postcode": postcode,
            "outward": outward,
            "coverage": "unknown",
            "action": "escalate_admin",
            "message": "Postcode not recognised â€” do NOT auto-confirm; pass to admin.",
            "customer_reply": _ESCALATE_LINE,
        }

    if outward in _EDGE_OUTWARD or area in _EDGE_AREAS:
        coverage = "edge"
    elif area in _STANDARD_AREAS:
        coverage = "standard"
    else:
        coverage = "out_of_area"

    if coverage == "standard":
        return {
            "postcode": postcode,
            "outward": outward,
            "coverage": "standard",
            "action": "confirm",
            "message": "In standard coverage â€” proceed with normal booking/confirmation.",
            "customer_reply": "",
        }

    return {
        "postcode": postcode,
        "outward": outward,
        "coverage": coverage,
        "action": "escalate_admin",
        "message": (
            f"{coverage.replace('_', ' ').title()} postcode â€” do NOT confirm or decline. "
            "Send customer_reply and create a NEEDS_REVIEW task for admin."
        ),
        "customer_reply": _ESCALATE_LINE,
    }
