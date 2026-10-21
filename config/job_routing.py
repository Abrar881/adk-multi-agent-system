"""Route bookings to the correct Monday.com board based on service type.

Single source of truth for which job types go on which board:
  - "inventories" → inventory reports (Check In/Out, SoC, Midterm, Snagging, etc.)
  - "contractors" → safety certs, cleaning, maintenance (Gas Safety, EICR, EOT Cleaning, etc.)
"""

import logging as _logging

# Service types that belong on the CONTRACTORS board (lowercase, normalised).
# All other types default to the INVENTORIES board.
_CONTRACTORS_TYPES: frozenset[str] = frozenset({
    # Safety certificates
    "gas safety",
    "eicr",
    "commercial eicr",
    "epc",
    "commercial epc",
    "epc / floorplan",
    "epc/floorplan",
    "fra",
    "fire risk assessment",
    "pat",
    "pat testing",
    "fire alarm certificate",
    "fire alarm",
    "emergency lighting certificate",
    "carbon monoxide alarm",
    "alarm service",
    "commercial fire assessments",
    "gas/boiler service",
    "gas boiler service",
    # Cleaning — canonical labels and common full-phrase variants the LLM may produce
    "eot cleaning",
    "eot clean",
    "end of tenancy cleaning",
    "end of tenancy clean",
    "professional end of tenancy cleaning",
    "eot / carpet clean",
    "eot/carpet clean",
    "eot & steam cleaning",
    "eot/ steam cleaning",
    "eot and steam cleaning",
    "carpet steam clean",
    "steam cleaning",
    "domestic cleaning",
    "upholstery clean",
    "sofa cleaning",
    "after build clean",
    # Maintenance / trades
    "handyman",
    "handyman services",
    "boiler repair",
    "boiler service",
    "emergency call out",
    "remedial work",
    "gardening",
    "landscaping services",
    "rubbish removal",
    "cockroach extermination",
    "install gas chain to wall for gas cooker",
    # Other contractor work
    "floor plan",
    "site visit",
})

# Service types that explicitly belong on the INVENTORIES board.
# Used for fast-path detection; anything not in either set defaults to inventories.
_INVENTORIES_TYPES: frozenset[str] = frozenset({
    "inventory & check in",
    "inventory & check-in",
    "inventory and check in",
    "inventory and check-in",
    "inventory & check out",
    "inventory & check-out",
    "inventory and check out",
    "inventory and check-out",
    "inventory & schedule of condition",
    "schedule of condition",
    "soc",
    "midterm",
    "mid-term",
    "mid term",
    "snagging",
    "snagging report",
    "check out / property valuation",
    "check out/property valuation",
    "property viewing",
    "property valuation",
    "revisit",
    "commercial unit",
    "key collection",
    "key return",
    "cancelled",
    "professional photography",
    "photography",
    "check in",
    "check-in",
    "check out",
    "check-out",
})


def classify_board_key(service_type: str | None, report_type: str | None = None) -> str:
    """Return the Monday board key ('inventories' or 'contractors') for a booking.

    Args:
        service_type: The job's service type (e.g. "Gas Safety", "Inventory & Check In").
                      This is the primary signal — use the `report_type` booking field
                      (call sites pass this as `_classify_board_key(report_type, spec)`).
        report_type:  Secondary fallback (e.g. "Furnished") — rarely determines board.

    Returns:
        "contractors" or "inventories".  Falls back to "inventories" when type is
        unknown or not provided.
    """
    for raw in (service_type, report_type):
        if not raw:
            continue
        normalized = " ".join(raw.lower().strip().split())
        if normalized in _CONTRACTORS_TYPES:
            return "contractors"
        if normalized in _INVENTORIES_TYPES:
            return "inventories"
    _logging.warning(
        "classify_board_key: unknown service type %r — defaulting to inventories", service_type
    )
    return "inventories"
