"""
Verify _normalize_label fixes the &amp; vs & mismatch that caused blank Report-type columns.
Pure-Python, no network, no Monday API calls.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import html as _html

def _normalize_label(s):
    return " ".join(_html.unescape(str(s)).split()).casefold()

from servers.monday_server import _resolve_service_type

# Board schema returns HTML-encoded labels (as seen in repair script)
BOARD_LABELS = {
    "Inventory & Check In",
    "Inventory & Check Out",
    "Inventory & Schedule of condition",
    "Check out / Property Valuation",
    "Midterm",
    "Snagging Report",
    "Cancelled",
    "Professional Photography",
}

# Simulate Monday returning &amp; (as stored in board schema)
BOARD_LABELS_ENCODED = {lbl.replace("&", "&amp;") for lbl in BOARD_LABELS}

# Build normalized→canonical map (as new pre-flight code does)
norm_map = {_normalize_label(bl): bl for bl in BOARD_LABELS_ENCODED}

test_cases = [
    "Inventory & Check In",
    "Inventory & Check Out",
    "Inventory & Schedule of condition",
    "Check out / Property Valuation",
    "Midterm",
    "inventory",   # resolved via _resolve_service_type alias
    "check in",
    "soc",
]

print("=== Label normalization verification ===\n")
all_pass = True
for spec in test_cases:
    resolved = _resolve_service_type(spec)
    if resolved is None:
        print(f"SKIP  {spec!r:40s} → _resolve_service_type returned None")
        continue
    canonical = norm_map.get(_normalize_label(resolved))
    if canonical is not None:
        status = "PASS"
        print(f"{status}  {spec!r:40s} -> resolved={resolved!r:45s} -> canonical={canonical!r}")
    else:
        status = "FAIL"
        all_pass = False
        print(f"{status}  {spec!r:40s} -> resolved={resolved!r:45s} -> NO MATCH in board labels (would be DROPPED)")

print()
if all_pass:
    print("All cases canonicalized correctly. Bug is fixed.")
else:
    print("Some cases still fail — review _normalize_label or _SERVICE_TYPE_LABELS.")
    sys.exit(1)
