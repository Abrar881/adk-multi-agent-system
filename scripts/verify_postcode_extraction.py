"""
Verify _extract_uk_postcode correctly pulls postcodes from address strings.
Pure-Python, no network.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from servers.monday_server import _extract_uk_postcode

cases = [
    # (input, expected)
    ("100 Crescent Road, RM10 7HT",              "RM10 7HT"),
    ("100 Crescent Road RM107HT",                "RM10 7HT"),   # no space
    ("100 Crescent Road, rm10 7ht",              "RM10 7HT"),   # lowercase
    ("123 Sanaa House, IG11 7DQ",                "IG11 7DQ"),
    ("14 Orchard Lane, Richmond, London, TW9 1PL", "TW9 1PL"),
    ("12 Example Street, London SW1A 1AA",        "SW1A 1AA"),   # alpha-numeric outward
    ("no postcode here",                          None),
    ("",                                          None),
    (None,                                        None),
]

print("=== Postcode extraction verification ===\n")
all_pass = True
for text, expected in cases:
    result = _extract_uk_postcode(text)
    ok = result == expected
    if not ok:
        all_pass = False
    status = "PASS" if ok else "FAIL"
    print(f"{status}  {str(text)!r:50s} -> {result!r:12s}  (expected {expected!r})")

print()
if all_pass:
    print("All cases pass.")
else:
    print("Some cases failed.")
    sys.exit(1)
