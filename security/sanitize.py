"""Webhook payload sanitization against prompt injection.

Applied to all external-controlled strings before they enter LLM prompts.
Sanitization is lightweight -- it narrows the attack surface without
attempting to fully parse or validate content.
"""

import re
import unicodedata

# Zero-width and invisible Unicode characters used in homoglyph injection attacks.
# U+200B-U+200F: zero-width space, non-joiner, joiner, LTR mark, RTL mark
# U+2060-U+2064: word joiner, invisible operators (function application, times, separator, plus)
# U+FEFF: BOM / zero-width no-break space
_ZERO_WIDTH_RE = re.compile(
    "[​‌‍‎‏⁠⁡⁢⁣⁤﻿]"
)

# Patterns known to act as LLM instruction delimiters in various model families.
_INJECTION_PATTERNS = re.compile(
    r"(\[INST\]|\[/INST\]|<<SYS>>|<</SYS>>|<\|im_start\|>|<\|im_end\|>"
    r"|<\|system\|>|<\|user\|>|<\|assistant\|>|###\s*System|###\s*Human"
    r"|###\s*Assistant|\|\|OVERRIDE\|\|)",
    re.IGNORECASE,
)


def sanitize_payload_field(value: object, max_len: int = 200) -> str:
    """Return a safe string from an untrusted payload field.

    Steps:
    1. Coerce to str; empty/missing values become empty string.
    2. NFKC-normalize to collapse Unicode homoglyphs to ASCII equivalents.
    3. Strip zero-width / invisible characters used in homoglyph injection.
    4. Replace newlines/carriage-returns with a space (prevents prompt line-breaks).
    5. Strip known injection delimiter patterns.
    6. Truncate to max_len and strip surrounding whitespace.
    """
    if value is None:
        return ""
    text = str(value)
    # Collapse homoglyphs: fullwidth brackets, lookalike letters -> ASCII equivalents
    text = unicodedata.normalize("NFKC", text)
    # Remove zero-width / invisible characters
    text = _ZERO_WIDTH_RE.sub("", text)
    # Collapse newlines -- multi-line injection relies on line breaks
    text = text.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    # Remove known injection delimiters
    text = _INJECTION_PATTERNS.sub("", text)
    # Truncate then strip
    return text[:max_len].strip()
