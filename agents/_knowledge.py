"""Shared knowledge-file loader for agent static instructions."""
import logging
from pathlib import Path

_log = logging.getLogger(__name__)


def _load_knowledge(*filenames: str) -> str:
    """Load knowledge files at module-import time for static instruction inclusion.

    Content is frozen into module-level constants once per container lifetime,
    so it contributes to the stable cache prefix fingerprinted by ADK
    ContextCacheConfig — the same lifecycle as get_static_instruction().
    """
    _kdir = Path(__file__).parent.parent / "knowledge"
    parts = []
    for fn in filenames:
        p = _kdir / fn
        try:
            parts.append(p.read_text(encoding="utf-8").strip())
        except (OSError, IOError) as exc:
            _log.warning("Knowledge file %s failed to load: %s", fn, exc)
    return "\n\n".join(parts)
