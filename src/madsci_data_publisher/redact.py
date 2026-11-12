"""Remove credentials before anything is written to the data store.

Several MADSci manager startup events carry database URLs with passwords, and the
database version checker logs migration commands with `--db_url`. These patterns come
from the RAPID-446 run export prototype, where they were checked against a month of
real events.
"""

from __future__ import annotations

import re
from typing import Any

PLACEHOLDERS = {"", "none", "null", "***redacted***", "<redacted>"}
CREDENTIAL_URL = re.compile(r"(?i)\b([a-z][a-z0-9+.\-]*://)[^\s@/\"']*:[^\s@\"']*@")
CREDENTIAL_ARG = re.compile(r"(?i)(--?(?:pass(?:word)?|db_url)['\",\s=]+)(?!None\b)[^'\",\s\]]+")
CREDENTIAL_KV = re.compile(
    r"(?i)\b(password|passwd|pwd|secret|token|api_key|access_key)(['\"]?\s*[:=]\s*['\"]?)(?!None\b)[^,)\s'\"]+"
)
CREDENTIAL_KEY = re.compile(r"(?i)(password|passwd|secret|token|api_?key|access_?key|db_url|mongo_db|document_db)")


def redact(obj: Any) -> Any:
    """Return a copy of `obj` with credentials replaced by `<redacted>`."""
    if isinstance(obj, dict):
        return {
            k: "<redacted>"
            if CREDENTIAL_KEY.search(str(k)) and isinstance(v, str) and v.strip().lower() not in PLACEHOLDERS
            else redact(v)
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [redact(v) for v in obj]
    if isinstance(obj, str):
        obj = CREDENTIAL_URL.sub(r"\1<redacted>@", obj)
        obj = CREDENTIAL_ARG.sub(r"\1<redacted>", obj)
        return CREDENTIAL_KV.sub(r"\1\2<redacted>", obj)
    return obj
