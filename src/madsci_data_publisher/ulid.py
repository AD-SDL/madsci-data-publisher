"""ULID time helpers.

MADSci stamps events, workflows and experiments with naive local timestamps, and the
containers and host processes of one lab can disagree on the time zone. The first ten
characters of a MADSci ULID hold the creation time in milliseconds since the epoch, in
UTC, so this package uses them wherever it needs a reliable time.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_VALUE = {c: i for i, c in enumerate(CROCKFORD)}
ISO = "%Y-%m-%dT%H:%M:%SZ"


def ulid_ms(ulid: Optional[str]) -> Optional[int]:
    if not ulid or len(ulid) != 26:
        return None
    ms = 0
    for c in ulid[:10].upper():
        if c not in _VALUE:
            return None
        ms = ms * 32 + _VALUE[c]
    return ms


def ulid_floor(ms: int) -> str:
    """The smallest ULID created at `ms`. Useful as a query bound."""
    chars = []
    for _ in range(10):
        chars.append(CROCKFORD[ms % 32])
        ms //= 32
    return "".join(reversed(chars)) + "0" * 16


def ms_to_iso(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime(ISO)


def ulid_iso(ulid: Optional[str]) -> Optional[str]:
    return ms_to_iso(ulid_ms(ulid))


def naive_to_iso(value: Optional[str]) -> Optional[str]:
    """Format a MADSci timestamp as ISO UTC.

    MADSci writes naive timestamps. The managers in the example lab and in Docker
    deployments run in UTC, so a naive value is read as UTC. A value that carries an
    offset is converted.
    """
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime(ISO)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime(ISO)
