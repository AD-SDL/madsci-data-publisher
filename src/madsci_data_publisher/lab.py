"""Read-only access to a MADSci lab over HTTP, and helpers for its timestamps.

Every request goes through `Lab`. It allows GET anywhere and POST only to query routes
that read data without changing it, so nothing in this package can change the lab.
The Lab Manager's `/context` lists every manager, so one URL is enough to find them.
"""

from __future__ import annotations

import json
import os
import socket
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

DEFAULT_LAB_URL = "http://localhost:8000/"
MANAGERS = ("lab", "event", "experiment", "data", "resource", "workcell", "location")
# POST routes that only read. Each takes a query body and returns matching records.
READ_ONLY_POSTS = frozenset({"/events/query", "/resource/query", "/history/query"})
USER_AGENT = "madsci-data-publisher/0.1 (read-only)"


class HttpError(RuntimeError):
    """A request failed. `status` is None when the server could not be reached."""

    def __init__(self, url: str, status: Optional[int], message: str) -> None:
        super().__init__(f"{url}: {message}")
        self.status = status


class Lab:
    """GET and read-only POST against the lab's managers, with a timeout and no retries.

    A failed request raises `HttpError` and the caller skips that source for one poll.
    Retrying inside a 30 second loop would only pile requests onto a struggling manager.
    """

    def __init__(self, urls: Mapping[str, str], timeout: float = 10.0) -> None:
        self.urls = dict(urls)
        self.timeout = timeout

    def get(self, manager: str, path: str, params: Optional[Mapping[str, Any]] = None) -> Any:
        return self._request("GET", self._url(manager, path, params))

    def query(self, manager: str, path: str, body: Any) -> Any:
        if path not in READ_ONLY_POSTS:
            raise ValueError(f"{path} is not an allowed read-only query route")
        return self._request("POST", self._url(manager, path), body)

    def _url(self, manager: str, path: str, params: Optional[Mapping[str, Any]] = None) -> str:
        base = self.urls.get(manager)
        if not base:
            raise HttpError(f"<{manager}>{path}", None, f"no URL for the {manager} manager")
        url = urllib.parse.urljoin(base if base.endswith("/") else base + "/", path.lstrip("/"))
        if params:
            url += "?" + urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        return url

    def _request(self, method: str, url: str, body: Any = None) -> Any:
        data = None if body is None else json.dumps(body).encode("utf-8")
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Accept", "application/json")
        req.add_header("User-Agent", USER_AGENT)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            raise HttpError(url, e.code, f"HTTP {e.code}") from None
        except (urllib.error.URLError, socket.timeout, OSError) as e:
            raise HttpError(url, None, f"{type(e).__name__}: {e}") from None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            raise HttpError(url, 200, "response was not JSON") from None


def discover(lab_url: Optional[str] = None, env: Mapping[str, str] = os.environ) -> dict[str, str]:
    """Manager URLs from the Lab Manager's /context, filled in from MADSci's env vars."""
    lab_url = lab_url or env.get("LAB_SERVER_URL") or DEFAULT_LAB_URL
    try:
        context = Lab({"lab": lab_url}).get("lab", "/context")
    except HttpError:
        context = {}
    urls = {m: context.get(f"{m}_server_url") or env.get(f"{m.upper()}_SERVER_URL") for m in MANAGERS}
    if context and not urls["lab"]:
        urls["lab"] = lab_url
    return {m: u for m, u in urls.items() if u}


def by_id(data: Any, key: str = "workflow_id") -> dict[str, dict]:
    """MADSci returns some collections as {id: record} and others as a list."""
    if isinstance(data, dict):
        return {k: v for k, v in data.items() if isinstance(v, dict)}
    return {item.get(key) or item.get("_id"): item for item in data or [] if isinstance(item, dict)}


# --- time --------------------------------------------------------------------------
# MADSci writes naive local timestamps, and the containers and host processes of one
# lab can disagree on the time zone. The first ten characters of a MADSci ULID hold the
# creation time in milliseconds since the epoch, in UTC, so those are used where a
# reliable time matters.

ISO = "%Y-%m-%dT%H:%M:%SZ"
CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_VALUE = {c: i for i, c in enumerate(CROCKFORD)}


def ulid_ms(ulid: Optional[str]) -> Optional[int]:
    if not ulid or len(ulid) != 26 or any(c not in _VALUE for c in ulid[:10].upper()):
        return None
    ms = 0
    for c in ulid[:10].upper():
        ms = ms * 32 + _VALUE[c]
    return ms


def ulid_floor(ms: int) -> str:
    """The smallest ULID created at `ms`. Used as a query bound."""
    chars = []
    for _ in range(10):
        chars.append(CROCKFORD[ms % 32])
        ms //= 32
    return "".join(reversed(chars)) + "0" * 16


def ms_to_iso(ms: Optional[int]) -> Optional[str]:
    return None if ms is None else datetime.fromtimestamp(ms / 1000, timezone.utc).strftime(ISO)


def ulid_iso(ulid: Optional[str]) -> Optional[str]:
    return ms_to_iso(ulid_ms(ulid))


def iso_ms(value: Any) -> Optional[int]:
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


def naive_to_iso(value: Optional[str]) -> Optional[str]:
    """A MADSci timestamp as ISO UTC. Naive values are read as UTC, which is what the
    managers use in the example lab and in Docker deployments."""
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
