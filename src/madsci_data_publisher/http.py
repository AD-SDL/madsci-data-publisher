"""Read-only HTTP access to the MADSci managers.

Every request in this package goes through `Http`. It allows GET anywhere and POST only
to a short list of query routes that read data without changing it. Nothing here can
submit a workflow, send an admin command or write a resource.
"""

from __future__ import annotations

import json
import socket
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Mapping, Optional

USER_AGENT = "madsci-data-publisher/0.1 (read-only)"

# POST routes that only read. Each takes a query body and returns matching records.
READ_ONLY_POSTS = frozenset({"/events/query", "/resource/query", "/history/query"})


class HttpError(RuntimeError):
    """A request failed. `status` is None when the server could not be reached."""

    def __init__(self, url: str, status: Optional[int], message: str) -> None:
        super().__init__(f"{url}: {message}")
        self.url = url
        self.status = status


def join(base: str, path: str, params: Optional[Mapping[str, Any]] = None) -> str:
    url = urllib.parse.urljoin(base if base.endswith("/") else base + "/", path.lstrip("/"))
    if params:
        url += "?" + urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    return url


class Http:
    """A small JSON client with a timeout and no retries.

    A failed request raises `HttpError` and the caller skips that source for one poll
    cycle. Retrying inside a 30 second loop would only pile requests onto a manager that
    is already struggling.
    """

    def __init__(self, timeout: float = 10.0) -> None:
        self.timeout = timeout

    def get(self, base: str, path: str, params: Optional[Mapping[str, Any]] = None) -> Any:
        return self._request("GET", join(base, path, params))

    def post_query(self, base: str, path: str, body: Any) -> Any:
        if path not in READ_ONLY_POSTS:
            raise ValueError(f"{path} is not an allowed read-only query route")
        return self._request("POST", join(base, path), body)

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
