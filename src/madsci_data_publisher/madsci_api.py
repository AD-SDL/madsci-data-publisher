"""Find the MADSci managers and read from them.

The Lab Manager's `GET /context` lists every manager's URL, so one setting,
`LAB_SERVER_URL`, is enough. When no Lab Manager runs, the individual URLs come from
the same environment variables MADSci itself uses.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, fields
from typing import Any, Mapping, Optional

from .http import Http

DEFAULT_LAB_URL = "http://localhost:8000/"


@dataclass
class ManagerUrls:
    lab: Optional[str] = None
    event: Optional[str] = None
    experiment: Optional[str] = None
    data: Optional[str] = None
    resource: Optional[str] = None
    workcell: Optional[str] = None
    location: Optional[str] = None

    def as_dict(self) -> dict[str, str]:
        return {f.name: getattr(self, f.name) for f in fields(self) if getattr(self, f.name)}


def discover(lab_url: Optional[str] = None, http: Optional[Http] = None,
             env: Mapping[str, str] = os.environ) -> ManagerUrls:
    """Manager URLs from the Lab Manager's context, filled in from the environment."""
    http = http or Http()
    urls = ManagerUrls()
    lab_url = lab_url or env.get("LAB_SERVER_URL") or DEFAULT_LAB_URL
    try:
        context = http.get(lab_url, "/context")
    except Exception:  # noqa: BLE001  no Lab Manager; fall back to the environment
        context = {}
    for f in fields(ManagerUrls):
        value = context.get(f"{f.name}_server_url") or env.get(f"{f.name.upper()}_SERVER_URL")
        setattr(urls, f.name, value)
    if not urls.lab and context:
        urls.lab = lab_url
    return urls


class MadsciReader:
    """Read-only calls against one MADSci lab. Each method is one HTTP request."""

    def __init__(self, urls: ManagerUrls, http: Optional[Http] = None) -> None:
        self.urls = urls
        self.http = http or Http()

    def lab_settings(self) -> dict[str, Any]:
        return self.http.get(self.urls.lab, "/settings").get("settings", {})

    def workcell_info(self) -> dict[str, Any]:
        return self.http.get(self.urls.workcell, "/workcell")

    def workcell_state(self) -> dict[str, Any]:
        """Workcell status, every node's status, state and info, and the workflow queue."""
        return self.http.get(self.urls.workcell, "/state")

    def active_workflows(self) -> dict[str, dict]:
        """Queued, running and paused workflows. Paused ones are missing from /state."""
        return _by_id(self.http.get(self.urls.workcell, "/workflows/active"), "workflow_id")

    def archived_workflows(self, number: int = 100) -> dict[str, dict]:
        return _by_id(self.http.get(self.urls.workcell, "/workflows/archived", {"number": number}), "workflow_id")

    def workflow(self, workflow_id: str) -> dict[str, Any]:
        return self.http.get(self.urls.workcell, f"/workflow/{workflow_id}")

    def experiments(self, number: int = 100) -> list[dict]:
        data = self.http.get(self.urls.experiment, "/experiments", {"number": number})
        return list(data.values()) if isinstance(data, dict) else list(data)

    def query_events(self, selector: dict) -> dict[str, dict]:
        return self.http.post_query(self.urls.event, "/events/query", selector)


def _by_id(data: Any, key: str) -> dict[str, dict]:
    """MADSci returns some collections as {id: record} and others as a list."""
    if isinstance(data, dict):
        return {k: v for k, v in data.items() if isinstance(v, dict)}
    return {item.get(key) or item.get("_id"): item for item in data or [] if isinstance(item, dict)}
