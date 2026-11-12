"""The targeted event feed.

Only a small part of the event log matters to a monitoring or debugging agent:
warnings, errors and the lifecycle of workflows, steps, nodes and experiments. This
module builds that query, keeps a cursor so each event is read once, and folds
repeated errors into one row with a count.

Three facts about MADSci 0.8 shape the code:

- Most events are stored with a generic top-level type (`log_info`, `log_error`) and
  the real type in `event_data.event_type`. The query matches both.
- `POST /events/query` has no result limit, and a large query stops the Event Manager
  from storing new events until it finishes. Every query is bounded on both sides by
  event ID and covers at most 5 minutes.
- Event timestamps are naive local times from each sender. Event IDs are ULIDs, whose
  time part is reliable, so the cursor uses IDs.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

from .lab import iso_ms, ms_to_iso, ulid_floor, ulid_ms

# Typed events worth publishing even at INFO level.
LIFECYCLE_TYPES = [
    "workflow_start", "workflow_step_start", "workflow_step_complete", "workflow_step_failed",
    "action_start", "action_failed", "action_status_change",
    "node_start", "node_error", "node_status_update",
    "experiment_start", "experiment_complete", "experiment_failed", "experiment_cancelled",
    "experiment_pause", "experiment_continued",
    "workcell_start",
]
# The few types MADSci still stores at the top level.
TOP_LEVEL_TYPES = ["workflow_complete", "workcell_start", "workcell_stop"]
# Logged on every scheduler tick by an idle workcell.
NOISE_MESSAGES = ("No workflows ready to run",)
LEVEL_NAMES = {10: "debug", 20: "info", 30: "warning", 40: "error", 50: "critical"}

EVENT_COLUMNS = [
    "event_id", "occurred_at", "last_seen_at", "repeat_count", "level", "severity",
    "event_type", "message", "node_name", "node_id", "workcell_id", "experiment_id",
    "workflow_id", "manager_id",
]


def selector(lo: str, hi: str) -> dict:
    """Events with lo <= ID < hi that are warnings, errors or lifecycle events."""
    return {
        "_id": {"$gte": lo, "$lt": hi},
        "archived": {"$ne": True},
        "$or": [
            {"log_level": {"$gte": 30}},
            {"event_type": {"$in": TOP_LEVEL_TYPES}},
            {"event_data.event_type": {"$in": LIFECYCLE_TYPES}},
        ],
    }


@dataclass
class EventCursor:
    """Which time windows to query next, and which IDs were already read.

    Each poll re-reads the last 90 seconds, because senders retry for up to about 40
    seconds and an event can land after later ones. If the publisher falls more than
    30 minutes behind, it skips ahead to 10 minutes before now and reports the gap
    instead of reading through an unbounded backlog.
    """

    lookback_ms: int = 10 * 60_000
    overlap_ms: int = 90_000
    window_ms: int = 5 * 60_000
    max_lag_ms: int = 30 * 60_000
    high_water_ms: Optional[int] = None
    seen: dict[str, int] = field(default_factory=dict)

    def windows(self, now_ms: Optional[int] = None) -> tuple[list[tuple[str, str]], Optional[dict]]:
        now_ms = now_ms or int(time.time() * 1000)
        start, gap = now_ms - self.lookback_ms, None
        if self.high_water_ms is not None:
            start = self.high_water_ms - self.overlap_ms
            if now_ms - start > self.max_lag_ms:
                gap = {"from": ms_to_iso(self.high_water_ms), "to": ms_to_iso(now_ms - self.lookback_ms)}
                start = now_ms - self.lookback_ms
        bounds = list(range(start, now_ms, self.window_ms)) + [now_ms]
        return [(ulid_floor(lo), ulid_floor(hi)) for lo, hi in zip(bounds, bounds[1:])], gap

    def accept(self, event_ids: list[str]) -> list[str]:
        """The IDs not read before, in order. Records them and moves the high-water mark."""
        new = sorted(set(event_ids) - set(self.seen))
        for eid in new:
            self.seen[eid] = ulid_ms(eid) or 0
        if self.seen:
            self.high_water_ms = max(self.high_water_ms or 0, max(self.seen.values()))
            floor = self.high_water_ms - self.overlap_ms - 60_000
            self.seen = {k: v for k, v in self.seen.items() if v >= floor}
        return new


def effective_type(event: dict) -> str:
    top = str(event.get("event_type") or "")
    data = event.get("event_data")
    if top.startswith("log") and isinstance(data, dict) and data.get("event_type"):
        return str(data["event_type"])
    return top


def normalize(event_id: str, event: dict, node_names: dict[str, str]) -> Optional[dict]:
    """One event as a flat row, or None for known noise."""
    data = event.get("event_data")
    data = data if isinstance(data, dict) else {"message": data}
    message = str(data.get("message") or "")
    if message.startswith(NOISE_MESSAGES):
        return None
    detail = data.get("error") or data.get("status") or data.get("step_status")
    if detail and str(detail) not in message:
        message = f"{message}: {detail}" if message else str(detail)
    source = event.get("source") or {}
    level = int(event.get("log_level") or 20)
    when = ms_to_iso(ulid_ms(event_id))
    return {
        "event_id": event_id,
        "occurred_at": when,
        "last_seen_at": when,
        "repeat_count": 1,
        "level": level,
        "severity": LEVEL_NAMES.get(level, str(level)),
        "event_type": effective_type(event),
        "message": message[:500],
        "node_name": data.get("node_name") or data.get("step_node") or node_names.get(source.get("node_id") or ""),
        "node_id": source.get("node_id"),
        "workcell_id": source.get("workcell_id"),
        "experiment_id": source.get("experiment_id") or data.get("experiment_id"),
        "workflow_id": data.get("workflow_id") or source.get("workflow_id"),
        "manager_id": source.get("manager_id"),
    }


class EventLog:
    """The published event table: newest first, repeats folded, at most `max_rows`."""

    def __init__(self, rows: Optional[list[dict]] = None, max_rows: int = 2000, fold_ms: int = 10 * 60_000) -> None:
        self.rows = list(rows or [])
        self.max_rows = max_rows
        self.fold_ms = fold_ms

    @staticmethod
    def _key(row: dict) -> tuple:
        return (row["event_type"], row.get("node_name") or row.get("manager_id"), row["message"])

    def add(self, new_rows: list[dict]) -> None:
        """Add rows in ID order. A repeat within `fold_ms` of the last one bumps its count."""
        latest: dict[tuple, dict] = {}
        for row in self.rows:  # newest first, so the first row per key is the latest
            latest.setdefault(self._key(row), row)
        for row in sorted(new_rows, key=lambda r: r["event_id"]):
            prev = latest.get(self._key(row))
            if prev and (ulid_ms(row["event_id"]) or 0) - (iso_ms(prev["last_seen_at"]) or 0) <= self.fold_ms:
                prev["repeat_count"] = int(prev["repeat_count"]) + 1
                prev["last_seen_at"] = row["occurred_at"]
                continue
            self.rows.insert(0, row)
            latest[self._key(row)] = row
        del self.rows[self.max_rows:]
