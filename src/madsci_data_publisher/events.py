"""The targeted event feed.

Only a small part of the event log matters to a monitoring or debugging agent:
warnings, errors and the lifecycle of workflows, steps, nodes and experiments. This
module builds that query, keeps a cursor so each event is read once, and folds
repeated errors into one row with a count.

Three facts about MADSci 0.8 shape the code:

- Most events are stored with a generic top-level type (`log_info`, `log_error`) and
  the real type in `event_data.event_type`. The query matches both.
- `POST /events/query` has no result limit, and a large query stops the Event Manager
  from storing new events until it finishes. Every query here is bounded on both sides
  by event ID and covers at most a few minutes.
- Event timestamps are naive local times from each sender. Event IDs are ULIDs, whose
  time part is reliable, so the cursor uses IDs.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Optional

from .redact import redact
from .ulid import ms_to_iso, ulid_floor, ulid_ms

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
# Logged on every scheduler tick by an idle workcell. Never useful.
NOISE_MESSAGES = ("No workflows ready to run",)

WARNING = 30
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
            {"log_level": {"$gte": WARNING}},
            {"event_type": {"$in": TOP_LEVEL_TYPES}},
            {"event_data.event_type": {"$in": LIFECYCLE_TYPES}},
        ],
    }


@dataclass
class EventCursor:
    """Which time windows to query next, and which IDs were already seen.

    Each poll re-reads the last `overlap_s` seconds, because senders retry for up to
    about 40 seconds and an event can arrive after later ones. If the publisher falls
    more than `max_lag_s` behind, it skips ahead to `lookback_s` before now and reports
    the gap instead of catching up through an unbounded backlog.
    """

    lookback_s: float = 600
    overlap_s: float = 90
    window_s: float = 300
    max_lag_s: float = 1800
    high_water_ms: Optional[int] = None
    seen: dict[str, int] = field(default_factory=dict)

    def windows(self, now_ms: Optional[int] = None) -> tuple[list[tuple[str, str]], Optional[dict]]:
        now_ms = now_ms or int(time.time() * 1000)
        gap = None
        if self.high_water_ms is None:
            start = now_ms - int(self.lookback_s * 1000)
        else:
            start = self.high_water_ms - int(self.overlap_s * 1000)
            if now_ms - start > self.max_lag_s * 1000:
                gap = {"from": ms_to_iso(self.high_water_ms), "to": ms_to_iso(now_ms - int(self.lookback_s * 1000))}
                start = now_ms - int(self.lookback_s * 1000)
        out = []
        step = int(self.window_s * 1000)
        lo = start
        while lo < now_ms:
            hi = min(lo + step, now_ms + 1)
            out.append((ulid_floor(lo), ulid_floor(hi)))
            lo = hi
        return out, gap

    def accept(self, event_ids: list[str]) -> list[str]:
        """Return the IDs not seen before, record them, and move the high-water mark."""
        new = []
        for eid in sorted(event_ids):
            if eid in self.seen:
                continue
            ms = ulid_ms(eid) or 0
            self.seen[eid] = ms
            new.append(eid)
            if self.high_water_ms is None or ms > self.high_water_ms:
                self.high_water_ms = ms
        if self.high_water_ms is not None:
            floor = self.high_water_ms - int((self.overlap_s + 60) * 1000)
            self.seen = {k: v for k, v in self.seen.items() if v >= floor}
        return new


def effective_type(event: dict) -> str:
    top = str(event.get("event_type") or "")
    data = event.get("event_data")
    if top.startswith("log") and isinstance(data, dict) and data.get("event_type"):
        return str(data["event_type"])
    return top


def normalize(event_id: str, event: dict, node_names: dict[str, str]) -> Optional[dict]:
    """One event as a flat, redacted row, or None for known noise."""
    data = event.get("event_data")
    data = data if isinstance(data, dict) else {"message": data}
    message = str(data.get("message") or "")
    if message.startswith(NOISE_MESSAGES):
        return None
    detail = data.get("error") or data.get("status") or data.get("step_status")
    if detail and str(detail) not in message:
        message = f"{message}: {detail}" if message else str(detail)
    source = event.get("source") or {}
    node_id = source.get("node_id")
    node_name = data.get("node_name") or data.get("step_node") or node_names.get(node_id or "")
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
        "message": redact(message)[:500],
        "node_name": node_name,
        "node_id": node_id,
        "workcell_id": source.get("workcell_id"),
        "experiment_id": source.get("experiment_id") or data.get("experiment_id"),
        "workflow_id": data.get("workflow_id") or source.get("workflow_id"),
        "manager_id": source.get("manager_id"),
    }


class EventLog:
    """The published event table: newest first, repeats folded, size capped."""

    def __init__(self, rows: Optional[list[dict]] = None, max_rows: int = 2000, fold_s: float = 600) -> None:
        self.rows = list(rows or [])
        self.max_rows = max_rows
        self.fold_ms = int(fold_s * 1000)

    @staticmethod
    def _key(row: dict) -> tuple:
        return (row["event_type"], row.get("node_name") or row.get("manager_id"), row["message"])

    def add(self, new_rows: list[dict]) -> int:
        """Add rows in ID order. A repeat of a recent row bumps its count instead."""
        recent = {}
        for i, row in enumerate(self.rows):
            if self._key(row) not in recent:
                recent[self._key(row)] = i
        added = 0
        for row in sorted(new_rows, key=lambda r: r["event_id"]):
            i = recent.get(self._key(row))
            if i is not None:
                prev = self.rows[i]
                prev_ms = ulid_ms(prev["event_id"]) or 0
                last_ms = _iso_ms(prev["last_seen_at"]) or prev_ms
                row_ms = ulid_ms(row["event_id"]) or 0
                if row_ms - last_ms <= self.fold_ms:
                    prev["repeat_count"] = int(prev["repeat_count"]) + 1
                    prev["last_seen_at"] = row["occurred_at"]
                    continue
            self.rows.insert(0, row)
            recent = {self._key(r): j for j, r in reversed(list(enumerate(self.rows)))}
            added += 1
        del self.rows[self.max_rows:]
        return added


def _iso_ms(value: Any) -> Optional[int]:
    from datetime import datetime

    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None
