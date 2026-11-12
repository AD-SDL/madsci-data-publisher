"""LabPublisher: the two tools agents use.

`poll()` reads the lab's live state and writes it to the data store. `run()` calls it
every 30 seconds. `describe_site()` collects a site description on request.

One poll is usually three requests: workcell `/state`, `/workflows/active`, and one
bounded events query. A workflow that left the active list since the last poll costs
one more request to read its final status.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from . import __version__
from .events import EVENT_COLUMNS, EventCursor, EventLog, normalize, selector
from .http import Http
from .madsci_api import MadsciReader, ManagerUrls, discover
from .mcp_tables import TABLE_COLUMNS, LabSnapshot, TableOptions, build_tables
from .redact import redact
from .site import collect, summarize
from .site.summary import fingerprint_fields
from .store import DataStore
from .ulid import now_iso, ulid_ms

STATUS_FILE = "publisher_status.json"
SITE_FILE = "site_description.json"
DEFAULT_SNAPSHOT_DIR = Path.home() / ".local" / "state" / "madsci-data-publisher" / "snapshots"


@dataclass
class PublisherOptions:
    interval_s: float = 30.0
    archived_on_start: int = 100
    experiments_number: int = 100
    max_workflows: int = 500
    # Lab and workcell names change rarely; re-read them every N polls.
    refresh_identity_every: int = 20
    snapshot_dir: Path = DEFAULT_SNAPSHOT_DIR
    tables: TableOptions = field(default_factory=TableOptions)


def _terminal(wf: dict) -> bool:
    s = wf.get("status") or {}
    return bool(s.get("completed") or s.get("failed") or s.get("cancelled"))


def _digest(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()[:16]


class LabPublisher:
    def __init__(self, store: DataStore, lab_url: Optional[str] = None, urls: Optional[ManagerUrls] = None,
                 options: Optional[PublisherOptions] = None, http: Optional[Http] = None,
                 log: Optional[Callable[[str], None]] = None) -> None:
        self.http = http or Http()
        self.urls = urls or discover(lab_url, self.http)
        if not self.urls.workcell:
            raise RuntimeError("No workcell manager URL. Set LAB_SERVER_URL or WORKCELL_SERVER_URL.")
        self.reader = MadsciReader(self.urls, self.http)
        self.store = store
        self.options = options or PublisherOptions()
        self.log = log or (lambda _msg: None)
        self.run_id = uuid.uuid4().hex[:12]
        self.cycle = 0
        self.lab_settings: dict = {}
        self.workcell_info: dict = {}
        self.state: dict = {}
        self.workflows: dict[str, dict] = {}
        self.experiments: list[dict] = []
        self.events = EventLog(store.read_table("events"))
        self.cursor = EventCursor()
        stored_ids = [r["event_id"] for r in self.events.rows if r.get("event_id")]
        if stored_ids:
            self.cursor.high_water_ms = max(ulid_ms(e) or 0 for e in stored_ids)
        previous = self._read_status()
        self.site = previous.get("site") or {"described_at": None, "fingerprint": None, "stale": True}

    # ------------------------------------------------------------------ live data

    def poll(self) -> dict:
        """Read the lab once and write every table. Returns the publisher status."""
        self.cycle += 1
        sources: dict[str, dict] = {}

        def attempt(name: str, fn: Callable[[], Any]) -> Any:
            try:
                value = fn()
                sources[name] = {"ok": True}
                return value
            except Exception as e:  # noqa: BLE001  one failing source must not stop the poll
                sources[name] = {"ok": False, "error": str(e)[:300]}
                return None

        if self.cycle == 1 or self.cycle % self.options.refresh_identity_every == 0:
            if self.urls.lab:
                self.lab_settings = attempt("lab", self.reader.lab_settings) or self.lab_settings
            self.workcell_info = attempt("workcell_info", self.reader.workcell_info) or self.workcell_info
        if self.cycle == 1:
            archived = attempt("archived_workflows", lambda: self.reader.archived_workflows(self.options.archived_on_start))
            self.workflows.update(archived or {})

        state = attempt("workcell_state", self.reader.workcell_state)
        if state is not None:
            self.state = state
        active = attempt("active_workflows", self.reader.active_workflows)
        if active is not None:
            queued = {w.get("workflow_id"): w for w in self.state.get("workflow_queue") or [] if w.get("workflow_id")}
            current = {**queued, **active}
            vanished = [wid for wid, wf in self.workflows.items() if wid not in current and not _terminal(wf)]
            self.workflows.update(current)
            for wid in vanished:
                final = attempt(f"workflow {wid}", lambda wid=wid: self.reader.workflow(wid))
                if final:
                    self.workflows[wid] = final
            for wid in sorted(self.workflows)[: max(len(self.workflows) - self.options.max_workflows, 0)]:
                del self.workflows[wid]

        experiments = attempt("experiments", lambda: self.reader.experiments(self.options.experiments_number))
        if experiments is not None:
            self.experiments = experiments

        gap = self._poll_events(sources)

        snap = LabSnapshot(
            lab_url=self.urls.lab, workcell_url=self.urls.workcell, lab_settings=self.lab_settings,
            workcell_info=self.workcell_info, state=self.state, workflows=self.workflows,
            experiments=self.experiments, events=self.events.rows,
        )
        tables = build_tables(snap, self.options.tables)
        changed = [name for name, rows in tables.items()
                   if self.store.write_table(name, redact(rows), TABLE_COLUMNS[name])]
        if self.store.write_table("events", self.events.rows, EVENT_COLUMNS):
            changed.append("events")

        if self.state:
            fp = _digest(fingerprint_fields(self.state))
            self.site["stale"] = self.site.get("fingerprint") != fp
            self.site["current_fingerprint"] = fp

        status = {
            "publisher": "madsci-data-publisher",
            "version": __version__,
            "run_id": self.run_id,
            "cycle": self.cycle,
            "as_of": now_iso(),
            "interval_s": self.options.interval_s,
            "lab_url": self.urls.lab,
            "workcell_url": self.urls.workcell,
            "workcell_ok": (self.state.get("status") or {}).get("ok"),
            "sources": sources,
            "event_gap": gap,
            "tables_changed": changed,
            "counts": {name: len(rows) for name, rows in tables.items()} | {"events": len(self.events.rows)},
            "site": self.site,
        }
        self.store.write_json(STATUS_FILE, status)
        return status

    def _poll_events(self, sources: dict) -> Optional[dict]:
        if not self.urls.event:
            sources["events"] = {"ok": False, "error": "no Event Manager URL"}
            return None
        node_names = {
            (n.get("info") or {}).get("node_id"): name for name, n in (self.state.get("nodes") or {}).items()
        }
        windows, gap = self.cursor.windows()
        try:
            for lo, hi in windows:
                raw = self.reader.query_events(selector(lo, hi))
                new_ids = self.cursor.accept(list(raw))
                rows = [normalize(eid, raw[eid], node_names) for eid in new_ids]
                self.events.add([r for r in rows if r])
            sources["events"] = {"ok": True}
        except Exception as e:  # noqa: BLE001
            sources["events"] = {"ok": False, "error": str(e)[:300]}
        return gap

    def run(self, interval_s: Optional[float] = None, cycles: Optional[int] = None) -> dict:
        """Poll every `interval_s` seconds until interrupted, or for `cycles` polls.

        Returns the status of the last poll.
        """
        interval = interval_s or self.options.interval_s
        done = 0
        status: dict = {}
        while cycles is None or done < cycles:
            started = time.monotonic()
            status = self.poll()
            done += 1
            failed = [k for k, v in status["sources"].items() if not v["ok"]]
            self.log(f"[{status['as_of']}] cycle {status['cycle']}: changed {status['tables_changed'] or 'nothing'}"
                     f"{'; failed ' + ', '.join(failed) if failed else ''}"
                     f"{'; site description is stale' if status['site'].get('stale') else ''}")
            if cycles is not None and done >= cycles:
                break
            time.sleep(max(0.0, interval - (time.monotonic() - started)))
        return status

    # ------------------------------------------------------------ site description

    def describe_site(self) -> dict:
        """Collect a site description now and write it to the data store.

        The raw snapshot holds node config, device addresses and poses, so it is kept
        on this host under `options.snapshot_dir` and only the summary is published.
        """
        snapshot = collect(self.urls.as_dict())
        snap_dir = Path(self.options.snapshot_dir)
        snap_dir.mkdir(parents=True, exist_ok=True)
        raw_path = snap_dir / f"snapshot.{snapshot['collected_at'].replace(':', '').replace('+0000', 'Z')}.json"
        fd = os.open(raw_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(snapshot, fh, indent=1, default=str)
        summary = redact(summarize(snapshot))
        self.store.write_json(SITE_FILE, summary)
        nodes = snapshot["managers"].get("workcell", {}).get("endpoints", {}).get("/nodes", {}).get("body") or {}
        fp = _digest(fingerprint_fields({"nodes": nodes}))
        self.site = {"described_at": summary.get("collected_at"), "fingerprint": fp,
                     "current_fingerprint": fp, "stale": False}
        status = self._read_status()
        status["site"] = self.site
        self.store.write_json(STATUS_FILE, status)
        return summary

    def _read_status(self) -> dict:
        path = self.store.root / STATUS_FILE
        try:
            return json.loads(path.read_text())
        except (OSError, ValueError):
            return {}
