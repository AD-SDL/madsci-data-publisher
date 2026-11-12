"""LabPublisher: the two tools agents use, and the command line.

`poll()` reads the lab's live state and writes it to the data store, and `run()` calls
it every 30 seconds. `describe_site()` writes a site description on request.

A poll is usually four requests: workcell `/state`, `/workflows/active`,
`/experiments`, and one bounded events query. A workflow that left the active list
since the last poll costs one more request to read its final status.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Optional

from . import site
from .events import EVENT_COLUMNS, EventCursor, EventLog, normalize, selector
from .lab import Lab, by_id, discover, now_iso, ulid_ms
from .store import DataStore
from .tables import TABLE_COLUMNS, build_tables

VERSION = "0.1.0"
STATUS_FILE = "publisher_status.json"
SITE_FILE = "site_description.json"
SNAPSHOT_DIR = Path.home() / ".local" / "state" / "madsci-data-publisher" / "snapshots"
ARCHIVED_ON_START = 100
EXPERIMENTS_PER_POLL = 100
MAX_WORKFLOWS = 500


def _terminal(wf: dict) -> bool:
    s = wf.get("status") or {}
    return bool(s.get("completed") or s.get("failed") or s.get("cancelled"))


class LabPublisher:
    def __init__(self, store: DataStore, lab_url: Optional[str] = None, urls: Optional[dict[str, str]] = None,
                 interval_s: float = 30.0, location: str = "", communication_type: str = "simulated",
                 snapshot_dir: Path = SNAPSHOT_DIR, log: Optional[Callable[[str], None]] = None) -> None:
        self.lab = Lab(urls or discover(lab_url))
        if "workcell" not in self.lab.urls:
            raise RuntimeError("No workcell manager URL. Set LAB_SERVER_URL or WORKCELL_SERVER_URL.")
        self.store = store
        self.interval_s = interval_s
        self.location = location
        self.communication_type = communication_type
        self.snapshot_dir = Path(snapshot_dir)
        self.log = log or (lambda _msg: None)
        self.run_id = uuid.uuid4().hex[:12]
        self.cycle = 0
        self.lab_settings: dict = {}
        self.history_loaded = False
        self.state: dict = {}
        self.workflows: dict[str, dict] = {}
        self.experiments: list[dict] = []
        self.events = EventLog(store.read_table("events"))
        stored = [ulid_ms(r.get("event_id")) for r in self.events.rows]
        self.cursor = EventCursor(high_water_ms=max([m for m in stored if m], default=None))

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

        # Read once, retried until it works.
        if not self.lab_settings and "lab" in self.lab.urls:
            self.lab_settings = (attempt("lab", lambda: self.lab.get("lab", "/settings")) or {}).get("settings", {})
        if not self.history_loaded:
            archived = attempt("archived_workflows",
                               lambda: by_id(self.lab.get("workcell", "/workflows/archived", {"number": ARCHIVED_ON_START})))
            if archived is not None:
                self.workflows.update(archived)
                self.history_loaded = True

        state = attempt("workcell_state", lambda: self.lab.get("workcell", "/state"))
        if state is not None:
            self.state = state
        active = attempt("active_workflows", lambda: by_id(self.lab.get("workcell", "/workflows/active")))
        if active is not None:
            vanished = [wid for wid, wf in self.workflows.items() if wid not in active and not _terminal(wf)]
            self.workflows.update(active)
            for wid in vanished:
                final = attempt(f"workflow {wid}", lambda wid=wid: self.lab.get("workcell", f"/workflow/{wid}"))
                if final:
                    self.workflows[wid] = final
            for wid in sorted(self.workflows)[: max(len(self.workflows) - MAX_WORKFLOWS, 0)]:
                del self.workflows[wid]
        experiments = attempt("experiments",
                              lambda: self.lab.get("experiment", "/experiments", {"number": EXPERIMENTS_PER_POLL}))
        if experiments is not None:
            self.experiments = list(experiments.values()) if isinstance(experiments, dict) else list(experiments)
        gap = self._poll_events(sources)

        tables = build_tables(lab_settings=self.lab_settings, state=self.state, workflows=self.workflows,
                              experiments=self.experiments, events=self.events.rows,
                              lab_url=self.lab.urls.get("lab"), workcell_url=self.lab.urls.get("workcell"),
                              location=self.location, communication_type=self.communication_type)
        changed = [name for name, rows in tables.items() if self.store.write_table(name, rows, TABLE_COLUMNS[name])]
        if self.store.write_table("events", self.events.rows, EVENT_COLUMNS):
            changed.append("events")

        described = self.store.read_json(SITE_FILE)
        current = site.fingerprint(self.state) if self.state else None
        status = {
            "publisher": "madsci-data-publisher",
            "version": VERSION,
            "run_id": self.run_id,
            "cycle": self.cycle,
            "as_of": now_iso(),
            "interval_s": self.interval_s,
            "lab_url": self.lab.urls.get("lab"),
            "workcell_url": self.lab.urls.get("workcell"),
            "workcell_ok": (self.state.get("status") or {}).get("ok"),
            "sources": sources,
            "event_gap": gap,
            "tables_changed": changed,
            "counts": {name: len(rows) for name, rows in tables.items()} | {"events": len(self.events.rows)},
            "site": {"described_at": described.get("collected_at"),
                     "stale": described.get("fingerprint") != current},
        }
        self.store.write_json(STATUS_FILE, status)
        return status

    def _poll_events(self, sources: dict) -> Optional[dict]:
        if "event" not in self.lab.urls:
            sources["events"] = {"ok": False, "error": "no Event Manager URL"}
            return None
        names = {(n.get("info") or {}).get("node_id"): name for name, n in (self.state.get("nodes") or {}).items()}
        windows, gap = self.cursor.windows()
        try:
            for lo, hi in windows:
                raw = self.lab.query("event", "/events/query", selector(lo, hi))
                rows = [normalize(eid, raw[eid], names) for eid in self.cursor.accept(list(raw))]
                self.events.add([r for r in rows if r])
            sources["events"] = {"ok": True}
        except Exception as e:  # noqa: BLE001
            sources["events"] = {"ok": False, "error": str(e)[:300]}
        return gap

    def run(self, cycles: Optional[int] = None) -> dict:
        """Poll every `interval_s` seconds until interrupted, or for `cycles` polls.
        Returns the status of the last poll."""
        status: dict = {}
        done = 0
        while cycles is None or done < cycles:
            started = time.monotonic()
            status = self.poll()
            done += 1
            failed = [k for k, v in status["sources"].items() if not v["ok"]]
            self.log(f"[{status['as_of']}] cycle {status['cycle']}: changed {status['tables_changed'] or 'nothing'}"
                     f"{'; failed ' + ', '.join(failed) if failed else ''}"
                     f"{'; site description is stale' if status['site']['stale'] else ''}")
            if cycles is None or done < cycles:
                time.sleep(max(0.0, self.interval_s - (time.monotonic() - started)))
        return status

    # ------------------------------------------------------------ site description

    def describe_site(self) -> dict:
        """Collect a site description now and write it to the data store.

        The raw responses hold node configuration, device addresses and robot poses, so
        they are kept on this host under `snapshot_dir` and only the summary is published.
        """
        if not self.lab_settings and "lab" in self.lab.urls:
            self.lab_settings = self.lab.get("lab", "/settings").get("settings", {})
        self.state = self.lab.get("workcell", "/state")
        raw = site.collect(self.lab, self.state, self.lab_settings)
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        path = self.snapshot_dir / f"snapshot.{raw['collected_at'].replace(':', '')}.json"
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(raw, fh, indent=1, default=str)
        summary = site.summarize(raw)
        self.store.write_json(SITE_FILE, summary)
        return summary


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="madsci-data-publisher",
                                 description="Publish a MADSci lab's live state and site description to a data store.")
    ap.add_argument("--lab-url", help="Lab Manager URL. Default: $LAB_SERVER_URL or http://localhost:8000/")
    ap.add_argument("--out", default="data", help="Data store directory. Default: ./data")
    ap.add_argument("--format", choices=["parquet", "json"], default="parquet")
    ap.add_argument("--location", default="", help="Free-text lab location for the labs table.")
    sub = ap.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="Poll live data every --interval seconds.")
    run.add_argument("--interval", type=float, default=30.0)
    run.add_argument("--once", action="store_true", help="Poll once and exit.")
    sub.add_parser("site", help="Collect a site description now.")
    args = ap.parse_args(argv)

    pub = LabPublisher(DataStore(args.out, args.format), lab_url=args.lab_url, location=args.location,
                       interval_s=getattr(args, "interval", 30.0), log=lambda m: print(m, flush=True))
    if args.command == "site":
        s = pub.describe_site()
        print(f"site description: {len(s['nodes'])} nodes, {len(s['locations'])} locations, "
              f"{len(s['transfers'])} transfer paths -> {pub.store.root / SITE_FILE}")
        return 0
    try:
        status = pub.run(cycles=1 if args.once else None)
    except KeyboardInterrupt:
        return 0
    if args.once:
        print(json.dumps({"counts": status["counts"], "sources": status["sources"]}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
