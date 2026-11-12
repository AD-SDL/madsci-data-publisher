#!/usr/bin/env python3
"""Read a running MADSci lab and return one JSON snapshot of it.

Adapted from `tools/collect_live.py` in AD-SDL/site_description_generator. Changes from
that version:

- Manager URLs come from the Lab Manager's `GET /context` (or are passed in), instead
  of fixed port offsets. The offsets matched the RAPID labs but swapped the data and
  resource managers on labs that use MADSci's default ports.
- Nodes are read from the workcell's `GET /nodes`, which the engine already caches. A
  node is contacted directly only when the workcell has no info for it. This keeps the
  collector off the nodes' rate limit, which the workcell engine already uses.
- Workcell `/health` (which calls every node in turn) and Event Manager `/health`
  (which counts every stored event) are no longer read.
- `collect()` returns the snapshot, so the publisher can call it in process.

It still issues HTTP GET only. There is one request helper and it hardcodes GET.

Usage: python -m madsci_data_publisher.site.collect_live --lab-url http://localhost:8000/ --out snapshot.json
"""

from __future__ import annotations

import argparse
import json
import socket
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Callable, Optional

SCHEMA_VERSION = "snapshot/0.2.0"
COLLECTOR_VERSION = "0.2.0"

# Endpoints tried per manager. Not all exist at every MADSci generation; a 404 is
# recorded as "absent" rather than treated as a failure.
MANAGER_PATHS = {
    "lab": ["/health", "/settings", "/context"],
    "event": ["/settings"],
    "experiment": ["/health", "/settings"],
    "data": ["/health", "/settings"],
    "resource": ["/health", "/settings", "/templates/query_all", "/templates/categories"],
    "workcell": ["/settings", "/workcell", "/nodes"],
    "location": [
        "/health",
        "/settings",
        "/locations",
        "/location_templates",
        "/representation_templates",
        "/transfer/graph",
        "/transfer/graph/detailed",
    ],
}

# Tried against a node only when the workcell has no info for it. MADSci nodes answer
# /info; WEI-era modules answer /about instead.
NODE_PATHS = ["/info", "/status", "/state", "/about"]

Log = Callable[[str], None]


def get(url: str, timeout: float):
    """The only request this module makes. GET, and nothing else, ever.

    Returns (status, parsed_body_or_text, error_string_or_None).
    """
    req = urllib.request.Request(url, method="GET")
    req.add_header("Accept", "application/json")
    req.add_header("User-Agent", "madsci-data-publisher/0.1 (read-only)")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            status = resp.status
    except urllib.error.HTTPError as e:
        return e.code, None, f"HTTP {e.code}"
    except (urllib.error.URLError, socket.timeout, OSError) as e:
        return None, None, f"{type(e).__name__}: {e}"
    try:
        return status, json.loads(raw), None
    except json.JSONDecodeError:
        return status, {"_non_json_body": raw[:4000]}, None


def probe(url: str, paths: list[str], timeout: float, log: Log) -> dict:
    """GET every path under one base URL and record what came back."""
    scheme = urllib.parse.urlparse(url).scheme.lower()
    if scheme not in ("http", "https"):
        log(f"    not an HTTP endpoint, scheme '{scheme}', skipped")
        return {
            "base_url": url,
            "endpoints": {},
            "skipped": f"unsupported scheme '{scheme}', this collector speaks HTTP only",
            "summary": {"paths_tried": 0, "paths_ok": 0},
        }
    out = {"base_url": url, "endpoints": {}}
    for path in paths:
        full = urllib.parse.urljoin(url if url.endswith("/") else url + "/", path.lstrip("/"))
        status, body, err = get(full, timeout)
        if err and status is None:
            out["endpoints"][path] = {"reachable": False, "error": err}
            log(f"    {path:<28} unreachable  {err}")
        elif status == 404:
            out["endpoints"][path] = {"reachable": True, "status": 404, "absent": True}
            log(f"    {path:<28} 404 absent at this MADSci generation")
        elif err:
            out["endpoints"][path] = {"reachable": True, "status": status, "error": err}
            log(f"    {path:<28} {err}")
        else:
            out["endpoints"][path] = {"reachable": True, "status": status, "body": body}
            log(f"    {path:<28} {status} ok")
    ok = sum(1 for v in out["endpoints"].values() if v.get("status") == 200)
    out["summary"] = {"paths_tried": len(paths), "paths_ok": ok}
    return out


def discover_nodes(workcell_block: dict) -> tuple[dict[str, dict], str]:
    """Node alias to record, from the workcell's /nodes, falling back to /workcell.

    `/nodes` at 0.8 is {alias: Node} and includes nodes added at runtime. `/workcell`
    lists only the nodes the workcell was started with.
    """
    eps = workcell_block.get("endpoints", {})
    body = eps.get("/nodes", {}).get("body")
    if isinstance(body, dict) and body and all(isinstance(v, dict) for v in body.values()):
        nodes = {alias: rec for alias, rec in body.items() if rec.get("node_url")}
        if nodes:
            return nodes, "workcell manager /nodes"
    body = eps.get("/workcell", {}).get("body")
    if isinstance(body, dict):
        for key in ("nodes", "workcell_nodes"):
            candidate = body.get(key)
            if isinstance(candidate, dict) and candidate:
                return {a: {"node_url": u} for a, u in candidate.items() if isinstance(u, str)}, f"workcell manager /workcell, key '{key}'"
    return {}, "workcell manager did not list any nodes"


def collect(manager_urls: dict[str, str], timeout: float = 8.0, log: Optional[Log] = None,
            extra_nodes: Optional[dict[str, str]] = None) -> dict:
    """Read every manager and node once and return the snapshot."""
    log = log or (lambda _msg: None)
    started = datetime.now(timezone.utc).replace(microsecond=0)
    snapshot = {
        "schema_version": SCHEMA_VERSION,
        "collector_version": COLLECTOR_VERSION,
        "collected_at": started.isoformat(),
        "access": "read-only, HTTP GET only",
        "manager_urls": dict(manager_urls),
        "managers": {},
        "nodes": {},
    }
    for name, paths in MANAGER_PATHS.items():
        base = manager_urls.get(name)
        if not base:
            snapshot["managers"][name] = {"base_url": None, "endpoints": {}, "skipped": "no URL"}
            continue
        log(f"  {name} manager  {base}")
        try:
            snapshot["managers"][name] = probe(base, paths, timeout, log)
        except Exception as exc:  # noqa: BLE001  one manager must not abort the run
            snapshot["managers"][name] = {"base_url": base, "endpoints": {}, "collector_error": f"{type(exc).__name__}: {exc}"}

    nodes, provenance = discover_nodes(snapshot["managers"].get("workcell", {}))
    registered = set(nodes)
    for alias, url in (extra_nodes or {}).items():
        nodes.setdefault(alias, {"node_url": url})
    snapshot["node_discovery"] = {"source": provenance, "count": len(nodes),
                                  "registered_with_workcell": sorted(registered),
                                  "supplied_by_caller": sorted(set(extra_nodes or {}) - registered)}
    log(f"  {len(nodes)} nodes, discovered from: {provenance}")

    for alias, rec in sorted(nodes.items()):
        url = rec.get("node_url")
        if rec.get("info"):
            block = {
                "base_url": url,
                "source": "workcell manager /nodes cache",
                "endpoints": {
                    "/info": {"reachable": True, "status": 200, "body": rec.get("info")},
                    "/status": {"reachable": True, "status": 200, "body": rec.get("status")},
                    "/state": {"reachable": True, "status": 200, "body": rec.get("state")},
                },
                "interface": "madsci",
            }
        else:
            log(f"  node {alias}  {url}")
            try:
                block = probe(url, NODE_PATHS, timeout, log)
            except Exception as exc:  # noqa: BLE001  one node must not abort the run
                block = {"base_url": url, "endpoints": {}, "collector_error": f"{type(exc).__name__}: {exc}"}
            eps = block.get("endpoints", {})
            if eps.get("/info", {}).get("status") == 200:
                block["interface"] = "madsci"
            elif eps.get("/about", {}).get("status") == 200:
                block["interface"] = "wei"
            elif block.get("skipped"):
                block["interface"] = "not_http"
            elif block.get("collector_error"):
                block["interface"] = "collector_error"
            else:
                block["interface"] = "unreachable"
        block["registered_with_workcell"] = alias in registered
        snapshot["nodes"][alias] = block

    finished = datetime.now(timezone.utc).replace(microsecond=0)
    kinds: dict[str, int] = {}
    for b in snapshot["nodes"].values():
        kinds[b["interface"]] = kinds.get(b["interface"], 0) + 1
    snapshot["summary"] = {
        "finished_at": finished.isoformat(),
        "duration_seconds": int((finished - started).total_seconds()),
        "managers_answering": sum(
            1 for m in snapshot["managers"].values() if m.get("summary", {}).get("paths_ok", 0) > 0
        ),
        "managers_total": len(MANAGER_PATHS),
        "nodes_total": len(snapshot["nodes"]),
        "nodes_by_interface": kinds,
    }
    return snapshot


def manager_urls_from_context(lab_url: str, timeout: float = 8.0) -> dict[str, str]:
    """Every manager URL, from one GET of the Lab Manager's /context."""
    status, body, err = get(urllib.parse.urljoin(lab_url if lab_url.endswith("/") else lab_url + "/", "context"), timeout)
    if err or not isinstance(body, dict):
        raise RuntimeError(f"could not read {lab_url} /context: {err or 'unexpected body'}")
    return {k.removesuffix("_server_url"): v for k, v in body.items() if k.endswith("_server_url") and v}


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Read-only snapshot of a running MADSci lab.",
                                 epilog="This script only ever issues HTTP GET. It cannot modify the lab.")
    ap.add_argument("--lab-url", default="http://localhost:8000/", help="Lab Manager URL. Default http://localhost:8000/")
    ap.add_argument("--out", help="Output path. Default: snapshot.<utc>.json in the working directory.")
    ap.add_argument("--timeout", type=float, default=8.0, help="Per-request timeout in seconds. Default 8.")
    ap.add_argument("--quiet", action="store_true", help="Suppress the running log.")
    args = ap.parse_args(argv)

    def log(msg: str) -> None:
        if not args.quiet:
            print(msg, file=sys.stderr, flush=True)

    snapshot = collect(manager_urls_from_context(args.lab_url, args.timeout), args.timeout, log)
    out = args.out or f"snapshot.{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(snapshot, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
    s = snapshot["summary"]
    print(f"Wrote {out}: {s['managers_answering']} of {s['managers_total']} managers, "
          f"{s['nodes_total']} nodes {s['nodes_by_interface']}, {s['duration_seconds']}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
