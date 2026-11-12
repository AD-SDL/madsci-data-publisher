"""The site description: what the lab can do and where things are.

It is built from the workcell's `/state`, which already holds every node's info and
action signatures, plus the location list, the transfer graph and the resource
templates. That is four requests, and no node is contacted directly.

The summary keeps what a planner needs: which nodes exist, what actions they take and
with which arguments, which named locations exist, which robot can move between them,
and which resource templates are defined. Robot poses and node configuration are left
out. The raw responses are kept on the lab host by the publisher.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .lab import HttpError, Lab, now_iso

SITE_SCHEMA = "madsci-data-publisher/site-description/0.1"
EXTRA_SOURCES = {
    "locations": ("location", "/locations"),
    "transfer_graph": ("location", "/transfer/graph/detailed"),
    "resource_templates": ("resource", "/templates/query_all"),
}


def collect(lab: Lab, state: dict, lab_settings: dict) -> dict:
    """Raw responses for a site description. A source that fails is recorded as None."""
    raw: dict[str, Any] = {"collected_at": now_iso(), "state": state, "lab_settings": lab_settings,
                           "manager_urls": lab.urls, "versions": {}}
    for key, (manager, path) in EXTRA_SOURCES.items():
        try:
            raw[key] = lab.get(manager, path)
        except HttpError:
            raw[key] = None
    # Workcell and Event Manager /health are skipped: the first calls every node in turn
    # and the second counts every stored event.
    for manager in ("lab", "experiment", "data", "resource", "location"):
        try:
            raw["versions"][manager] = lab.get(manager, "/health").get("version")
        except (HttpError, AttributeError):
            pass
    return raw


def _params(spec: dict) -> list[dict]:
    return [
        {"name": name, "type": p.get("argument_type"), "required": bool(p.get("required")),
         "default": p.get("default"), "description": p.get("description")}
        for name, p in sorted((spec or {}).items()) if isinstance(p, dict)
    ]


def summarize(raw: dict) -> dict:
    state = raw.get("state") or {}
    nodes = []
    for name, node in sorted((state.get("nodes") or {}).items()):
        info = node.get("info") or {}
        nodes.append({
            "name": name,
            "node_id": info.get("node_id"),
            "node_type": info.get("node_type"),
            "module_name": info.get("module_name"),
            "module_version": info.get("module_version"),
            "description": info.get("node_description"),
            "admin_commands": sorted((info.get("capabilities") or {}).get("admin_commands") or []),
            "actions": [
                {"name": aname, "description": a.get("description"), "blocking": a.get("blocking"),
                 "args": _params(a.get("args")), "locations": _params(a.get("locations")),
                 "files": _params(a.get("files")), "results": sorted((a.get("results") or {}).keys())}
                for aname, a in sorted((info.get("actions") or {}).items())
            ],
        })

    locs = [loc for loc in raw.get("locations") or [] if isinstance(loc, dict)]
    names = {loc.get("location_id"): loc.get("location_name") for loc in locs}
    locations = [
        {"name": loc.get("location_name"), "description": loc.get("description"),
         "allow_transfers": loc.get("allow_transfers"), "managed_by": loc.get("managed_by"),
         "resource_template": loc.get("resource_template_name"),
         # Which nodes have a pose for this location. The poses themselves are omitted.
         "reachable_by": sorted((loc.get("representations") or {}).keys())}
        for loc in sorted(locs, key=lambda x: x.get("location_name") or "")
    ]
    transfers = [
        {"source": names.get(e.get("source_location_id"), e.get("source_location_id")),
         "target": names.get(e.get("target_location_id"), e.get("target_location_id")),
         "nodes": e.get("node_names"), "cost": e.get("min_cost")}
        for e in (raw.get("transfer_graph") or {}).get("edges", [])
    ]
    templates = raw.get("resource_templates") or []
    info = state.get("workcell_info") or {}
    settings = raw.get("lab_settings") or {}
    return {
        "schema": SITE_SCHEMA,
        "collected_at": raw.get("collected_at"),
        "fingerprint": fingerprint(state),
        "lab": {"lab_id": settings.get("manager_id"), "name": settings.get("manager_name")},
        "workcell": {"workcell_id": info.get("manager_id"), "name": info.get("name")},
        "madsci_versions": raw.get("versions") or {},
        "nodes": nodes,
        "locations": locations,
        "transfers": transfers,
        "resource_templates": sorted({t.get("resource_name") for t in templates
                                      if isinstance(t, dict) and t.get("resource_name")}),
    }


def fingerprint(state: dict) -> str:
    """A short hash of the parts of workcell /state that change only when the site does:
    node names, URLs, module versions and action signatures."""
    out = []
    for name, node in sorted((state.get("nodes") or {}).items()):
        info = node.get("info") or {}
        actions = [
            (aname, [(kind, p, spec.get("argument_type"), bool(spec.get("required")))
                     for kind in ("args", "locations", "files")
                     for p, spec in sorted((a.get(kind) or {}).items()) if isinstance(spec, dict)])
            for aname, a in sorted((info.get("actions") or {}).items())
        ]
        out.append((name, node.get("node_url"), info.get("module_name"), info.get("module_version"), actions))
    return hashlib.sha256(json.dumps(out, default=str).encode()).hexdigest()[:16]
