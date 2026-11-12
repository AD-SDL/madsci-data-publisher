"""Turn a raw lab snapshot into the site description agents read.

The raw snapshot holds node config, device addresses and robot poses, so it stays on
the lab host. The summary keeps what a planner needs: which nodes exist, what actions
they take and with which arguments, which named locations exist, which robot can move
between them, and which resource templates are defined. Pose values are left out.
"""

from __future__ import annotations

from typing import Any

SITE_SCHEMA = "madsci-data-publisher/site-description/0.1"


def _body(block: dict, path: str) -> Any:
    ep = (block or {}).get("endpoints", {}).get(path, {})
    return ep.get("body") if ep.get("status") == 200 else None


def _params(spec: dict) -> list[dict]:
    return [
        {
            "name": name,
            "type": p.get("argument_type"),
            "required": bool(p.get("required")),
            "default": p.get("default"),
            "description": p.get("description"),
        }
        for name, p in sorted((spec or {}).items())
        if isinstance(p, dict)
    ]


def summarize(snapshot: dict) -> dict:
    managers = snapshot.get("managers", {})
    lab_settings = (_body(managers.get("lab", {}), "/settings") or {}).get("settings", {})
    workcell = _body(managers.get("workcell", {}), "/workcell") or {}

    nodes = []
    for alias, block in sorted(snapshot.get("nodes", {}).items()):
        info = _body(block, "/info") or {}
        actions = []
        for name, a in sorted((info.get("actions") or {}).items()):
            actions.append({
                "name": name,
                "description": a.get("description"),
                "blocking": a.get("blocking"),
                "args": _params(a.get("args")),
                "locations": _params(a.get("locations")),
                "files": _params(a.get("files")),
                "results": sorted((a.get("results") or {}).keys()),
            })
        nodes.append({
            "name": alias,
            "node_id": info.get("node_id"),
            "node_type": info.get("node_type"),
            "module_name": info.get("module_name"),
            "module_version": info.get("module_version"),
            "description": info.get("node_description"),
            "interface": block.get("interface"),
            "admin_commands": sorted((info.get("capabilities") or {}).get("admin_commands") or []),
            "actions": actions,
        })

    loc_body = _body(managers.get("location", {}), "/locations") or []
    names = {loc.get("location_id"): loc.get("location_name") for loc in loc_body if isinstance(loc, dict)}
    locations = [
        {
            "name": loc.get("location_name"),
            "description": loc.get("description"),
            "allow_transfers": loc.get("allow_transfers"),
            "managed_by": loc.get("managed_by"),
            "resource_template": loc.get("resource_template_name"),
            # Which nodes have a pose for this location. The poses themselves are omitted.
            "reachable_by": sorted((loc.get("representations") or {}).keys()),
        }
        for loc in sorted(loc_body, key=lambda x: x.get("location_name") or "")
        if isinstance(loc, dict)
    ]
    detailed = _body(managers.get("location", {}), "/transfer/graph/detailed") or {}
    transfers = [
        {
            "source": names.get(e.get("source_location_id"), e.get("source_location_id")),
            "target": names.get(e.get("target_location_id"), e.get("target_location_id")),
            "nodes": e.get("node_names"),
            "cost": e.get("min_cost"),
        }
        for e in detailed.get("edges", [])
    ]
    templates = _body(managers.get("resource", {}), "/templates/query_all") or []
    resource_templates = sorted(
        {t.get("resource_name") for t in templates if isinstance(t, dict) and t.get("resource_name")}
    )

    versions = {}
    for name, block in managers.items():
        health = _body(block, "/health")
        if isinstance(health, dict) and health.get("version"):
            versions[name] = health["version"]

    return {
        "schema": SITE_SCHEMA,
        "collected_at": snapshot.get("collected_at"),
        "lab": {"lab_id": lab_settings.get("manager_id"), "name": lab_settings.get("manager_name")},
        "workcell": {"workcell_id": workcell.get("manager_id"), "name": workcell.get("name")},
        "madsci_versions": versions,
        "nodes": nodes,
        "locations": locations,
        "transfers": transfers,
        "resource_templates": resource_templates,
    }


def fingerprint_fields(state: dict) -> list:
    """The parts of workcell /state that change only when the site changes.

    Node names, module versions and action signatures. Status, module state and
    anything else that moves every cycle is left out.
    """
    out = []
    for alias, node in sorted((state.get("nodes") or {}).items()):
        info = node.get("info") or {}
        actions = []
        for name, a in sorted((info.get("actions") or {}).items()):
            sig = []
            for kind in ("args", "locations", "files"):
                for pname, p in sorted((a.get(kind) or {}).items()):
                    if isinstance(p, dict):
                        sig.append((kind, pname, p.get("argument_type"), bool(p.get("required"))))
            actions.append((name, sig))
        out.append((alias, node.get("node_url"), info.get("module_name"), info.get("module_version"), actions))
    return out
