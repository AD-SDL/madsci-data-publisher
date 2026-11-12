"""Map MADSci records onto the MCP layer's data store tables.

The tables and columns match the Data Exposer in the madsci-mcp project
(`dummy-data/app/models.py`): labs, workcells, nodes, campaigns, experiments and
workflows. Where MADSci has no matching field, the choice made here is noted inline.
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Optional

from .lab import naive_to_iso, now_iso, ulid_iso

TABLE_COLUMNS = {
    "labs": ["lab_id", "name", "description", "location", "lab_url", "created_at"],
    "workcells": ["workcell_id", "name", "lab_id", "description", "workcell_url", "created_at"],
    "nodes": ["node_id", "name", "workcell_id", "node_type", "module_name", "module_version",
              "node_url", "communication_type", "status", "created_at"],
    "campaigns": ["campaign_id", "name", "description", "created_at", "ended_at", "is_active"],
    "experiments": ["experiment_id", "name", "run_name", "campaign_id", "workcell_id", "status",
                    "started_at", "ended_at", "run_count", "is_running"],
    "workflows": ["workflow_id", "name", "experiment_id", "campaign_id", "workcell_id", "status",
                  "current_step_index", "total_steps", "current_node_id", "current_action",
                  "submitted_at", "started_at", "ended_at"],
}

NODE_TYPES = {"device", "compute", "resource_manager", "event_manager", "workcell_manager",
              "data_manager", "transfer_manager"}
TERMINAL_EXPERIMENT = {"completed", "failed", "cancelled"}
# MADSci 0.8 has no campaign service, and most experiments carry no campaign ID. The MCP
# schema requires one, so such records point at this placeholder campaign.
UNASSIGNED_CAMPAIGN = "unassigned"


def node_status(status: dict[str, Any]) -> str:
    """Collapse MADSci's status flags into the one value the MCP schema allows.

    Precedence: disconnected, errored, locked, paused (MADSci's `stopped` has no slot
    and counts as paused), initializing, busy, ready.
    """
    if status.get("disconnected"):
        return "disconnected"
    if status.get("errored"):
        return "errored"
    if status.get("locked"):
        return "locked"
    if status.get("paused") or status.get("stopped"):
        return "paused"
    if status.get("initializing") or status.get("waiting_for_config"):
        return "initializing"
    if status.get("busy") or status.get("running_actions"):
        return "busy"
    return "ready"


def workflow_status(status: dict[str, Any]) -> str:
    for flag in ("cancelled", "failed", "completed", "paused", "running"):
        if status.get(flag):
            return flag
    return "queued"


def experiment_status(record: dict[str, Any], events: list[dict]) -> str:
    """The experiment's status, corrected from its events where MADSci records it wrongly.

    The Experiment Manager's `/end` always records `completed`, even when the
    application reported a failure. The application logs its own `experiment_failed`
    or `experiment_cancelled`, which win.
    """
    exp_id = record.get("_id") or record.get("experiment_id")
    for ev in events:
        if ev.get("experiment_id") == exp_id and ev.get("event_type") in ("experiment_failed", "experiment_cancelled"):
            return ev["event_type"].removeprefix("experiment_")
    return str(record.get("status") or "unknown")


def build_tables(*, lab_settings: dict, state: dict, workflows: dict[str, dict], experiments: list[dict],
                 events: list[dict], lab_url: Optional[str], workcell_url: Optional[str],
                 location: str = "", communication_type: str = "simulated") -> dict[str, list[dict]]:
    """The six MCP tables from one poll's worth of MADSci responses.

    `communication_type` fills a column MADSci does not record (how a node talks to its
    instrument). `location` is free text for the labs table.
    """
    observed = now_iso()
    lab_id = lab_settings.get("manager_id") or "lab"
    info = state.get("workcell_info") or {}
    workcell_id = info.get("manager_id") or "workcell"

    labs = [{
        "lab_id": lab_id,
        "name": lab_settings.get("manager_name") or "MADSci Lab",
        "description": lab_settings.get("manager_description") or lab_settings.get("description"),
        "location": location,
        "lab_url": lab_url or "",
        "created_at": ulid_iso(lab_id) or observed,
    }]
    workcells = [{
        "workcell_id": workcell_id,
        "name": info.get("name") or "Workcell",
        "lab_id": lab_id,
        "description": info.get("description"),
        "workcell_url": workcell_url or "",
        "created_at": ulid_iso(workcell_id) or observed,
    }]

    nodes, node_ids = [], {}
    for name, node in sorted((state.get("nodes") or {}).items()):
        ninfo = node.get("info") or {}
        node_id = node_ids[name] = ninfo.get("node_id") or name
        node_type = str(ninfo.get("node_type") or "device")
        nodes.append({
            "node_id": node_id,
            "name": name,
            "workcell_id": workcell_id,
            "node_type": node_type if node_type in NODE_TYPES else "device",
            "module_name": ninfo.get("module_name") or "",
            "module_version": str(ninfo.get("module_version") or ""),
            "node_url": str(node.get("node_url") or ninfo.get("node_url") or ""),
            "communication_type": communication_type,
            "status": node_status(node.get("status") or {}),
            "created_at": ulid_iso(node_id) or observed,
        })

    wf_rows = []
    for wf_id, wf in sorted(workflows.items()):
        status = workflow_status(wf.get("status") or {})
        steps = wf.get("steps") or []
        index = int((wf.get("status") or {}).get("current_step_index") or 0)
        step = steps[index] if 0 <= index < len(steps) else {}
        owner = wf.get("ownership_info") or {}
        running = status == "running"
        wf_rows.append({
            "workflow_id": wf_id,
            "name": wf.get("name") or "",
            "experiment_id": owner.get("experiment_id") or "",
            "campaign_id": owner.get("campaign_id") or UNASSIGNED_CAMPAIGN,
            "workcell_id": owner.get("workcell_id") or workcell_id,
            "status": status,
            "current_step_index": index,
            "total_steps": max(len(steps), 1),
            "current_node_id": node_ids.get(step.get("node"), step.get("node")) if running and step.get("node") else None,
            "current_action": step.get("action") if running else None,
            "submitted_at": naive_to_iso(wf.get("submitted_time")) or ulid_iso(wf_id),
            "started_at": naive_to_iso(wf.get("start_time")),
            "ended_at": naive_to_iso(wf.get("end_time")),
        })

    wf_workcell = {w["experiment_id"]: w["workcell_id"] for w in wf_rows if w["experiment_id"]}
    design_name = lambda e: (e.get("experiment_design") or {}).get("experiment_name") or ""  # noqa: E731
    runs = Counter(design_name(e) for e in experiments)
    exp_rows = []
    for exp in sorted(experiments, key=lambda e: e.get("_id") or ""):
        exp_id = exp.get("_id") or exp.get("experiment_id")
        status = experiment_status(exp, events)
        exp_rows.append({
            "experiment_id": exp_id,
            "name": design_name(exp),
            "run_name": exp.get("run_name") or "",
            "campaign_id": (exp.get("ownership_info") or {}).get("campaign_id") or UNASSIGNED_CAMPAIGN,
            "workcell_id": wf_workcell.get(exp_id, workcell_id),
            "status": status,
            "started_at": naive_to_iso(exp.get("started_at")) or ulid_iso(exp_id),
            "ended_at": naive_to_iso(exp.get("ended_at")) if status in TERMINAL_EXPERIMENT else None,
            "run_count": max(runs.get(design_name(exp), 1), 1),
            "is_running": status == "in_progress",
        })

    campaigns = []
    for cid in sorted({e["campaign_id"] for e in exp_rows} | {w["campaign_id"] for w in wf_rows}):
        starts = [e["started_at"] for e in exp_rows if e["campaign_id"] == cid and e["started_at"]]
        campaigns.append({
            "campaign_id": cid,
            "name": "Unassigned (no campaign recorded in MADSci)" if cid == UNASSIGNED_CAMPAIGN else cid,
            "description": None,
            "created_at": min(starts) if starts else (ulid_iso(cid) or observed),
            "ended_at": None,
            "is_active": True,
        })

    return {"labs": labs, "workcells": workcells, "nodes": nodes, "campaigns": campaigns,
            "experiments": exp_rows, "workflows": wf_rows}
