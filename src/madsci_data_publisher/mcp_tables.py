"""Map MADSci records onto the MCP layer's data store tables.

The tables and columns match the Data Exposer in AD-SDL/madsci-mcp (`dummy-data/app/
models.py`): labs, workcells, nodes, campaigns, experiments and workflows. Where MADSci
has no matching field, the choice made here is noted next to the column.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Optional

from .ulid import naive_to_iso, now_iso, ulid_iso

LAB_COLUMNS = ["lab_id", "name", "description", "location", "lab_url", "created_at"]
WORKCELL_COLUMNS = ["workcell_id", "name", "lab_id", "description", "workcell_url", "created_at"]
NODE_COLUMNS = ["node_id", "name", "workcell_id", "node_type", "module_name", "module_version",
                "node_url", "communication_type", "status", "created_at"]
CAMPAIGN_COLUMNS = ["campaign_id", "name", "description", "created_at", "ended_at", "is_active"]
EXPERIMENT_COLUMNS = ["experiment_id", "name", "run_name", "campaign_id", "workcell_id", "status",
                      "started_at", "ended_at", "run_count", "is_running"]
WORKFLOW_COLUMNS = ["workflow_id", "name", "experiment_id", "campaign_id", "workcell_id", "status",
                    "current_step_index", "total_steps", "current_node_id", "current_action",
                    "submitted_at", "started_at", "ended_at"]

TABLE_COLUMNS = {
    "labs": LAB_COLUMNS,
    "workcells": WORKCELL_COLUMNS,
    "nodes": NODE_COLUMNS,
    "campaigns": CAMPAIGN_COLUMNS,
    "experiments": EXPERIMENT_COLUMNS,
    "workflows": WORKFLOW_COLUMNS,
}

NODE_TYPES = {"device", "compute", "resource_manager", "event_manager", "workcell_manager",
              "data_manager", "transfer_manager"}
TERMINAL_EXPERIMENT = {"completed", "failed", "cancelled"}

# MADSci 0.8 has no campaign service, and most experiments carry no campaign ID. The MCP
# schema requires one, so such records point at this placeholder campaign.
UNASSIGNED_CAMPAIGN = "unassigned"


@dataclass
class TableOptions:
    location: str = ""
    # How each node talks to its instrument. MADSci does not record this.
    default_communication_type: str = "simulated"
    communication_types: dict[str, str] = field(default_factory=dict)


@dataclass
class LabSnapshot:
    """Everything one poll read from the lab, as MADSci returned it."""

    lab_url: Optional[str]
    workcell_url: Optional[str]
    lab_settings: dict[str, Any]
    workcell_info: dict[str, Any]
    state: dict[str, Any]
    workflows: dict[str, dict]
    experiments: list[dict]
    events: list[dict] = field(default_factory=list)
    observed_at: str = field(default_factory=now_iso)


def node_status(status: dict[str, Any]) -> str:
    """Collapse MADSci's status flags into the one value the MCP schema allows.

    Order of precedence: disconnected, errored, locked, paused (MADSci's `stopped` has
    no slot and counts as paused), initializing, busy, ready.
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
    if status.get("cancelled"):
        return "cancelled"
    if status.get("failed"):
        return "failed"
    if status.get("completed"):
        return "completed"
    if status.get("paused"):
        return "paused"
    if status.get("running"):
        return "running"
    return "queued"


def experiment_status(record: dict[str, Any], events: list[dict]) -> str:
    """The experiment's status, corrected from its events where MADSci records it wrongly.

    The Experiment Manager's `/end` always records `completed`, even when the
    application reported a failure, and logs `experiment_complete` too. The application
    logs its own `experiment_failed` or `experiment_cancelled`, which win.
    """
    exp_id = record.get("_id") or record.get("experiment_id")
    for ev in events:
        if ev.get("experiment_id") == exp_id and ev.get("event_type") in ("experiment_failed", "experiment_cancelled"):
            return ev["event_type"].removeprefix("experiment_")
    return str(record.get("status") or "unknown")


def build_tables(snap: LabSnapshot, options: Optional[TableOptions] = None) -> dict[str, list[dict]]:
    options = options or TableOptions()
    settings = snap.lab_settings or {}
    lab_id = settings.get("manager_id") or "lab"
    info = snap.workcell_info or snap.state.get("workcell_info") or {}
    workcell_id = info.get("manager_id") or "workcell"

    labs = [{
        "lab_id": lab_id,
        "name": settings.get("manager_name") or settings.get("name") or "MADSci Lab",
        "description": settings.get("manager_description") or settings.get("description"),
        "location": options.location,
        "lab_url": snap.lab_url or "",
        "created_at": ulid_iso(lab_id) or snap.observed_at,
    }]
    workcells = [{
        "workcell_id": workcell_id,
        "name": info.get("name") or "Workcell",
        "lab_id": lab_id,
        "description": info.get("description"),
        "workcell_url": snap.workcell_url or "",
        "created_at": ulid_iso(workcell_id) or snap.observed_at,
    }]

    nodes, node_ids = [], {}
    for name, node in sorted((snap.state.get("nodes") or {}).items()):
        ninfo = node.get("info") or {}
        node_id = ninfo.get("node_id") or name
        node_ids[name] = node_id
        node_type = str(ninfo.get("node_type") or "device")
        nodes.append({
            "node_id": node_id,
            "name": name,
            "workcell_id": workcell_id,
            "node_type": node_type if node_type in NODE_TYPES else "device",
            "module_name": ninfo.get("module_name") or "",
            "module_version": str(ninfo.get("module_version") or ""),
            "node_url": str(node.get("node_url") or ninfo.get("node_url") or ""),
            "communication_type": options.communication_types.get(name, options.default_communication_type),
            "status": node_status(node.get("status") or {}),
            "created_at": ulid_iso(node_id) or snap.observed_at,
        })

    workflows = []
    for wf_id, wf in sorted(snap.workflows.items()):
        status = wf.get("status") or {}
        flat = workflow_status(status)
        steps = wf.get("steps") or []
        index = int(status.get("current_step_index") or 0)
        step = steps[index] if 0 <= index < len(steps) else {}
        owner = wf.get("ownership_info") or {}
        running = flat == "running"
        workflows.append({
            "workflow_id": wf_id,
            "name": wf.get("name") or "",
            "experiment_id": owner.get("experiment_id") or "",
            "campaign_id": owner.get("campaign_id") or UNASSIGNED_CAMPAIGN,
            "workcell_id": owner.get("workcell_id") or workcell_id,
            "status": flat,
            "current_step_index": index,
            "total_steps": max(len(steps), 1),
            "current_node_id": node_ids.get(step.get("node"), step.get("node")) if running and step.get("node") else None,
            "current_action": step.get("action") if running else None,
            "submitted_at": naive_to_iso(wf.get("submitted_time")) or ulid_iso(wf_id),
            "started_at": naive_to_iso(wf.get("start_time")),
            "ended_at": naive_to_iso(wf.get("end_time")),
        })

    wf_workcell = {w["experiment_id"]: w["workcell_id"] for w in workflows if w["experiment_id"]}
    design_runs = Counter(
        ((e.get("experiment_design") or {}).get("experiment_name") or "") for e in snap.experiments
    )
    experiments = []
    for exp in sorted(snap.experiments, key=lambda e: e.get("_id") or ""):
        exp_id = exp.get("_id") or exp.get("experiment_id")
        design_name = (exp.get("experiment_design") or {}).get("experiment_name") or ""
        status = experiment_status(exp, snap.events)
        experiments.append({
            "experiment_id": exp_id,
            "name": design_name,
            "run_name": exp.get("run_name") or "",
            "campaign_id": (exp.get("ownership_info") or {}).get("campaign_id") or UNASSIGNED_CAMPAIGN,
            "workcell_id": wf_workcell.get(exp_id, workcell_id),
            "status": status,
            "started_at": naive_to_iso(exp.get("started_at")) or ulid_iso(exp_id),
            "ended_at": naive_to_iso(exp.get("ended_at")) if status in TERMINAL_EXPERIMENT else None,
            "run_count": max(design_runs.get(design_name, 1), 1),
            "is_running": status == "in_progress",
        })

    campaign_ids = {e["campaign_id"] for e in experiments} | {w["campaign_id"] for w in workflows}
    campaigns = []
    for cid in sorted(campaign_ids):
        starts = [e["started_at"] for e in experiments if e["campaign_id"] == cid and e["started_at"]]
        placeholder = cid == UNASSIGNED_CAMPAIGN
        campaigns.append({
            "campaign_id": cid,
            "name": "Unassigned (no campaign recorded in MADSci)" if placeholder else cid,
            "description": None,
            "created_at": min(starts) if starts else (ulid_iso(cid) or snap.observed_at),
            "ended_at": None,
            "is_active": True,
        })

    return {
        "labs": labs,
        "workcells": workcells,
        "nodes": nodes,
        "campaigns": campaigns,
        "experiments": experiments,
        "workflows": workflows,
    }
