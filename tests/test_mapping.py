"""Mapping from MADSci responses to the MCP data store tables.

Fixtures were captured from the MADSci example lab (0.8.0) with one failed workflow,
one workflow blocked behind an errored robot arm, and two experiments.
"""

import json
from pathlib import Path

import pytest

from madsci_data_publisher.events import EventLog, normalize
from madsci_data_publisher.mcp_tables import (
    TABLE_COLUMNS,
    LabSnapshot,
    TableOptions,
    build_tables,
    experiment_status,
    node_status,
    workflow_status,
)

FIXTURES = Path(__file__).parent / "fixtures" / "example_lab"

# Allowed values in the MCP layer's Data Exposer models (madsci-mcp dummy-data/app/models.py).
NODE_STATUSES = {"ready", "busy", "paused", "locked", "errored", "disconnected", "initializing"}
WORKFLOW_STATUSES = {"queued", "running", "paused", "completed", "failed", "cancelled"}
EXPERIMENT_STATUSES = {"in_progress", "paused", "completed", "failed", "cancelled", "unknown"}
COMMUNICATION_TYPES = {"usb", "serial", "tcp", "ethercat", "zmq", "gpib", "simulated"}


def load(name):
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture
def snapshot():
    state = load("state.json")
    workflows = {**load("workflows_archived.json"), **load("workflows_active.json")}
    events = load("events.json")
    node_names = {(n.get("info") or {}).get("node_id"): name for name, n in state["nodes"].items()}
    log = EventLog()
    log.add([r for r in (normalize(k, v, node_names) for k, v in events.items()) if r])
    return LabSnapshot(
        lab_url="http://localhost:8000/", workcell_url="http://localhost:8005/",
        lab_settings=load("lab_settings.json")["settings"], workcell_info=load("workcell.json"),
        state=state, workflows=workflows, experiments=load("experiments.json"), events=log.rows,
    )


def test_tables_have_exactly_the_mcp_columns(snapshot):
    tables = build_tables(snapshot)
    assert set(tables) == set(TABLE_COLUMNS)
    for name, rows in tables.items():
        for row in rows:
            assert list(row) == TABLE_COLUMNS[name], name


def test_values_fit_the_mcp_enums(snapshot):
    tables = build_tables(snapshot)
    assert {n["status"] for n in tables["nodes"]} <= NODE_STATUSES
    assert {n["communication_type"] for n in tables["nodes"]} <= COMMUNICATION_TYPES
    assert {w["status"] for w in tables["workflows"]} <= WORKFLOW_STATUSES
    assert {e["status"] for e in tables["experiments"]} <= EXPERIMENT_STATUSES
    for w in tables["workflows"]:
        assert w["total_steps"] >= 1
        assert isinstance(w["experiment_id"], str) and isinstance(w["campaign_id"], str)
        if w["status"] != "running":
            assert w["current_node_id"] is None
    for e in tables["experiments"]:
        assert e["run_count"] >= 1
        assert e["is_running"] == (e["status"] == "in_progress")
        if e["status"] not in ("completed", "failed", "cancelled"):
            assert e["ended_at"] is None


def test_example_lab_contents(snapshot):
    tables = build_tables(snapshot, TableOptions(location="test bench"))
    assert tables["labs"][0]["name"] == "Example Lab"
    assert tables["labs"][0]["location"] == "test bench"
    assert tables["workcells"][0]["name"] == "Example Workcell"
    nodes = {n["name"]: n for n in tables["nodes"]}
    assert len(nodes) == 6
    assert nodes["robotarm_1"]["status"] == "errored"
    statuses = sorted(w["status"] for w in tables["workflows"])
    assert "failed" in statuses and "queued" in statuses
    # Every workflow points at a campaign row, real or the placeholder.
    campaign_ids = {c["campaign_id"] for c in tables["campaigns"]}
    assert {w["campaign_id"] for w in tables["workflows"]} <= campaign_ids
    assert {e["campaign_id"] for e in tables["experiments"]} <= campaign_ids


def test_timestamps_are_utc_iso(snapshot):
    tables = build_tables(snapshot)
    for w in tables["workflows"]:
        assert w["submitted_at"].endswith("Z")
    for n in tables["nodes"]:
        assert n["created_at"].endswith("Z")


@pytest.mark.parametrize("flags, expected", [
    ({}, "ready"),
    ({"busy": True}, "busy"),
    ({"running_actions": ["A"]}, "busy"),
    ({"busy": True, "errored": True}, "errored"),
    ({"errored": True, "disconnected": True}, "disconnected"),
    ({"stopped": True}, "paused"),
    ({"locked": True, "paused": True}, "locked"),
    ({"waiting_for_config": ["x"]}, "initializing"),
])
def test_node_status_precedence(flags, expected):
    assert node_status(flags) == expected


@pytest.mark.parametrize("flags, expected", [
    ({}, "queued"),
    ({"running": True}, "running"),
    ({"paused": True}, "paused"),
    ({"completed": True}, "completed"),
    ({"failed": True, "completed": True}, "failed"),
    ({"cancelled": True, "failed": True}, "cancelled"),
])
def test_workflow_status(flags, expected):
    assert workflow_status(flags) == expected


def test_application_failure_overrides_recorded_completion():
    record = {"_id": "E1", "status": "completed"}
    events = [{"experiment_id": "E1", "event_type": "experiment_complete"},
              {"experiment_id": "E1", "event_type": "experiment_failed"}]
    assert experiment_status(record, events) == "failed"
    assert experiment_status(record, events[:1]) == "completed"
