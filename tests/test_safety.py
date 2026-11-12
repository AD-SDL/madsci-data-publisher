"""The publisher must never change the lab and never publish credentials."""

import json
import re
from pathlib import Path

import pytest

from madsci_data_publisher import site
from madsci_data_publisher.lab import Lab
from madsci_data_publisher.store import DataStore, redact

SRC = Path(__file__).parent.parent / "src" / "madsci_data_publisher"
FIXTURES = Path(__file__).parent / "fixtures" / "example_lab"


def test_only_read_only_posts_are_allowed():
    lab = Lab({"event": "http://localhost:8001/", "workcell": "http://localhost:8005/"})
    for manager, path in (("workcell", "/workflow"), ("workcell", "/admin/reset/robotarm_1"),
                          ("event", "/event"), ("event", "/events/archive"), ("event", "/backup")):
        with pytest.raises(ValueError):
            lab.query(manager, path, {})


def test_all_requests_go_through_lab():
    for path in SRC.rglob("*.py"):
        text = path.read_text()
        assert not re.search(r"method=\"(PUT|PATCH|DELETE)\"", text), path
        if path.name != "lab.py":
            assert "urlopen(" not in text, path


@pytest.mark.parametrize("raw", [
    "connecting to mongodb://madsci:hunter2secret@localhost:27017/",
    "postgresql://user:pa55word@db:5432/resources",
    "run madsci migrate --db_url mongodb://a:b@c/ now",
])
def test_credentials_are_removed(raw):
    out = redact(raw)
    assert "hunter2secret" not in out and "pa55word" not in out and "a:b@" not in out
    assert "<redacted>" in out


def test_store_redacts_everything_it_writes(tmp_path):
    store = DataStore(tmp_path, fmt="json")
    store.write_table("events", [{"message": "db at mongodb://u:topsecret@h/"}], ["message"])
    store.write_json("status.json", {"mongo_db": "mongodb://u:topsecret@h/"})
    for f in tmp_path.iterdir():
        assert "topsecret" not in f.read_text()


def test_site_description_from_fixtures():
    load = lambda n: json.loads((FIXTURES / n).read_text())  # noqa: E731
    raw = {"collected_at": "2026-11-12T23:00:00Z", "state": load("state.json"),
           "lab_settings": load("lab_settings.json")["settings"], "locations": load("locations.json"),
           "transfer_graph": load("transfer_graph_detailed.json"),
           "resource_templates": load("resource_templates.json"), "versions": {"lab": "0.8.0"}}
    s = site.summarize(raw)
    assert len(s["nodes"]) == 6 and len(s["locations"]) == 10 and s["transfers"]
    arm = next(n for n in s["nodes"] if n["name"] == "robotarm_1")
    transfer = next(a for a in arm["actions"] if a["name"] == "transfer")
    assert {p["name"] for p in transfer["locations"]} == {"source", "target"}
    assert all(not t["source"].startswith("01") for t in s["transfers"])  # names, not IDs
    # Poses are never published, only which nodes can reach a location.
    assert "representations" not in json.dumps(s["locations"])
    assert s["fingerprint"] == site.fingerprint(raw["state"])
