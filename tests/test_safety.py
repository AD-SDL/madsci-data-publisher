"""The publisher must never change the lab and never publish credentials."""

import re
from pathlib import Path

import pytest

from madsci_data_publisher.http import Http
from madsci_data_publisher.redact import redact
from madsci_data_publisher.site.collect_live import discover_nodes

SRC = Path(__file__).parent.parent / "src" / "madsci_data_publisher"


def test_only_read_only_posts_are_allowed():
    http = Http()
    for path in ("/workflow", "/admin/reset/robotarm_1", "/event", "/events/archive", "/backup"):
        with pytest.raises(ValueError):
            http.post_query("http://localhost:8001/", path, {})


def test_no_other_http_methods_in_source():
    for path in SRC.rglob("*.py"):
        text = path.read_text()
        assert not re.search(r"method=\"(PUT|PATCH|DELETE)\"", text), path
        if path.name != "http.py":
            assert "urlopen(" not in text or path.name == "collect_live.py", path
    assert 'method="GET"' in (SRC / "site" / "collect_live.py").read_text()


@pytest.mark.parametrize("raw", [
    "connecting to mongodb://madsci:hunter2secret@localhost:27017/",
    "postgresql://user:pa55word@db:5432/resources",
    "run madsci migrate --db_url mongodb://a:b@c/ now",
])
def test_credentials_are_removed(raw):
    out = redact(raw)
    assert "hunter2secret" not in out and "pa55word" not in out and "a:b@" not in out
    assert "<redacted>" in out


def test_credential_keys_are_removed():
    out = redact({"event_data": {"mongo_db": "mongodb://x:y@h/", "message": "ok"}})
    assert out["event_data"]["mongo_db"] == "<redacted>"
    assert out["event_data"]["message"] == "ok"


def test_site_collector_reads_runtime_nodes_from_workcell():
    block = {"endpoints": {
        "/nodes": {"status": 200, "body": {"late_node": {"node_url": "http://localhost:2010/", "info": {}}}},
        "/workcell": {"status": 200, "body": {"nodes": {"early_node": "http://localhost:2000/"}}},
    }}
    nodes, source = discover_nodes(block)
    assert set(nodes) == {"late_node"}
    assert "/nodes" in source
