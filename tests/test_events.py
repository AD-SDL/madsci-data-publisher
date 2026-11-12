import json
from pathlib import Path

from madsci_data_publisher.events import EventCursor, EventLog, effective_type, normalize, selector
from madsci_data_publisher.lab import ulid_floor, ulid_ms

FIXTURES = Path(__file__).parent / "fixtures" / "example_lab"
MIN = 60_000


def test_every_window_is_bounded_and_short():
    cursor = EventCursor()
    now = 1_800_000_000_000
    windows, gap = cursor.windows(now)
    assert gap is None
    assert windows[0][0] == ulid_floor(now - 10 * MIN)
    for lo, hi in windows:
        assert lo < hi
        assert ulid_ms(hi) - ulid_ms(lo) <= 5 * MIN
        sel = selector(lo, hi)
        assert sel["_id"] == {"$gte": lo, "$lt": hi}


def test_overlap_and_gap():
    now = 1_800_000_000_000
    cursor = EventCursor(high_water_ms=now - 2 * MIN)
    windows, gap = cursor.windows(now)
    assert gap is None
    assert ulid_ms(windows[0][0]) == now - 2 * MIN - 90_000

    behind = EventCursor(high_water_ms=now - 3 * 60 * MIN)
    windows, gap = behind.windows(now)
    assert gap is not None
    assert ulid_ms(windows[0][0]) == now - 10 * MIN


def test_accept_reads_each_event_once():
    cursor = EventCursor()
    a, b = ulid_floor(1_800_000_000_000)[:10] + "A" * 16, ulid_floor(1_800_000_001_000)[:10] + "B" * 16
    assert cursor.accept([b, a]) == [a, b]
    assert cursor.accept([a, b]) == []
    assert cursor.high_water_ms == 1_800_000_001_000


def test_real_type_is_read_from_event_data():
    assert effective_type({"event_type": "log_error", "event_data": {"event_type": "node_error"}}) == "node_error"
    assert effective_type({"event_type": "workflow_complete", "event_data": {"status": "failed"}}) == "workflow_complete"


def test_idle_spam_is_dropped():
    ev = {"event_type": "log_info", "log_level": 20,
          "event_data": {"message": "No workflows ready to run", "event_type": "workcell_status_update"}}
    assert normalize(ulid_floor(1_800_000_000_000), ev, {}) is None


def test_repeats_fold_into_one_row():
    log = EventLog()
    rows = []
    for i in range(5):
        eid = ulid_floor(1_800_000_000_000 + i * 2000)[:10] + f"{i:016d}"
        rows.append(normalize(eid, {"event_type": "log_error", "log_level": 40,
                                    "event_data": {"message": "PF400 POWER OFF", "event_type": "node_error"},
                                    "source": {"node_id": "N1"}}, {"N1": "pf400"}))
    log.add(rows)
    assert len(log.rows) == 1
    assert log.rows[0]["repeat_count"] == 5
    assert log.rows[0]["node_name"] == "pf400"


def test_fixture_events_normalize_and_redact():
    events = json.loads((FIXTURES / "events.json").read_text())
    rows = [r for r in (normalize(k, v, {}) for k, v in events.items()) if r]
    assert rows
    types = {r["event_type"] for r in rows}
    assert "workflow_step_failed" in types or "node_error" in types
    assert all(not r["event_type"].startswith("log_info") for r in rows)
