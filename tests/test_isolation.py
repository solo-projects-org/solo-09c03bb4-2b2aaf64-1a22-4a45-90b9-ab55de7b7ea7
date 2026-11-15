"""End-to-end tests for station/time-range isolation marks.

Isolation marks are ``isolate`` actions committed atomically to the Delta
log.  They hide instrument-outage observations from the available-
observations query without rewriting parquet, so raw snapshots stay
readable and every historical version keeps its own effective mark set.
"""

from __future__ import annotations

import multiprocessing
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import delta
from app.external_writer import append_rows, delete_files
from app.sample_data import create_sample_table

T0 = 1_731_600_000  # sample-data UTC base (see app/sample_data.py)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def table(tmp_path: Path) -> Path:
    t = tmp_path / "water_level"
    create_sample_table(t)
    return t


def _available(table: Path, version: int | None = None):
    data, excluded, snap = delta.read_available(table, version)
    return data.to_pylist(), excluded, snap


def _stations_at(rows, station):
    return sorted(r["utc_seconds"] for r in rows if r["station"] == station)


# ---------------------------------------------------------------------------
# Basic semantics
# ---------------------------------------------------------------------------


def test_isolate_excludes_marked_range_and_keeps_raw_snapshot(table: Path) -> None:
    # ST-001 has rows at T0+0..5; isolate [T0+1, T0+4).
    result = delta.isolate_range(table, "ST-001", T0 + 1, T0 + 4)
    assert result["version"] == 5
    assert result["mark_id"]

    rows, excluded, snap = _available(table)
    assert snap.version == 5
    assert excluded == 3  # T0+1, T0+2, T0+3
    assert len(rows) == 21
    # Half-open interval: start excluded, end NOT excluded.
    assert _stations_at(rows, "ST-001") == [T0, T0 + 4, T0 + 5] + [
        T0 + 3600 + i for i in range(6)
    ]
    # Other stations at the same timestamps are untouched.
    assert _stations_at(rows, "ST-002") == [T0 + i for i in range(6)]

    # Raw snapshot is fully preserved for review.
    assert delta.read_table(table).num_rows == 24
    assert len(delta.load_snapshot(table).isolations) == 1


def test_isolation_half_open_boundaries(table: Path) -> None:
    delta.isolate_range(table, "ST-003", T0, T0 + 1)  # exactly the T0 row
    delta.isolate_range(table, "ST-003", T0 + 5, T0 + 6)  # exactly the T0+5 row
    rows, excluded, _ = _available(table)
    assert excluded == 2
    assert _stations_at(rows, "ST-003") == [T0 + 1, T0 + 2, T0 + 3, T0 + 4]


def test_isolation_commit_is_one_atomic_log_version(table: Path) -> None:
    delta.isolate_range(table, "ST-001", T0 + 1, T0 + 4)
    assert delta.list_versions(table) == [0, 1, 2, 3, 4, 5]  # still contiguous

    actions = delta.read_log(table, 5)
    info = [a["commitInfo"] for a in actions if "commitInfo" in a]
    marks = [a["isolate"] for a in actions if "isolate" in a]
    assert len(marks) == 1
    assert marks[0]["station"] == "ST-001"
    assert marks[0]["startUtcSeconds"] == T0 + 1
    assert marks[0]["endUtcSeconds"] == T0 + 4
    assert marks[0]["dataChange"] is False
    assert info and info[0]["operation"] == "ISOLATE"
    # No data files were added or removed: parquet is never rewritten.
    assert not any("add" in a or "remove" in a for a in actions)
    assert len(delta.load_snapshot(table).active) == 4


def test_isolation_validation_rejects_bad_requests(table: Path) -> None:
    before = delta.list_versions(table)
    for station, start, end in [
        ("", T0, T0 + 1),  # empty station
        ("ST-001", T0 + 5, T0 + 5),  # empty interval
        ("ST-001", T0 + 6, T0 + 5),  # inverted interval
        ("ST-001", True, T0 + 5),  # bool is not an int timestamp
        ("ST-001", T0, 2**63),  # outside int64
        ("ST-001", "soon", T0 + 5),  # not an integer
    ]:
        with pytest.raises(delta.InvalidRequestError):
            delta.isolate_range(table, station, start, end)
    # Nothing was committed and nothing is isolated.
    assert delta.list_versions(table) == before
    assert delta.load_snapshot(table).isolations == []


# ---------------------------------------------------------------------------
# Version consistency: rows and marks always come from the same version
# ---------------------------------------------------------------------------


def test_available_query_never_mixes_versions(table: Path) -> None:
    # v5: isolate the whole second ST-001 batch ([T0+3600, T0+3606)).
    delta.isolate_range(table, "ST-001", T0 + 3600, T0 + 3606)
    # v6: late-arriving observations inside the outage + one clean row.
    append_rows(
        table,
        [
            {"station": "ST-001", "utc_seconds": T0 + 3601, "level_mm": 9999},
            {"station": "ST-001", "utc_seconds": T0 + 3602, "level_mm": 9998},
            {"station": "ST-004", "utc_seconds": T0, "level_mm": 42},
        ],
    )
    # v7: a second mark, on the clean ST-004 row.
    delta.isolate_range(table, "ST-004", T0, T0 + 10)
    assert delta.list_versions(table)[-1] == 7

    # v4 (pre-mark): everything available, no marks in effect.
    rows, excluded, snap = _available(table, version=4)
    assert (len(rows), excluded, len(snap.isolations)) == (24, 0, 0)

    # v5: the mark hides its 6 rows; the v6 append does not exist here and
    # the v7 mark must not leak into this version's query.
    rows, excluded, snap = _available(table, version=5)
    assert (len(rows), excluded, len(snap.isolations)) == (18, 6, 1)

    # v6: the late same-range rows are isolated too (marks apply whenever
    # rows arrived); the still-unmarked ST-004 row is available.
    rows, excluded, snap = _available(table, version=6)
    assert (len(rows), excluded, len(snap.isolations)) == (19, 8, 1)
    assert 9999 not in {r["level_mm"] for r in rows}
    assert _stations_at(rows, "ST-004") == [T0]

    # v7 (latest): the second mark takes the ST-004 row out as well.
    rows, excluded, snap = _available(table)
    assert (len(rows), excluded, len(snap.isolations)) == (18, 9, 2)

    # Raw snapshots remain complete at every version.
    assert delta.read_table(table, version=4).num_rows == 24
    assert delta.read_table(table).num_rows == 27


def test_available_query_on_empty_version(table: Path) -> None:
    rows, excluded, snap = _available(table, version=0)  # no data files yet
    assert (rows, excluded, snap.version) == ([], 0, 0)


# ---------------------------------------------------------------------------
# Overlapping marks and concurrent replicas
# ---------------------------------------------------------------------------


def test_overlapping_marks_count_each_row_once(table: Path) -> None:
    delta.isolate_range(table, "ST-001", T0, T0 + 3)  # covers T0,1,2
    delta.isolate_range(table, "ST-001", T0 + 2, T0 + 5)  # covers T0+2,3,4
    rows, excluded, snap = _available(table)
    assert len(snap.isolations) == 2
    # Union covers T0..T0+4 = 5 rows, not 3+3=6: no double counting.
    assert excluded == 5
    assert _stations_at(rows, "ST-001") == [T0 + 5] + [T0 + 3600 + i for i in range(6)]


def _worker_isolate(table_str: str, start: int, end: int, q) -> None:  # pragma: no cover
    try:
        q.put(("ok", delta.isolate_range(Path(table_str), "ST-001", start, end)))
    except delta.DeltaError as exc:
        q.put(("err", str(exc)))


def test_concurrent_marks_from_two_replicas_keep_full_coverage(table: Path) -> None:
    """Two replicas marking overlapping ranges: both marks land, union applies."""
    ctx = multiprocessing.get_context("spawn")
    q1, q2 = ctx.Queue(), ctx.Queue()
    p1 = ctx.Process(target=_worker_isolate, args=(str(table), T0, T0 + 3, q1))
    p2 = ctx.Process(target=_worker_isolate, args=(str(table), T0 + 1, T0 + 5, q2))
    p1.start(); p2.start()
    p1.join(30); p2.join(30)
    assert p1.exitcode == 0 and p2.exitcode == 0
    r1, r2 = q1.get(), q2.get()
    assert r1[0] == "ok" and r2[0] == "ok", f"lost a concurrent mark: {r1} {r2}"
    # Distinct versions, distinct mark ids, contiguous log.
    assert r1[1]["version"] != r2[1]["version"]
    assert r1[1]["mark_id"] != r2[1]["mark_id"]
    versions = delta.list_versions(table)
    assert versions == list(range(0, versions[-1] + 1))

    snap = delta.load_snapshot(table)
    assert len(snap.isolations) == 2  # no lost coverage
    rows, excluded, _ = _available(table)
    assert excluded == 5  # union [T0, T0+5), counted once per row
    assert _stations_at(rows, "ST-001") == [T0 + 5] + [T0 + 3600 + i for i in range(6)]


# ---------------------------------------------------------------------------
# Coexistence with compaction and external writers
# ---------------------------------------------------------------------------


def test_compaction_after_isolation_keeps_exclusion(table: Path) -> None:
    delta.isolate_range(table, "ST-002", T0, T0 + 6)  # whole ST-002 batch
    before_rows, before_excluded, _ = _available(table)
    assert before_excluded == 6

    chosen = sorted(delta.load_snapshot(table).active)[:2]
    result = delta.compact(table, chosen)  # commits as usual
    assert result["version"] == 6

    rows, excluded, snap = _available(table)
    assert snap.version == 6
    assert excluded == before_excluded
    key = lambda r: (r["station"], r["utc_seconds"], r["level_mm"])  # noqa: E731
    assert sorted(map(key, rows)) == sorted(map(key, before_rows))
    assert delta.read_table(table).num_rows == 24  # raw data intact


def test_external_writers_commit_normally_around_marks(table: Path) -> None:
    delta.isolate_range(table, "ST-001", T0, T0 + 6)
    info = append_rows(
        table, [{"station": "ST-009", "utc_seconds": T0, "level_mm": 1}]
    )
    assert info["version"] == 6
    victim = sorted(delta.load_snapshot(table).active)[0]
    gone = delete_files(table, [victim])
    assert gone["version"] == 7
    rows, excluded, snap = _available(table)
    assert snap.version == 7
    assert _stations_at(rows, "ST-009") == [T0]
    # The ST-001 mark still applies to whatever rows remain active.
    assert _stations_at(rows, "ST-001") == [T0 + 3600 + i for i in range(6)]


# ---------------------------------------------------------------------------
# Failure atomicity and restart behaviour
# ---------------------------------------------------------------------------


def test_failed_publish_exposes_no_partial_mark(
    table: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(*args, **kwargs):
        raise delta.DeltaError("simulated publish failure")

    monkeypatch.setattr(delta, "_commit_actions", boom)
    with pytest.raises(delta.DeltaError):
        delta.isolate_range(table, "ST-001", T0, T0 + 3)

    # Log, marks and query results are exactly as before the failed publish.
    assert delta.list_versions(table) == [0, 1, 2, 3, 4]
    assert delta.load_snapshot(table).isolations == []
    rows, excluded, _ = _available(table)
    assert (len(rows), excluded) == (24, 0)


def test_inflight_tmp_log_with_mark_is_invisible(table: Path) -> None:
    # A crashed publisher can leave its prepared .tmp-* log behind; readers
    # must never treat it as a committed mark.
    tmp = table / "_delta_log" / ".tmp-deadbeef-00000000000000000005.json"
    tmp.write_text(
        '{"isolate":{"id":"x","station":"ST-001","startUtcSeconds":0,'
        '"endUtcSeconds":99999999999,"timestamp":0,"dataChange":false}}\n'
    )
    try:
        assert delta.list_versions(table) == [0, 1, 2, 3, 4]
        assert delta.load_snapshot(table).isolations == []
        rows, excluded, _ = _available(table)
        assert (len(rows), excluded) == (24, 0)
    finally:
        tmp.unlink()


def _worker_read_available(table_str: str, version: int | None, q) -> None:  # pragma: no cover
    rows, excluded, snap = _available(Path(table_str), version)
    q.put((len(rows), excluded, len(snap.isolations)))


def test_marks_survive_restart_and_old_versions_stay_pinned(table: Path) -> None:
    delta.isolate_range(table, "ST-001", T0, T0 + 6)
    expected = {}
    for v in (4, 5, None):
        rows, excluded, snap = _available(table, v)
        expected[v] = (len(rows), excluded, len(snap.isolations))
    assert expected[4] == (24, 0, 0)
    assert expected[5] == (18, 6, 1)
    assert expected[None] == expected[5]

    # A brand-new process (i.e. after a restart) replays the log and sees
    # exactly the same per-version results.
    ctx = multiprocessing.get_context("spawn")
    for v in (4, 5, None):
        q = ctx.Queue()
        p = ctx.Process(target=_worker_read_available, args=(str(table), v, q))
        p.start()
        p.join(30)
        assert p.exitcode == 0
        assert q.get() == expected[v]


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------


@pytest.fixture()
def client(tmp_path: Path, monkeypatch) -> TestClient:
    data_root = tmp_path / "data"
    monkeypatch.setenv("DATA_ROOT", str(data_root))
    monkeypatch.setenv("TABLE_NAME", "water_level")
    import app.main as main

    monkeypatch.setattr(main, "DATA_ROOT", data_root)
    monkeypatch.setattr(main, "TABLE_DIR", data_root / "water_level")
    with TestClient(main.app) as c:
        yield c


def test_api_isolation_flow(client: TestClient) -> None:
    r = client.post(
        "/isolation",
        json={
            "station": "ST-001",
            "start_utc_seconds": T0 + 3600,
            "end_utc_seconds": T0 + 3606,
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["committed_version"] == 5
    assert body["station"] == "ST-001" and body["mark_id"]

    marks = client.get("/isolation").json()
    assert marks["mark_count"] == 1
    assert marks["marks"][0]["start_utc_seconds"] == T0 + 3600
    # Marks are versioned: the pre-mark version has none.
    assert client.get("/isolation", params={"version": 4}).json()["mark_count"] == 0

    avail = client.get("/observations/available").json()
    assert avail["version"] == 5
    assert avail["available_count"] == 18
    assert avail["excluded_count"] == 6
    assert avail["isolation_mark_count"] == 1
    assert all(
        not (r["station"] == "ST-001" and T0 + 3600 <= r["utc_seconds"] < T0 + 3606)
        for r in avail["rows"]
    )

    # Time travel on the available query and on the raw snapshot.
    old = client.get("/observations/available", params={"version": 4}).json()
    assert (old["available_count"], old["excluded_count"]) == (24, 0)
    raw = client.get("/table/snapshot").json()
    assert raw["row_count"] == 24  # raw values preserved for review


def test_api_isolation_validation_and_version_errors(client: TestClient) -> None:
    r = client.post(
        "/isolation",
        json={"station": "ST-001", "start_utc_seconds": 10, "end_utc_seconds": 10},
    )
    assert r.status_code == 400  # empty [start, end) interval
    r = client.post(
        "/isolation",
        json={"station": "", "start_utc_seconds": 0, "end_utc_seconds": 1},
    )
    assert r.status_code == 422  # pydantic min_length on station
    r = client.get("/observations/available", params={"version": 99})
    assert r.status_code == 404
    r = client.get("/isolation", params={"version": 99})
    assert r.status_code == 404
