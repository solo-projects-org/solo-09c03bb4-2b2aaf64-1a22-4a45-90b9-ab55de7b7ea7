"""Demo 4 - isolate an instrument-outage window from forecast reads.

Flow (replica #1 marks, replica #2 queries - the shared log synchronises):

1. an external process appends observations for a station, some inside the
   outage window reported by the data operator
2. POST /isolation marks [start, end) for that station as one atomic log
   commit - parquet files are never rewritten
3. GET /observations/available hides the in-window rows and reports the
   excluded count; the raw snapshot keeps every value for review
4. late-arriving rows inside the same window are hidden too: marks apply
   whenever the rows were appended
5. time travel: the pre-mark version still shows every row

Usage:
    python scripts/demo_isolation.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts._demo_common import API_1, API_2, TABLE_DIR, banner, api  # noqa: E402

STATION = "ST-DOWN"
OUTAGE_START = 1_732_000_000  # fixed UTC window reported by the operator
OUTAGE_END = 1_732_003_600  # one hour later (end exclusive)


def _rows(station: str, t0: int, count: int, level0: int) -> list[dict]:
    return [
        {"station": station, "utc_seconds": t0 + i * 60, "level_mm": level0 + i}
        for i in range(count)
    ]


def _append(rows: list[dict]) -> int:
    from app.external_writer import append_rows

    return append_rows(TABLE_DIR, rows)["version"]


def _available(client, version: int | None = None) -> dict:
    params = {"version": version} if version is not None else {}
    r = client.get("/observations/available", params=params)
    r.raise_for_status()
    return r.json()


def main() -> None:
    writer, reader = api(API_1), api(API_2)
    banner("Demo 4: isolate an instrument-outage window (atomic log marks)")

    pre_mark_version = writer.get("/table/versions").json()["latest_version"]
    before = _available(reader)
    print(
        f"latest v{pre_mark_version}: available={before['available_count']}, "
        f"excluded={before['excluded_count']}, marks={before['isolation_mark_count']}"
    )

    print(f"\nexternal writer appends 3 in-window + 1 outside row for {STATION}")
    v = _append(_rows(STATION, OUTAGE_START, 3, 3000) + _rows(STATION, OUTAGE_END + 60, 1, 3100))
    print(f"    [external-writer] append committed at v{v}")

    banner(f"POST /isolation on {API_1}: {STATION} [{OUTAGE_START}, {OUTAGE_END})")
    r = writer.post(
        "/isolation",
        json={
            "station": STATION,
            "start_utc_seconds": OUTAGE_START,
            "end_utc_seconds": OUTAGE_END,
        },
    )
    r.raise_for_status()
    mark = r.json()
    print(f"mark {mark['mark_id'][:12]}… committed at v{mark['committed_version']}")

    after = _available(reader)
    print(
        f"\nGET /observations/available on {API_2}: "
        f"available={after['available_count']}, excluded={after['excluded_count']}"
    )
    assert after["excluded_count"] == before["excluded_count"] + 3
    assert after["available_count"] == before["available_count"] + 1  # outside row
    assert all(
        not (row["station"] == STATION and OUTAGE_START <= row["utc_seconds"] < OUTAGE_END)
        for row in after["rows"]
    )

    banner("Late backfilled rows in the same window stay out of forecast reads")
    v = _append(_rows(STATION, OUTAGE_START + 180, 2, 3200))
    print(f"    [external-writer] late append committed at v{v}")
    late = _available(reader)
    print(f"available={late['available_count']}, excluded={late['excluded_count']}")
    assert late["excluded_count"] == after["excluded_count"] + 2
    assert late["available_count"] == after["available_count"]

    banner("Raw snapshot and old versions are untouched")
    raw = reader.get("/table/snapshot").json()
    print(f"raw snapshot rows={raw['row_count']} (every value kept for review)")
    assert raw["row_count"] == late["available_count"] + late["excluded_count"]
    old = _available(reader, version=pre_mark_version)
    print(
        f"time travel v{pre_mark_version}: available={old['available_count']}, "
        f"excluded={old['excluded_count']} (pre-mark results unchanged)"
    )
    assert old["excluded_count"] == before["excluded_count"]

    marks = reader.get("/isolation").json()
    ours = [m for m in marks["marks"] if m["station"] == STATION]
    assert ours, "mark must be listed"
    print(f"\nGET /isolation lists {marks['mark_count']} mark(s); ours: {ours[0]}")
    print("\nOK: outage window isolated atomically, raw values preserved, "
          "late rows covered, old versions pinned.")


if __name__ == "__main__":
    main()
