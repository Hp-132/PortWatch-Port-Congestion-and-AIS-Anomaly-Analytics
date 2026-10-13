"""API tests against the real MongoDB serving layer (skipped if it is not loaded)."""
import re

import pytest

pymongo = pytest.importorskip("pymongo")
from fastapi.testclient import TestClient  # noqa: E402

from dashboard.backend.app import app  # noqa: E402
from dashboard.backend.db import get_db  # noqa: E402


def _mongo_ready():
    try:
        db = get_db()
        db.command("ping")
        return db.vessels.estimated_document_count() > 0
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _mongo_ready(), reason="MongoDB serving layer not loaded")
client = TestClient(app)


def get(path, **params):
    r = client.get(path, params=params)
    assert r.status_code == 200, (path, r.status_code, r.text[:300])
    return r.json()


def test_overview_kpis_are_real_counts():
    k = get("/api/overview")["kpis"]
    assert k["raw_rows"] > 40_000_000 and k["clean_rows"] <= k["raw_rows"]
    assert k["vessels"] > 20_000 and k["ports_monitored"] >= 30
    assert 0 < k["high_priority_gaps"] <= k["elevated_gaps"] <= k["ais_gaps"]
    assert k["avg_waiting_h"] < k["avg_turnaround_h"]


@pytest.mark.parametrize("page", ["overview", "ports", "vessels", "anomalies", "trends"])
def test_insights_are_generated_from_data(page):
    ins = get(f"/api/insights/{page}")
    assert ins, page
    for key, text in ins.items():
        assert isinstance(text, str) and len(text) > 20, key
        assert not re.search(r"\bNone\b|\bnan\b|\{|\}", text), (key, text)


def test_unknown_insight_page_404():
    assert client.get("/api/insights/nope").status_code == 404


def test_port_detail_and_insights():
    ports = get("/api/ports")
    top = ports[0]
    assert top["pci"] == max(p["pci"] for p in ports if p.get("pci") is not None)
    d = get(f"/api/ports/{top['port_id']}")
    assert d["daily"] and d["port_id"] == top["port_id"]
    ins = get(f"/api/ports/{top['port_id']}/insights")
    assert top["port_name"] in ins["summary"]


def test_positions_have_zone_state_and_counts():
    meta = get("/api/meta")
    t = meta["window_start_epoch"] + 36 * 3600
    fc = get("/api/positions", t=t, window_hours=3)
    assert fc["count"] > 1000 and fc["count"] == len(fc["features"])
    assert set(fc["counts"]) == {"waiting", "at_berth", "in_port_area"}
    waiting = get("/api/positions", t=t, window_hours=3, waiting_only=True)
    assert waiting["count"] == fc["counts"]["waiting"]
    assert all(f["properties"]["w"] == 1 for f in waiting["features"])


def test_anomaly_filters():
    rows = get("/api/anomalies", level="High Priority", limit=100)
    assert rows and all(r["risk_level"] == "High Priority" and r["risk_score"] > 80 for r in rows)
    long_gaps = get("/api/anomalies", min_gap_h=24, limit=200)
    assert all(r["gap_h"] >= 24 for r in long_gaps)
    offshore = get("/api/anomalies", proximity="offshore", limit=200)
    assert all(r["start_dist_to_port_nm"] > 50 for r in offshore)
    one = rows[0]
    by_vessel = get("/api/anomalies", vessel=str(one["mmsi"]))
    assert {r["mmsi"] for r in by_vessel} == {one["mmsi"]}


def test_anomaly_detail_and_vessel_ranking():
    top = get("/api/anomalies", limit=1)[0]
    d = get(f"/api/anomalies/gap/{top['gap_id']}")
    for f in ("start_lat", "end_lat", "gap_h", "dist_nm", "implied_kn", "start_dist_to_port_nm", "risk_score"):
        assert f in d
    assert d["vessel"]["mmsi"] == top["mmsi"]
    ranked = get("/api/anomalies/vessels", limit=10)
    assert ranked == sorted(ranked, key=lambda r: (-r["max_score"], -r["elevated_gaps"]))


def test_vessel_detail_track_and_context():
    v = get("/api/vessels", vessel_group="Cargo", min_visits=2, sort="port_visits", limit=1)[0]
    d = get(f"/api/vessels/{v['mmsi']}")
    assert d["visits"] and "port_context" in d
    tr = get(f"/api/vessels/{v['mmsi']}/track")
    epochs = [p[0] for p in tr["points"]]
    assert epochs == sorted(epochs) and len(epochs) > 10


def test_trends_include_visit_flow():
    t = get("/api/trends")
    assert len(t["daily"]) == 7 and len(t["hourly"]) == 168
    assert t["visits"] and all("avg_waiting_h" in r for r in t["visits"])
