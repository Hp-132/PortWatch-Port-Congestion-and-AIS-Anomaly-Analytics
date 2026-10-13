from src.analytics.turnaround.port_visits import build_visits

H = 3600
SCHEMA = "mmsi long, epoch long, port_id string, state string"
WINDOW = (0, 7 * 24 * H)


def _visits(spark, rows, **kw):
    pts = spark.createDataFrame(rows, SCHEMA)
    _, v = build_visits(pts, visit_break_hours=kw.get("brk", 12), min_visit_minutes=30,
                        window_start=WINDOW[0], window_end=WINDOW[1], censor_margin_hours=2)
    return sorted(v.collect(), key=lambda r: (r["mmsi"], r["arrival_epoch"]))


def test_arrival_anchor_berth_departure(spark):
    t0 = 10 * H
    rows = [(1, t0, "P", "MOVING"),
            (1, t0 + 1 * H, "P", "ANCHOR"), (1, t0 + 2 * H, "P", "ANCHOR"), (1, t0 + 3 * H, "P", "ANCHOR"),
            (1, t0 + 4 * H, "P", "BERTH"), (1, t0 + 10 * H, "P", "BERTH"),
            (1, t0 + 11 * H, "P", "MOVING")]
    (v,) = _visits(spark, rows)
    assert v["visit_type"] == "BERTHED"
    assert v["waiting_h"] == 3.0          # three 1h anchor intervals before first berth fix
    assert v["dwell_h"] == 6.0            # berth_start -> berth_end
    assert v["turnaround_h"] == 11.0      # arrival -> departure
    assert v["complete"] is True


def test_new_visit_on_port_change_and_long_absence(spark):
    rows = [(1, 10 * H, "A", "BERTH"), (1, 11 * H, "A", "BERTH"),
            (1, 15 * H, "B", "BERTH"), (1, 16 * H, "B", "BERTH"),
            (1, 40 * H, "B", "BERTH"), (1, 41 * H, "B", "BERTH")]   # 24h absence -> new visit
    v = _visits(spark, rows)
    assert [r["port_id"] for r in v] == ["A", "B", "B"]


def test_censoring_and_pass_through(spark):
    rows = [(1, 0, "P", "BERTH"), (1, 5 * H, "P", "BERTH"),           # starts at window start
            (2, 50 * H, "P", "MOVING"), (2, 50 * H + 600, "P", "MOVING")]  # 10 min transit
    v = {r["mmsi"]: r for r in _visits(spark, rows)}
    assert v[1]["censored_arrival"] is True and v[1]["complete"] is False
    assert v[2]["visit_type"] == "PASS_THROUGH"


def test_anchor_interval_capped_at_3h(spark):
    rows = [(1, 20 * H, "P", "ANCHOR"), (1, 30 * H, "P", "BERTH"), (1, 31 * H, "P", "BERTH")]
    (v,) = _visits(spark, rows)
    assert v["waiting_h"] == 3.0          # 10h reporting gap is not counted as waiting


def test_short_berth_blip_does_not_start_berth_phase(spark):
    # Vessel stops for 6 min in the channel, anchors 12 h, then berths 3 days.
    t0 = 10 * H
    rows = [(1, t0, "P", "MOVING"), (1, t0 + 60, "P", "BERTH"), (1, t0 + 360, "P", "BERTH"),
            *[(1, t0 + 420 + i * H, "P", "ANCHOR") for i in range(12)],
            *[(1, t0 + 13 * H + i * H, "P", "BERTH") for i in range(73)],
            (1, t0 + 87 * H, "P", "MOVING")]
    (v,) = _visits(spark, rows)
    assert v["berth_start_epoch"] == t0 + 13 * H
    assert v["dwell_h"] == 72.0
    assert v["waiting_h"] > 11.5
