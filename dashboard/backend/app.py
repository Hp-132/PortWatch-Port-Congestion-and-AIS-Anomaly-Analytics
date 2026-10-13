"""MARITIME-X dashboard API (FastAPI).

Serves pre-computed analytical results from MongoDB. It never touches the raw
AIS data: all heavy processing happened in Spark.

Run::  uvicorn dashboard.backend.app:app --port 8000
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pymongo import ASCENDING, DESCENDING

from . import insights
from .db import get_db

FRONTEND = Path(__file__).resolve().parents[1] / "frontend"
NO_ID = {"_id": 0}
RISK_LEVELS = ["Normal", "Watch", "Suspicious", "High Priority"]

app = FastAPI(title="MARITIME-X API", version="1.0",
              description="Maritime Port Intelligence - results served from MongoDB")
app.add_middleware(GZipMiddleware, minimum_size=1024)


def _csv(v: Optional[str]) -> Optional[list[str]]:
    return [x for x in v.split(",") if x] if v else None


# ---------------------------------------------------------------- health / meta
@app.get("/api/health")
def health():
    db = get_db()
    db.command("ping")
    return {"status": "ok", "database": db.name,
            "collections": {c: db[c].estimated_document_count() for c in sorted(db.list_collection_names())}}


@app.get("/api/pipeline")
def pipeline():
    return {d["stage"]: d for d in get_db().pipeline_metrics.find({}, NO_ID)}


@app.get("/api/insights/{page}")
def page_insights(page: str):
    """Insight sentences generated from the stored results (see insights.py)."""
    if page not in insights.PAGES:
        raise HTTPException(404, f"unknown page; choose from {sorted(insights.PAGES)}")
    return insights.PAGES[page](get_db())


@app.get("/api/meta")
def meta():
    db = get_db()
    clean = db.pipeline_metrics.find_one({"stage": "clean_ais"}, NO_ID) or {}
    groups = sorted(g for g in db.vessels.distinct("vessel_group") if g)
    ports = list(db.ports.find({}, {"_id": 0, "port_id": 1, "port_name": 1}).sort("port_name", ASCENDING))
    return {"window_start_epoch": clean.get("window_start_epoch"), "window_end_epoch": clean.get("window_end_epoch"),
            "vessel_groups": groups, "ports": ports, "risk_levels": RISK_LEVELS,
            "pci_weights": insights.PCI_WEIGHTS, "risk_weights": insights.FACTOR_WEIGHTS}


# ---------------------------------------------------------------- overview
@app.get("/api/overview")
def overview():
    db = get_db()
    m = {d["stage"]: d for d in db.pipeline_metrics.find({}, NO_ID)}
    clean, ident = m.get("clean_ais", {}), m.get("identity", {})
    overall = db.turnaround_stats.find_one({"port_id": "ALL", "vessel_group": "ALL"}, NO_ID) or {}
    risk = {r["_id"]: r["n"] for r in db.anomalies.aggregate(
        [{"$group": {"_id": "$risk_level", "n": {"$sum": 1}}}])}
    groups = list(db.vessels.aggregate([
        {"$group": {"_id": "$vessel_group", "vessels": {"$sum": 1}}}, {"$sort": {"vessels": -1}}]))
    ports = list(db.ports.find({}, {"_id": 0, "port_id": 1, "port_name": 1, "pci": 1, "pci_band": 1,
                                    "avg_queue": 1, "arrivals": 1, "avg_waiting_h": 1, "utilization": 1})
                 .sort("pci", DESCENDING))
    return {
        "kpis": {
            "raw_rows": clean.get("raw_rows"), "clean_rows": clean.get("clean_rows"),
            "vessels": clean.get("vessels"), "ports_monitored": len(ports),
            "port_visits": overall.get("visits"), "avg_turnaround_h": overall.get("avg_turnaround_h"),
            "avg_waiting_h": overall.get("avg_waiting_h"), "avg_dwell_h": overall.get("avg_dwell_h"),
            "ais_gaps": sum(risk.values()), "high_priority_gaps": risk.get("High Priority", 0),
            "elevated_gaps": risk.get("High Priority", 0) + risk.get("Suspicious", 0),
            "anomaly_vessels": len(db.anomalies.distinct("mmsi", {"risk_score": {"$gt": 60}})),
            "suspicious_gaps": risk.get("Suspicious", 0),
            "identity_flagged": ident.get("vessels_flagged"),
            "window_start_epoch": clean.get("window_start_epoch"), "window_end_epoch": clean.get("window_end_epoch"),
        },
        "risk_by_level": [{"level": lv, "count": risk.get(lv, 0)} for lv in RISK_LEVELS],
        "vessels_by_group": [{"group": g["_id"], "vessels": g["vessels"]} for g in groups],
        "ports": ports,
    }


# ---------------------------------------------------------------- ports / congestion
@app.get("/api/ports")
def ports():
    return list(get_db().ports.find({}, NO_ID).sort("pci", DESCENDING))


@app.get("/api/ports/{port_id}")
def port_detail(port_id: str):
    db = get_db()
    p = db.ports.find_one({"port_id": port_id}, NO_ID)
    if not p:
        raise HTTPException(404, "port not found")
    p["turnaround_by_group"] = list(db.turnaround_stats.find(
        {"port_id": port_id, "vessel_group": {"$ne": "ALL"}}, NO_ID).sort("visits", DESCENDING))
    p["daily"] = list(db.congestion_daily.find({"port_id": port_id}, NO_ID).sort("date", ASCENDING))
    return p


@app.get("/api/ports/{port_id}/insights")
def port_insights(port_id: str):
    return insights.port_detail(get_db(), port_id)


@app.get("/api/congestion/hourly")
def congestion_hourly(port_id: Optional[str] = None, start: Optional[int] = None, end: Optional[int] = None):
    q: dict = {}
    if port_id:
        q["port_id"] = {"$in": _csv(port_id)}
    if start or end:
        q["hour_epoch"] = {k: v for k, v in (("$gte", start), ("$lte", end)) if v is not None}
    proj = {"_id": 0, "port_id": 1, "hour_epoch": 1, "in_area": 1, "at_berth": 1, "queue": 1,
            "arrivals": 1, "departures": 1}
    return list(get_db().congestion_hourly.find(q, proj).sort([("port_id", 1), ("hour_epoch", 1)]))


@app.get("/api/congestion/daily")
def congestion_daily(port_id: Optional[str] = None):
    q = {"port_id": {"$in": _csv(port_id)}} if port_id else {}
    return list(get_db().congestion_daily.find(q, NO_ID).sort([("port_id", 1), ("date", 1)]))


# ---------------------------------------------------------------- turnaround / visits
@app.get("/api/turnaround/stats")
def turnaround_stats(port_id: Optional[str] = None):
    q = {"port_id": port_id} if port_id else {}
    return list(get_db().turnaround_stats.find(q, NO_ID))


@app.get("/api/visits")
def visits(port_id: Optional[str] = None, vessel_group: Optional[str] = None,
           visit_type: Optional[str] = None, complete_only: bool = False,
           sort: str = "arrival_epoch", order: int = -1, limit: int = Query(200, le=2000)):
    q: dict = {}
    if port_id:
        q["port_id"] = port_id
    if vessel_group:
        q["vessel_group"] = {"$in": _csv(vessel_group)}
    if visit_type:
        q["visit_type"] = visit_type
    if complete_only:
        q["complete"] = True
    allowed = {"arrival_epoch", "turnaround_h", "waiting_h", "dwell_h", "departure_epoch"}
    sort = sort if sort in allowed else "arrival_epoch"
    return list(get_db().port_visits.find(q, NO_ID).sort(sort, DESCENDING if order < 0 else ASCENDING).limit(limit))


# ---------------------------------------------------------------- vessels
@app.get("/api/vessels/search")
def vessel_search(q: str = Query(..., min_length=2), limit: int = 15):
    db = get_db()
    proj = {"_id": 0, "mmsi": 1, "vessel_name": 1, "vessel_group": 1, "imo": 1, "last_lat": 1, "last_lon": 1,
            "max_risk_score": 1, "flagged": 1}
    if q.isdigit():
        lo, hi = int(q.ljust(9, "0")), int(q.ljust(9, "9"))
        cur = db.vessels.find({"mmsi": {"$gte": lo, "$lte": hi}}, proj).limit(limit)
    else:
        cur = db.vessels.find({"vessel_name": {"$regex": "^" + re.escape(q.upper())}}, proj).limit(limit)
    return list(cur)


@app.get("/api/vessels")
def vessels(vessel_group: Optional[str] = None, risk_level: Optional[str] = None, flagged: Optional[bool] = None,
            min_visits: int = 0, sort: str = "max_risk_score", limit: int = Query(200, le=2000)):
    q: dict = {}
    if vessel_group:
        q["vessel_group"] = {"$in": _csv(vessel_group)}
    if risk_level:
        q["vessel_risk_level"] = {"$in": _csv(risk_level)}
    if flagged is not None:
        q["flagged"] = flagged
    if min_visits:
        q["port_visits"] = {"$gte": min_visits}
    allowed = {"max_risk_score", "distance_nm", "port_visits", "ais_gaps", "avg_turnaround_h", "fixes"}
    sort = sort if sort in allowed else "max_risk_score"
    return list(get_db().vessels.find(q, {"_id": 0, "location": 0}).sort(sort, DESCENDING).limit(limit))


@app.get("/api/vessels/{mmsi}")
def vessel_detail(mmsi: int):
    db = get_db()
    v = db.vessels.find_one({"mmsi": mmsi}, {"_id": 0, "location": 0})
    if not v:
        raise HTTPException(404, "vessel not found")
    v["visits"] = list(db.port_visits.find({"mmsi": mmsi}, NO_ID).sort("arrival_epoch", ASCENDING))
    v["gaps"] = list(db.anomalies.find({"mmsi": mmsi}, {"_id": 0, "start_location": 0, "end_location": 0})
                     .sort("start_epoch", ASCENDING))
    v["identity"] = db.identity_vessels.find_one({"mmsi": mmsi}, NO_ID)
    v["port_context"] = _port_context(db, v)
    v["identity_events"] = list(db.identity_events.find({"mmsi": mmsi}, NO_ID).sort("epoch", ASCENDING).limit(200))
    return v


def _port_context(db, v: dict) -> dict:
    """Current port area (from the vessel's latest hourly position) and its previous port visit."""
    names = {p["port_id"]: p["port_name"] for p in db.ports.find({}, {"_id": 0, "port_id": 1, "port_name": 1})}
    last = db.vessel_positions.find_one({"mmsi": v["mmsi"]}, {"_id": 0}, sort=[("t", -1)])
    ctx = {"current_port": None, "current_state": None, "previous_port": None}
    if last and last.get("p"):
        ctx.update(current_port=names.get(last["p"], last["p"]), current_state=last.get("z"))
    visits = v.get("visits") or []
    if ctx["current_port"] and visits and visits[-1].get("port_name") == ctx["current_port"]:
        visits = visits[:-1]          # the ongoing visit is the current port, not the previous one
    if visits:
        p = visits[-1]
        ctx["previous_port"] = {"port": p.get("port_name"), "arrival_ts": p.get("arrival_ts"),
                                "departure_ts": p.get("departure_ts"), "visit_type": p.get("visit_type")}
    return ctx


@app.get("/api/vessels/{mmsi}/track")
def vessel_track(mmsi: int, start: Optional[int] = None, end: Optional[int] = None):
    doc = get_db().vessel_tracks.find_one({"mmsi": mmsi}, NO_ID)
    if not doc:
        raise HTTPException(404, "no track")
    pts = [p for p in doc["points"] if (start is None or p[0] >= start) and (end is None or p[0] <= end)]
    return {"mmsi": mmsi, "vessel_group": doc.get("vessel_group"),
            "points": [[int(p[0]), p[1], p[2], (None if p[3] < 0 else p[3])] for p in pts]}


# ---------------------------------------------------------------- map positions
@app.get("/api/positions")
def positions(t: int, groups: Optional[str] = None, window_hours: int = Query(1, ge=1, le=24),
              port_id: Optional[str] = None, waiting_only: bool = False):
    """Vessel positions for the hour bucket containing ``t`` (UTC epoch).

    ``window_hours`` > 1 returns each vessel's latest fix within the previous
    N hours, which fills the map for vessels that report infrequently. Each
    feature carries its port-zone state (``z``: BERTH / ANCHOR / MOVING inside a
    monitored port area) and ``w`` = 1 for commercial vessels waiting at anchor.
    """
    db = get_db()
    bucket = (t // 3600) * 3600
    match: dict = {"t": bucket} if window_hours == 1 else {"t": {"$gt": bucket - window_hours * 3600, "$lte": bucket}}
    if groups:
        match["g"] = {"$in": _csv(groups)}
    if port_id:
        match["p"] = port_id
    # one indexed range scan (t, g); keep each vessel's latest fix in the window
    latest: dict = {}
    for r in db.vessel_positions.find(match, {"_id": 0}).sort("t", ASCENDING):
        latest[r["mmsi"]] = r
    rows = list(latest.values())
    if waiting_only:   # waiting = the vessel's latest fix in the window is at anchor, not any earlier fix
        rows = [r for r in rows if r.get("w") == 1]
    risk = {}
    missing = [r["mmsi"] for r in rows if "n" not in r]
    if missing:   # older loads without the denormalised name / risk fields
        risk = {d["mmsi"]: d for d in db.vessels.find(
            {"mmsi": {"$in": missing}}, {"_id": 0, "mmsi": 1, "vessel_name": 1, "max_risk_score": 1, "flagged": 1})}
    feats, counts = [], {"waiting": 0, "at_berth": 0, "in_port_area": 0}
    for r in rows:
        v = risk.get(r["mmsi"]) or {"vessel_name": r.get("n"), "max_risk_score": r.get("r", 0), "flagged": r.get("f")}
        z = r.get("z")
        if r.get("w"):
            counts["waiting"] += 1
        if z == "BERTH" and r.get("g") in ("Cargo", "Tanker"):
            counts["at_berth"] += 1
        if r.get("p"):
            counts["in_port_area"] += 1
        feats.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [r["x"], r["y"]]},
                      "properties": {"mmsi": r["mmsi"], "g": r.get("g"), "s": r.get("s"), "c": r.get("c"),
                                     "st": r.get("st"), "t": r["t"], "p": r.get("p"), "z": z, "w": r.get("w", 0),
                                     "name": v.get("vessel_name"), "risk": v.get("max_risk_score", 0),
                                     "flag": bool(v.get("flagged"))}})
    return {"type": "FeatureCollection", "bucket": bucket, "count": len(feats), "counts": counts, "features": feats}


# ---------------------------------------------------------------- anomalies
def _anomaly_query(level, group, port, min_score, start, end, mmsi=None, min_gap_h=None, max_gap_h=None,
                   proximity=None, day=None, vessel=None):
    q: dict = {}
    if min_gap_h or max_gap_h:
        q["gap_h"] = {k: v for k, v in (("$gte", min_gap_h), ("$lte", max_gap_h)) if v}
    # port proximity of the gap start: near (<= 10 nm), coastal (10-50 nm), offshore (> 50 nm)
    if proximity == "near":
        q["start_dist_to_port_nm"] = {"$lte": 10}
    elif proximity == "coastal":
        q["start_dist_to_port_nm"] = {"$gt": 10, "$lte": 50}
    elif proximity == "offshore":
        q["start_dist_to_port_nm"] = {"$gt": 50}
    if day:
        q["event_date"] = day
    if vessel:
        if vessel.isdigit():
            q["mmsi"] = int(vessel)
        else:
            ids = get_db().vessels.distinct("mmsi", {"vessel_name": {"$regex": re.escape(vessel.upper())}})
            q["mmsi"] = {"$in": ids[:500]}
    if level:
        q["risk_level"] = {"$in": _csv(level)}
    if group:
        q["vessel_group"] = {"$in": _csv(group)}
    if port:
        q["start_nearest_port"] = port
    if min_score:
        q["risk_score"] = {"$gte": min_score}
    if start or end:
        q["start_epoch"] = {k: v for k, v in (("$gte", start), ("$lte", end)) if v is not None}
    if mmsi:
        q["mmsi"] = mmsi
    return q


@app.get("/api/anomalies")
def anomalies(level: Optional[str] = None, group: Optional[str] = None, port: Optional[str] = None,
              min_score: int = 0, start: Optional[int] = None, end: Optional[int] = None,
              mmsi: Optional[int] = None, min_gap_h: Optional[float] = None, max_gap_h: Optional[float] = None,
              proximity: Optional[str] = None, day: Optional[str] = None, vessel: Optional[str] = None,
              fields: Optional[str] = None, limit: int = Query(500, le=5000)):
    q = _anomaly_query(level, group, port, min_score, start, end, mmsi, min_gap_h, max_gap_h, proximity, day, vessel)
    proj = {"_id": 0, "start_location": 0, "end_location": 0, "label": 0}
    if fields == "map":   # lean payload for map layers
        proj = {"_id": 0, **{k: 1 for k in ("gap_id", "mmsi", "risk_level", "risk_score", "gap_h", "start_epoch",
                                            "start_lat", "start_lon", "end_lat", "end_lon", "dist_nm", "vessel_group")}}
    rows = list(get_db().anomalies.find(q, proj).sort("risk_score", DESCENDING).limit(limit))
    names = {d["mmsi"]: d.get("vessel_name") for d in get_db().vessels.find(
        {"mmsi": {"$in": list({r["mmsi"] for r in rows})}}, {"_id": 0, "mmsi": 1, "vessel_name": 1})}
    for r in rows:
        r["vessel_name"] = names.get(r["mmsi"])
    return rows


@app.get("/api/anomalies/vessels")
def anomaly_vessels(limit: int = Query(25, le=200)):
    """Vessels ranked by their elevated (score > 60) AIS gaps."""
    db = get_db()
    rows = list(db.anomalies.aggregate([
        {"$match": {"risk_score": {"$gt": 60}}},
        {"$group": {"_id": "$mmsi", "elevated_gaps": {"$sum": 1}, "max_score": {"$max": "$risk_score"},
                    "high_priority": {"$sum": {"$cond": [{"$eq": ["$risk_level", "High Priority"]}, 1, 0]}},
                    "total_gap_h": {"$sum": "$gap_h"}, "group": {"$first": "$vessel_group"},
                    "transceiver": {"$first": "$transceiver"}}},
        {"$sort": {"max_score": -1, "elevated_gaps": -1}}, {"$limit": limit}]))
    info = {d["mmsi"]: d for d in db.vessels.find({"mmsi": {"$in": [r["_id"] for r in rows]}},
                                                  {"_id": 0, "mmsi": 1, "vessel_name": 1, "ais_gaps": 1, "flagged": 1})}
    return [{"mmsi": r["_id"], "vessel_name": info.get(r["_id"], {}).get("vessel_name"),
             "vessel_group": r["group"], "transceiver": r["transceiver"], "elevated_gaps": r["elevated_gaps"],
             "high_priority": r["high_priority"], "max_score": r["max_score"],
             "total_gap_h": round(r["total_gap_h"], 1), "all_gaps": info.get(r["_id"], {}).get("ais_gaps"),
             "identity_flag": bool(info.get(r["_id"], {}).get("flagged"))} for r in rows]


@app.get("/api/anomalies/gap/{gap_id}")
def anomaly_detail(gap_id: str):
    """One AIS gap with its score factors, vessel context and AIS visibility event."""
    db = get_db()
    g = db.anomalies.find_one({"gap_id": gap_id}, {"_id": 0, "start_location": 0, "end_location": 0})
    if not g:
        raise HTTPException(404, "gap not found")
    g["vessel"] = db.vessels.find_one({"mmsi": g["mmsi"]}, {"_id": 0, "location": 0})
    names = {p["port_id"]: p["port_name"] for p in db.ports.find({}, {"_id": 0, "port_id": 1, "port_name": 1})}
    g["start_port_name"] = names.get(g.get("start_nearest_port"))
    g["end_port_name"] = names.get(g.get("end_nearest_port"))
    vis = db.ais_visibility.find_one({"gap_id": gap_id}, {"_id": 0})
    if vis and vis.get("neighbours"):
        nb_names = {d["mmsi"]: d.get("vessel_name") for d in db.vessels.find(
            {"mmsi": {"$in": [n["nb_mmsi"] for n in vis["neighbours"]]}}, {"_id": 0, "mmsi": 1, "vessel_name": 1})}
        for n in vis["neighbours"]:
            n["vessel_name"] = nb_names.get(n["nb_mmsi"])
    g["visibility"] = vis
    return g


@app.get("/api/visibility/summary")
def visibility_summary():
    rows = list(get_db().ais_visibility.aggregate([{"$group": {
        "_id": {"v": "$visibility", "l": "$risk_level"}, "n": {"$sum": 1}}}]))
    return [{"visibility": r["_id"]["v"], "risk_level": r["_id"]["l"], "gaps": r["n"]} for r in rows]


@app.get("/api/anomalies/summary")
def anomalies_summary(group: Optional[str] = None, start: Optional[int] = None, end: Optional[int] = None):
    db = get_db()
    q = _anomaly_query(None, group, None, 0, start, end)
    by_level = {r["_id"]: r["n"] for r in db.anomalies.aggregate(
        [{"$match": q}, {"$group": {"_id": "$risk_level", "n": {"$sum": 1}}}])}
    hist = list(db.anomalies.aggregate([
        {"$match": q}, {"$bucket": {"groupBy": "$risk_score", "boundaries": list(range(0, 101, 10)) + [101],
                                    "default": "other", "output": {"n": {"$sum": 1}}}}]))
    by_group = list(db.anomalies.aggregate([
        {"$match": q}, {"$group": {"_id": "$vessel_group", "gaps": {"$sum": 1},
                                   "elevated": {"$sum": {"$cond": [{"$gt": ["$risk_score", 60]}, 1, 0]}},
                                   "avg_score": {"$avg": "$risk_score"}}}, {"$sort": {"gaps": -1}}]))
    by_cluster = list(db.anomalies.aggregate([
        {"$match": q}, {"$group": {"_id": "$cluster", "gaps": {"$sum": 1}, "avg_score": {"$avg": "$risk_score"}}},
        {"$sort": {"_id": 1}}]))
    by_day = list(db.anomalies.aggregate([
        {"$match": q}, {"$group": {"_id": {"d": "$event_date", "l": "$risk_level"}, "n": {"$sum": 1}}}]))
    return {
        "by_level": [{"level": lv, "count": by_level.get(lv, 0)} for lv in RISK_LEVELS],
        "score_histogram": [{"from": h["_id"], "count": h["n"]} for h in hist],
        "by_group": [{"group": g["_id"], "gaps": g["gaps"], "elevated": g["elevated"],
                      "avg_score": round(g["avg_score"] or 0, 1)} for g in by_group],
        "by_cluster": [{"cluster": c["_id"], "gaps": c["gaps"], "avg_score": round(c["avg_score"] or 0, 1)}
                       for c in by_cluster],
        "by_day": [{"date": d["_id"]["d"], "level": d["_id"]["l"], "count": d["n"]} for d in by_day],
    }


@app.get("/api/identity")
def identity(flagged_only: bool = True, limit: int = Query(200, le=2000)):
    db = get_db()
    q = {"flagged": True} if flagged_only else {}
    rows = list(db.identity_vessels.find(q, NO_ID).sort("conflict_events", DESCENDING).limit(limit))
    names = {d["mmsi"]: d for d in db.vessels.find(
        {"mmsi": {"$in": [r["mmsi"] for r in rows]}},
        {"_id": 0, "mmsi": 1, "vessel_name": 1, "last_lat": 1, "last_lon": 1, "n_names": 1})}
    for r in rows:
        v = names.get(r["mmsi"], {})
        r.update({"vessel_name": v.get("vessel_name"), "last_lat": v.get("last_lat"),
                  "last_lon": v.get("last_lon"), "n_names": v.get("n_names")})
    return rows


@app.get("/api/identity/{mmsi}/events")
def identity_events(mmsi: int):
    return list(get_db().identity_events.find({"mmsi": mmsi}, NO_ID).sort("epoch", ASCENDING).limit(500))


@app.get("/api/ml")
def ml():
    db = get_db()
    return {"model": db.ml_model.find_one({}, NO_ID),
            "clusters": list(db.cluster_profiles.find({}, NO_ID).sort("cluster", ASCENDING))}


# ---------------------------------------------------------------- trends
@app.get("/api/trends")
def trends():
    db = get_db()
    return {"visits": insights.visit_trends(db),
            "daily": list(db.daily_trends.find({}, NO_ID).sort("date", ASCENDING)),
            "hourly": list(db.hourly_activity.find({}, NO_ID).sort("hour_epoch", ASCENDING)),
            "groups": list(db.daily_group_activity.find({}, NO_ID).sort("date", ASCENDING)),
            "port_daily": list(db.congestion_daily.find(
                {}, {"_id": 0, "port_id": 1, "date": 1, "pci": 1, "arrivals": 1, "departures": 1,
                     "avg_queue": 1, "avg_waiting_h": 1}).sort([("port_id", 1), ("date", 1)]))}


# ---------------------------------------------------------------- frontend
app.mount("/static", StaticFiles(directory=FRONTEND), name="static")


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(FRONTEND / "index.html")
