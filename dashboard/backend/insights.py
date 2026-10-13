"""Data-driven insight sentences for the dashboard.

Every sentence is computed from the analytical results stored in MongoDB.
A rule only returns text when the data supports it (e.g. "most gaps occur
away from ports" is only emitted when more than half of them do); otherwise
it returns None and the dashboard shows nothing. Numbers are never typed in.
"""
from __future__ import annotations

import statistics
from datetime import datetime, timezone

from pymongo.database import Database

from src.analytics.dark_vessel.risk_scoring import RULE_WEIGHTS  # noqa: E402  (same weights Spark used)
from src.common.config import load_config  # noqa: E402

PCI_WEIGHTS = load_config()["congestion"]["pci_weights"]
PCI_COMPONENT = {"queue": ("norm_avg_queue", "the anchorage queue"),
                 "waiting": ("norm_avg_waiting_h", "waiting time before berth"),
                 "dwell": ("norm_avg_dwell_h", "time at berth"),
                 "utilization": ("norm_utilization", "berth utilisation")}
FACTORS = {"c_duration": "long reporting gap", "c_underway": "vessel underway when reports stopped",
           "c_coverage": "other vessels received nearby", "c_speed": "implied speed inconsistent with reported speed",
           "c_course": "large course change across the gap", "c_offshore": "distance from port",
           "c_night": "gap began at night", "c_repeat": "repeated gaps by the same vessel"}
FACTOR_WEIGHTS = {f"c_{k}": w for k, w in RULE_WEIGHTS.items()}


def _f(v, d=1):
    return f"{v:,.{d}f}"


def _pct(a, b):
    return 100.0 * a / b if b else 0.0


def _day(date_str: str) -> str:
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%a %d %b")


def _hour(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%a %d %b %H:00 UTC")


def pci_drivers(port: dict) -> list[tuple[str, float]]:
    """PCI points contributed by each component, largest first."""
    parts = [(PCI_COMPONENT[k][1], 100 * w * (port.get(PCI_COMPONENT[k][0]) or 0)) for k, w in PCI_WEIGHTS.items()]
    return sorted(parts, key=lambda x: -x[1])


# --------------------------------------------------------------------------- overview
def overview(db: Database) -> dict:
    out = {}
    clean = db.pipeline_metrics.find_one({"stage": "clean_ais"}) or {}
    if clean.get("window_start_epoch"):
        s = datetime.fromtimestamp(clean["window_start_epoch"], tz=timezone.utc).strftime("%d %b %Y")
        e = datetime.fromtimestamp(clean["window_end_epoch"], tz=timezone.utc).strftime("%d %b %Y")
        out["scope"] = (f"All figures are computed from {clean['raw_rows']:,} observed AIS position reports "
                        f"({s} – {e}, UTC) and describe this observed week only.")
    ports = list(db.ports.find({"pci": {"$ne": None}}, {"_id": 0}).sort("pci", -1))
    if ports:
        top = ports[0]
        drv = pci_drivers(top)
        out["pci"] = (f"{top['port_name']} records the highest congestion score ({_f(top['pci'])}) in the observed "
                      f"week; its largest PCI component is {drv[0][0]} ({_f(drv[0][1], 0)} of {_f(top['pci'], 0)} points).")
        hi = [p for p in ports if p.get("pci_band") in ("High", "Severe")]
        if hi:
            out["pci"] += f" {len(hi)} of {len(ports)} scored ports fall in the High or Severe band."
    risk = {r["_id"]: r["n"] for r in db.anomalies.aggregate([{"$group": {"_id": "$risk_level", "n": {"$sum": 1}}}])}
    total = sum(risk.values())
    if total:
        hp = risk.get("High Priority", 0)
        low = risk.get("Normal", 0) + risk.get("Watch", 0)
        txt = f"{_f(_pct(low, total))}% of {total:,} AIS gaps rank Normal or Watch."
        if hp:
            grp = list(db.anomalies.aggregate([{"$match": {"risk_level": "High Priority"}},
                                               {"$group": {"_id": "$vessel_group", "n": {"$sum": 1}}},
                                               {"$sort": {"n": -1}}, {"$limit": 1}]))
            txt += f" {hp:,} gaps ({_f(_pct(hp, total), 2)}%) reach High Priority"
            txt += f", most often for {grp[0]['_id']} vessels ({grp[0]['n']} of {hp})." if grp else "."
        out["risk"] = txt
    groups = list(db.vessels.aggregate([{"$group": {"_id": "$vessel_group", "n": {"$sum": 1}}}, {"$sort": {"n": -1}}]))
    nv = sum(g["n"] for g in groups)
    if groups:
        com = sum(g["n"] for g in groups if g["_id"] in ("Cargo", "Tanker"))
        out["fleet"] = (f"{groups[0]['_id']} is the largest observed group ({groups[0]['n']:,} MMSIs, "
                        f"{_f(_pct(groups[0]['n'], nv))}%). Cargo and tanker vessels, the basis of the port "
                        f"analytics, are {_f(_pct(com, nv))}% of observed vessels.")
    vis = visibility_summary(db)
    if vis:
        out["visibility"] = vis
    return out


# --------------------------------------------------------------------------- ports
def ports_page(db: Database) -> dict:
    out = {}
    ports = list(db.ports.find({"pci": {"$ne": None}}, {"_id": 0}).sort("pci", -1))
    if len(ports) < 2:
        return out
    top, low = ports[0], ports[-1]
    out["ranking"] = (f"{top['port_name']} ranks first (PCI {_f(top['pci'])}) and {low['port_name']} last "
                      f"(PCI {_f(low['pci'])}) among {len(ports)} ports with enough berthed visits to score.")
    # share of waiting in waiting+berth time: five most congested vs the rest
    def share(ps):
        w = sum(p.get("avg_waiting_h") or 0 for p in ps)
        d = sum(p.get("avg_dwell_h") or 0 for p in ps)
        return _pct(w, w + d)
    s_top, s_rest = share(ports[:5]), share(ports[5:])
    wmax = max(ports, key=lambda p: p.get("avg_waiting_h") or 0)
    txt = (f"Average waiting before berth is longest at {wmax['port_name']} ({_f(wmax['avg_waiting_h'])} h). "
           f"Waiting is {_f(s_top, 0)}% of waiting-plus-berth time at the five most congested ports, "
           f"against {_f(s_rest, 0)}% at the other ports")
    out["waiting_berth"] = txt + (", so queueing contributes more to turnaround where congestion is highest."
                                  if s_top > s_rest * 1.25 else ".")
    t = [p for p in ports if p.get("median_turnaround_h") is not None]
    if t:
        hi = max(t, key=lambda p: p["median_turnaround_h"])
        lo = min(t, key=lambda p: p["median_turnaround_h"])
        out["turnaround"] = (f"Median turnaround ranges from {_f(lo['median_turnaround_h'])} h at {lo['port_name']} "
                             f"to {_f(hi['median_turnaround_h'])} h at {hi['port_name']} (complete berthed visits).")
    comp = [(p["port_name"], pci_drivers(p)[0][0]) for p in ports[:5]]
    dominant = statistics.mode([c[1] for c in comp])
    n_dom = sum(1 for c in comp if c[1] == dominant)
    out["comparison"] = f"For {n_dom} of the 5 most congested ports the largest PCI component is {dominant}."
    return out


def port_detail(db: Database, port_id: str) -> dict:
    p = db.ports.find_one({"port_id": port_id}, {"_id": 0})
    if not p:
        return {}
    out = {}
    if p.get("pci") is not None:
        n = db.ports.count_documents({"pci": {"$ne": None}})
        drv = pci_drivers(p)
        out["summary"] = (f"{p['port_name']} ranks {p['pci_rank']} of {n} by congestion (PCI {_f(p['pci'])}, "
                          f"{p['pci_band']}). The largest contributions come from {drv[0][0]} "
                          f"({_f(drv[0][1], 0)} pts) and {drv[1][0]} ({_f(drv[1][1], 0)} pts).")
    else:
        out["summary"] = f"{p['port_name']} has too few berthed visits in the observed week to compute a PCI."
    hourly = list(db.congestion_hourly.find({"port_id": port_id}, {"_id": 0, "hour_epoch": 1, "queue": 1, "at_berth": 1}))
    if hourly:
        pk = max(hourly, key=lambda h: h.get("queue") or 0)
        if pk.get("queue"):
            out["hourly"] = (f"The anchorage queue peaked at {pk['queue']} vessels ({_hour(pk['hour_epoch'])}); "
                             f"on average {_f(p.get('avg_at_berth') or 0)} vessels were at berth and "
                             f"{_f(p.get('avg_queue') or 0)} waiting.")
    daily = list(db.congestion_daily.find({"port_id": port_id}, {"_id": 0, "date": 1, "arrivals": 1}))
    if daily:
        b = max(daily, key=lambda d: d.get("arrivals") or 0)
        out["daily"] = (f"{sum(d.get('arrivals') or 0 for d in daily)} commercial arrivals over the week; "
                        f"busiest day {_day(b['date'])} with {b['arrivals']} arrivals.")
    groups = list(db.turnaround_stats.find({"port_id": port_id, "vessel_group": {"$ne": "ALL"},
                                            "median_turnaround_h": {"$ne": None}}, {"_id": 0}))
    if len(groups) >= 2:
        g = sorted(groups, key=lambda x: -x["median_turnaround_h"])
        out["groups"] = (f"{g[0]['vessel_group']} vessels have the longer median turnaround here "
                         f"({_f(g[0]['median_turnaround_h'])} h vs {_f(g[-1]['median_turnaround_h'])} h for "
                         f"{g[-1]['vessel_group'].lower()}).")
    elif groups:
        out["groups"] = f"Only {groups[0]['vessel_group'].lower()} visits are complete enough to measure turnaround here."
    return out


# --------------------------------------------------------------------------- vessel performance
def vessels_page(db: Database) -> dict:
    out = {}
    st = list(db.turnaround_stats.find({"vessel_group": "ALL", "port_id": {"$ne": "ALL"},
                                        "complete_visits": {"$gte": 3}}, {"_id": 0}))
    names = {p["port_id"]: p["port_name"] for p in db.ports.find({}, {"_id": 0, "port_id": 1, "port_name": 1})}
    if st:
        hi = max(st, key=lambda s: s.get("median_turnaround_h") or 0)
        lo = min(st, key=lambda s: s.get("median_turnaround_h") or 1e9)
        out["by_port"] = (f"Median turnaround is longest at {names.get(hi['port_id'], hi['port_id'])} "
                          f"({_f(hi['median_turnaround_h'])} h) and shortest at "
                          f"{names.get(lo['port_id'], lo['port_id'])} ({_f(lo['median_turnaround_h'])} h).")
        berth_dom = sum(1 for x in st if (x.get("median_dwell_h") or 0) > (x.get("median_waiting_h") or 0))
        out["by_port"] += f" Median berth time exceeds median waiting at {berth_dom} of {len(st)} ports."
    q = {"visit_type": "BERTHED", "complete": True}
    n = db.port_visits.count_documents(q)
    waited = db.port_visits.count_documents(dict(q, waited=True))
    if n:
        out["scatter"] = f"{_f(_pct(waited, n), 0)}% of {n:,} complete berthed visits waited more than 30 min at anchorage"
        out["scatter"] += ("; most visits go straight to berth, so the spread between visits is mainly in berth time."
                           if waited < n / 2 else ".")
    return out


# --------------------------------------------------------------------------- anomalies
def visibility_summary(db: Database) -> str | None:
    if "ais_visibility" not in db.list_collection_names():
        return None
    rows = {r["_id"]: r["n"] for r in db.ais_visibility.aggregate([{"$group": {"_id": "$visibility", "n": {"$sum": 1}}}])}
    total = sum(rows.values())
    if not total:
        return None
    obs = rows.get("coverage_observed", 0)
    return (f"For {obs:,} of {total:,} Suspicious/High Priority gaps ({_f(_pct(obs, total), 0)}%), other vessels "
            f"kept being received within 10 nm of the last known position during the gap, so receiver coverage "
            f"existed there; {rows.get('isolated', 0):,} gaps had no other vessel nearby before or after.")


def anomalies_page(db: Database) -> dict:
    out = {}
    total = db.anomalies.estimated_document_count()
    if not total:
        return out
    elev_q = {"risk_score": {"$gt": 60}}
    elev = db.anomalies.count_documents(elev_q)
    hp_q = {"risk_level": "High Priority"}
    hp = db.anomalies.count_documents(hp_q)
    out["distribution"] = (f"{elev:,} of {total:,} gaps ({_f(_pct(elev, total))}%) score above 60 (Suspicious or "
                           f"High Priority).")
    stats = list(db.anomalies.aggregate([{"$group": {"_id": None, "mean": {"$avg": "$risk_score"},
                                                     "scores": {"$push": "$risk_score"}}}]))[0]
    med = statistics.median(stats["scores"])
    if stats["mean"] > med:
        out["distribution"] += (f" Scores are right-skewed (mean {_f(stats['mean'])} > median {_f(med)}), so review "
                                f"effort concentrates on a small tail.")
    far = db.anomalies.count_documents(dict(elev_q, start_dist_to_port_nm={"$gt": 10}))
    if elev and far / elev > 0.5:
        out["proximity"] = (f"Most elevated gaps occur outside immediate port proximity: {_f(_pct(far, elev), 0)}% "
                            f"begin more than 10 nm from the nearest monitored port.")
    elif elev:
        out["proximity"] = (f"{_f(_pct(far, elev), 0)}% of elevated gaps begin more than 10 nm from the nearest "
                            f"monitored port; the rest start near port areas.")
    if hp:
        sums = list(db.anomalies.aggregate([{"$match": hp_q}, {"$group": {"_id": None, **{
            k: {"$avg": {"$multiply": [f"${k}", 100 * w]}} for k, w in FACTOR_WEIGHTS.items()}}}]))[0]
        ranked = sorted(((FACTORS[k], sums[k] or 0) for k in FACTOR_WEIGHTS), key=lambda x: -x[1])
        combo = db.anomalies.count_documents(dict(hp_q, gap_h={"$gte": 12}, c_speed={"$gte": 0.5}))
        txt = (f"Among the {hp} High Priority gaps the largest average score contributions are "
               f"{ranked[0][0]} ({_f(ranked[0][1])} pts) and {ranked[1][0]} ({_f(ranked[1][1])} pts).")
        if combo:
            txt += (f" {combo} of them combine a gap of 12 h or more with an implied speed that is inconsistent "
                    f"with the reported speed.")
        out["factors"] = txt
    days = list(db.anomalies.aggregate([{"$match": elev_q}, {"$group": {"_id": "$event_date", "n": {"$sum": 1}}},
                                        {"$sort": {"n": -1}}]))
    if days:
        out["by_day"] = (f"Elevated gaps peak on {_day(days[0]['_id'])} ({days[0]['n']:,}) and are lowest on "
                         f"{_day(days[-1]['_id'])} ({days[-1]['n']:,}).")
        last_day = max(d["_id"] for d in days)
        if days[-1]["_id"] == last_day:
            out["by_day"] += (" The final day is partly truncated: a gap is only detected once the vessel reports "
                              "again, so gaps that run past the end of the window are not observed.")
    vs = list(db.anomalies.aggregate([{"$match": elev_q}, {"$group": {"_id": "$mmsi", "n": {"$sum": 1}}},
                                      {"$sort": {"n": -1}}]))
    if vs:
        top10 = sum(v["n"] for v in vs[:10])
        out["vessels"] = (f"{len(vs):,} vessels account for the {elev:,} elevated gaps; the ten most affected "
                          f"vessels contribute {top10} ({_f(_pct(top10, elev), 0)}%).")
    cl = list(db.cluster_profiles.find({}, {"_id": 0}).sort("avg_ml_score", -1))
    if cl:
        out["clusters"] = (f"MLlib KMeans separates {len(cl)} gap profiles; cluster {cl[0]['cluster']} "
                           f"({_f(100 * cl[0]['share'])}% of gaps) is the most unusual (avg ML score "
                           f"{_f(cl[0]['avg_ml_score'])}): {cl[0]['description']}.")
    flagged = db.identity_vessels.count_documents({"flagged": True})
    if flagged:
        g = list(db.identity_vessels.aggregate([{"$match": {"flagged": True}},
                                                {"$group": {"_id": "$vessel_group", "n": {"$sum": 1}}},
                                                {"$sort": {"n": -1}}, {"$limit": 1}]))
        out["identity"] = (f"{flagged} MMSIs show a potential identity inconsistency (repeated position jumps "
                           f"that one vessel could not make); the largest group is {g[0]['_id']} ({g[0]['n']}).")
    vis = visibility_summary(db)
    if vis:
        out["visibility"] = vis
    return out


# --------------------------------------------------------------------------- trends
def trends_page(db: Database) -> dict:
    out = {}
    hourly = list(db.hourly_activity.find({}, {"_id": 0}).sort("hour_epoch", 1))
    if len(hourly) > 24:
        med_m = statistics.median(h["messages"] for h in hourly)
        med_v = statistics.median(h["active_vessels"] for h in hourly)
        lo = min(hourly, key=lambda h: h["messages"])
        hi = max(hourly, key=lambda h: h["messages"])
        txt = (f"Received AIS messages range from {lo['messages']:,} to {hi['messages']:,} per hour "
               f"(median {med_m:,.0f}). The lowest hour, {_hour(lo['hour_epoch'])}, is {_f(_pct(lo['messages'], med_m), 0)}% "
               f"of the median")
        ratio_v = _pct(lo["active_vessels"], med_v)
        out["messages"] = txt + (f" while active vessels stay at {_f(ratio_v, 0)}% of normal, which points to a "
                                 f"drop in received reports rather than in traffic." if ratio_v > 80 else ".")
        out["vessels"] = (f"Between {min(h['active_vessels'] for h in hourly):,} and "
                          f"{max(h['active_vessels'] for h in hourly):,} distinct vessels report each hour "
                          f"(median {med_v:,.0f}).")
    cd = list(db.congestion_daily.find({"pci": {"$ne": None}}, {"_id": 0, "port_id": 1, "date": 1, "pci": 1}))
    if cd:
        names = {p["port_id"]: p["port_name"] for p in db.ports.find({}, {"_id": 0, "port_id": 1, "port_name": 1})}
        m = max(cd, key=lambda r: r["pci"])
        by_port: dict[str, list] = {}
        for r in cd:
            by_port.setdefault(r["port_id"], []).append(r["pci"])
        var = max(((k, max(v) - min(v)) for k, v in by_port.items() if len(v) >= 5), key=lambda x: x[1])
        out["heatmap"] = (f"The highest daily congestion is {names.get(m['port_id'])} on {_day(m['date'])} "
                          f"(PCI {_f(m['pci'])}). {names.get(var[0])} is the most variable port "
                          f"(daily PCI range {_f(var[1], 0)} points).")
    daily = list(db.daily_trends.find({}, {"_id": 0}).sort("date", 1))
    if daily:
        b = max(daily, key=lambda d: d.get("port_arrivals") or 0)
        out["arrivals"] = (f"Commercial port arrivals peak on {_day(b['date'])} ({b['port_arrivals']}); "
                           f"{sum(d.get('port_arrivals') or 0 for d in daily):,} arrivals in total across monitored ports.")
        g = max(daily, key=lambda d: (d.get("gaps_high_priority") or 0) + (d.get("gaps_suspicious") or 0))
        out["gaps"] = (f"{_day(g['date'])} has the most elevated gaps "
                       f"({(g.get('gaps_high_priority') or 0) + (g.get('gaps_suspicious') or 0):,}).")
        levels = ("gaps_normal", "gaps_watch", "gaps_suspicious", "gaps_high_priority")
        top_each = {max(levels, key=lambda k: d.get(k) or 0) for d in daily}
        if len(top_each) == 1:
            lvl = top_each.pop().replace("gaps_", "").replace("_", " ").title()
            out["gaps"] += f" {lvl}-level gaps are the largest category on every day."
    flow = visit_trends(db)
    if flow:
        w = max(flow, key=lambda r: r["avg_waiting_h"] or 0)
        t = max(flow, key=lambda r: r["avg_turnaround_h"] or 0)
        out["waiting"] = (f"Vessels arriving on {_day(w['date'])} waited longest on average "
                          f"({_f(w['avg_waiting_h'])} h before berth, {w['observed_arrivals']} observed berthed arrivals).")
        out["turnaround"] = (f"Average turnaround of complete visits is highest for arrivals on {_day(t['date'])} "
                             f"({_f(t['avg_turnaround_h'])} h).")
        if flow[-1]["complete"] < flow[0]["complete"]:
            out["turnaround"] += (f" Complete visits fall from {flow[0]['complete']} (first day) to {flow[-1]['complete']} "
                                  f"(last day): long stays that start late in the week end after the observed window, "
                                  f"so late-week averages are biased towards short visits.")
    grp = list(db.daily_group_activity.find({}, {"_id": 0}))
    if grp:
        tot: dict[str, int] = {}
        for r in grp:
            tot[r["vessel_group"]] = tot.get(r["vessel_group"], 0) + r["vessels"]
        top = max(tot, key=tot.get)
        days: dict[str, dict] = {}
        for r in grp:
            days.setdefault(r["date"], {})[r["vessel_group"]] = r["vessels"]
        every_day = all(max(d, key=d.get) == top for d in days.values())
        series = [d.get(top, 0) for _, d in sorted(days.items())]
        cv = statistics.pstdev(series) / statistics.mean(series) if series and statistics.mean(series) else 1
        out["groups"] = (f"{top} has the most active vessels" + (" on every day" if every_day else " overall")
                         + f" (daily range {min(series):,}–{max(series):,})"
                         + ("; daily activity varies by less than 10%." if cv < 0.10 else "."))
    return out


def visit_trends(db: Database) -> list[dict]:
    """Per arrival day: berthed arrivals, avg waiting, avg turnaround of complete visits."""
    rows = db.port_visits.aggregate([
        {"$match": {"visit_type": "BERTHED"}},
        {"$group": {"_id": {"$substrBytes": ["$arrival_ts", 0, 10]},
                    "visits": {"$sum": 1},
                    "observed_arrivals": {"$sum": {"$cond": [{"$eq": ["$censored_arrival", False]}, 1, 0]}},
                    "avg_waiting_h": {"$avg": {"$cond": [{"$eq": ["$censored_arrival", False]}, "$waiting_h", None]}},
                    "avg_dwell_h": {"$avg": {"$cond": ["$complete", "$dwell_h", None]}},
                    "avg_turnaround_h": {"$avg": {"$cond": ["$complete", "$turnaround_h", None]}},
                    "complete": {"$sum": {"$cond": ["$complete", 1, 0]}}}},
        {"$sort": {"_id": 1}}])
    return [{"date": r["_id"], "visits": r["visits"], "observed_arrivals": r["observed_arrivals"], "complete": r["complete"],
             "avg_waiting_h": None if r["avg_waiting_h"] is None else round(r["avg_waiting_h"], 2),
             "avg_dwell_h": None if r["avg_dwell_h"] is None else round(r["avg_dwell_h"], 2),
             "avg_turnaround_h": None if r["avg_turnaround_h"] is None else round(r["avg_turnaround_h"], 2)}
            for r in rows]


PAGES = {"overview": overview, "ports": ports_page, "vessels": vessels_page,
         "anomalies": anomalies_page, "trends": trends_page}
