// Vessel Performance: vessel explorer (details, trajectory, speed profile, visits, reporting
// frequency) and fleet-level port performance (by-port medians, waiting vs berth, visit table).
import { api } from '../api.js';
import { addZoneLayers, createMap, fc, fitTo, hoverPopup, point, portZones, setSource } from '../map.js';
import { C, GROUP_COLORS, RISK, baseOption, chart, esc, fmt, fmtDay, fmtH, fmtInt, fmtTs, insight, pill, xAxis, yAxis } from '../theme.js';

let map, ports = [], portById = {}, visits = [], current = null;
let resolveReady;
export const ready = new Promise((r) => { resolveReady = r; });
const GAP_S = 3600;            // track points are 15-min buckets; > 1 h between points = reporting gap
const SPEED_STOPS = [0, '#9DB9CC', 4, '#2F8F8B', 10, '#005B8F', 18, '#003153'];

export async function init() {
  ports = await api('ports');
  portById = Object.fromEntries(ports.map((p) => [p.port_id, p]));
  const sel = document.getElementById('vp-port');
  sel.innerHTML += ports.map((p) => `<option value="${p.port_id}">${esc(p.port_name)}</option>`).join('');
  ['vp-port', 'vp-group', 'vp-type', 'vp-complete'].forEach((id) => document.getElementById(id).addEventListener('change', loadFleet));
  setupExplorer();
  map = createMap('vp-map', { zoom: 3 });
  await map.ready;
  setSource(map, 'zones', portZones(ports)); addZoneLayers(map, 'zones');
  setSource(map, 'vtrack', fc([]));
  map.addLayer({ id: 'vtrack-gap', type: 'line', source: 'vtrack', filter: ['==', ['get', 'kind'], 'gap'],
    paint: { 'line-color': RISK.Suspicious, 'line-width': 1.6, 'line-dasharray': [2, 2] } });
  map.addLayer({ id: 'vtrack-seg', type: 'line', source: 'vtrack', filter: ['==', ['get', 'kind'], 'seg'],
    layout: { 'line-cap': 'round', 'line-join': 'round' },
    paint: { 'line-color': ['interpolate', ['linear'], ['get', 'sog'], ...SPEED_STOPS], 'line-width': 2.6 } });
  map.addLayer({ id: 'vtrack-port', type: 'circle', source: 'vtrack', filter: ['==', ['get', 'kind'], 'port'],
    paint: { 'circle-radius': 7, 'circle-color': '#fff', 'circle-stroke-color': C.prussian, 'circle-stroke-width': 2.5 } });
  map.addLayer({ id: 'vtrack-end', type: 'circle', source: 'vtrack', filter: ['in', ['get', 'kind'], ['literal', ['start', 'end']]],
    paint: { 'circle-radius': 5.5, 'circle-color': ['match', ['get', 'kind'], 'end', C.prussian, '#fff'],
      'circle-stroke-color': C.prussian, 'circle-stroke-width': 2 } });
  hoverPopup(map, 'vtrack-seg', (p) => `${fmtTs(+p.t)} UTC<br>SOG ${p.sog < 0 ? '—' : fmt(+p.sog)} kn`);
  hoverPopup(map, 'vtrack-gap', (p) => `<b>AIS reporting gap</b><br>${fmtTs(+p.t0)} → ${fmtTs(+p.t1)} UTC<br>${fmt(+p.h)} h without reports (path unobserved)`);
  hoverPopup(map, 'vtrack-port', (p) => `<b>${esc(p.name)}</b><br>${esc(p.info)}`);
  document.getElementById('vp-legend').innerHTML = `<h3>Speed over ground</h3><div class="speed-ramp"></div>
    <div class="row muted small" style="justify-content:space-between;width:140px"><span>0</span><span>4</span><span>10</span><span>18+ kn</span></div>
    <div class="row"><span style="display:inline-block;width:18px;border-top:2px dashed ${RISK.Suspicious}"></span>AIS gap (&gt; 1 h)</div>
    <div class="row"><span class="swatch" style="background:#fff;border:2px solid ${C.prussian}"></span>Port visited</div>`;
  await loadFleet();
  resolveReady();
  if (!current) {
    const first = document.querySelector('#vp-pick option[value]:not([value=""])');
    if (first) selectVessel(Number(first.value));
  }
}

export function activate() { map?.resize(); }

// ------------------------------------------------------------------ explorer
async function setupExplorer() {
  const pick = document.getElementById('vp-pick');
  const top = await api('vessels', { vessel_group: 'Cargo,Tanker', flagged: false, sort: 'port_visits', limit: 40 });
  pick.innerHTML += top.map((v) => `<option value="${v.mmsi}">${esc(v.vessel_name || 'MMSI ' + v.mmsi)} · ${esc(v.vessel_group)} · ${v.port_visits} visits</option>`).join('');
  pick.addEventListener('change', () => pick.value && selectVessel(Number(pick.value)));
  const input = document.getElementById('vp-search');
  const box = document.getElementById('vp-search-results');
  let timer;
  input.addEventListener('input', () => {
    clearTimeout(timer);
    const q = input.value.trim();
    if (q.length < 2) { box.hidden = true; return; }
    timer = setTimeout(async () => {
      const rows = await api('vessels/search', { q }, { useCache: false });
      box.innerHTML = rows.length ? rows.map((r) => `<button data-mmsi="${r.mmsi}"><span><b>${esc(r.vessel_name || 'Unnamed')}</b><br>
        <span class="muted small">MMSI ${r.mmsi} · ${esc(r.vessel_group)}</span></span></button>`).join('')
        : '<div style="padding:10px 12px" class="muted">No vessel found</div>';
      box.hidden = false;
    }, 220);
  });
  box.addEventListener('click', (e) => {
    const b = e.target.closest('button[data-mmsi]');
    if (!b) return;
    box.hidden = true; input.value = '';
    selectVessel(Number(b.dataset.mmsi));
  });
  document.addEventListener('click', (e) => { if (!e.target.closest('.inline-search')) box.hidden = true; });
}

function fact(label, value) { return `<div class="fact"><span>${label}</span><b>${value}</b></div>`; }

export async function selectVessel(mmsi) {
  current = mmsi;
  await map?.ready;
  const [v, tr] = await Promise.all([api(`vessels/${mmsi}`), api(`vessels/${mmsi}/track`).catch(() => null)]);
  if (current !== mmsi) return;
  const pts = tr?.points || [];
  const ctx = v.port_context || {};
  document.getElementById('vp-selected-label').textContent = `Showing ${v.vessel_name || 'MMSI ' + v.mmsi}`;
  document.getElementById('vp-v-title').textContent = v.vessel_name || `MMSI ${v.mmsi}`;
  document.getElementById('vp-v-pills').innerHTML = `${pill(v.vessel_risk_level || 'Normal')} ${v.flagged ? '<span class="pill flag">identity flag</span>' : ''}`;
  const med = v.median_report_interval_s, p95 = v.p95_report_interval_s;
  document.getElementById('vp-v-detail').innerHTML = `
    <div class="facts">
      ${fact('MMSI', v.mmsi)}${fact('IMO / call sign', `${esc(v.imo || '—')} / ${esc(v.call_sign || '—')}`)}
      ${fact('Vessel type', `${esc(v.vessel_group)} (AIS ${v.vessel_type ?? '—'})`)}${fact('Size', `${v.length ?? '—'} × ${v.width ?? '—'} m`)}
      ${fact('Last AIS report', fmtTs(v.last_ts) + ' UTC')}${fact('Latest position', `${fmt(v.last_lat, 4)}, ${fmt(v.last_lon, 4)}`)}
      ${fact('SOG · COG · heading', `${v.last_sog ?? '—'} kn · ${v.last_cog ?? '—'}° · ${v.last_heading ?? '—'}°`)}${fact('Navigational status', esc(v.status_text || '—'))}
      ${fact('Current port area', esc(ctx.current_port || '—'))}${fact('Previous port', esc(ctx.previous_port?.port || '—'))}
    </div>
    <div class="subhead">Movement (observed week)</div>
    <div class="facts three">
      ${fact('Distance', fmtInt(v.distance_nm) + ' nm')}${fact('Avg underway speed', fmt(v.avg_underway_sog) + ' kn')}${fact('Max SOG', fmt(v.max_sog) + ' kn')}
      ${fact('Time underway', fmt(v.pct_time_underway, 0) + '%')}${fact('Active days', v.active_days ?? '—')}${fact('Port visits', fmtInt(v.port_visits))}
    </div>
    <div class="subhead">AIS reporting frequency</div>
    <div class="facts three">
      ${fact('Reports', fmtInt(v.fixes))}${fact('Median interval', med != null ? fmt(med / 60, 1) + ' min' : '—')}${fact('95th pct interval', p95 != null ? fmt(p95 / 60, 0) + ' min' : '—')}
      ${fact('Reports / hour', fmt(v.reports_per_hour, 1))}${fact('AIS gaps', `${fmtInt(v.ais_gaps)} (${fmt(v.total_gap_h, 0)} h)`)}${fact('Max anomaly score', `${v.max_risk_score ?? 0} / 100`)}
    </div>`;

  // map: speed-coloured segments, dashed gaps, visited ports
  const feats = [];
  for (let i = 1; i < pts.length; i++) {
    const [t0, x0, y0] = pts[i - 1], [t1, x1, y1, s1] = pts[i];
    const gap = t1 - t0 > GAP_S;
    feats.push({ type: 'Feature', geometry: { type: 'LineString', coordinates: [[x0, y0], [x1, y1]] },
      properties: gap ? { kind: 'gap', t0, t1, h: (t1 - t0) / 3600 } : { kind: 'seg', t: t1, sog: s1 ?? -1 } });
  }
  if (pts.length) { feats.push(point(pts[0][1], pts[0][2], { kind: 'start' })); feats.push(point(pts.at(-1)[1], pts.at(-1)[2], { kind: 'end' })); }
  const seen = new Set();
  (v.visits || []).forEach((x) => {
    const p = portById[x.port_id];
    if (!p || seen.has(p.port_id)) return;
    seen.add(p.port_id);
    const n = v.visits.filter((y) => y.port_id === p.port_id).length;
    feats.push(point(p.port_lon, p.port_lat, { kind: 'port', name: p.port_name, info: `${n} visit${n > 1 ? 's' : ''} · ${x.visit_type.toLowerCase().replace('_', ' ')}` }));
  });
  setSource(map, 'vtrack', fc(feats));
  if (pts.length) fitTo(map, pts.map((p) => [p[1], p[2]]), 40, 11);

  renderSpeed(v, pts);
  renderVesselVisits(v);
  const gapsH = v.total_gap_h || 0;
  insight('ins-vp-vessel', `${v.vessel_name || 'This vessel'} covered ${fmtInt(v.distance_nm)} nm over ${v.active_days ?? '—'} active days, ` +
    `underway ${fmt(v.pct_time_underway, 0)}% of its reports, with ${fmtInt(v.port_visits)} port visit${v.port_visits === 1 ? '' : 's'}` +
    `${v.ports_visited?.length ? ' (' + v.ports_visited.map((id) => portById[id]?.port_name || id).join(', ') + ')' : ''}. ` +
    `It reported every ${med != null ? fmt(med / 60, 1) : '—'} min (median)` +
    (v.ais_gaps ? ` and had ${fmtInt(v.ais_gaps)} AIS gaps totalling ${fmt(gapsH, 0)} h; its highest gap score is ${v.max_risk_score} (${v.vessel_risk_level}).` : ' with no AIS gaps above the gap threshold.'));
}

function renderSpeed(v, pts) {
  const data = pts.filter((p) => p[3] != null).map((p) => [p[0] * 1000, p[3]]);
  const gapAreas = (v.gaps || []).map((g) => [{ xAxis: g.start_epoch * 1000, itemStyle: { color: (RISK[g.risk_level] || RISK.Normal) + '33' } }, { xAxis: g.end_epoch * 1000 }]);
  const visitAreas = (v.visits || []).map((x) => [{ xAxis: x.arrival_epoch * 1000, name: x.port_id, itemStyle: { color: 'rgba(47,143,139,.10)' } }, { xAxis: x.departure_epoch * 1000 }]);
  chart('vp-speed').setOption(baseOption({
    grid: { left: 40, right: 14, top: 14, bottom: 26 },
    tooltip: { trigger: 'axis', formatter: (ps) => `<b>${fmtDay(ps[0].value[0] / 1000)} UTC</b><br>SOG ${fmt(ps[0].value[1])} kn` },
    xAxis: xAxis({ type: 'time', axisLabel: { color: C.ink3, fontSize: 11, formatter: '{ee} {dd}' } }),
    yAxis: yAxis({ type: 'value', name: 'kn', min: 0 }),
    series: [{ type: 'line', showSymbol: false, data, lineStyle: { width: 1.3, color: C.marine }, itemStyle: { color: C.marine },
      areaStyle: { color: 'rgba(0,91,143,.07)' }, connectNulls: false,
      markArea: { silent: true, label: { show: false }, data: [...visitAreas, ...gapAreas] } }],
  }), true);
  const max = data.reduce((m, d) => Math.max(m, d[1]), 0);
  const under = data.filter((d) => d[1] >= 2);
  insight('ins-vp-speed', data.length
    ? `Highest reported speed ${fmt(max)} kn; mean ${fmt(under.reduce((s, d) => s + d[1], 0) / Math.max(1, under.length))} kn while underway (≥ 2 kn). ` +
      `${(v.visits || []).length} port-visit band${(v.visits || []).length === 1 ? '' : 's'} and ${(v.gaps || []).length} AIS gap${(v.gaps || []).length === 1 ? '' : 's'} are shaded.`
    : null);
}

function visitBar(x) {
  const t0 = x.arrival_epoch, t1 = x.departure_epoch, span = Math.max(1, t1 - t0);
  const e = (iso) => (iso ? Date.parse(iso) / 1000 : null);
  const aS = e(x.anchor_start_ts), aE = e(x.anchor_end_ts), bS = e(x.berth_start_ts), bE = e(x.berth_end_ts);
  const segs = [];
  let cur = t0;
  const push = (to, label, color) => { if (to != null && to > cur) { segs.push({ w: (to - cur) / span, label, color, h: (to - cur) / 3600 }); cur = to; } };
  if (aS && (!bS || aS < bS)) { push(aS, 'approach', '#B7C6D0'); push(aE ?? bS, 'waiting', C.amber); }
  if (bS) { push(bS, 'transit', '#B7C6D0'); push(bE, 'at berth', C.marine); }
  push(t1, 'in port area', '#B7C6D0');
  return `<div class="tl-bar" style="height:20px">${segs.map((s) => `<div class="tl-seg" style="flex:${s.w};background:${s.color}" title="${s.label}: ${fmt(s.h)} h">${s.w > 0.16 ? s.label : ''}</div>`).join('')}</div>`;
}

function renderVesselVisits(v) {
  const el = document.getElementById('vp-v-visits');
  if (!(v.visits || []).length) { el.innerHTML = '<p class="muted">No visits to monitored ports in the observed week (port analytics cover cargo and tanker vessels).</p>'; return; }
  el.innerHTML = v.visits.map((x) => `<div style="margin-bottom:10px">
      <div style="display:flex;justify-content:space-between;gap:8px;font-size:12px"><b>${esc(x.port_name)}</b>
        <span class="muted">${fmtTs(x.arrival_ts)} → ${fmtTs(x.departure_ts)}${x.complete ? '' : ' · censored'}</span></div>
      ${visitBar(x)}
      <div class="tl-legend" style="margin-top:3px">waiting ${fmtH(x.waiting_h ?? x.anchor_only_h)} · berth ${fmtH(x.dwell_h)} · turnaround <b>${fmtH(x.turnaround_h)}</b></div>
    </div>`).join('') + `<div class="tl-legend"><span><span class="swatch sq" style="background:${C.amber}"></span> waiting at anchorage</span>
      <span><span class="swatch sq" style="background:${C.marine}"></span> at berth</span><span><span class="swatch sq" style="background:#B7C6D0"></span> moving in port area</span></div>`;
}

// ------------------------------------------------------------------ fleet
async function loadFleet() {
  const port = document.getElementById('vp-port').value;
  const group = document.getElementById('vp-group').value || 'Cargo,Tanker';
  const type = document.getElementById('vp-type').value;
  const complete = document.getElementById('vp-complete').checked;
  const [v, ins] = await Promise.all([
    api('visits', { port_id: port, vessel_group: group, visit_type: type, complete_only: complete, sort: 'turnaround_h', limit: 1500 }),
    api('insights/vessels')]);
  visits = v;
  document.getElementById('vp-count').textContent = `${fmtInt(visits.length)} visits (longest turnaround first, max 1,500) · click a row to open the vessel`;
  renderTable();
  renderScatter();
  await renderPortChart();
  insight('ins-vp-byport', ins.by_port);
  insight('ins-vp-scatter', ins.scatter);
}

async function renderPortChart() {
  const stats = (await api('turnaround/stats')).filter((s) => s.vessel_group === 'ALL' && s.port_id !== 'ALL' && s.complete_visits >= 3);
  stats.sort((a, b) => (a.median_turnaround_h || 0) - (b.median_turnaround_h || 0));
  // One row per port: waiting + berth time stacked, turnaround as a marker (it also
  // includes time moving inside the zones, so it is not the sum of the two).
  document.getElementById('vp-ports').style.height = `${Math.max(300, stats.length * 22 + 70)}px`;
  chart('vp-ports').resize();
  chart('vp-ports').setOption(baseOption({
    legend: { data: ['Median waiting', 'Median berth time', 'Median turnaround'] },
    grid: { left: 150, right: 24, top: 30, bottom: 26 },
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, formatter: (ps) => {
      const s = stats[ps[0].dataIndex];
      return `<b>${esc(portById[s.port_id]?.port_name)}</b> · ${s.complete_visits} complete visits<br>` + ps.map((q) => `${q.marker}${q.seriesName}: ${fmtH(q.value)}`).join('<br>') +
        `<br>p90 turnaround ${fmtH(s.p90_turnaround_h)}`;
    } },
    xAxis: yAxis({ type: 'value', name: 'hours' }),
    yAxis: xAxis({ type: 'category', data: stats.map((s) => portById[s.port_id]?.port_name), axisLabel: { color: C.ink2, width: 140, overflow: 'truncate', interval: 0 } }),
    series: [
      { name: 'Median waiting', type: 'bar', stack: 'phases', barWidth: 10, itemStyle: { color: C.amber }, data: stats.map((s) => s.median_waiting_h) },
      { name: 'Median berth time', type: 'bar', stack: 'phases', barWidth: 10, itemStyle: { color: C.teal, borderRadius: [0, 3, 3, 0] }, data: stats.map((s) => s.median_dwell_h) },
      { name: 'Median turnaround', type: 'scatter', symbol: 'diamond', symbolSize: 10, itemStyle: { color: C.prussian }, data: stats.map((s) => s.median_turnaround_h) },
    ],
  }), true);
}

function renderTable() {
  const t = document.getElementById('vp-table');
  t.innerHTML = `<thead><tr><th>Vessel</th><th>Group</th><th>Port</th><th>Arrival (UTC)</th><th class="num">Waiting</th>
    <th class="num">Berth</th><th class="num">Turnaround</th><th>Type</th></tr></thead>
    <tbody>${visits.slice(0, 400).map((v, i) => `<tr data-i="${i}">
      <td>${esc(v.vessel_name || 'MMSI ' + v.mmsi)}</td><td>${esc(v.vessel_group)}</td><td>${esc(v.port_name || v.port_id)}</td>
      <td class="mono">${fmtTs(v.arrival_ts)}</td><td class="num">${fmt(v.waiting_h)}</td><td class="num">${fmt(v.dwell_h)}</td>
      <td class="num"><b>${fmt(v.turnaround_h)}</b></td>
      <td class="small">${v.visit_type === 'BERTHED' ? 'berthed' : 'anchored only'}${v.complete ? '' : ' · censored'}</td></tr>`).join('')}</tbody>`;
  t.querySelectorAll('tbody tr').forEach((tr) => tr.addEventListener('click', () => {
    t.querySelectorAll('tr.sel').forEach((x) => x.classList.remove('sel'));
    tr.classList.add('sel');
    selectVessel(visits[Number(tr.dataset.i)].mmsi);
    document.getElementById('view-vessels').scrollIntoView({ behavior: 'smooth' });
  }));
}

function renderScatter() {
  const pts = visits.filter((v) => v.waiting_h != null && v.dwell_h != null);
  const groups = ['Cargo', 'Tanker'];
  chart('vp-scatter').setOption(baseOption({
    legend: { data: groups },
    grid: { left: 46, right: 16, top: 28, bottom: 36 },
    tooltip: { trigger: 'item', formatter: (p) => {
      const v = pts[p.data[2]];
      return `<b>${esc(v.vessel_name || v.mmsi)}</b> · ${esc(v.port_name || v.port_id)}<br>waiting ${fmtH(v.waiting_h)} · berth ${fmtH(v.dwell_h)}<br>turnaround ${fmtH(v.turnaround_h)}`;
    } },
    xAxis: yAxis({ type: 'value', name: 'waiting (h)', nameLocation: 'middle', nameGap: 22 }),
    yAxis: yAxis({ type: 'value', name: 'berth time (h)' }),
    series: groups.map((g) => ({
      name: g, type: 'scatter', symbolSize: 8,
      itemStyle: { color: GROUP_COLORS[g], opacity: 0.75, borderColor: '#fff', borderWidth: 1 },
      data: pts.map((v, i) => [v.waiting_h, v.dwell_h, i, v.vessel_group]).filter((d) => d[3] === g),
    })),
  }), true);
  chart('vp-scatter').off('click');
  chart('vp-scatter').on('click', (p) => { selectVessel(pts[p.data[2]].mmsi); document.getElementById('view-vessels').scrollIntoView({ behavior: 'smooth' }); });
}
