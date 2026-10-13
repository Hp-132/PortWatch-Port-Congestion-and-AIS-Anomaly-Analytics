// AIS Anomalies: dark-vessel gap review (filters, map, table, detail with contributing
// factors), AIS proximity / visibility event, distributions, vessels, MLlib clusters,
// potential MMSI identity inconsistency.
import { api } from '../api.js';
import { openVessel } from '../app.js';
import { addGapLineLayer, createMap, fc, fitTo, hoverPopup, point, riskColorExpr, setSource } from '../map.js';
import { C, RISK, RISK_ORDER, baseOption, chart, esc, fmt, fmtDay, fmtH, fmtInt, fmtTs, insight, pill, xAxis, yAxis } from '../theme.js';

let map, rows = [], meta, ins = {}, ports = [];
let resolveReady;
export const ready = new Promise((r) => { resolveReady = r; });
const levels = { 'Normal': false, 'Watch': true, 'Suspicious': true, 'High Priority': true };
const FACTORS = [
  ['c_duration', 'Long reporting gap'], ['c_underway', 'Underway when reports stopped'],
  ['c_coverage', 'Other vessels received nearby'], ['c_speed', 'Implied vs reported speed'],
  ['c_course', 'Course change across gap'], ['c_offshore', 'Distance from port'],
  ['c_night', 'Gap began at night'], ['c_repeat', 'Repeated gaps by vessel'],
];
let W = {};
const pts = (r, k) => 100 * (W[k] || 0) * (r[k] || 0) * (r.stationary_in_port ? 0.3 : 1);

export async function init(state) {
  meta = state.meta;
  W = meta.risk_weights;
  const chips = document.getElementById('an-levels');
  chips.innerHTML = RISK_ORDER.map((l) => `<button class="chip" data-level="${l}" aria-pressed="${levels[l]}"><span class="swatch" style="background:${RISK[l]}"></span>${l}</button>`).join('');
  chips.addEventListener('click', (e) => {
    const b = e.target.closest('.chip'); if (!b) return;
    levels[b.dataset.level] = !levels[b.dataset.level];
    b.setAttribute('aria-pressed', levels[b.dataset.level]); load();
  });
  document.getElementById('an-group').innerHTML += meta.vessel_groups.map((g) => `<option>${esc(g)}</option>`).join('');
  const days = [];
  for (let t = meta.window_start_epoch; t <= meta.window_end_epoch; t += 86400) days.push(fmtTs(t).slice(0, 10));
  document.getElementById('an-day').innerHTML += days.map((d) => `<option value="${d}">${d}</option>`).join('');
  ['an-group', 'an-day', 'an-min', 'an-prox', 'an-mingap'].forEach((id) => document.getElementById(id).addEventListener('change', load));
  let vt;
  document.getElementById('an-vessel').addEventListener('input', () => { clearTimeout(vt); vt = setTimeout(load, 350); });

  document.getElementById('an-legend').innerHTML = `<h3>Review priority</h3>${RISK_ORDER.map((l) => `<div class="row"><span class="swatch" style="background:${RISK[l]}"></span>${l}</div>`).join('')}
    <div class="row"><span class="swatch" style="background:#fff;border:2px solid ${C.prussian}"></span>Identity flag (last position)</div>
    <div class="row"><span class="swatch" style="background:${C.teal}"></span>Vessel received nearby</div>
    <div class="row muted small" style="margin-top:4px">dashed: unobserved gap path</div>`;

  [ins, ports] = await Promise.all([api('insights/anomalies'), api('ports')]);
  map = createMap('an-map', { zoom: 3.2 });
  await map.ready;
  setSource(map, 'gaplines', fc([])); addGapLineLayer(map, 'gaplines');
  setSource(map, 'gaps', fc([]));
  map.addLayer({ id: 'gaps', type: 'circle', source: 'gaps', paint: {
    'circle-color': riskColorExpr(), 'circle-radius': ['interpolate', ['linear'], ['get', 'risk_score'], 0, 3, 100, 8],
    'circle-stroke-color': '#fff', 'circle-stroke-width': 1.5 } });
  setSource(map, 'idv', fc([]));
  map.addLayer({ id: 'idv', type: 'circle', source: 'idv', paint: {
    'circle-color': '#fff', 'circle-radius': 5, 'circle-stroke-color': C.prussian, 'circle-stroke-width': 2 } });
  setSource(map, 'selgap', fc([]));
  map.addLayer({ id: 'selgap-nb-line', type: 'line', source: 'selgap', filter: ['==', ['get', 'kind'], 'link'],
    paint: { 'line-color': C.teal, 'line-width': 1, 'line-opacity': 0.6 } });
  map.addLayer({ id: 'selgap-line', type: 'line', source: 'selgap', filter: ['==', ['get', 'kind'], 'gap'],
    paint: { 'line-color': C.prussian, 'line-width': 2.4, 'line-dasharray': [2, 1.5] } });
  map.addLayer({ id: 'selgap-nb', type: 'circle', source: 'selgap', filter: ['==', ['get', 'kind'], 'nb'],
    paint: { 'circle-radius': 5, 'circle-color': C.teal, 'circle-stroke-color': '#fff', 'circle-stroke-width': 1.5 } });
  map.addLayer({ id: 'selgap-end', type: 'circle', source: 'selgap', filter: ['in', ['get', 'kind'], ['literal', ['last', 'recovered']]],
    paint: { 'circle-radius': 7, 'circle-color': ['match', ['get', 'kind'], 'last', '#fff', C.prussian],
      'circle-stroke-color': C.prussian, 'circle-stroke-width': 2.5 } });
  hoverPopup(map, 'gaps', (p) => `${pill(p.risk_level)} score ${p.risk_score}<br><b>${esc(p.name || 'MMSI ' + p.mmsi)}</b><br>gap ${fmt(+p.gap_h)} h · ${esc(p.group)}<br><span class="muted">click for details</span>`);
  hoverPopup(map, 'idv', (p) => `<b>Potential MMSI identity inconsistency</b><br>${esc(p.name || 'MMSI ' + p.mmsi)}<br>${p.conflicts} conflicting position jumps`);
  hoverPopup(map, 'selgap-nb', (p) => `<b>${esc(p.name || 'MMSI ' + p.mmsi)}</b> · ${esc(p.group)}<br>closest ${fmt(+p.d)} nm at ${fmtTs(+p.t)} UTC<br>${p.during} reports during the gap (${p.near} within 10 nm)`);
  hoverPopup(map, 'selgap-end', (p) => `<b>${p.kind === 'last' ? 'Last known position' : 'Recovered position'}</b><br>${fmtTs(+p.t)} UTC`);
  map.on('click', 'gaps', (e) => selectGap(e.features[0].properties.gap_id));

  await Promise.all([load(), renderKpis(), renderFactors(), renderVessels(), renderMl(), renderIdentity()]);
  insight('ins-an-visibility', ins.visibility);
  if (rows[0]) await selectGap(rows[0].gap_id, false);   // open the highest-scoring gap by default
  resolveReady();
}

export function activate() { map?.resize(); }

function query() {
  const q = {
    level: RISK_ORDER.filter((l) => levels[l]).join(',') || 'none',
    group: document.getElementById('an-group').value,
    min_score: document.getElementById('an-min').value,
    day: document.getElementById('an-day').value,
    proximity: document.getElementById('an-prox').value,
    min_gap_h: Number(document.getElementById('an-mingap').value) || undefined,
    vessel: document.getElementById('an-vessel').value.trim() || undefined,
  };
  return q;
}

async function load() {
  const q = query();
  const day = q.day ? Date.parse(q.day + 'T00:00:00Z') / 1000 : undefined;
  const [list, summary] = await Promise.all([api('anomalies', { ...q, limit: 3000 }, { useCache: false }),
    api('anomalies/summary', { group: q.group, start: day, end: day ? day + 86399 : undefined })]);
  rows = list;
  document.getElementById('an-count').textContent = `${fmtInt(rows.length)} matching gaps (highest score first, max 3,000)`;
  setSource(map, 'gaps', fc(rows.map((r) => point(r.start_lon, r.start_lat, {
    gap_id: r.gap_id, mmsi: r.mmsi, risk_level: r.risk_level, risk_score: r.risk_score, gap_h: r.gap_h, name: r.vessel_name, group: r.vessel_group }))));
  setSource(map, 'gaplines', fc(rows.filter((r) => r.dist_nm > 0.5).map((r) => ({ type: 'Feature',
    geometry: { type: 'LineString', coordinates: [[r.start_lon, r.start_lat], [r.end_lon, r.end_lat]] },
    properties: { risk_level: r.risk_level } }))));
  renderTable();
  renderSummary(summary);
}

async function renderKpis() {
  const [ov, vis] = await Promise.all([api('overview'), api('visibility/summary').catch(() => [])]);
  const k = ov.kpis;
  const tot = vis.reduce((s, r) => s + r.gaps, 0);
  const obs = vis.filter((r) => r.visibility === 'coverage_observed').reduce((s, r) => s + r.gaps, 0);
  const tile = (l, v, s, c = '') => `<div class="kpi ${c}"><div class="kpi-label">${l}</div><div class="kpi-value">${v}</div><div class="kpi-sub">${s}</div></div>`;
  document.getElementById('an-kpis').innerHTML = [
    tile('AIS gaps detected', fmtInt(k.ais_gaps), 'Class A ≥ 60 min, Class B ≥ 120 min'),
    tile('Potential AIS anomalies', fmtInt(k.elevated_gaps), 'score > 60 (Suspicious + High Priority)', 'warn'),
    tile('High-priority events', fmtInt(k.high_priority_gaps), 'score > 80 · for investigation', 'alert'),
    tile('Vessels involved', fmtInt(k.anomaly_vessels), 'with at least one score > 60'),
    tile('Coverage observed', tot ? fmt(100 * obs / tot, 0) + '%' : '—', tot ? `${fmtInt(obs)} of ${fmtInt(tot)} elevated gaps had neighbours received nearby during the gap` : 'visibility analysis not loaded'),
    tile('Identity inconsistencies', fmtInt(k.identity_flagged), 'potential, MMSI-level'),
  ].join('');
}

function topFactors(r) {
  return FACTORS.map(([k, n]) => [n, pts(r, k)]).filter(([, v]) => v >= 5).sort((a, b) => b[1] - a[1]).slice(0, 2)
    .map(([n, v]) => `${n} (${fmt(v, 0)})`).join(' · ');
}

function renderTable() {
  const t = document.getElementById('an-table');
  t.innerHTML = `<thead><tr><th class="num">Score</th><th>Priority</th><th>Vessel</th><th>Gap start → end (UTC)</th><th class="num">Duration</th>
    <th class="num">Distance</th><th class="num">Implied / reported</th><th class="num">To port</th><th>Top factors (pts)</th></tr></thead>
    <tbody>${rows.slice(0, 300).map((r, i) => `<tr data-i="${i}"><td class="num"><b>${r.risk_score}</b></td><td>${pill(r.risk_level)}</td>
      <td>${esc(r.vessel_name || 'MMSI ' + r.mmsi)}<div class="muted small">MMSI ${r.mmsi} · ${esc(r.vessel_group)} · Class ${esc(r.transceiver)}</div></td>
      <td class="mono">${fmtTs(r.start_ts)}<div class="muted">→ ${fmtTs(r.end_ts)}</div></td><td class="num">${fmtH(r.gap_h)}</td>
      <td class="num">${fmt(r.dist_nm)} nm</td><td class="num">${fmt(r.implied_kn)} / ${fmt(r.avg_reported_sog)} kn</td>
      <td class="num">${fmt(r.start_dist_to_port_nm, 0)} nm</td>
      <td class="small" title="${esc(r.explanation || '')}">${esc(topFactors(r))}</td></tr>`).join('')
      || '<tr><td colspan="9" class="muted">No gap matches these filters.</td></tr>'}</tbody>`;
  t.querySelectorAll('tbody tr[data-i]').forEach((tr) => tr.addEventListener('click', () => {
    t.querySelectorAll('tr.sel').forEach((x) => x.classList.remove('sel')); tr.classList.add('sel');
    selectGap(rows[Number(tr.dataset.i)].gap_id);
  }));
}

function fact(label, value) { return `<div class="fact"><span>${label}</span><b>${value}</b></div>`; }

export async function selectGap(gapId, scroll = true) {
  const r = await api(`anomalies/gap/${encodeURIComponent(gapId)}`);
  document.getElementById('an-detail-pill').innerHTML = pill(r.risk_level);
  const ranked = FACTORS.map(([k, n]) => [k, n, pts(r, k)]).sort((a, b) => b[2] - a[2]);
  const maxPts = Math.max(1, ...ranked.map((f) => 100 * (W[f[0]] || 0)));
  document.getElementById('an-detail').innerHTML = `
    <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:8px">
      <div><b style="font-size:14px;color:${C.prussian}">${esc(r.vessel?.vessel_name || 'MMSI ' + r.mmsi)}</b>
        <div class="muted small">MMSI ${r.mmsi} · ${esc(r.vessel_group)} · Class ${esc(r.transceiver)}</div></div>
      <button class="btn" id="an-open">Open vessel on map →</button></div>
    <div class="score-split">
      <div><span class="muted small">Anomaly score</span><b>${r.risk_score}</b><span class="muted small">${esc(r.risk_level)}</span></div>
      <div><span class="muted small">Rule score (60%)</span><b>${fmt(r.rule_score, 0)}</b></div>
      <div><span class="muted small">MLlib score (40%)</span><b>${fmt(r.ml_score, 0)}</b><span class="muted small">KMeans cluster ${r.cluster ?? '—'}</span></div>
    </div>
    <div class="facts">
      ${fact('Gap start (last report)', fmtTs(r.start_ts) + ' UTC')}${fact('Gap end (recovered)', fmtTs(r.end_ts) + ' UTC')}
      ${fact('Gap duration', fmtH(r.gap_h))}${fact('Distance during gap', fmt(r.dist_nm) + ' nm')}
      ${fact('Last known position', `${fmt(r.start_lat, 4)}, ${fmt(r.start_lon, 4)}`)}${fact('Recovered position', `${fmt(r.end_lat, 4)}, ${fmt(r.end_lon, 4)}`)}
      ${fact('Implied speed', fmt(r.implied_kn) + ' kn')}${fact('Reported SOG before / after', `${fmt(r.sog_before)} / ${fmt(r.sog_after)} kn`)}
      ${fact('Course change', fmt(r.course_change_deg, 0) + '°')}${fact('Local solar time at start', fmt(r.local_hour, 1) + ' h')}
      ${fact('Nearest port at start', `${esc(r.start_port_name || '—')} · ${fmt(r.start_dist_to_port_nm)} nm`)}${fact('Nearest port at end', `${esc(r.end_port_name || '—')} · ${fmt(r.end_dist_to_port_nm)} nm`)}
      ${fact('Vessels in cell-hour', fmtInt(r.density_start))}${fact('Vessel gaps / day', fmt(r.vessel_gaps_per_day, 2))}
    </div>
    <div class="subhead">Contributing factors (rule score points, largest first)</div>
    <div class="factor-list">${ranked.map(([k, n, v], i) => `<div class="factor"><span>${n}</span>
      <div class="bar"><span class="${i < 2 && v >= 5 ? 'top' : ''}" style="width:${Math.round(100 * v / maxPts)}%"></span></div><span class="num small">${fmt(v, 1)}</span></div>`).join('')}</div>
    <p class="note">${r.stationary_in_port ? 'Stationary in port on both sides: rule score down-weighted ×0.3 (routine low-rate reporting). ' : ''}
      Potential AIS anomaly for review; this is not evidence of intentional AIS disabling or illegal activity.</p>`;
  document.getElementById('an-open').onclick = () => openVessel(r.mmsi);
  renderVisibility(r);
  const coords = [[r.start_lon, r.start_lat], [r.end_lon, r.end_lat]];
  const nb = r.visibility?.neighbours || [];
  const feats = [
    { type: 'Feature', geometry: { type: 'LineString', coordinates: coords }, properties: { kind: 'gap' } },
    point(r.start_lon, r.start_lat, { kind: 'last', t: r.start_epoch }), point(r.end_lon, r.end_lat, { kind: 'recovered', t: r.end_epoch }),
    ...nb.flatMap((n) => [point(n.closest_lon, n.closest_lat, { kind: 'nb', mmsi: n.nb_mmsi, name: n.vessel_name, group: n.nb_group, d: n.min_dist_nm,
      t: n.closest_epoch, during: n.reports_during, near: n.reports_near_during }),
      { type: 'Feature', geometry: { type: 'LineString', coordinates: [[n.closest_lon, n.closest_lat], n.side_start ? coords[0] : coords[1]] }, properties: { kind: 'link' } }]),
  ];
  setSource(map, 'selgap', fc(feats));
  fitTo(map, [...coords, ...nb.map((n) => [n.closest_lon, n.closest_lat])], 70, 10);
  if (scroll) document.getElementById('an-detail-panel').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

// ---- AIS proximity / visibility event: who was received around the silent vessel
function renderVisibility(r) {
  const el = document.getElementById('an-vis');
  const v = r.visibility;
  if (!v) {
    el.innerHTML = `<p class="muted">Visibility analysis is computed for Suspicious and High Priority gaps (score ≥ 61). This gap scores ${r.risk_score}.</p>`;
    return;
  }
  const nb = (v.neighbours || []).slice(0, 8);
  const pad = 2 * 3600;
  const t0 = r.start_epoch - pad, t1 = r.end_epoch + pad;
  const Wd = 640, L = 168, R = 70, rowH = 26, top = 34;
  const H = top + rowH * (nb.length + 1) + 30;
  const x = (t) => L + (Wd - L - R) * Math.min(1, Math.max(0, (t - t0) / (t1 - t0)));
  const gx0 = x(r.start_epoch), gx1 = x(r.end_epoch);
  const ticks = (arr, y, color, h = 12) => (arr || []).map((t) => `<line x1="${x(t).toFixed(1)}" x2="${x(t).toFixed(1)}" y1="${y - h / 2}" y2="${y + h / 2}" stroke="${color}" stroke-width="1.6"/>`).join('');
  const nameOf = (n) => esc((n.vessel_name || 'MMSI ' + n.nb_mmsi).slice(0, 18));
  const rowsSvg = nb.map((n, i) => {
    const y = top + rowH * (i + 1) + rowH / 2;
    const col = n.reports_near_during > 0 ? C.teal : '#9FB3C0';
    return `<text x="8" y="${y + 4}" class="${n.reports_near_during > 0 ? 'lbl' : ''}">${String.fromCharCode(66 + i)} · ${nameOf(n)}</text>
      <text x="${L - 8}" y="${y + 4}" text-anchor="end">${fmt(n.min_dist_nm, 1)} nm</text>
      <line x1="${L}" x2="${Wd - R}" y1="${y}" y2="${y}" stroke="#EEF2F4"/>
      ${ticks(n.timeline, y, col)}
      <text x="${Wd - R + 6}" y="${y + 4}">${n.reports_near_during}/${n.reports_during}</text>`;
  }).join('');
  const yA = top + rowH / 2;
  const tickLabels = [r.start_epoch, r.end_epoch].map((t, i) => `<text x="${x(t)}" y="${H - 10}" text-anchor="${i ? 'start' : 'end'}">${i ? 'recovered' : 'last report'} ${fmtTs(t).slice(5)}</text>`).join('');
  el.innerHTML = `
    <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:6px">
      <span class="vis-status ${v.visibility}">${esc(v.visibility_text)}</span>
      <span class="muted small">${v.neighbours_total} vessels within 10 nm (±30 min) · ${v.neighbours_reporting_during} kept reporting · ${v.neighbours_near_during} near the position during the gap</span>
    </div>
    <svg class="vis-diagram" viewBox="0 0 ${Wd} ${H}" role="img" aria-label="AIS reporting timeline around the gap">
      <rect x="${gx0}" y="${top - 6}" width="${Math.max(2, gx1 - gx0)}" height="${rowH * (nb.length + 1) + 6}" fill="${RISK[r.risk_level] || RISK.Watch}" opacity=".08"/>
      <line x1="${gx0}" x2="${gx0}" y1="${top - 10}" y2="${H - 24}" stroke="${C.prussian}" stroke-dasharray="3 3"/>
      <line x1="${gx1}" x2="${gx1}" y1="${top - 10}" y2="${H - 24}" stroke="${C.prussian}" stroke-dasharray="3 3"/>
      <text x="${(gx0 + gx1) / 2}" y="${top - 12}" text-anchor="middle" class="lbl">no reports from A · ${fmtH(r.gap_h)}</text>
      <text x="8" y="${yA + 4}" class="lbl">A · ${esc((r.vessel?.vessel_name || 'MMSI ' + r.mmsi).slice(0, 18))}</text>
      <text x="${L - 8}" y="${yA + 4}" text-anchor="end">target</text>
      <line x1="${L}" x2="${Wd - R}" y1="${yA}" y2="${yA}" stroke="#E3E9ED"/>
      ${ticks(v.target_timeline, yA, C.prussian, 14)}
      <text x="${Wd - R + 6}" y="${top - 12}">near/all</text>
      ${rowsSvg}
      ${tickLabels}
    </svg>
    <div class="vis-legend"><span><i style="border-color:${C.prussian}"></i>A: AIS reports of the vessel with the gap</span>
      <span><i style="border-color:${C.teal}"></i>neighbour received within 10 nm during the gap</span>
      <span><i style="border-color:#9FB3C0"></i>neighbour (not near the position during the gap)</span></div>
    <p class="note">Each tick is a 15-minute AIS position report. The diagram shows AIS reporting/observation around the event, not radio communication: the dataset contains no message content or vessel-to-vessel exchanges.
      ${v.visibility === 'coverage_observed' ? 'Because nearby vessels kept being received around the last known position, a receiver-coverage hole is a less likely explanation; the gap still remains a potential anomaly for review, not proof of intent.' : ''}</p>`;
}

function renderSummary(s) {
  const hist = s.score_histogram.filter((h) => h.from !== 'other');
  const binColor = (from) => from > 80 ? RISK['High Priority'] : from > 60 ? RISK.Suspicious : from >= 30 ? RISK.Watch : RISK.Normal;
  chart('an-hist').setOption(baseOption({
    grid: { left: 52, right: 12, top: 14, bottom: 30 },
    tooltip: { trigger: 'item', formatter: (p) => `score ${hist[p.dataIndex].from}-${hist[p.dataIndex].from + 9}: ${fmtInt(p.value)} gaps` },
    xAxis: xAxis({ type: 'category', data: hist.map((h) => `${h.from}`), name: 'score', nameLocation: 'middle', nameGap: 20 }),
    yAxis: yAxis({ type: 'log', name: 'gaps (log)', min: 1 }),
    series: [{ type: 'bar', barCategoryGap: '8%', data: hist.map((h) => ({ value: h.count, itemStyle: { color: binColor(h.from), borderRadius: [3, 3, 0, 0] } })) }],
  }), true);
  insight('ins-an-distribution', ins.distribution);

  const dates = [...new Set(s.by_day.map((d) => d.date))].sort();
  chart('an-days').setOption(baseOption({
    legend: { data: RISK_ORDER.slice(1) },
    grid: { left: 46, right: 12, top: 28, bottom: 26 },
    xAxis: xAxis({ type: 'category', data: dates.map((d) => d.slice(5)) }),
    yAxis: yAxis({ type: 'value' }),
    series: RISK_ORDER.slice(1).map((l, i, a) => ({ name: l, type: 'bar', stack: 'd', barMaxWidth: 26,
      itemStyle: { color: RISK[l], borderColor: '#fff', borderWidth: 1, borderRadius: i === a.length - 1 ? [3, 3, 0, 0] : 0 },
      data: dates.map((d) => s.by_day.find((x) => x.date === d && x.level === l)?.count || 0) })),
  }), true);
  insight('ins-an-byday', ins.by_day);
  insight('ins-an-proximity', ins.proximity);
}

async function renderFactors() {
  const hp = await api('anomalies', { level: 'High Priority', limit: 1000 });
  const avg = FACTORS.map(([k, n]) => [n, hp.reduce((s, r) => s + pts(r, k), 0) / Math.max(1, hp.length)]).sort((a, b) => a[1] - b[1]);
  chart('an-factors').setOption(baseOption({
    grid: { left: 190, right: 40, top: 6, bottom: 22 },
    tooltip: { trigger: 'item', formatter: (p) => `${p.name}: ${fmt(p.value)} pts on average (${hp.length} High Priority gaps)` },
    xAxis: yAxis({ type: 'value' }),
    yAxis: xAxis({ type: 'category', data: avg.map((a) => a[0]), axisLabel: { color: C.ink2, fontSize: 11 } }),
    series: [{ type: 'bar', barWidth: 10, data: avg.map((a, i) => ({ value: +a[1].toFixed(2), itemStyle: { color: i >= avg.length - 2 ? C.prussian : C.marine, borderRadius: [0, 3, 3, 0] } })),
      label: { show: true, position: 'right', color: C.ink2, fontSize: 10.5, formatter: (p) => fmt(p.value) } }],
  }), true);
  insight('ins-an-factors', ins.factors);
}

async function renderVessels() {
  const vs = await api('anomalies/vessels', { limit: 40 });
  const t = document.getElementById('an-vessels');
  t.innerHTML = `<thead><tr><th>Vessel</th><th>Type</th><th class="num">Max score</th><th class="num">Elevated gaps</th>
    <th class="num">High priority</th><th class="num">Gap hours</th><th class="num">All gaps</th></tr></thead>
    <tbody>${vs.map((v) => `<tr data-mmsi="${v.mmsi}"><td>${esc(v.vessel_name || 'MMSI ' + v.mmsi)}<div class="muted small">MMSI ${v.mmsi} · Class ${esc(v.transceiver)}${v.identity_flag ? ' · identity flag' : ''}</div></td>
      <td>${esc(v.vessel_group)}</td><td class="num"><b>${v.max_score}</b></td><td class="num">${v.elevated_gaps}</td>
      <td class="num">${v.high_priority}</td><td class="num">${fmt(v.total_gap_h, 0)}</td><td class="num">${fmtInt(v.all_gaps)}</td></tr>`).join('')}</tbody>`;
  t.querySelectorAll('tbody tr').forEach((tr) => tr.addEventListener('click', () => {
    document.getElementById('an-vessel').value = tr.dataset.mmsi;
    // show all of this vessel's gaps: clear the other filters
    ['an-prox', 'an-group', 'an-day'].forEach((i) => { document.getElementById(i).value = ''; });
    ['an-mingap', 'an-min'].forEach((i) => { document.getElementById(i).value = 0; });
    Object.keys(levels).forEach((l) => { levels[l] = true; });
    document.querySelectorAll('#an-levels .chip').forEach((c) => c.setAttribute('aria-pressed', 'true'));
    load().then(() => rows[0] && selectGap(rows[0].gap_id));
  }));
  insight('ins-an-vessels', ins.vessels);
}

async function renderMl() {
  const { model, clusters } = await api('ml');
  if (!model) return;
  document.getElementById('an-ml-caption').textContent =
    `VectorAssembler → StandardScaler → KMeans · k=${model.k} chosen by silhouette (${fmt(model.silhouette_by_k[model.k], 3)}) · ${fmtInt(model.training_rows)} gaps`;
  document.getElementById('an-clusters').innerHTML = `<thead><tr><th>Cluster</th><th>Profile (centroid vs average)</th><th class="num">Gaps</th>
    <th class="num">Mean gap</th><th class="num">Mean dist.</th><th class="num">Density</th><th class="num">Avg ML score</th></tr></thead>
    <tbody>${clusters.map((c) => `<tr><td><b>${c.cluster}</b></td><td class="wrap">${esc(c.description)}</td>
      <td class="num">${fmtInt(c.size)} <span class="muted small">(${fmt(100 * c.share, 1)}%)</span></td>
      <td class="num">${fmtH(c.mean_gap_h)}</td><td class="num">${fmt(c.mean_dist_nm)} nm</td><td class="num">${fmt(c.mean_density, 0)}</td>
      <td class="num">${fmt(c.avg_ml_score, 1)}</td></tr>`).join('')}</tbody>`;
  insight('ins-an-clusters', ins.clusters);
}

async function renderIdentity() {
  const idr = await api('identity', { flagged_only: true, limit: 300 });
  setSource(map, 'idv', fc(idr.filter((r) => r.last_lon != null).map((r) => point(r.last_lon, r.last_lat,
    { mmsi: r.mmsi, name: r.vessel_name, conflicts: r.conflict_events }))));
  const t = document.getElementById('an-identity');
  t.innerHTML = `<thead><tr><th>Vessel</th><th>Group</th><th class="num">Conflict jumps</th><th class="num">Simultaneous</th>
    <th class="num">Isolated outliers</th><th class="num">Max implied</th><th class="num">Days</th></tr></thead>
    <tbody>${idr.map((r) => `<tr data-mmsi="${r.mmsi}"><td>${esc(r.vessel_name || 'MMSI ' + r.mmsi)}<div class="muted small">MMSI ${r.mmsi}${r.n_names > 1 ? ` · ${r.n_names} names reported` : ''}</div></td>
      <td>${esc(r.vessel_group)}</td><td class="num"><b>${fmtInt(r.conflict_events)}</b></td><td class="num">${fmtInt(r.simultaneous_reports)}</td>
      <td class="num">${fmtInt(r.isolated_outliers)}</td><td class="num">${fmtInt(r.max_implied_kn)} kn</td><td class="num">${r.days_with_events}</td></tr>`).join('')
      || '<tr><td colspan="7" class="muted">No MMSI met the flag threshold.</td></tr>'}</tbody>`;
  t.querySelectorAll('tbody tr[data-mmsi]').forEach((tr) => tr.addEventListener('click', () => openVessel(Number(tr.dataset.mmsi))));
  insight('ins-an-identity', ins.identity);
}
