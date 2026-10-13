// Overview: KPIs, operational map (vessels, waiting vessels, ports, zones, anomalies,
// selected vessel + track), congestion / activity / anomaly / fleet charts with insights.
import { api } from '../api.js';
import { addAnomalyLayers, addPortLayer, addSelectedLayer, addTrackLayer, addVesselLayer, addZoneLayers,
  createMap, fc, fitTo, hoverPopup, point, portZones, setSource, trackFeatures } from '../map.js';
import { C, GROUP_COLORS, GROUP_ORDER, PCI_BANDS, RISK, RISK_ORDER, baseOption, chart, esc, fmt, fmtCompact,
  fmtDay, fmtH, fmtInt, fmtTs, fold, insight, pill, xAxis, yAxis } from '../theme.js';

let map, meta, data, ins, ports = [], hourly = [];
const layerState = { vessels: true, waiting: false, ports: true, zones: false, suspicious: true, high: true, track: true };
const groupState = Object.fromEntries(GROUP_ORDER.map((g) => [g, true]));
const RAW_GROUPS = {
  'Cargo': ['Cargo'], 'Tanker': ['Tanker'], 'Passenger': ['Passenger', 'High Speed Craft'],
  'Tug & service': ['Tug', 'Towing', 'Port Service', 'Dredging', 'Search and Rescue', 'Law Enforcement', 'Military'],
  'Fishing': ['Fishing'], 'Leisure & other': ['Pleasure Craft', 'Sailing', 'Other', 'Unknown'],
};
let playTimer = null;
let resolveReady;
export const ready = new Promise((r) => { resolveReady = r; });

export async function init(state) {
  meta = state.meta;
  let tins;
  [data, ins, ports, { hourly }, tins] = await Promise.all([api('overview'), api('insights/overview'), api('ports'), api('trends'),
    api('insights/trends')]);
  insight('ins-ov-scope', ins.scope);
  insight('ins-ov-activity', tins.vessels);
  renderKpis(data.kpis);
  renderCharts(data);
  await initMap();
  resolveReady();
}

export function activate() { map?.resize(); }

function kpi(label, value, sub = '', cls = '', id = '') {
  return `<div class="kpi ${cls}" ${id ? `id="${id}"` : ''}><div class="kpi-label">${label}</div><div class="kpi-value">${value}</div><div class="kpi-sub">${sub}</div></div>`;
}

function renderKpis(k) {
  document.getElementById('ov-kpis').innerHTML = [
    kpi('AIS records', fmtCompact(k.raw_rows), `${fmtCompact(k.clean_rows)} valid after cleaning`),
    kpi('Unique vessels', fmtInt(k.vessels), 'distinct MMSI in the week'),
    kpi('Ports analysed', fmtInt(k.ports_monitored), 'curated berth + anchorage zones'),
    kpi('Waiting / at berth', '—', 'cargo & tanker at map time', '', 'ov-kpi-waiting'),
    kpi('Avg turnaround', fmtH(k.avg_turnaround_h), `${fmtInt(k.port_visits)} commercial visits`),
    kpi('Avg waiting time', fmtH(k.avg_waiting_h), 'anchorage before first berth'),
    kpi('Avg berth time', fmtH(k.avg_dwell_h), 'first to last berth fix'),
    kpi('AIS anomalies', fmtInt(k.elevated_gaps), `score > 60 of ${fmtInt(k.ais_gaps)} gaps · ${fmtInt(k.anomaly_vessels)} vessels`, 'warn'),
    kpi('High-priority', fmtInt(k.high_priority_gaps), `events for investigation · ${fmtInt(k.identity_flagged)} identity flags`, 'alert'),
  ].join('');
}

function renderCharts(d) {
  const top = d.ports.filter((p) => p.pci != null).slice(0, 24).reverse();
  chart('ov-pci').setOption(baseOption({
    grid: { left: 165, right: 34, top: 6, bottom: 22 },
    tooltip: { trigger: 'item', formatter: (p) => {
      const r = top[p.dataIndex];
      return `<b>${esc(r.port_name)}</b><br>PCI ${fmt(r.pci)} · ${r.pci_band}<br>avg queue ${fmt(r.avg_queue, 2)} · waiting ${fmtH(r.avg_waiting_h)}<br>utilisation ${fmt(100 * r.utilization, 0)}% · arrivals ${fmtInt(r.arrivals)}<br><span style="color:${C.ink3}">click for port analysis</span>`;
    } },
    xAxis: yAxis({ type: 'value', max: 100 }),
    yAxis: xAxis({ type: 'category', data: top.map((p) => p.port_name), axisLabel: { color: C.ink2, fontSize: 11, width: 155, overflow: 'truncate', interval: 0 } }),
    series: [{ type: 'bar', barWidth: 11, data: top.map((p) => ({ value: p.pci, itemStyle: { color: PCI_BANDS[p.pci_band], borderRadius: [0, 4, 4, 0] } })),
      label: { show: true, position: 'right', color: C.ink2, fontSize: 11, formatter: (p) => fmt(p.value, 0) } }],
  }));
  chart('ov-pci').on('click', (p) => openPort(top[p.dataIndex].port_id));
  insight('ins-ov-pci', ins.pci);

  chart('ov-risk').setOption(baseOption({
    grid: { left: 92, right: 54, top: 4, bottom: 4 },
    tooltip: { trigger: 'item', formatter: (p) => `${p.name}: ${fmtInt(p.value)} gaps` },
    xAxis: { type: 'value', show: false },
    yAxis: xAxis({ type: 'category', data: RISK_ORDER, inverse: true, axisLine: { show: false }, axisLabel: { color: C.ink2, fontSize: 11.5 } }),
    series: [{ type: 'bar', barWidth: 12,
      data: d.risk_by_level.map((r) => ({ value: r.count, itemStyle: { color: RISK[r.level], borderRadius: [0, 4, 4, 0] } })),
      label: { show: true, position: 'right', color: C.ink2, fontSize: 11, formatter: (p) => fmtInt(p.value) } }],
  }));
  insight('ins-ov-risk', ins.risk);

  const folded = {};
  d.vessels_by_group.forEach((g) => { const f = fold(g.group); folded[f] = (folded[f] || 0) + g.vessels; });
  chart('ov-fleet').setOption(baseOption({
    grid: { left: 108, right: 54, top: 4, bottom: 4 },
    tooltip: { trigger: 'item', formatter: (p) => `${p.name}: ${fmtInt(p.value)} vessels` },
    xAxis: { type: 'value', show: false },
    yAxis: xAxis({ type: 'category', data: GROUP_ORDER, inverse: true, axisLine: { show: false }, axisLabel: { color: C.ink2, fontSize: 11.5 } }),
    series: [{ type: 'bar', barWidth: 12,
      data: GROUP_ORDER.map((g) => ({ value: folded[g] || 0, itemStyle: { color: GROUP_COLORS[g], borderRadius: [0, 4, 4, 0] } })),
      label: { show: true, position: 'right', color: C.ink2, fontSize: 11, formatter: (p) => fmtInt(p.value) } }],
  }));
  insight('ins-ov-fleet', ins.fleet);
  renderActivity();
}

function renderActivity() {
  const ct = currentT();
  const t = ct ? ct - 3599 : null;
  chart('ov-activity').setOption(baseOption({
    grid: { left: 44, right: 10, top: 8, bottom: 22 },
    tooltip: { trigger: 'axis', formatter: (ps) => `<b>${fmtDay(ps[0].value[0] / 1000)} UTC</b><br>${fmtInt(ps[0].value[1])} vessels reporting` },
    xAxis: xAxis({ type: 'time', axisLabel: { color: C.ink3, fontSize: 10.5, formatter: '{ee} {dd}' } }),
    yAxis: yAxis({ type: 'value', min: (v) => Math.floor(v.min * 0.95 / 1000) * 1000, axisLabel: { color: C.ink3, fontSize: 10.5, formatter: (v) => fmtCompact(v) } }),
    series: [{ type: 'line', showSymbol: false, lineStyle: { width: 1.6, color: C.teal }, itemStyle: { color: C.teal },
      areaStyle: { color: C.teal + '14' }, data: hourly.map((h) => [h.hour_epoch * 1000, h.active_vessels]),
      markLine: t && meta ? { symbol: 'none', silent: true, label: { show: false }, lineStyle: { color: C.prussian, type: 'solid', width: 1.2 },
        data: [{ xAxis: t * 1000 }] } : undefined }],
  }), { replaceMerge: ['series'] });
}

// ------------------------------------------------------------------ map
function currentT() {
  if (!meta) return null;
  const h = Number(document.getElementById('ov-time').value);
  return meta.window_start_epoch + h * 3600 + 3599;
}

async function initMap() {
  const hours = Math.floor((meta.window_end_epoch - meta.window_start_epoch) / 3600);
  const slider = document.getElementById('ov-time');
  slider.max = hours; slider.value = Math.min(hours, 36);
  map = createMap('ov-map');
  await map.ready;

  setSource(map, 'zones', portZones(ports));
  addZoneLayers(map, 'zones');
  setSource(map, 'vessels', fc([]));
  addVesselLayer(map, 'vessels', 'vessels');
  setSource(map, 'anoms', fc([]));
  addAnomalyLayers(map, 'anoms');
  setSource(map, 'ports', fc(ports.map((p) => point(p.port_lon, p.port_lat, p))));
  addPortLayer(map, 'ports');
  setSource(map, 'track', fc([]));
  addTrackLayer(map, 'track');
  addSelectedLayer(map, 'selected');

  hoverPopup(map, 'vessels', (p) => `<b>${esc(p.name || 'MMSI ' + p.mmsi)}</b><br>${esc(p.g)} · ${p.s ?? '—'} kn${p.w == 1 ? '<br><b style="color:#9A6B12">waiting at anchorage</b>' : p.z === 'BERTH' ? '<br>at berth' : ''}<br><span class="muted">${fmtTs(+p.t)} UTC</span>`);
  hoverPopup(map, 'ports-circle', (p) => `<b>${esc(p.port_name)}</b><br>PCI ${p.pci === 'null' || p.pci == null ? 'n/a' : fmt(+p.pci)} · ${esc(p.pci_band)}<br>arrivals ${fmtInt(+p.arrivals)} · avg queue ${fmt(+p.avg_queue, 2)}<br><span class="muted">click for port analysis</span>`);
  hoverPopup(map, 'anoms', (p) => `${pill(p.risk_level)} score ${p.risk_score}<br><b>${esc(p.name || 'MMSI ' + p.mmsi)}</b><br>gap ${fmt(+p.gap_h)} h from ${fmtTs(+p.start_epoch)} UTC<br><span class="muted">click to open in AIS Anomalies</span>`);
  map.on('click', 'vessels', (e) => {
    const f = e.features[0];
    selectVessel(f.properties.mmsi, { ...f.properties, lon: f.geometry.coordinates[0], lat: f.geometry.coordinates[1] });
  });
  map.on('click', 'anoms', (e) => openAnomaly(e.features[0].properties.gap_id));
  map.on('click', 'ports-circle', (e) => openPort(e.features[0].properties.port_id));

  const portSel = document.getElementById('ov-port');
  portSel.innerHTML += ports.slice().sort((a, b) => a.port_name.localeCompare(b.port_name))
    .map((p) => `<option value="${p.port_id}">${esc(p.port_name)}</option>`).join('');
  portSel.addEventListener('change', () => {
    const p = ports.find((x) => x.port_id === portSel.value);
    if (p) map.flyTo({ center: [p.port_lon, p.port_lat], zoom: 8.6, duration: 700 });
    else map.flyTo({ center: [-96, 37.5], zoom: 3.3, duration: 700 });
    layerState.zones = !!p; syncLayers(); loadPositions();
  });

  renderLayerControls();
  renderLegend();
  slider.addEventListener('input', () => { updateTimeLabel(); debounceLoad(); });
  document.getElementById('ov-window').addEventListener('change', loadPositions);
  document.getElementById('ov-play').addEventListener('click', togglePlay);
  updateTimeLabel();
  await Promise.all([loadPositions(), loadAnomalies()]);
}

function updateTimeLabel() { document.getElementById('ov-time-label').textContent = fmtDay(currentT() - 3599) + ' UTC'; }

let loadTimer;
function debounceLoad() { clearTimeout(loadTimer); loadTimer = setTimeout(loadPositions, 160); }

let loadSeq = 0;
async function loadPositions() {
  const groups = Object.entries(groupState).filter(([, on]) => on).map(([g]) => g);
  const raw = Object.entries(RAW_GROUPS).filter(([k]) => groups.includes(k)).flatMap(([, v]) => v);
  if (!raw.length) { setSource(map, 'vessels', fc([])); return; }
  const w = document.getElementById('ov-window').value;
  const port = document.getElementById('ov-port').value;
  const seq = ++loadSeq;
  const el = document.getElementById('ov-map');
  el.classList.add('loading');
  const params = { t: currentT(), groups: raw.join(','), window_hours: w };
  if (port) params.port_id = port;
  if (layerState.waiting) params.waiting_only = true;
  const gj = await api('positions', params, { useCache: false });
  if (seq !== loadSeq) return;           // a newer request superseded this one
  el.classList.remove('loading');
  setSource(map, 'vessels', gj);
  const pname = port ? ports.find((p) => p.port_id === port)?.port_name : null;
  document.getElementById('ov-map-caption').textContent =
    `${fmtInt(gj.count)} vessels${pname ? ' in the ' + pname + ' area' : ''} · latest fix within ${w} h of ${fmtDay(gj.bucket)} UTC`;
  const k = document.querySelector('#ov-kpi-waiting .kpi-value');
  if (k) k.textContent = `${fmtInt(gj.counts.waiting)} / ${fmtInt(gj.counts.at_berth)}`;
  const kSub = document.querySelector('#ov-kpi-waiting .kpi-sub');
  if (kSub) kSub.textContent = `cargo & tanker · ${fmtDay(gj.bucket)} UTC`;
  insight('ins-ov-map', `At ${fmtDay(gj.bucket)} UTC, ${fmtInt(gj.count)} vessels${pname ? ' in the ' + pname + ' area' : ''} had reported within the previous ${w} h; ` +
    `${fmtInt(gj.counts.waiting)} cargo/tanker vessels were waiting at a monitored anchorage and ${fmtInt(gj.counts.at_berth)} were at berth ` +
    `(${fmtInt(gj.counts.in_port_area)} vessels of all types inside monitored port areas).`);
  renderActivity();
}

async function loadAnomalies() {
  const rows = await api('anomalies', { level: 'High Priority,Suspicious', fields: 'map', limit: 3000 });
  setSource(map, 'anoms', fc(rows.map((r) => point(r.start_lon, r.start_lat, {
    gap_id: r.gap_id, mmsi: r.mmsi, risk_level: r.risk_level, risk_score: r.risk_score, gap_h: r.gap_h,
    start_epoch: r.start_epoch, name: r.vessel_name }))));
  syncLayers();
}

function togglePlay() {
  const btn = document.getElementById('ov-play');
  const slider = document.getElementById('ov-time');
  if (playTimer) { clearInterval(playTimer); playTimer = null; btn.innerHTML = '<svg viewBox="0 0 16 16"><path d="M4 3l9 5-9 5z" fill="currentColor"/></svg>'; return; }
  btn.innerHTML = '<svg viewBox="0 0 16 16"><path d="M4 3h3v10H4zM9 3h3v10H9z" fill="currentColor"/></svg>';
  playTimer = setInterval(() => {
    slider.value = (Number(slider.value) + 1) % (Number(slider.max) + 1);
    updateTimeLabel(); loadPositions();
  }, 1100);
}

const LAYER_IDS = { vessels: ['vessels'], ports: ['ports-circle', 'ports-label'], zones: ['zones-fill', 'zones-line'],
  track: ['track-line', 'track-pts'] };
function syncLayers() {
  if (!map?.getLayer('vessels')) return;
  Object.entries(LAYER_IDS).forEach(([k, ls]) => ls.forEach((l) => map.setLayoutProperty(l, 'visibility', layerState[k] ? 'visible' : 'none')));
  const levels = [layerState.suspicious && 'Suspicious', layerState.high && 'High Priority'].filter(Boolean);
  map.setFilter('anoms', ['in', ['get', 'risk_level'], ['literal', levels]]);
  map.setLayoutProperty('anoms-ring', 'visibility', layerState.high ? 'visible' : 'none');
  const z = document.querySelector('#ov-layers input[data-layer=zones]');
  if (z) z.checked = layerState.zones;
}

function renderLayerControls() {
  const L = [['vessels', 'Vessels'], ['waiting', 'Only waiting vessels'], ['ports', 'Ports (PCI)'], ['zones', 'Port zones'],
    ['suspicious', 'Suspicious AIS gaps'], ['high', 'High-priority AIS gaps'], ['track', 'Selected vessel track']];
  const el = document.getElementById('ov-layers');
  el.innerHTML = `<h3>Layers</h3>${L.map(([k, n]) => `<label><input type="checkbox" data-layer="${k}" ${layerState[k] ? 'checked' : ''}> ${n}</label>`).join('')}
    <div class="sep"></div><h3>Vessel groups</h3>
    ${GROUP_ORDER.map((g) => `<label><input type="checkbox" data-group="${g}" checked><span class="swatch" style="background:${GROUP_COLORS[g]}"></span>${g}</label>`).join('')}`;
  syncLayers();
  el.addEventListener('change', (e) => {
    const t = e.target;
    if (t.dataset.layer) {
      layerState[t.dataset.layer] = t.checked; syncLayers();
      if (t.dataset.layer === 'waiting') loadPositions();
    }
    if (t.dataset.group) { groupState[t.dataset.group] = t.checked; loadPositions(); }
  });
}

function renderLegend() {
  const dot = (bg, ring = '#fff', size = 9) => `<span class="swatch" style="background:${bg};border-color:${ring};width:${size}px;height:${size}px"></span>`;
  document.getElementById('ov-legend').innerHTML = `<h3>Markers</h3>
    <div class="row">${dot(GROUP_COLORS.Cargo)}Vessel (colour = group)</div>
    <div class="row">${dot(GROUP_COLORS.Tanker, C.amber, 11)}Waiting at anchorage</div>
    <div class="row">${dot(PCI_BANDS.High, '#fff', 12)}Port (size = arrivals)</div>
    <div class="row">${dot(RISK.Suspicious)}Suspicious AIS gap</div>
    <div class="row"><span class="swatch" style="background:${RISK['High Priority']};box-shadow:0 0 0 3px #fff,0 0 0 4.5px ${RISK['High Priority']}"></span>&nbsp;High-priority gap</div>
    <div class="row"><span class="swatch" style="background:${C.prussian};box-shadow:0 0 0 3px #fff,0 0 0 5px ${C.prussian}"></span>&nbsp;Selected vessel</div>
    <h3 style="margin-top:6px">Port congestion</h3>
    ${['Low', 'Moderate', 'High', 'Severe'].map((b) => `<div class="row">${dot(PCI_BANDS[b])}${b}</div>`).join('')}`;
}

// ------------------------------------------------------------------ cross-view navigation
function openPort(portId) {
  import('./ports.js').then(async (m) => { location.hash = '#ports'; await m.ready; m.selectPort(portId, true); });
}
function openAnomaly(gapId) {
  import('./anomalies.js').then(async (m) => { location.hash = '#anomalies'; await m.ready; m.selectGap(gapId); });
}
function openPerformance(mmsi) {
  import('./vessels.js').then(async (m) => { location.hash = '#vessels'; await m.ready; m.selectVessel(mmsi); });
}

// ------------------------------------------------------------------ selected vessel
export async function selectVessel(mmsi, atTime = null) {
  await ready.catch(() => {});
  if (!map) return;
  await map.ready;
  const [v, tr] = await Promise.all([api(`vessels/${mmsi}`), api(`vessels/${mmsi}/track`).catch(() => null)]);
  const gapPts = (v.gaps || []).filter((g) => g.risk_score > 30).map((g) => point(g.start_lon, g.start_lat, { kind: 'gap' }));
  setSource(map, 'track', trackFeatures(tr?.points, gapPts));
  // halo at the clicked position (map time) or, from search, at the last AIS report
  const selCoords = atTime?.lon != null ? [atTime.lon, atTime.lat] : (v.last_lon != null ? [v.last_lon, v.last_lat] : null);
  setSource(map, 'selected', fc(selCoords ? [point(selCoords[0], selCoords[1], { mmsi })] : []));
  if (tr?.points?.length) fitTo(map, tr.points.map((p) => [p[1], p[2]]));
  else if (selCoords) map.flyTo({ center: selCoords, zoom: 8 });

  const ctx = v.port_context || {};
  const gaps = v.gaps || [];
  const worst = gaps.slice().sort((a, b) => b.risk_score - a.risk_score)[0];
  const card = document.getElementById('ov-vessel');
  const ap = atTime && atTime.mmsi ? atTime : null;
  card.innerHTML = `<button class="close" aria-label="Close">×</button>
    <h4>${esc(v.vessel_name || 'Unnamed vessel')}</h4>
    <div class="muted small">MMSI ${v.mmsi}${v.imo ? ' · ' + esc(v.imo) : ''}${v.call_sign ? ' · ' + esc(v.call_sign) : ''}</div>
    <div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">${pill(v.vessel_risk_level || 'Normal')}
      ${v.flagged ? '<span class="pill flag">Potential MMSI identity inconsistency</span>' : ''}</div>
    <dl class="kv">
      <dt>Vessel type</dt><dd>${esc(v.vessel_group)} (AIS type ${v.vessel_type ?? '—'})</dd>
      <dt>Size</dt><dd>${v.length ?? '—'} × ${v.width ?? '—'} m</dd>
      <dt>Transceiver</dt><dd>Class ${esc(v.transceiver)}</dd>
      ${ap ? `<dt>At map time</dt><dd>${fmtTs(+ap.t)} UTC</dd>
      <dt>&nbsp;· SOG / COG</dt><dd>${ap.s ?? '—'} kn / ${ap.c ?? '—'}°</dd>
      <dt>&nbsp;· Port state</dt><dd>${ap.w == 1 ? 'waiting at anchorage' : esc(ap.z || 'outside port areas')}</dd>` : ''}
      <dt>Last AIS report</dt><dd>${fmtTs(v.last_ts)} UTC</dd>
      <dt>Latest position</dt><dd>${fmt(v.last_lat, 4)}, ${fmt(v.last_lon, 4)}</dd>
      <dt>SOG · COG · heading</dt><dd>${v.last_sog ?? '—'} kn · ${v.last_cog ?? '—'}° · ${v.last_heading ?? '—'}°</dd>
      <dt>Nav status</dt><dd>${esc(v.status_text || '—')}</dd>
      <dt>Current port area</dt><dd>${esc(ctx.current_port || '—')}${ctx.current_state ? ' (' + esc(ctx.current_state.toLowerCase()) + ')' : ''}</dd>
      <dt>Previous port</dt><dd>${ctx.previous_port ? esc(ctx.previous_port.port) + ' · left ' + fmtTs(ctx.previous_port.departure_ts) : '—'}</dd>
      <dt>Reporting</dt><dd>${fmtInt(v.fixes)} reports · median every ${v.median_report_interval_s != null ? fmt(v.median_report_interval_s / 60, 1) + ' min' : '—'}</dd>
      <dt>AIS gaps</dt><dd>${fmtInt(v.ais_gaps)} (${fmt(v.total_gap_h)} h total)</dd>
      <dt>Anomaly score</dt><dd>${v.max_risk_score ?? 0} / 100</dd>
      ${worst ? `<dt>Top gap</dt><dd>${fmtTs(worst.start_ts)} · ${fmtH(worst.gap_h)}</dd>` : ''}
    </dl>
    <div class="card-actions">
      <button class="btn" data-act="perf">Vessel performance →</button>
      ${worst && worst.risk_score > 60 ? `<button class="btn" data-act="gap">Open top gap →</button>` : ''}
    </div>`;
  card.hidden = false;
  card.querySelector('.close').onclick = () => { card.hidden = true; setSource(map, 'track', fc([])); setSource(map, 'selected', fc([])); };
  card.querySelector('[data-act=perf]').onclick = () => openPerformance(v.mmsi);
  const g = card.querySelector('[data-act=gap]');
  if (g) g.onclick = () => openAnomaly(worst.gap_id);
}
