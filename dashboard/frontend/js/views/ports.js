// Port Congestion: all-ports map, PCI ranking by component, waiting vs berth, turnaround,
// ranking table, then a selected-port analysis (KPIs, composition, zones, hourly, daily, groups).
import { api } from '../api.js';
import { addPortLayer, addVesselLayer, addZoneLayers, createMap, fc, fitTo, hoverPopup, point, portZones, setSource } from '../map.js';
import { C, GROUP_COLORS, PCI_BANDS, baseOption, chart, esc, fmt, fmtDay, fmtH, fmtInt, insight, xAxis, yAxis } from '../theme.js';

let ports = [], scored = [], current = null, map, allMap, W, initDone = false, pending = null;
let resolveReady;
export const ready = new Promise((r) => { resolveReady = r; });
const COMP = [['queue', 'norm_avg_queue', 'Anchorage queue', C.prussian], ['waiting', 'norm_avg_waiting_h', 'Waiting time', C.amber],
  ['dwell', 'norm_avg_dwell_h', 'Berth time', C.teal], ['utilization', 'norm_utilization', 'Utilisation', '#7FA7C4']];

export async function init(state) {
  W = state.meta.pci_weights;
  const ins = await api('insights/ports');
  ports = await api('ports');
  scored = ports.filter((p) => p.pci != null);
  const sel = document.getElementById('pc-port');
  sel.innerHTML = ports.map((p) => `<option value="${p.port_id}">${esc(p.port_name)}</option>`).join('');
  sel.addEventListener('change', () => selectPort(sel.value, true));
  renderRanking(); renderWaitBerth(); renderTurnaround(); renderTable();
  insight('ins-pc-ranking', ins.ranking);
  insight('ins-pc-comparison', ins.comparison);
  insight('ins-pc-waitberth', ins.waiting_berth);
  insight('ins-pc-turnaround', ins.turnaround);
  await initMaps();
  initDone = true;
  resolveReady();
  selectPort(pending || scored[0]?.port_id || ports[0].port_id);
}

export function activate() { map?.resize(); allMap?.resize(); }

async function initMaps() {
  allMap = createMap('pc-allmap', { zoom: 2.9, center: [-100, 33] });
  map = createMap('pc-map', { zoom: 8 });
  await Promise.all([allMap.ready, map.ready]);
  setSource(allMap, 'ports', fc(ports.map((p) => point(p.port_lon, p.port_lat, p))));
  addPortLayer(allMap, 'ports', { minRadius: 5 });
  hoverPopup(allMap, 'ports-circle', (p) => `<b>${esc(p.port_name)}</b><br>PCI ${p.pci == null || p.pci === 'null' ? 'n/a' : fmt(+p.pci)} · ${esc(p.pci_band)}<br>arrivals ${fmtInt(+p.arrivals)} · avg waiting ${fmtH(+p.avg_waiting_h)}`);
  allMap.on('click', 'ports-circle', (e) => selectPort(e.features[0].properties.port_id, true));
  document.getElementById('pc-legend').innerHTML = `<h3>Congestion band</h3>${['Low', 'Moderate', 'High', 'Severe', 'Insufficient data']
    .map((b) => `<div class="row"><span class="swatch" style="background:${PCI_BANDS[b]}"></span>${b}</div>`).join('')}`;

  setSource(map, 'zones', portZones(ports)); addZoneLayers(map, 'zones');
  setSource(map, 'pvessels', fc([])); addVesselLayer(map, 'pvessels', 'pvessels');
  setSource(map, 'ports', fc(ports.map((p) => point(p.port_lon, p.port_lat, p)))); addPortLayer(map, 'ports', { minRadius: 4 });
  hoverPopup(map, 'zones-fill', (p) => `<b>${esc(p.name)}</b><br>${p.kind} zone`);
  hoverPopup(map, 'pvessels', (p) => `<b>${esc(p.name || 'MMSI ' + p.mmsi)}</b><br>${esc(p.g)} · ${p.s ?? '—'} kn<br>${p.w == 1 ? 'waiting at anchorage' : esc((p.z || '').toLowerCase())}`);
}

function renderRanking() {
  const rows = scored.slice().reverse();     // highest at top of a horizontal bar chart
  document.getElementById('pc-rankchart').style.height = `${rows.length * 17 + 70}px`;
  chart('pc-rankchart').resize();
  chart('pc-rankchart').setOption(baseOption({
    legend: { data: COMP.map((c) => c[2]) },
    grid: { left: 150, right: 30, top: 30, bottom: 22 },
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, formatter: (ps) => {
      const p = rows[ps[0].dataIndex];
      return `<b>${esc(p.port_name)}</b> · PCI ${fmt(p.pci)} (${p.pci_band})<br>` + ps.map((q) => `${q.marker}${q.seriesName}: ${fmt(q.value)} pts`).join('<br>');
    } },
    xAxis: yAxis({ type: 'value', max: 100 }),
    yAxis: xAxis({ type: 'category', data: rows.map((p) => p.port_name), axisLabel: { color: C.ink2, fontSize: 10.5, width: 140, overflow: 'truncate', interval: 0 } }),
    series: COMP.map(([k, f, name, color], i) => ({ name, type: 'bar', stack: 'pci', barWidth: 7,
      itemStyle: { color, borderRadius: i === COMP.length - 1 ? [0, 3, 3, 0] : 0 },
      data: rows.map((p) => +(100 * W[k] * (p[f] || 0)).toFixed(2)) })),
  }));
  chart('pc-rankchart').on('click', (p) => selectPort(rows[p.dataIndex].port_id, true));
}

function renderWaitBerth() {
  const rows = scored.slice(0, 18).reverse();
  chart('pc-waitberth').setOption(baseOption({
    legend: { data: ['Avg waiting', 'Avg berth time'] },
    grid: { left: 150, right: 24, top: 30, bottom: 26 },
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, formatter: (ps) => {
      const p = rows[ps[0].dataIndex];
      return `<b>${esc(p.port_name)}</b><br>avg waiting ${fmtH(p.avg_waiting_h)} (${fmt(p.pct_visits_waited, 0)}% of visits waited)<br>avg berth time ${fmtH(p.avg_dwell_h)}<br>avg turnaround ${fmtH(p.avg_turnaround_h)}`;
    } },
    xAxis: yAxis({ type: 'value', name: 'hours' }),
    yAxis: xAxis({ type: 'category', data: rows.map((p) => p.port_name), axisLabel: { color: C.ink2, fontSize: 10.5, width: 140, overflow: 'truncate', interval: 0 } }),
    series: [
      { name: 'Avg waiting', type: 'bar', barWidth: 6, barGap: '20%', itemStyle: { color: C.amber, borderRadius: [0, 3, 3, 0] }, data: rows.map((p) => p.avg_waiting_h) },
      { name: 'Avg berth time', type: 'bar', barWidth: 6, itemStyle: { color: C.teal, borderRadius: [0, 3, 3, 0] }, data: rows.map((p) => p.avg_dwell_h) },
    ],
  }));
  chart('pc-waitberth').on('click', (p) => selectPort(rows[p.dataIndex].port_id, true));
}

function renderTurnaround() {
  const rows = ports.filter((p) => p.median_turnaround_h != null).sort((a, b) => a.median_turnaround_h - b.median_turnaround_h);
  document.getElementById('pc-turnaround').style.height = `${rows.length * 17 + 70}px`;
  chart('pc-turnaround').resize();
  chart('pc-turnaround').setOption(baseOption({
    legend: { data: ['Median turnaround', '90th percentile'] },
    grid: { left: 150, right: 24, top: 30, bottom: 26 },
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, formatter: (ps) => {
      const p = rows[ps[0].dataIndex];
      return `<b>${esc(p.port_name)}</b> · ${p.complete_berthed_visits} complete visits<br>median ${fmtH(p.median_turnaround_h)} · p90 ${fmtH(p.p90_turnaround_h)}`;
    } },
    xAxis: yAxis({ type: 'value', name: 'hours' }),
    yAxis: xAxis({ type: 'category', data: rows.map((p) => p.port_name), axisLabel: { color: C.ink2, fontSize: 10.5, width: 140, overflow: 'truncate', interval: 0 } }),
    series: [
      { name: '90th percentile', type: 'bar', barWidth: 7, barGap: '-100%', z: 1, itemStyle: { color: '#DCE7EE', borderRadius: [0, 3, 3, 0] }, data: rows.map((p) => p.p90_turnaround_h) },
      { name: 'Median turnaround', type: 'bar', barWidth: 7, z: 2, itemStyle: { color: C.marine, borderRadius: [0, 3, 3, 0] }, data: rows.map((p) => p.median_turnaround_h) },
    ],
  }));
  chart('pc-turnaround').on('click', (p) => selectPort(rows[p.dataIndex].port_id, true));
}

function renderTable() {
  const t = document.getElementById('pc-table');
  t.innerHTML = `<thead><tr><th>#</th><th>Port</th><th class="num">PCI</th><th>Band</th><th class="num">Vessels</th><th class="num">Arrivals</th>
    <th class="num">Avg queue</th><th class="num">Peak queue</th><th class="num">Waiting (h)</th><th class="num">Berth (h)</th>
    <th class="num">Turnaround (h)</th><th class="num">Utilisation</th></tr></thead>
    <tbody>${ports.map((p) => `<tr data-port="${p.port_id}">
      <td class="muted">${p.pci_rank ?? '–'}</td><td>${esc(p.port_name)}</td><td class="num"><b>${p.pci == null ? '—' : fmt(p.pci)}</b></td>
      <td><span class="pill band" style="color:${p.pci_band === 'Insufficient data' ? C.ink3 : C.marine}">${esc(p.pci_band)}</span></td>
      <td class="num">${fmtInt(p.unique_vessels)}</td><td class="num">${fmtInt(p.arrivals)}</td><td class="num">${fmt(p.avg_queue, 2)}</td>
      <td class="num">${fmtInt(p.peak_queue)}</td><td class="num">${fmt(p.avg_waiting_h)}</td><td class="num">${fmt(p.avg_dwell_h)}</td>
      <td class="num">${fmt(p.avg_turnaround_h)}</td>
      <td class="num">${p.utilization == null ? '—' : fmt(100 * p.utilization, 0) + '%'}</td></tr>`).join('')}</tbody>`;
  t.querySelectorAll('tbody tr').forEach((tr) => tr.addEventListener('click', () => selectPort(tr.dataset.port, true)));
}

export async function selectPort(portId, scroll = false) {
  if (!initDone) { pending = portId; return; }
  current = portId;
  document.getElementById('pc-port').value = portId;
  document.querySelectorAll('#pc-table tbody tr').forEach((tr) => tr.classList.toggle('sel', tr.dataset.port === portId));
  const [p, hourly, ins] = await Promise.all([api(`ports/${portId}`), api('congestion/hourly', { port_id: portId }),
    api(`ports/${portId}/insights`)]);
  if (current !== portId) return;
  document.getElementById('pc-detail-title').textContent = `Port analysis · ${p.port_name}`;
  insight('ins-pc-summary', ins.summary);
  insight('ins-pc-hourly', ins.hourly);
  insight('ins-pc-daily', ins.daily);
  insight('ins-pc-groups', ins.groups);
  renderKpis(p);
  renderComponents(p);
  renderHourly(hourly);
  renderDaily(p.daily);
  renderGroups(p.turnaround_by_group);
  fitTo(map, [[p.port_lon, p.port_lat], [p.anch_lon, p.anch_lat]], 50, 10.5);
  allMap.flyTo({ center: [p.port_lon, p.port_lat], zoom: Math.max(allMap.getZoom(), 4), duration: 600 });
  if (scroll) document.getElementById('pc-detail-title').scrollIntoView({ behavior: 'smooth', block: 'start' });
  // commercial vessels in this port's zones at the hour of peak queue
  const pk = hourly.reduce((a, b) => ((b.queue || 0) > (a?.queue || 0) ? b : a), null);
  const t = (pk?.hour_epoch ?? hourly[Math.floor(hourly.length / 2)]?.hour_epoch) + 3599;
  if (t) {
    const gj = await api('positions', { t, groups: 'Cargo,Tanker', window_hours: 2, port_id: portId });
    if (current === portId) setSource(map, 'pvessels', gj);
  }
}

function kpi(label, value, sub = '') {
  return `<div class="kpi"><div class="kpi-label">${label}</div><div class="kpi-value">${value}</div><div class="kpi-sub">${sub}</div></div>`;
}

function renderKpis(p) {
  document.getElementById('pc-kpis').innerHTML = [
    kpi('Congestion score', p.pci == null ? '—' : fmt(p.pci), p.pci_band + (p.pci_rank ? ` · rank ${p.pci_rank}` : '')),
    kpi('Vessels / arrivals', `${fmtInt(p.unique_vessels)} / ${fmtInt(p.arrivals)}`, `${fmtInt(p.departures)} departures`),
    kpi('Waiting vessels', fmt(p.avg_queue, 1), `average · peak ${fmtInt(p.peak_queue)} at anchorage`),
    kpi('Waiting time', fmtH(p.avg_waiting_h), `median ${fmtH(p.median_waiting_h)} · ${fmt(p.pct_visits_waited, 0)}% waited`),
    kpi('Berth time', fmtH(p.avg_dwell_h), `median ${fmtH(p.median_dwell_h)}`),
    kpi('Turnaround', fmtH(p.avg_turnaround_h), `median ${fmtH(p.median_turnaround_h)} · p90 ${fmtH(p.p90_turnaround_h)}`),
    kpi('Dwell in port area', fmt(p.avg_in_area, 1), 'avg commercial vessels present'),
    kpi('Berth utilisation', p.utilization == null ? '—' : fmt(100 * p.utilization, 0) + '%', `avg ${fmt(p.avg_at_berth, 1)} / capacity proxy ${fmt(p.capacity_proxy, 0)}`),
  ].join('');
}

function renderComponents(p) {
  document.getElementById('pc-pci-label').textContent = p.pci == null ? 'insufficient berthed visits for PCI' : `PCI ${fmt(p.pci)} = Σ weight × normalised component`;
  const raw = { queue: `${fmt(p.avg_queue, 2)} vessels`, waiting: fmtH(p.avg_waiting_h), dwell: fmtH(p.avg_dwell_h),
    utilization: p.utilization == null ? '—' : fmt(100 * p.utilization, 0) + '%' };
  chart('pc-components').setOption(baseOption({
    grid: { left: 110, right: 80, top: 22, bottom: 24 },
    legend: { data: ['Contribution to PCI', 'Maximum possible'], top: 0, right: 0 },
    tooltip: { trigger: 'axis', formatter: (ps) => {
      const [k, f, name] = COMP[ps[0].dataIndex];
      return `<b>${name}</b><br>raw: ${raw[k]}<br>normalised: ${fmt(p[f], 2)}<br>weight: ${W[k]}<br>contribution: ${fmt(100 * W[k] * (p[f] || 0), 1)} pts`;
    } },
    xAxis: yAxis({ type: 'value', max: 100 * Math.max(...Object.values(W)) }),
    yAxis: xAxis({ type: 'category', data: COMP.map((c) => c[2]), inverse: true, axisLabel: { color: C.ink2 } }),
    series: [
      { name: 'Maximum possible', type: 'bar', barWidth: 12, barGap: '-100%', silent: true, z: 1,
        itemStyle: { color: '#EEF2F4', borderRadius: [0, 4, 4, 0] }, data: COMP.map(([k]) => 100 * W[k]) },
      { name: 'Contribution to PCI', type: 'bar', barWidth: 12, z: 2,
        data: COMP.map(([k, f, , color]) => ({ value: +(100 * W[k] * (p[f] || 0)).toFixed(1), itemStyle: { color, borderRadius: [0, 4, 4, 0] } })),
        label: { show: true, position: 'right', color: C.ink2, fontSize: 11, formatter: (q) => raw[COMP[q.dataIndex][0]], distance: 30 } },
    ],
  }), true);
}

function renderHourly(rows) {
  const x = rows.map((r) => r.hour_epoch * 1000);
  chart('pc-hourly').setOption(baseOption({
    legend: { data: ['At berth', 'Waiting at anchorage'] },
    grid: { left: 40, right: 16, top: 28, bottom: 28 },
    tooltip: { trigger: 'axis', formatter: (ps) => `<b>${fmtDay(ps[0].value[0] / 1000)} UTC</b><br>` +
      ps.map((s) => `${s.marker}${s.seriesName}: ${s.value[1]}`).join('<br>') },
    xAxis: xAxis({ type: 'time', axisLabel: { color: C.ink3, fontSize: 11, formatter: '{ee} {dd}' } }),
    yAxis: yAxis({ type: 'value', minInterval: 1, name: 'vessels' }),
    series: [
      { name: 'At berth', type: 'line', step: 'end', showSymbol: false, lineStyle: { width: 2, color: C.marine },
        itemStyle: { color: C.marine }, areaStyle: { color: 'rgba(0,91,143,.08)' }, data: rows.map((r, i) => [x[i], r.at_berth]) },
      { name: 'Waiting at anchorage', type: 'line', step: 'end', showSymbol: false, lineStyle: { width: 2, color: C.amber },
        itemStyle: { color: C.amber }, data: rows.map((r, i) => [x[i], r.queue]) },
    ],
  }), true);
}

function renderDaily(rows) {
  chart('pc-daily').setOption(baseOption({
    legend: { data: ['Arrivals', 'Departures'] },
    grid: { left: 36, right: 12, top: 28, bottom: 26 },
    xAxis: xAxis({ type: 'category', data: rows.map((r) => r.date.slice(5)) }),
    yAxis: yAxis({ type: 'value', minInterval: 1 }),
    series: [
      { name: 'Arrivals', type: 'bar', barGap: '15%', barMaxWidth: 16, itemStyle: { color: C.marine, borderRadius: [4, 4, 0, 0] }, data: rows.map((r) => r.arrivals) },
      { name: 'Departures', type: 'bar', barMaxWidth: 16, itemStyle: { color: C.teal, borderRadius: [4, 4, 0, 0] }, data: rows.map((r) => r.departures) },
    ],
  }), true);
}

function renderGroups(rows) {
  rows = rows.filter((r) => r.avg_turnaround_h != null);
  chart('pc-groups').setOption(baseOption({
    legend: { data: ['Median waiting', 'Median berth time'] },
    grid: { left: 70, right: 16, top: 28, bottom: 26 },
    tooltip: { trigger: 'axis', formatter: (ps) => {
      const r = rows[ps[0].dataIndex];
      return `<b>${esc(r.vessel_group)}</b> · ${r.complete_visits} complete visits<br>median waiting ${fmtH(r.median_waiting_h)}<br>median berth time ${fmtH(r.median_dwell_h)}<br>median turnaround ${fmtH(r.median_turnaround_h)}`;
    } },
    xAxis: yAxis({ type: 'value', name: 'hours' }),
    yAxis: xAxis({ type: 'category', data: rows.map((r) => r.vessel_group) }),
    series: [
      { name: 'Median waiting', type: 'bar', stack: 't', barWidth: 14, itemStyle: { color: C.amber, borderColor: '#fff', borderWidth: 1 }, data: rows.map((r) => r.median_waiting_h || 0) },
      { name: 'Median berth time', type: 'bar', stack: 't', barWidth: 14, itemStyle: { color: GROUP_COLORS.Cargo, borderRadius: [0, 4, 4, 0], borderColor: '#fff', borderWidth: 1 }, data: rows.map((r) => r.median_dwell_h || 0) },
    ],
  }), true);
}
