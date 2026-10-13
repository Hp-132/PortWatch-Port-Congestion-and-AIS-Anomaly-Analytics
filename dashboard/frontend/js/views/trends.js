// Historical Trends: hourly traffic, PCI heatmap (port x day), daily arrivals, gaps, fleet activity.
import { api } from '../api.js';
import { C, GROUP_COLORS, GROUP_ORDER, PCI_RAMP, RISK, RISK_ORDER, baseOption, chart, esc, fmt, fmtCompact, fmtDay, fmtH, fmtInt, fold, insight, xAxis, yAxis } from '../theme.js';

export async function init() {
  const [t, ports, ins] = await Promise.all([api('trends'), api('ports'), api('insights/trends')]);
  hourly(t.hourly);
  heatmap(t.port_daily, ports);
  arrivals(t.daily);
  visitFlow(t.visits);
  gaps(t.daily);
  groups(t.groups);
  table(t.daily);
  [['messages', 'messages'], ['vessels', 'vessels'], ['heatmap', 'heatmap'], ['arrivals', 'arrivals'], ['waiting', 'waiting'],
    ['turnaround', 'turnaround'], ['gaps', 'gaps'], ['groups', 'groups']].forEach(([k, id]) => insight(`ins-tr-${id}`, ins[k]));
}

function line(id, rows, key, name, color) {
  chart(id).setOption(baseOption({
    grid: { left: 52, right: 16, top: 16, bottom: 28 },
    tooltip: { trigger: 'axis', formatter: (ps) => `<b>${fmtDay(ps[0].value[0] / 1000)} UTC</b><br>${name}: ${fmtInt(ps[0].value[1])}` },
    xAxis: xAxis({ type: 'time', axisLabel: { color: C.ink3, fontSize: 11, formatter: '{ee} {dd}' } }),
    yAxis: yAxis({ type: 'value', axisLabel: { color: C.ink3, fontSize: 11, formatter: (v) => fmtCompact(v) } }),
    series: [{ name, type: 'line', showSymbol: false, lineStyle: { width: 2, color }, itemStyle: { color },
      areaStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [{ offset: 0, color: color + '22' }, { offset: 1, color: color + '02' }]) },
      data: rows.map((r) => [r.hour_epoch * 1000, r[key]]) }],
  }));
}

function hourly(rows) {
  line('tr-hourly', rows, 'messages', 'AIS messages', C.marine);
  line('tr-vessels', rows, 'active_vessels', 'Active vessels', C.teal);
}

function heatmap(rows, ports) {
  const name = Object.fromEntries(ports.map((p) => [p.port_id, p.port_name]));
  const order = ports.filter((p) => p.pci != null).map((p) => p.port_id).reverse();
  const dates = [...new Set(rows.map((r) => r.date))].sort();
  const data = rows.filter((r) => order.includes(r.port_id))
    .map((r) => [dates.indexOf(r.date), order.indexOf(r.port_id), r.pci == null ? '-' : r.pci, r]);
  chart('tr-heat').setOption(baseOption({
    grid: { left: 190, right: 80, top: 10, bottom: 34 },
    tooltip: { trigger: 'item', formatter: (p) => {
      const r = p.data[3];
      return `<b>${esc(name[r.port_id])}</b> · ${r.date}<br>PCI ${r.pci == null ? 'n/a' : fmt(r.pci)} (${r.pci_band})<br>arrivals ${r.arrivals} · avg queue ${fmt(r.avg_queue, 2)} · waiting ${fmt(r.avg_waiting_h)} h`;
    } },
    xAxis: xAxis({ type: 'category', data: dates, splitArea: { show: false } }),
    yAxis: xAxis({ type: 'category', data: order.map((id) => name[id]), axisLabel: { color: C.ink2, fontSize: 11, width: 180, overflow: 'truncate' } }),
    // dimension 2 = PCI; the default (last dimension) is the row object -> no colour
    visualMap: { dimension: 2, min: 0, max: 100, calculable: false, orient: 'vertical', right: 0, top: 'middle', itemHeight: 140, itemWidth: 10,
      text: ['100', '0'], textStyle: { color: C.ink3, fontSize: 11 }, inRange: { color: PCI_RAMP } },
    series: [{ type: 'heatmap', data, itemStyle: { borderColor: '#fff', borderWidth: 2, borderRadius: 3 },
      label: { show: true, fontSize: 10, color: C.ink, formatter: (p) => p.data[2] === '-' ? '' : Math.round(p.data[2]) },
      emphasis: { itemStyle: { borderColor: C.prussian, borderWidth: 1 } } }],
  }));
  // light labels on dark cells
  chart('tr-heat').setOption({ series: [{ label: { color: '#1C2933', textBorderColor: '#fff', textBorderWidth: 2 } }] });
}

function arrivals(rows) {
  chart('tr-arrivals').setOption(baseOption({
    legend: { data: ['Arrivals', 'Departures'] },
    grid: { left: 40, right: 12, top: 28, bottom: 26 },
    xAxis: xAxis({ type: 'category', data: rows.map((r) => r.date.slice(5)) }),
    yAxis: yAxis({ type: 'value' }),
    series: [
      { name: 'Arrivals', type: 'bar', barMaxWidth: 14, barGap: '15%', itemStyle: { color: C.marine, borderRadius: [3, 3, 0, 0] }, data: rows.map((r) => r.port_arrivals) },
      { name: 'Departures', type: 'bar', barMaxWidth: 14, itemStyle: { color: C.teal, borderRadius: [3, 3, 0, 0] }, data: rows.map((r) => r.port_departures) },
    ],
  }));
}

// Waiting and turnaround by arrival day (berthed commercial visits)
function visitFlow(rows) {
  const days = rows.map((r) => r.date.slice(5));
  chart('tr-waiting').setOption(baseOption({
    legend: { data: ['Avg waiting (h)', 'Observed arrivals'] },
    grid: { left: 40, right: 40, top: 30, bottom: 26 },
    tooltip: { trigger: 'axis', formatter: (ps) => { const r = rows[ps[0].dataIndex];
      return `<b>${r.date}</b><br>avg waiting ${fmtH(r.avg_waiting_h)}<br>${r.observed_arrivals} observed berthed arrivals`; } },
    xAxis: xAxis({ type: 'category', data: days }),
    yAxis: [yAxis({ type: 'value', name: 'h' }), yAxis({ type: 'value', name: 'visits', splitLine: { show: false } })],
    series: [
      { name: 'Observed arrivals', type: 'bar', yAxisIndex: 1, barMaxWidth: 16, itemStyle: { color: '#DCE7EE', borderRadius: [3, 3, 0, 0] }, data: rows.map((r) => r.observed_arrivals) },
      { name: 'Avg waiting (h)', type: 'line', symbolSize: 7, lineStyle: { width: 2.2, color: C.amber }, itemStyle: { color: C.amber }, data: rows.map((r) => r.avg_waiting_h) },
    ],
  }));
  chart('tr-turnaround').setOption(baseOption({
    legend: { data: ['Avg turnaround (h)', 'Avg berth time (h)', 'Complete visits'] , itemGap: 6, textStyle: { fontSize: 10.5, color: C.ink2 } },
    grid: { left: 40, right: 40, top: 34, bottom: 26 },
    tooltip: { trigger: 'axis', formatter: (ps) => { const r = rows[ps[0].dataIndex];
      return `<b>${r.date}</b><br>avg turnaround ${fmtH(r.avg_turnaround_h)}<br>avg berth time ${fmtH(r.avg_dwell_h)}<br>${r.complete} complete visits`; } },
    xAxis: xAxis({ type: 'category', data: days }),
    yAxis: [yAxis({ type: 'value', name: 'h' }), yAxis({ type: 'value', name: 'visits', splitLine: { show: false } })],
    series: [
      { name: 'Complete visits', type: 'bar', yAxisIndex: 1, barMaxWidth: 16, itemStyle: { color: '#DCE7EE', borderRadius: [3, 3, 0, 0] }, data: rows.map((r) => r.complete) },
      { name: 'Avg turnaround (h)', type: 'line', symbolSize: 7, lineStyle: { width: 2.2, color: C.prussian }, itemStyle: { color: C.prussian }, data: rows.map((r) => r.avg_turnaround_h) },
      { name: 'Avg berth time (h)', type: 'line', symbolSize: 6, lineStyle: { width: 1.8, color: C.teal }, itemStyle: { color: C.teal }, data: rows.map((r) => r.avg_dwell_h) },
    ],
  }));
}

function gaps(rows) {
  const key = { 'Normal': 'gaps_normal', 'Watch': 'gaps_watch', 'Suspicious': 'gaps_suspicious', 'High Priority': 'gaps_high_priority' };
  chart('tr-gaps').setOption(baseOption({
    legend: { data: RISK_ORDER, itemGap: 6, textStyle: { fontSize: 10.5, color: C.ink2 } },
    grid: { left: 46, right: 12, top: 34, bottom: 26 },
    xAxis: xAxis({ type: 'category', data: rows.map((r) => r.date.slice(5)) }),
    yAxis: yAxis({ type: 'value', axisLabel: { color: C.ink3, fontSize: 11, formatter: (v) => fmtCompact(v) } }),
    series: RISK_ORDER.map((l, i) => ({ name: l, type: 'bar', stack: 'g', barMaxWidth: 26,
      itemStyle: { color: RISK[l], borderColor: '#fff', borderWidth: 1, borderRadius: i === 3 ? [3, 3, 0, 0] : 0 },
      data: rows.map((r) => r[key[l]] || 0) })),
  }));
}

function groups(rows) {
  const dates = [...new Set(rows.map((r) => r.date))].sort();
  const agg = {};
  rows.forEach((r) => { const f = fold(r.vessel_group); agg[f] ??= {}; agg[f][r.date] = (agg[f][r.date] || 0) + r.vessels; });
  chart('tr-groups').setOption(baseOption({
    legend: { data: GROUP_ORDER, itemGap: 6, textStyle: { fontSize: 10.5, color: C.ink2 } },
    grid: { left: 46, right: 12, top: 46, bottom: 26 },
    xAxis: xAxis({ type: 'category', data: dates.map((d) => d.slice(5)), boundaryGap: false }),
    yAxis: yAxis({ type: 'value', axisLabel: { color: C.ink3, fontSize: 11, formatter: (v) => fmtCompact(v) } }),
    series: GROUP_ORDER.map((g) => ({ name: g, type: 'line', symbolSize: 6, lineStyle: { width: 2, color: GROUP_COLORS[g] },
      itemStyle: { color: GROUP_COLORS[g], borderColor: '#fff', borderWidth: 1 }, data: dates.map((d) => agg[g]?.[d] || 0) })),
  }));
}

function table(rows) {
  document.getElementById('tr-table').innerHTML = `<thead><tr><th>Date</th><th class="num">AIS messages</th><th class="num">Active vessels</th>
    <th class="num">Underway vessels</th><th class="num">Class A msgs</th><th class="num">Class B msgs</th><th class="num">Port arrivals</th>
    <th class="num">Port departures</th><th class="num">Mean daily PCI</th><th class="num">AIS gaps</th><th class="num">Suspicious + High</th></tr></thead>
    <tbody>${rows.map((r) => `<tr><td class="mono">${r.date}</td><td class="num">${fmtInt(r.messages)}</td><td class="num">${fmtInt(r.active_vessels)}</td>
      <td class="num">${fmtInt(r.underway_vessels)}</td><td class="num">${fmtInt(r.class_a_messages)}</td><td class="num">${fmtInt(r.class_b_messages)}</td>
      <td class="num">${fmtInt(r.port_arrivals)}</td><td class="num">${fmtInt(r.port_departures)}</td><td class="num">${fmt(r.mean_daily_pci)}</td>
      <td class="num">${fmtInt(r.ais_gaps)}</td><td class="num">${fmtInt((r.gaps_suspicious || 0) + (r.gaps_high_priority || 0))}</td></tr>`).join('')}</tbody>`;
}
