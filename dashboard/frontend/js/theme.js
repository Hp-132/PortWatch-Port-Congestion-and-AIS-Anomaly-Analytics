// Shared palette, vessel-group folding, formatters and ECharts defaults.

export const C = {
  prussian: '#003153', deep: '#00263F', marine: '#005B8F', teal: '#2F8F8B', tealSoft: '#DCEFED',
  steel: '#D5DEE4', steel2: '#E6ECEF', ink: '#1C2933', ink2: '#4A5A66', ink3: '#7A8892',
  amber: '#C88A27', red: '#B94A48', surface: '#FFFFFF',
};

// Risk review priority = status colours (reserved; always shown with a label).
export const RISK = {
  'Normal': '#8FA3AF', 'Watch': '#D2A73C', 'Suspicious': '#C8702A', 'High Priority': '#B94A48',
};
export const RISK_ORDER = ['Normal', 'Watch', 'Suspicious', 'High Priority'];

// Vessel groups folded to six display classes. Categorical slots validated
// with the dataviz palette validator (light surface): fixed order, never cycled.
export const FOLD = {
  'Cargo': 'Cargo', 'Tanker': 'Tanker',
  'Passenger': 'Passenger', 'High Speed Craft': 'Passenger',
  'Tug': 'Tug & service', 'Towing': 'Tug & service', 'Port Service': 'Tug & service',
  'Dredging': 'Tug & service', 'Search and Rescue': 'Tug & service',
  'Law Enforcement': 'Tug & service', 'Military': 'Tug & service',
  'Fishing': 'Fishing',
  'Pleasure Craft': 'Leisure & other', 'Sailing': 'Leisure & other',
  'Other': 'Leisure & other', 'Unknown': 'Leisure & other',
};
// No purple/pink: blues, teal-green, bronze, olive and grey stay inside the maritime palette.
export const GROUP_COLORS = {
  'Cargo': '#2A78D6', 'Tanker': '#1B9E77', 'Passenger': '#3E5C9A',
  'Tug & service': '#9A6B3F', 'Fishing': '#8A7A00', 'Leisure & other': '#A7B3BB',
};
export const GROUP_ORDER = Object.keys(GROUP_COLORS);
export const fold = (g) => FOLD[g] || 'Leisure & other';

// Sequential (single hue, light -> dark) for PCI magnitude.
export const PCI_RAMP = ['#E3EEF5', '#A9C8DD', '#5E95BD', '#2A6797', '#003153'];
export const PCI_BANDS = { 'Low': '#A9C8DD', 'Moderate': '#5E95BD', 'High': '#2A6797', 'Severe': '#003153',
  'Insufficient data': '#E6ECEF' };

// ---------- formatters ----------
export const fmtInt = (v) => v == null ? '—' : Math.round(v).toLocaleString('en-US');
export const fmt = (v, d = 1) => v == null || Number.isNaN(v) ? '—' : Number(v).toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
export const fmtCompact = (v) => {
  if (v == null) return '—';
  const a = Math.abs(v);
  if (a >= 1e9) return (v / 1e9).toFixed(2) + 'B';
  if (a >= 1e6) return (v / 1e6).toFixed(1) + 'M';
  if (a >= 1e4) return (v / 1e3).toFixed(1) + 'k';
  return fmtInt(v);
};
export const fmtH = (v) => v == null ? '—' : `${fmt(v, 1)} h`;
export const pad = (n) => String(n).padStart(2, '0');
export const fmtTs = (epochOrIso) => {
  if (epochOrIso == null) return '—';
  const d = typeof epochOrIso === 'number' ? new Date(epochOrIso * 1000) : new Date(epochOrIso);
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
};
export const fmtDay = (epoch) => {
  const d = new Date(epoch * 1000);
  return `${['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][d.getUTCDay()]} ${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:00`;
};
export const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
export const pill = (level) => `<span class="pill ${String(level).replace(' ', '-')}">${esc(level)}</span>`;

// ---------- ECharts ----------
const axisCommon = {
  axisLine: { lineStyle: { color: C.steel } },
  axisTick: { show: false },
  axisLabel: { color: C.ink3, fontSize: 11, fontFamily: 'Inter' },
  splitLine: { lineStyle: { color: '#EEF2F4', type: 'solid' } },
  nameTextStyle: { color: C.ink3, fontSize: 11 },
};

export function baseOption(extra = {}) {
  return {
    animationDuration: 300,
    textStyle: { fontFamily: 'Inter', color: C.ink },
    grid: { left: 48, right: 16, top: 28, bottom: 30, containLabel: false },
    tooltip: {
      trigger: 'axis', backgroundColor: 'rgba(255,255,255,.96)', borderColor: C.steel, borderWidth: 1,
      textStyle: { color: C.ink, fontSize: 12 }, extraCssText: 'box-shadow:0 4px 14px rgba(0,38,63,.12);border-radius:6px;',
      axisPointer: { type: 'line', lineStyle: { color: '#9FB3C0' } },
    },
    legend: { top: 0, right: 0, icon: 'roundRect', itemWidth: 10, itemHeight: 10, textStyle: { color: C.ink2, fontSize: 11.5 } },
    ...extra,
  };
}
export const xAxis = (o = {}) => ({ ...axisCommon, splitLine: { show: false }, ...o });
export const yAxis = (o = {}) => ({ ...axisCommon, ...o });

const charts = new Map();
export function chart(id) {
  const el = document.getElementById(id);
  if (!el) return null;
  let c = charts.get(id);
  if (!c || c.isDisposed()) {
    c = echarts.init(el, null, { renderer: 'canvas' });
    charts.set(id, c);
  }
  return c;
}
export function resizeCharts() { charts.forEach((c) => !c.isDisposed() && c.resize()); }
window.addEventListener('resize', () => resizeCharts());

// "Insight:" line under a figure. Text comes from the API (computed from the
// stored results); an empty/undefined text hides the line instead of guessing.
export function insight(id, text) {
  const el = document.getElementById(id);
  if (!el) return;
  el.hidden = !text;
  el.innerHTML = text ? `<b>Insight</b>${esc(text)}` : '';
}

export function toast(msg, ms = 3200) {
  const t = document.getElementById('toast');
  t.textContent = msg; t.hidden = false;
  clearTimeout(t._h); t._h = setTimeout(() => { t.hidden = true; }, ms);
}
