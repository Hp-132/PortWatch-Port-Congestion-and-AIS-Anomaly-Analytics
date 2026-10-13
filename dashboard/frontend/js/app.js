// MARITIME-X dashboard bootstrap: routing, global search, shared state.
import { api } from './api.js';
import { esc, fmtTs, resizeCharts, toast } from './theme.js';
import * as overview from './views/overview.js';
import * as ports from './views/ports.js';
import * as vessels from './views/vessels.js';
import * as anomalies from './views/anomalies.js';
import * as trends from './views/trends.js';

const VIEWS = { overview, ports, vessels, anomalies, trends };
const initialised = new Set();
export const state = { meta: null };

async function show(name) {
  if (!VIEWS[name]) name = 'overview';
  document.querySelectorAll('.tab').forEach((t) => t.classList.toggle('active', t.dataset.view === name));
  document.querySelectorAll('.view').forEach((v) => v.classList.toggle('active', v.id === `view-${name}`));
  if (location.hash !== `#${name}`) history.replaceState(null, '', `#${name}`);
  try {
    if (!initialised.has(name)) { initialised.add(name); await VIEWS[name].init(state); }
    VIEWS[name].activate?.();   // also after first init: maps built while hidden need a resize
  } catch (e) {
    console.error(e); toast(`Could not load ${name}: ${e.message}`);
  }
  requestAnimationFrame(resizeCharts);
}

export async function openVessel(mmsi) {
  await show('overview');
  overview.selectVessel(mmsi);
}

function setupSearch() {
  const input = document.getElementById('vessel-search');
  const box = document.getElementById('search-results');
  let timer;
  input.addEventListener('input', () => {
    clearTimeout(timer);
    const q = input.value.trim();
    if (q.length < 2) { box.hidden = true; return; }
    timer = setTimeout(async () => {
      const rows = await api('vessels/search', { q }, { useCache: false });
      box.innerHTML = rows.length ? rows.map((r) => `<button data-mmsi="${r.mmsi}">
          <span><b>${esc(r.vessel_name || 'Unnamed')}</b><br><span class="muted small">MMSI ${r.mmsi} · ${esc(r.vessel_group)}</span></span>
          <span class="muted small">${r.flagged ? 'identity flag' : ''}</span></button>`).join('')
        : '<div style="padding:10px 12px" class="muted">No vessel found</div>';
      box.hidden = false;
    }, 220);
  });
  box.addEventListener('click', (e) => {
    const b = e.target.closest('button[data-mmsi]');
    if (!b) return;
    box.hidden = true; input.value = '';
    openVessel(Number(b.dataset.mmsi));
  });
  document.addEventListener('click', (e) => { if (!e.target.closest('.search')) box.hidden = true; });
}

async function boot() {
  document.querySelectorAll('.tab').forEach((t) => t.addEventListener('click', () => show(t.dataset.view)));
  setupSearch();
  try {
    state.meta = await api('meta');
    const m = state.meta;
    document.getElementById('window-chip').textContent =
      `${fmtTs(m.window_start_epoch).slice(0, 10)} → ${fmtTs(m.window_end_epoch).slice(0, 10)} UTC`;
    const p = await api('pipeline');
    const st = p.load_mongodb?.recorded_at || p.serving_views?.recorded_at;
    document.getElementById('footer-pipeline').textContent =
      `Spark master: ${p.clean_ais?.spark_master ?? '—'}${st ? ' · last run ' + st.replace('T', ' ').replace('Z', ' UTC') : ''}`;
  } catch (e) {
    toast('API unavailable - is MongoDB loaded and the backend running?', 8000);
    console.error(e);
    return;
  }
  show(location.hash.replace('#', '') || 'overview');
}

window.addEventListener('hashchange', () => show(location.hash.replace('#', '')));
boot();
