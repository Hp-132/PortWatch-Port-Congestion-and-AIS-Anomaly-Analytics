// Headless-browser test of the running dashboard (Chrome DevTools Protocol, Node 18+).
//   node scripts/ui_check.mjs [baseUrl] [screenshotDir]
// For every view: no console errors / exceptions / failed requests, no error toast,
// every map has a canvas, every chart rendered, and the expected insights are shown.
// Exits 1 on any failure. Screenshots are written when a directory is given.
import { execSync, spawn } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const BASE = process.argv[2] || 'http://localhost:8000';
const SHOTS = process.argv[3] || null;
const CHROME = ['C:/Program Files/Google/Chrome/Application/chrome.exe',
  'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe'].find(existsSync);
if (!CHROME) { console.log('SKIP: no Chrome/Edge found'); process.exit(0); }
const VIEWS = { overview: 5, ports: 7, vessels: 4, anomalies: 7, trends: 8 };   // view -> min insights
const PORT = 9400 + Math.floor(Math.random() * 500);
const profile = mkdtempSync(join(tmpdir(), 'mx-ui-'));
const proc = spawn(CHROME, ['--headless=new', `--remote-debugging-port=${PORT}`, '--window-size=1600,1000',
  `--user-data-dir=${profile}`, '--use-angle=swiftshader', '--enable-unsafe-swiftshader', 'about:blank']);
const kill = () => { try { execSync(`taskkill /PID ${proc.pid} /T /F`, { stdio: 'ignore' }); } catch { proc.kill(); } };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
setTimeout(() => { console.log('FAIL: timeout'); kill(); process.exit(1); }, 300000);

let targets = [];
for (let i = 0; i < 60 && !targets.length; i++) {
  try { targets = (await (await fetch(`http://127.0.0.1:${PORT}/json`)).json()).filter((t) => t.type === 'page'); } catch {}
  await sleep(250);
}
const ws = new WebSocket(targets[0].webSocketDebuggerUrl);
await new Promise((r) => ws.addEventListener('open', r));
let id = 0; const pending = new Map(); const issues = [];
const send = (method, params = {}) => new Promise((res) => { const i = ++id; pending.set(i, res); ws.send(JSON.stringify({ id: i, method, params })); });
const evaluate = async (expression) => (await send('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true })).result?.value;
ws.addEventListener('message', (ev) => {
  const m = JSON.parse(ev.data);
  if (m.id && pending.has(m.id)) { pending.get(m.id)(m.result || m.error); pending.delete(m.id); return; }
  if (m.method === 'Runtime.consoleAPICalled' && m.params.type === 'error')
    issues.push('console.error: ' + m.params.args.map((a) => a.value ?? a.description).join(' ').slice(0, 300));
  if (m.method === 'Runtime.exceptionThrown')
    issues.push('exception: ' + (m.params.exceptionDetails.exception?.description || m.params.exceptionDetails.text).slice(0, 300));
  if (m.method === 'Network.responseReceived' && m.params.response.status >= 400)
    issues.push(`http ${m.params.response.status}: ${m.params.response.url}`);
  if (m.method === 'Network.loadingFailed' && !m.params.canceled) issues.push(`request failed: ${m.params.errorText}`);
});
await send('Runtime.enable'); await send('Network.enable'); await send('Page.enable');
if (SHOTS) mkdirSync(SHOTS, { recursive: true });

let failed = 0;
for (const [view, minInsights] of Object.entries(VIEWS)) {
  const before = issues.length;
  if (view === 'overview') await send('Page.navigate', { url: `${BASE}/#overview` });
  else await evaluate(`document.querySelector('.tab[data-view=${view}]').click()`);
  // wait until the view has rendered its insights (or 40 s)
  for (let i = 0; i < 80; i++) {
    const n = await evaluate(`document.querySelectorAll('#view-${view} .insight:not([hidden])').length`);
    if (n >= minInsights) break;
    await sleep(500);
  }
  await sleep(2500);
  const r = await evaluate(`(() => {
    const v = document.getElementById('view-${view}');
    const maps = [...v.querySelectorAll('.map')];
    const charts = [...v.querySelectorAll('.chart')];
    return {
      active: v.classList.contains('active'),
      maps: maps.length, mapsOk: maps.filter((m) => m.querySelector('canvas.maplibregl-canvas')?.width > 0).length,
      charts: charts.length, chartsOk: charts.filter((c) => c.querySelector('canvas')).length,
      insights: v.querySelectorAll('.insight:not([hidden])').length,
      toast: document.getElementById('toast').hidden ? '' : document.getElementById('toast').textContent,
      kpis: v.querySelectorAll('.kpi').length,
    };
  })()`);
  const problems = issues.slice(before);
  if (!r.active) problems.push('view not active');
  if (r.mapsOk < r.maps) problems.push(`maps rendered ${r.mapsOk}/${r.maps}`);
  if (r.chartsOk < r.charts) problems.push(`charts rendered ${r.chartsOk}/${r.charts}`);
  if (r.insights < minInsights) problems.push(`insights ${r.insights} < ${minInsights}`);
  if (r.toast) problems.push(`toast: ${r.toast}`);
  console.log(`${problems.length ? 'FAIL' : 'PASS'} ${view.padEnd(10)} maps ${r.mapsOk}/${r.maps} charts ${r.chartsOk}/${r.charts} insights ${r.insights} kpis ${r.kpis}`);
  problems.forEach((p) => console.log('     ' + p));
  failed += problems.length ? 1 : 0;
  if (SHOTS) {
    const h = await evaluate('document.documentElement.scrollHeight');
    await send('Emulation.setDeviceMetricsOverride', { width: 1600, height: Math.min(h, 4200), deviceScaleFactor: 1, mobile: false });
    await sleep(1500);
    const shot = await send('Page.captureScreenshot', { format: 'png' });
    writeFileSync(join(SHOTS, `dashboard_${view}.png`), Buffer.from(shot.data, 'base64'));
    await send('Emulation.clearDeviceMetricsOverride');
  }
}
ws.close(); kill();
console.log(failed ? `${failed} view(s) failed` : 'all views passed');
process.exit(failed ? 1 : 0);
