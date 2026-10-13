// Thin fetch wrapper around the FastAPI endpoints (with a small cache).
const cache = new Map();

export async function api(path, params = {}, { useCache = true } = {}) {
  const qs = new URLSearchParams(Object.entries(params).filter(([, v]) => v !== undefined && v !== null && v !== ''));
  const url = `/api/${path}${qs.toString() ? '?' + qs : ''}`;
  if (useCache && cache.has(url)) return cache.get(url);
  const p = fetch(url).then(async (r) => {
    if (!r.ok) throw new Error(`${r.status} ${url}`);
    return r.json();
  });
  if (useCache) cache.set(url, p);
  p.catch(() => cache.delete(url));
  return p;
}
