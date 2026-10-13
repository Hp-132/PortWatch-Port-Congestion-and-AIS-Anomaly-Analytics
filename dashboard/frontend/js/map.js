// MapLibre helpers: light basemap, vessel / port / track / zone layers.
import { C, FOLD, GROUP_COLORS, PCI_BANDS, RISK } from './theme.js';

export const BASEMAP = 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json';

export function createMap(container, opts = {}) {
  const map = new maplibregl.Map({
    container, style: BASEMAP, center: opts.center || [-96, 37.5], zoom: opts.zoom ?? 3.3,
    attributionControl: { compact: true }, dragRotate: false, pitchWithRotate: false,
  });
  map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'bottom-left');
  map.addControl(new maplibregl.ScaleControl({ unit: 'nautical' }), 'bottom-left');
  map.ready = new Promise((res) => map.on('load', () => res(map)));
  return map;
}

// MapLibre expression mapping raw vessel_group -> folded colour
export const groupColorExpr = (prop = 'g') => {
  const m = ['match', ['get', prop]];
  Object.entries(FOLD).forEach(([g, f]) => m.push(g, GROUP_COLORS[f]));
  m.push(GROUP_COLORS['Leisure & other']);
  return m;
};
export const riskColorExpr = (prop = 'risk_level') =>
  ['match', ['get', prop], 'High Priority', RISK['High Priority'], 'Suspicious', RISK.Suspicious,
    'Watch', RISK.Watch, RISK.Normal];
export const pciColorExpr = () => {
  const m = ['match', ['get', 'pci_band']];
  Object.entries(PCI_BANDS).forEach(([k, v]) => m.push(k, v));
  m.push(PCI_BANDS['Insufficient data']);
  return m;
};

export function setSource(map, id, data) {
  const s = map.getSource(id);
  if (s) s.setData(data); else map.addSource(id, { type: 'geojson', data });
}

export const fc = (features) => ({ type: 'FeatureCollection', features });
export const point = (lon, lat, props = {}) => ({ type: 'Feature', geometry: { type: 'Point', coordinates: [lon, lat] }, properties: props });

// Approximate geodesic circle polygon (radius in nautical miles)
export function circle(lon, lat, radiusNm, props = {}, steps = 64) {
  const coords = [];
  const R = 3440.065;
  const d = radiusNm / R;
  const lat1 = lat * Math.PI / 180, lon1 = lon * Math.PI / 180;
  for (let i = 0; i <= steps; i++) {
    const b = (i / steps) * 2 * Math.PI;
    const lat2 = Math.asin(Math.sin(lat1) * Math.cos(d) + Math.cos(lat1) * Math.sin(d) * Math.cos(b));
    const lon2 = lon1 + Math.atan2(Math.sin(b) * Math.sin(d) * Math.cos(lat1), Math.cos(d) - Math.sin(lat1) * Math.sin(lat2));
    coords.push([lon2 * 180 / Math.PI, lat2 * 180 / Math.PI]);
  }
  return { type: 'Feature', geometry: { type: 'Polygon', coordinates: [coords] }, properties: props };
}

export function portZones(ports) {
  const feats = [];
  ports.forEach((p) => {
    feats.push(circle(p.anch_lon, p.anch_lat, p.anch_radius_nm, { kind: 'Anchorage', port_id: p.port_id, name: p.port_name }));
    feats.push(circle(p.port_lon, p.port_lat, p.port_radius_nm, { kind: 'Berth area', port_id: p.port_id, name: p.port_name }));
  });
  return fc(feats);
}

export function addZoneLayers(map, src = 'zones') {
  map.addLayer({ id: `${src}-fill`, type: 'fill', source: src, paint: {
    'fill-color': ['match', ['get', 'kind'], 'Anchorage', C.amber, C.marine],
    'fill-opacity': ['match', ['get', 'kind'], 'Anchorage', 0.07, 0.08] } });
  map.addLayer({ id: `${src}-line`, type: 'line', source: src, paint: {
    'line-color': ['match', ['get', 'kind'], 'Anchorage', C.amber, C.marine], 'line-width': 1,
    'line-opacity': 0.7 } });
}

export function addPortLayer(map, src = 'ports', { minRadius = 5 } = {}) {
  map.addLayer({ id: `${src}-circle`, type: 'circle', source: src, paint: {
    'circle-color': pciColorExpr(),
    'circle-radius': ['interpolate', ['linear'], ['coalesce', ['get', 'arrivals'], 0], 0, minRadius, 60, minRadius + 6, 250, minRadius + 12],
    'circle-stroke-color': '#fff', 'circle-stroke-width': 2, 'circle-opacity': 0.95 } });
  map.addLayer({ id: `${src}-label`, type: 'symbol', source: src, minzoom: 4.2, layout: {
    'text-field': ['get', 'port_name'], 'text-size': 11, 'text-offset': [0, 1.3], 'text-anchor': 'top',
    'text-font': ['Open Sans Semibold', 'Arial Unicode MS Regular'] },
    paint: { 'text-color': C.prussian, 'text-halo-color': '#fff', 'text-halo-width': 1.4 } });
}

// Vessel markers: fill = vessel group; commercial vessels waiting at anchorage
// (property w = 1) get a larger marker with an amber ring.
export function addVesselLayer(map, src = 'vessels', id = 'vessels') {
  const waiting = ['==', ['get', 'w'], 1];
  map.addLayer({ id, type: 'circle', source: src, paint: {
    'circle-color': groupColorExpr('g'),
    'circle-radius': ['interpolate', ['linear'], ['zoom'],
      3, ['case', waiting, 3.6, 1.8], 6, ['case', waiting, 5, 3.2], 9, ['case', waiting, 7, 5], 12, ['case', waiting, 9, 7]],
    'circle-stroke-color': ['case', waiting, C.amber, '#fff'],
    'circle-stroke-width': ['interpolate', ['linear'], ['zoom'], 3, ['case', waiting, 1.8, 0.3], 8, ['case', waiting, 2.2, 1]],
    'circle-opacity': 0.92 } });
}

// AIS gap markers at the last known position; High Priority gets an outer red ring.
export function addAnomalyLayers(map, src = 'anoms') {
  map.addLayer({ id: `${src}-ring`, type: 'circle', source: src, filter: ['==', ['get', 'risk_level'], 'High Priority'],
    paint: { 'circle-radius': ['interpolate', ['linear'], ['zoom'], 3, 8, 8, 12], 'circle-color': 'rgba(0,0,0,0)',
      'circle-stroke-color': RISK['High Priority'], 'circle-stroke-width': 1.6, 'circle-stroke-opacity': 0.85 } });
  map.addLayer({ id: src, type: 'circle', source: src, paint: {
    'circle-color': riskColorExpr(),
    'circle-radius': ['interpolate', ['linear'], ['zoom'], 3, ['match', ['get', 'risk_level'], 'High Priority', 5, 3.5],
      8, ['match', ['get', 'risk_level'], 'High Priority', 8, 6]],
    'circle-stroke-color': '#fff', 'circle-stroke-width': 1.5 } });
}

// Selected vessel: Prussian-blue halo drawn above every other marker.
export function addSelectedLayer(map, src = 'selected') {
  setSource(map, src, fc([]));
  map.addLayer({ id: `${src}-halo`, type: 'circle', source: src, paint: {
    'circle-radius': 15, 'circle-color': 'rgba(0,49,83,0.12)', 'circle-stroke-color': C.prussian, 'circle-stroke-width': 2.5 } });
  map.addLayer({ id: `${src}-dot`, type: 'circle', source: src, paint: {
    'circle-radius': 5, 'circle-color': C.prussian, 'circle-stroke-color': '#fff', 'circle-stroke-width': 2 } });
}

// Straight dashed connection from last known to recovered position (path unobserved).
export function addGapLineLayer(map, src = 'gaplines') {
  map.addLayer({ id: src, type: 'line', source: src, paint: {
    'line-color': riskColorExpr(), 'line-width': 1.4, 'line-dasharray': [2, 2], 'line-opacity': 0.75 } });
}

export function addTrackLayer(map, src = 'track') {
  map.addLayer({ id: `${src}-line`, type: 'line', source: src, filter: ['==', ['geometry-type'], 'LineString'],
    layout: { 'line-cap': 'round', 'line-join': 'round' },
    paint: { 'line-color': C.prussian, 'line-width': 2, 'line-opacity': 0.85 } });
  map.addLayer({ id: `${src}-pts`, type: 'circle', source: src, filter: ['==', ['geometry-type'], 'Point'],
    paint: { 'circle-radius': ['match', ['get', 'kind'], 'end', 6, 'start', 5, 3],
      'circle-color': ['match', ['get', 'kind'], 'end', C.prussian, 'start', '#fff', 'gap', RISK.Suspicious, C.marine],
      'circle-stroke-color': ['match', ['get', 'kind'], 'start', C.prussian, '#fff'], 'circle-stroke-width': 2 } });
}

export function trackFeatures(points, extra = []) {
  if (!points?.length) return fc(extra);
  const coords = points.map((p) => [p[1], p[2]]);
  const feats = [{ type: 'Feature', geometry: { type: 'LineString', coordinates: coords }, properties: {} },
    point(coords[0][0], coords[0][1], { kind: 'start' }), point(...coords[coords.length - 1], { kind: 'end' }), ...extra];
  return fc(feats);
}

export function fitTo(map, coords, padding = 60, maxZoom = 10) {
  if (!coords.length) return;
  const b = coords.reduce((bb, c) => bb.extend(c), new maplibregl.LngLatBounds(coords[0], coords[0]));
  map.fitBounds(b, { padding, maxZoom, duration: 600 });
}

export function hoverPopup(map, layer, html) {
  const popup = new maplibregl.Popup({ closeButton: false, closeOnClick: false, offset: 8 });
  map.on('mousemove', layer, (e) => {
    map.getCanvas().style.cursor = 'pointer';
    const f = e.features[0];
    popup.setLngLat(e.lngLat).setHTML(html(f.properties, f)).addTo(map);
  });
  map.on('mouseleave', layer, () => { map.getCanvas().style.cursor = ''; popup.remove(); });
  return popup;
}
