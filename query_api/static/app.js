// ── Buffer ────────────────────────────────────────────────────
const MAX_BUFFER = 3600;
const buf = []; // {ts, rate, alerts, avg_lat}
let lastPushTs = 0;
let lastDash = null;
let streamPaused = false;

// ── Time picker state ─────────────────────────────────────────
let timeMode   = 'relative'; // 'relative' | 'absolute'
let timeWindow = 30;         // seconds (relative mode)
let timeFrom   = null;       // ms timestamp (absolute mode)
let timeTo     = null;       // ms timestamp (absolute mode)

function windowSlice() {
  if (!buf.length) return [];
  if (timeMode === 'relative') {
    const end   = Date.now();
    const start = end - timeWindow * 1000;
    return buf.filter(b => b.ts >= start && b.ts <= end);
  }
  return buf.filter(b => b.ts >= timeFrom && b.ts <= timeTo);
}

function isLive() { return timeMode === 'relative'; }

// ── Time picker UI ────────────────────────────────────────────
function toggleTimePicker() {
  const dd = document.getElementById('tp-dropdown');
  dd.classList.toggle('open');
}

document.addEventListener('click', e => {
  if (!document.getElementById('time-picker').contains(e.target))
    document.getElementById('tp-dropdown').classList.remove('open');
});

function setRelative(secs, label) {
  timeMode = 'relative'; timeWindow = secs;
  document.getElementById('tp-label').textContent = 'Last ' + label;
  document.getElementById('tp-dropdown').classList.remove('open');
  document.getElementById('time-mode-badge').textContent = '● LIVE';
  document.getElementById('time-mode-badge').className = 'time-mode-badge live';
  if (lastDash) render(lastDash); else renderCharts();
}

function applyCustomRange() {
  const f = document.getElementById('tp-from').value;
  const t = document.getElementById('tp-to').value;
  if (!f || !t) return;
  timeMode = 'absolute';
  timeFrom = new Date(f).getTime();
  timeTo   = new Date(t).getTime();
  const fmt = d => new Date(d).toLocaleTimeString('en-GB', {hour:'2-digit', minute:'2-digit'});
  document.getElementById('tp-label').textContent = fmt(timeFrom) + ' → ' + fmt(timeTo);
  document.getElementById('tp-dropdown').classList.remove('open');
  document.getElementById('time-mode-badge').textContent = '⏸ RANGE';
  document.getElementById('time-mode-badge').className = 'time-mode-badge frozen';
  renderCharts();
}

// ── Buffer accumulation ───────────────────────────────────────
function accumulate(d) {
  const now = Date.now();
  if (now - lastPushTs >= 1000) {
    lastPushTs = now;
    buf.push({ ts: now, rate: d.rate, alerts: d.alerts, avg_lat: d.avg_lat_ms });
    if (buf.length > MAX_BUFFER) buf.shift();
  }
}

// ── Canvas ────────────────────────────────────────────────────
const rateCanvas  = document.getElementById('rate-chart');
const fraudCanvas = document.getElementById('fraud-chart');

function resizeCanvas(c) { c.width = c.parentElement.clientWidth; c.height = 80; }
[rateCanvas, fraudCanvas].forEach(resizeCanvas);
window.addEventListener('resize', () => { [rateCanvas, fraudCanvas].forEach(resizeCanvas); renderCharts(); });

const PAD = { l: 32, b: 18, t: 6 };

function drawChart(canvas, slice, valKey, color, hoverIdx) {
  const ctx = canvas.getContext('2d');
  const w = canvas.width, h = canvas.height;
  const cw = w - PAD.l, ch = h - PAD.b - PAD.t;
  ctx.clearRect(0, 0, w, h);

  const vals = slice.map(b => b[valKey]);
  const max  = Math.max(...vals, 1);
  const step = cw / Math.max(vals.length, 1);

  // Grid + y-axis labels
  ctx.font = '8px monospace';
  ctx.textAlign = 'right';
  [0, 0.5, 1].forEach(frac => {
    const y  = PAD.t + ch * (1 - frac);
    const lv = Math.round(max * frac);
    ctx.strokeStyle = '#27272a'; ctx.lineWidth = 0.5;
    ctx.beginPath(); ctx.moveTo(PAD.l, y); ctx.lineTo(w, y); ctx.stroke();
    ctx.fillStyle = '#52525b';
    ctx.fillText(lv, PAD.l - 3, y + 3);
  });

  if (!vals.length) return;

  // X-axis time labels
  if (slice.length > 1) {
    const fmt = ts => new Date(ts).toLocaleTimeString('en-GB', {hour:'2-digit', minute:'2-digit', second:'2-digit'});
    ctx.font = '8px monospace'; ctx.fillStyle = '#52525b';
    ctx.textAlign = 'left';
    ctx.fillText(fmt(slice[0].ts), PAD.l, h - 2);
    ctx.textAlign = 'right';
    ctx.fillText(fmt(slice[slice.length - 1].ts), w, h - 2);
  }

  // Line + fill
  ctx.beginPath();
  vals.forEach((v, i) => {
    const x = PAD.l + i * step + step / 2;
    const y = PAD.t + ch - (v / max) * ch;
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.strokeStyle = color; ctx.lineWidth = 1.5; ctx.stroke();
  const ex = PAD.l + (vals.length - 1) * step + step / 2;
  ctx.lineTo(ex, PAD.t + ch); ctx.lineTo(PAD.l, PAD.t + ch); ctx.closePath();
  ctx.fillStyle = color.replace('rgb(', 'rgba(').replace(')', ',0.08)');
  ctx.fill();

  // Hover crosshair + tooltip
  if (hoverIdx != null && hoverIdx >= 0 && hoverIdx < vals.length) {
    const x = PAD.l + hoverIdx * step + step / 2;
    const y = PAD.t + ch - (vals[hoverIdx] / max) * ch;

    ctx.beginPath(); ctx.moveTo(x, PAD.t); ctx.lineTo(x, PAD.t + ch);
    ctx.strokeStyle = '#52525b'; ctx.lineWidth = 1;
    ctx.setLineDash([3, 3]); ctx.stroke(); ctx.setLineDash([]);

    ctx.beginPath(); ctx.arc(x, y, 3, 0, Math.PI * 2);
    ctx.fillStyle = color; ctx.fill();

    const ts  = slice[hoverIdx].ts;
    const tim = new Date(ts).toLocaleTimeString('en-GB', {hour:'2-digit', minute:'2-digit', second:'2-digit'});
    const tip = `${vals[hoverIdx]}  ${tim}`;
    ctx.font = 'bold 9px monospace';
    const tw = ctx.measureText(tip).width + 10;
    const tx = x + 8 + tw > w ? x - tw - 4 : x + 4;
    const ty = Math.max(y - 4, PAD.t + 10);
    ctx.fillStyle = '#27272a';
    ctx.fillRect(tx - 2, ty - 11, tw, 14);
    ctx.fillStyle = '#e4e4e7';
    ctx.textAlign = 'left';
    ctx.fillText(tip, tx + 2, ty);
  }
}

const _hover = { rate: null, fraud: null };

function addHover(canvas, key, color) {
  canvas.style.cursor = 'crosshair';
  canvas.addEventListener('mousemove', e => {
    const rect = canvas.getBoundingClientRect();
    const mx   = (e.clientX - rect.left) * (canvas.width / rect.width);
    const s    = windowSlice();
    const step = (canvas.width - PAD.l) / Math.max(s.length, 1);
    _hover[key] = Math.min(Math.floor((mx - PAD.l) / step), s.length - 1);
    drawChart(canvas, s, key, color, _hover[key]);
  });
  canvas.addEventListener('mouseleave', () => {
    _hover[key] = null;
    drawChart(canvas, windowSlice(), key, color, null);
  });
}

addHover(rateCanvas,  'rate',   'rgb(59,130,246)');
addHover(fraudCanvas, 'alerts', 'rgb(239,68,68)');

function renderCharts() {
  const s = windowSlice();
  drawChart(rateCanvas,  s, 'rate',   'rgb(59,130,246)', _hover.rate);
  drawChart(fraudCanvas, s, 'alerts', 'rgb(239,68,68)',  _hover.fraud);
}

// ── Dashboard SSE ─────────────────────────────────────────────
const dash = new EventSource('/events/dashboard');
dash.onopen  = () => { document.getElementById('sse-dot').className = 'dot dot-on'; document.getElementById('sse-status').textContent = 'connected'; };
dash.onerror = () => { document.getElementById('sse-dot').className = 'dot'; document.getElementById('sse-status').textContent = 'disconnected'; };
dash.onmessage = (e) => {
  lastDash = JSON.parse(e.data);
  accumulate(lastDash);
  if (isLive()) render(lastDash);
};

function render(d) {
  document.getElementById('stat-rate').textContent   = d.rate;
  document.getElementById('stat-total').textContent  = d.total.toLocaleString();
  document.getElementById('stat-avg').textContent    = d.avg_lat_ms + ' ms';
  document.getElementById('stat-p95').textContent    = 'p95 ' + d.p95_lat_ms + ' ms';
  document.getElementById('stat-alerts').textContent = d.alerts;
  renderCharts();
  setRiskBar('low', d.risk.low); setRiskBar('med', d.risk.med); setRiskBar('hi', d.risk.hi);
  updateFlags(d.flags);
  updateSamples(d.samples);
}

function setRiskBar(key, pct) {
  document.getElementById('rf-' + key).style.width = pct + '%';
  document.getElementById('rp-' + key).textContent = pct + '%';
}

function updateFlags(flags) {
  const el = document.getElementById('fraud-flags');
  if (!flags.length) { el.innerHTML = '<div class="empty">No flags yet</div>'; return; }
  el.innerHTML = flags.map(f => {
    const cls = f.label === 'FRAUD' ? 'flag-fraud' : 'flag-review';
    return `<div class="flag-row"><span class="flag-id">${f.id}…</span><span class="flag-risk">${f.risk.toFixed(2)}</span><span class="flag-badge ${cls}">${f.label}</span></div>`;
  }).join('');
}

function updateSamples(samples) {
  const tbody = document.getElementById('sample-body');
  if (!samples.length) return;
  tbody.innerHTML = samples.map(s => {
    const rc    = s.fraud ? 'c-risk-hi' : (s.risk > 0.4 ? 'c-risk-med' : 'c-risk-lo');
    const lat   = (s.lat_ms != null && s.lat_ms < 60000) ? s.lat_ms + ' ms' : '—';
    const badge = s.fraud ? '<span class="badge b-fraud">fraud</span>' : '<span class="badge b-ok">ok</span>';
    return `<tr><td class="c-id">${s.id}…</td><td class="${rc}">${s.risk.toFixed(4)}</td><td class="c-lat">${lat}</td><td>${badge}</td></tr>`;
  }).join('');
}

// ── Stream pause/resume ───────────────────────────────────────
function _applyStreamState(paused) {
  streamPaused = paused;
  const btn = document.getElementById('stream-btn');
  if (!btn) return;  // ponytail: button may be hidden
  btn.textContent = paused ? '▶ Resume stream' : '⏸ Pause stream';
  btn.classList.toggle('paused', paused);
}

fetch('/stream/status').then(r => r.json()).then(d => _applyStreamState(d.status === 'paused')).catch(() => {});

async function toggleStream() {
  const action = streamPaused ? 'resume' : 'pause';
  const btn = document.getElementById('stream-btn');
  btn.disabled = true;
  try {
    const res = await fetch('/stream/' + action, { method: 'POST' });
    if (!res.ok) throw new Error(res.status);
    _applyStreamState(action === 'pause');
  } catch {
    btn.textContent = 'unreachable';
    setTimeout(() => _applyStreamState(streamPaused), 2000);
  } finally {
    btn.disabled = false;
  }
}

// ── Manual submit ─────────────────────────────────────────────
const manualIds = new Set();
const es = new EventSource('/events');
es.onmessage = (e) => {
  const ev = JSON.parse(e.data);
  if (manualIds.has(ev.transaction_id)) updateManualRow(ev);
};

function timePickerToSecs(val) {
  if (!val) return null;
  const [h, m] = val.split(':').map(Number);
  return h * 3600 + m * 60;
}

async function submitManual() {
  const amount = parseFloat(document.getElementById('m-amount').value);
  const secs   = timePickerToSecs(document.getElementById('m-time').value);
  const el     = document.getElementById('m-status');
  el.textContent = 'Submitting...';
  try {
    const res = await fetch('/transactions/submit', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ amount, time: secs }),
    });
    if (!res.ok) throw new Error(res.status);
    const { transaction_id: id } = await res.json();
    manualIds.add(id);
    prependManualRow(id, amount);
    el.textContent = id.slice(0, 8) + '... submitted';
    setTimeout(() => { el.textContent = ''; }, 4000);
  } catch (e) { el.textContent = 'Error: ' + e.message; }
}

function prependManualRow(id, amount) {
  const results = document.getElementById('m-results');
  const empty   = results.querySelector('.empty');
  if (empty) empty.remove();
  const row = document.createElement('div');
  row.id = 'mx-' + id;
  row.className = 'manual-row';
  row.innerHTML = `<span class="c-id" title="${id}">${id.slice(0,8)}…</span><span class="c-amt">$${amount.toFixed(2)}</span><span class="badge b-pend" style="min-width:52px;text-align:center">pending</span>`;
  results.insertBefore(row, results.firstChild);
}

function fmtLat(s) {
  if (s == null || s > 60) return '—';
  return s < 1 ? (s * 1000).toFixed(0) + ' ms' : s.toFixed(2) + ' s';
}

function updateManualRow(ev) {
  const row = document.getElementById('mx-' + ev.transaction_id);
  if (!row) return;
  const risk = ev.risk_score;
  const rc   = ev.is_fraud ? 'c-risk-hi' : (risk > 0.4 ? 'c-risk-med' : 'c-risk-lo');
  const badge = ev.is_fraud
    ? '<span class="badge b-fraud" style="min-width:52px;text-align:center">fraud</span>'
    : '<span class="badge b-ok" style="min-width:52px;text-align:center">ok</span>';
  row.innerHTML = `<span class="c-id" title="${ev.transaction_id}">${ev.transaction_id.slice(0,8)}…</span><span class="c-amt">$${(+ev.amount).toFixed(2)}</span><span class="${rc}">${risk.toFixed(4)}</span><span class="c-lat">${fmtLat(ev.latency_s)}</span>${badge}`;
}
