let mCount = 0, aCount = 0;
const manualIds = new Set();

function timePickerToSecs(val) {
  if (!val) return null;
  const [h, m] = val.split(':').map(Number);
  return h * 3600 + m * 60;
}

function fmtTime(isoStr) {
  if (!isoStr) return '-';
  return new Date(isoStr).toLocaleTimeString('en-GB', {hour:'2-digit', minute:'2-digit', second:'2-digit'});
}

function buildPending(id, amount, submittedAt) {
  return `
    <td class="c-id" title="${id}">${id.slice(0,8)}…</td>
    <td class="c-amt">${(+amount).toFixed(2)}</td>
    <td class="c-time">${fmtTime(submittedAt)}</td>
    <td class="c-lat">-</td>
    <td>-</td><td>-</td>
    <td class="c-model">-</td>
    <td><span class="badge b-pend">pending</span></td>`;
}

function buildScored(ev) {
  const rc = ev.risk_score > 0.5 ? 'c-risk-hi' : 'c-risk-lo';
  const badge = ev.is_fraud ? '<span class="badge b-fraud">fraud</span>' : '<span class="badge b-ok">ok</span>';
  const lat = ev.latency_s != null ? ev.latency_s.toFixed(2) + 's' : '-';
  const amt = ev.amount != null ? (+ev.amount).toFixed(2) : '-';
  return `
    <td class="c-id" title="${ev.transaction_id}">${ev.transaction_id.slice(0,8)}…</td>
    <td class="c-amt">${amt}</td>
    <td class="c-time">${fmtTime(ev.submitted_at)}</td>
    <td class="c-lat">${lat}</td>
    <td class="${rc}">${ev.risk_score.toFixed(4)}</td>
    <td class="c-anom">${ev.anomaly_score.toFixed(6)}</td>
    <td class="c-model">${ev.model_version}</td>
    <td>${badge}</td>`;
}

function prependRow(tbody, id, html) {
  const empty = tbody.querySelector('[colspan]');
  if (empty) empty.closest('tr').remove();
  const row = document.createElement('tr');
  row.id = 'tx-' + id;
  row.innerHTML = html;
  tbody.insertBefore(row, tbody.firstChild);
}

const es = new EventSource('/events');
es.onopen = () => {
  document.getElementById('sse-dot').className = 'dot dot-on';
  document.getElementById('sse-status').textContent = 'connected';
};
es.onerror = () => {
  document.getElementById('sse-dot').className = 'dot';
  document.getElementById('sse-status').textContent = 'disconnected';
};
es.onmessage = (e) => {
  const ev = JSON.parse(e.data);

  aCount++;
  document.getElementById('sse-total').textContent = aCount;
  document.getElementById('sse-last').textContent = fmtTime(ev.submitted_at || new Date().toISOString());

  const aBody = document.getElementById('a-body');
  const empty = aBody.querySelector('[colspan]');
  if (empty) empty.closest('tr').remove();
  const r = document.createElement('tr');
  r.innerHTML = buildScored(ev);
  aBody.insertBefore(r, aBody.firstChild);
  while (aBody.rows.length > 20) aBody.deleteRow(aBody.rows.length - 1);
  document.getElementById('a-count').textContent = aCount + ' scored';

  if (manualIds.has(ev.transaction_id)) {
    const row = document.getElementById('tx-' + ev.transaction_id);
    if (row) row.innerHTML = buildScored(ev);
  }
};

async function submitManual() {
  const amount = parseFloat(document.getElementById('m-amount').value);
  const timeVal = document.getElementById('m-time').value;
  const secs = timePickerToSecs(timeVal);
  const el = document.getElementById('m-status');
  el.textContent = 'Submitting...';

  try {
    const res = await fetch('/transactions/submit', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({amount, time: secs}),
    });
    if (!res.ok) throw new Error(res.status);
    const { transaction_id: id } = await res.json();
    manualIds.add(id);
    const mBody = document.getElementById('m-body');
    prependRow(mBody, id, buildPending(id, amount, new Date().toISOString()));
    mCount++;
    document.getElementById('m-count').textContent = mCount + ' transaction' + (mCount !== 1 ? 's' : '');
    el.textContent = id.slice(0, 8) + '... submitted';
    setTimeout(() => { el.textContent = ''; }, 4000);
  } catch(e) { el.textContent = 'Error: ' + e.message; }
}
