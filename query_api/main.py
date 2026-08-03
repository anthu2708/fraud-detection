import asyncio
import json
import os
import random
import threading
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, Producer
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    String,
    create_engine,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Session

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./fraud_local.db")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SCORED_TOPIC = "transactions.scored"
RAW_TOPIC = "transactions.raw"
SKIP_KAFKA = os.getenv("SKIP_KAFKA", "false").lower() == "true"

_connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=_connect_args)

_kafka_producer: Producer | None = None

# SSE broadcast
_sse_queues: list[asyncio.Queue] = []
_main_loop: asyncio.AbstractEventLoop | None = None

# Auto stream
_auto_config: dict = {"interval": 3.0, "amount": 1000.0}
_auto_stop_ev = threading.Event()
_auto_running = False
_auto_lock = threading.Lock()

# amount cache: tx_id → amount (scoring service doesn't echo Amount back)
_tx_amount_cache: dict[str, float] = {}


class Base(DeclarativeBase):
    pass


class Transaction(Base):
    __tablename__ = "transactions"
    id = Column(Integer, primary_key=True)
    transaction_id = Column(String, unique=True, index=True)
    submitted_at = Column(DateTime)
    scored_at = Column(DateTime, server_default=func.now())
    anomaly_score = Column(Float)
    risk_score = Column(Float)
    is_fraud = Column(Boolean)
    model_version = Column(String(10))
    source = Column(String(10), default="manual")
    amount = Column(Float)


def _push_event(payload: dict):
    if _main_loop is None:
        return
    msg = json.dumps(payload)
    for q in list(_sse_queues):
        _main_loop.call_soon_threadsafe(q.put_nowait, msg)


def _produce(amount: float, time_s: float | None, source: str) -> str:
    now = datetime.now(timezone.utc)
    t = time_s if time_s is not None else (now.hour * 3600 + now.minute * 60 + now.second)
    tx_id = str(uuid.uuid4())
    tx = {f"V{i}": round(random.gauss(0, 1), 6) for i in range(1, 29)}
    tx.update({
        "Amount": amount,
        "Time": float(t),
        "transaction_id": tx_id,
        "sent_at": now.isoformat(),
        "source": source,
    })
    _kafka_producer.produce(RAW_TOPIC, key=tx_id, value=json.dumps(tx))
    _kafka_producer.poll(0)
    _tx_amount_cache[tx_id] = amount
    return tx_id


def _consume_loop():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "query-api",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([SCORED_TOPIC])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[query_api] kafka error: {msg.error()}")
                continue
            data = json.loads(msg.value())
            tx_id = data["transaction_id"]

            submitted_at = None
            if sent := data.get("sent_at"):
                try:
                    submitted_at = datetime.fromisoformat(sent)
                except ValueError:
                    pass

            source = data.get("source", "manual")
            amount = _tx_amount_cache.pop(tx_id, None) or data.get("Amount")

            with Session(engine) as s:
                if s.query(Transaction).filter_by(transaction_id=tx_id).first():
                    continue
                s.add(Transaction(
                    transaction_id=tx_id,
                    submitted_at=submitted_at,
                    anomaly_score=data["anomaly_score"],
                    risk_score=data["risk_score"],
                    is_fraud=data["is_fraud"],
                    model_version=data["model_version"],
                    source=source,
                    amount=amount,
                ))
                s.commit()

            scored_at = datetime.now(timezone.utc)
            latency = None
            if submitted_at:
                sub = submitted_at if submitted_at.tzinfo else submitted_at.replace(tzinfo=timezone.utc)
                latency = round((scored_at - sub).total_seconds(), 2)

            _push_event({
                "transaction_id": tx_id,
                "amount": amount,
                "risk_score": round(data["risk_score"], 4),
                "anomaly_score": round(data["anomaly_score"], 6),
                "is_fraud": data["is_fraud"],
                "model_version": data["model_version"],
                "source": source,
                "submitted_at": data.get("sent_at"),
                "latency_s": latency,
            })
    finally:
        consumer.close()


def _auto_loop():
    global _auto_running
    while not _auto_stop_ev.is_set():
        if _kafka_producer is not None:
            try:
                _produce(_auto_config["amount"], None, "auto")
            except Exception as e:
                print(f"[auto] {e}")
        _auto_stop_ev.wait(_auto_config["interval"])
    with _auto_lock:
        _auto_running = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _kafka_producer, _main_loop
    _main_loop = asyncio.get_event_loop()
    Base.metadata.create_all(engine)
    if not SKIP_KAFKA:
        _kafka_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
        threading.Thread(target=_consume_loop, daemon=True).start()
    yield
    _auto_stop_ev.set()
    if _kafka_producer:
        _kafka_producer.flush()


app = FastAPI(title="Fraud Detection Query API", lifespan=lifespan)


class SubmitRequest(BaseModel):
    amount: float
    time: float | None = None


@app.post("/transactions/submit")
def submit_transaction(req: SubmitRequest):
    if _kafka_producer is None:
        raise HTTPException(503, "Kafka unavailable")
    tx_id = _produce(req.amount, req.time, "manual")
    return {"transaction_id": tx_id}


@app.get("/transactions/{transaction_id}/status")
def transaction_status(transaction_id: str):
    with Session(engine) as s:
        row = s.query(Transaction).filter_by(transaction_id=transaction_id).first()
    if row is None:
        return {"status": "pending"}
    return {
        "status": "scored",
        "transaction_id": row.transaction_id,
        "is_fraud": row.is_fraud,
        "risk_score": round(row.risk_score, 4),
        "anomaly_score": round(row.anomaly_score, 6),
        "model_version": row.model_version,
        "source": row.source,
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/transactions")
def list_transactions(limit: int = 100, source: str | None = None):
    with Session(engine) as s:
        q = s.query(Transaction).order_by(Transaction.scored_at.desc())
        if source:
            q = q.filter_by(source=source)
        rows = q.limit(limit).all()
        return [
            {
                "transaction_id": r.transaction_id,
                "is_fraud": r.is_fraud,
                "risk_score": r.risk_score,
                "model_version": r.model_version,
                "source": r.source,
            }
            for r in rows
        ]


@app.get("/transactions/fraud")
def fraud_transactions(limit: int = 100):
    with Session(engine) as s:
        rows = (
            s.query(Transaction)
            .filter_by(is_fraud=True)
            .order_by(Transaction.scored_at.desc())
            .limit(limit)
            .all()
        )
        return [
            {
                "transaction_id": r.transaction_id,
                "risk_score": r.risk_score,
                "anomaly_score": r.anomaly_score,
                "model_version": r.model_version,
            }
            for r in rows
        ]


@app.get("/stats")
def stats():
    with Session(engine) as s:
        total = s.query(Transaction).count()
        fraud = s.query(Transaction).filter_by(is_fraud=True).count()
        return {"total": total, "fraud": fraud, "fraud_rate": round(fraud / total, 4) if total else 0}


@app.post("/auto/start")
def auto_start(interval: float = 3.0, amount: float = 1000.0):
    global _auto_running
    if _kafka_producer is None:
        raise HTTPException(503, "Kafka unavailable")
    with _auto_lock:
        if _auto_running:
            return {"status": "already running"}
        _auto_config["interval"] = max(1.0, interval)
        _auto_config["amount"] = amount
        _auto_stop_ev.clear()
        _auto_running = True
        threading.Thread(target=_auto_loop, daemon=True).start()
    return {"status": "started", "interval": _auto_config["interval"], "amount": amount}


@app.post("/auto/stop")
def auto_stop():
    _auto_stop_ev.set()
    return {"status": "stopped"}


@app.get("/auto/status")
def auto_status():
    return {"running": _auto_running, "interval": _auto_config["interval"], "amount": _auto_config["amount"]}


@app.get("/events")
async def sse_events():
    q: asyncio.Queue = asyncio.Queue()
    _sse_queues.append(q)

    async def stream():
        try:
            while True:
                data = await q.get()
                yield f"data: {data}\n\n"
        finally:
            try:
                _sse_queues.remove(q)
            except ValueError:
                pass

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/demo", response_class=HTMLResponse)
def demo():
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Fraud Detection</title>
<style>
:root {
  --bg: #09090b;
  --surface: #18181b;
  --border: #27272a;
  --border-soft: #1c1c1f;
  --text: #fafafa;
  --muted: #a1a1aa;
  --accent: #3b82f6;
  --fraud: #ef4444;
  --fraud-bg: #1c0a0a;
  --ok: #22c55e;
  --ok-bg: #081a0e;
  --mono: 'JetBrains Mono','Fira Code','Cascadia Code',ui-monospace,monospace;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: var(--mono);
  background: var(--bg);
  color: var(--text);
  min-height: 100dvh;
  padding: 32px 24px;
  max-width: 1200px;
  margin: 0 auto;
}
.header { margin-bottom: 28px; }
.header h1 { font-size: 13px; font-weight: 600; letter-spacing: 0.12em; text-transform: uppercase; margin-bottom: 4px; }
.header p { font-size: 11px; color: var(--muted); }

.controls { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; margin-bottom: 32px; }
.panel { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 18px; }
.panel-label { font-size: 10px; letter-spacing: 0.14em; text-transform: uppercase; color: var(--muted); margin-bottom: 14px; display: flex; align-items: center; gap: 8px; }
.dot { width: 6px; height: 6px; border-radius: 50%; display: inline-block; background: var(--muted); flex-shrink: 0; }
.dot-on { background: var(--ok); animation: blink 1.6s ease-in-out infinite; }
@keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.3} }

.field-row { display: flex; gap: 10px; align-items: flex-end; flex-wrap: wrap; }
.field { display: flex; flex-direction: column; gap: 5px; }
.field label { font-size: 10px; color: var(--muted); letter-spacing: 0.06em; }
input[type=number], input[type=time] {
  font-family: var(--mono); font-size: 12px;
  background: var(--bg); border: 1px solid var(--border);
  color: var(--text); padding: 7px 10px; border-radius: 6px;
  -moz-appearance: textfield;
  color-scheme: dark;
}
input[type=number] { width: 108px; }
input[type=time] { width: 108px; }
input:focus { outline: none; border-color: var(--accent); }
input[type=number]::-webkit-inner-spin-button { -webkit-appearance: none; }

.btn {
  font-family: var(--mono); font-size: 10px; font-weight: 700;
  letter-spacing: 0.1em; text-transform: uppercase;
  padding: 8px 14px; border-radius: 6px; border: none; cursor: pointer;
}
.btn:active { transform: translateY(1px); }
.btn-blue { background: var(--accent); color: #fff; }
.btn-blue:hover { opacity: 0.88; }
.btn-green { background: #16a34a; color: #fff; }
.btn-green:hover { opacity: 0.88; }
.btn-red { background: var(--fraud); color: #fff; }
.btn-red:hover { opacity: 0.88; }
.status-line { margin-top: 10px; font-size: 11px; color: var(--muted); min-height: 16px; }

.stream { margin-bottom: 24px; }
.stream-header {
  display: flex; align-items: center; gap: 8px;
  padding-bottom: 8px; border-bottom: 1px solid var(--border);
}
.stream-title { font-size: 10px; letter-spacing: 0.14em; text-transform: uppercase; color: var(--muted); }
.stream-count { font-size: 10px; color: var(--muted); margin-left: auto; }

table { width: 100%; border-collapse: collapse; }
th {
  font-size: 10px; letter-spacing: 0.1em; text-transform: uppercase;
  color: var(--muted); text-align: left; padding: 9px 12px;
  font-weight: 400; border-bottom: 1px solid var(--border-soft); white-space: nowrap;
}
td { font-size: 12px; padding: 9px 12px; border-bottom: 1px solid var(--border-soft); vertical-align: middle; }
tr:last-child td { border-bottom: none; }
tr:hover td { background: rgba(255,255,255,0.02); }

.c-id { color: var(--muted); font-size: 11px; }
.c-amt { color: var(--text); }
.c-time { color: var(--muted); font-size: 11px; }
.c-lat { color: var(--muted); font-size: 11px; }
.c-risk-hi { color: var(--fraud); }
.c-risk-lo { color: var(--ok); }
.c-anom { color: var(--muted); }
.c-model { color: var(--muted); font-size: 11px; }

.badge {
  display: inline-block; font-size: 10px; font-weight: 700;
  letter-spacing: 0.07em; padding: 3px 8px; border-radius: 4px; text-transform: uppercase;
}
.b-fraud { background: var(--fraud-bg); color: var(--fraud); border: 1px solid #7f1d1d; }
.b-ok    { background: var(--ok-bg);    color: var(--ok);    border: 1px solid #14532d; }
.b-pend  { background: transparent;     color: var(--muted); border: 1px solid var(--border); }

.empty { font-size: 11px; color: var(--muted); padding: 18px 12px; }

@media (max-width: 680px) { .controls { grid-template-columns: 1fr; } }
</style>
</head>
<body>

<div class="header">
  <h1>Fraud Detection</h1>
  <p>Isolation Forest v2 · Real-time scoring via Kafka · SSE push</p>
</div>

<div class="controls">
  <div class="panel">
    <div class="panel-label">Manual Submit</div>
    <div class="field-row">
      <div class="field">
        <label>Amount ($)</label>
        <input type="number" id="m-amount" value="5000" min="0" step="0.01">
      </div>
      <div class="field">
        <label>Time</label>
        <input type="time" id="m-time">
      </div>
      <button class="btn btn-blue" onclick="submitManual()">Submit</button>
    </div>
    <div class="status-line" id="m-status"></div>
  </div>

  <div class="panel">
    <div class="panel-label">
      Auto Stream
      <span class="dot" id="auto-dot"></span>
    </div>
    <div class="field-row">
      <div class="field">
        <label>Interval (s)</label>
        <input type="number" id="a-interval" value="3" min="1" max="60">
      </div>
      <div class="field">
        <label>Amount ($)</label>
        <input type="number" id="a-amount" value="1000" min="0" step="0.01">
      </div>
      <button class="btn btn-green" id="auto-btn" onclick="toggleAuto()">Start</button>
    </div>
    <div class="status-line" id="a-status">Stopped</div>
  </div>
</div>

<div class="stream">
  <div class="stream-header">
    <span class="stream-title">Manual</span>
    <span class="stream-count" id="m-count">0 transactions</span>
  </div>
  <table>
    <thead><tr>
      <th>ID</th><th>Amount ($)</th><th>Submitted</th><th>Latency</th>
      <th>Risk Score</th><th>Anomaly Score</th><th>Model</th><th>Status</th>
    </tr></thead>
    <tbody id="m-body"><tr><td colspan="8" class="empty">No transactions yet</td></tr></tbody>
  </table>
</div>

<div class="stream">
  <div class="stream-header">
    <span class="stream-title">Auto Stream</span>
    <span class="stream-count" id="a-count">0 transactions</span>
  </div>
  <table>
    <thead><tr>
      <th>ID</th><th>Amount ($)</th><th>Submitted</th><th>Latency</th>
      <th>Risk Score</th><th>Anomaly Score</th><th>Model</th><th>Status</th>
    </tr></thead>
    <tbody id="a-body"><tr><td colspan="8" class="empty">Auto stream not started</td></tr></tbody>
  </table>
</div>

<script>
let mCount = 0, aCount = 0;
const meta = {};

function timePickerToSecs(val) {
  if (!val) return null;
  const [h, m] = val.split(':').map(Number);
  return h * 3600 + m * 60;
}

function fmtTime(isoStr) {
  if (!isoStr) return '-';
  const d = new Date(isoStr);
  return d.toLocaleTimeString('en-GB', {hour:'2-digit', minute:'2-digit', second:'2-digit'});
}

function buildPending(id, amount, submittedAt) {
  const sid = id.slice(0, 8) + '…';
  return `
    <td class="c-id" title="${id}">${sid}</td>
    <td class="c-amt">${(+amount).toFixed(2)}</td>
    <td class="c-time">${fmtTime(submittedAt)}</td>
    <td class="c-lat">-</td>
    <td>-</td><td>-</td>
    <td class="c-model">-</td>
    <td><span class="badge b-pend">pending</span></td>`;
}

function buildScored(ev) {
  const sid = ev.transaction_id.slice(0, 8) + '…';
  const rc = ev.risk_score > 0.5 ? 'c-risk-hi' : 'c-risk-lo';
  const badge = ev.is_fraud
    ? '<span class="badge b-fraud">fraud</span>'
    : '<span class="badge b-ok">ok</span>';
  const lat = ev.latency_s != null ? ev.latency_s.toFixed(2) + 's' : '-';
  const amt = ev.amount != null ? (+ev.amount).toFixed(2) : '-';
  return `
    <td class="c-id" title="${ev.transaction_id}">${sid}</td>
    <td class="c-amt">${amt}</td>
    <td class="c-time">${fmtTime(ev.submitted_at)}</td>
    <td class="c-lat">${lat}</td>
    <td class="${rc}">${ev.risk_score.toFixed(4)}</td>
    <td class="c-anom">${ev.anomaly_score.toFixed(6)}</td>
    <td class="c-model">${ev.model_version}</td>
    <td>${badge}</td>`;
}

function addPendingRow(id, amount, submittedAt, source) {
  const tbody = document.getElementById(source === 'manual' ? 'm-body' : 'a-body');
  const empty = tbody.querySelector('[colspan]');
  if (empty) empty.closest('tr').remove();
  const row = document.createElement('tr');
  row.id = 'tx-' + id;
  row.innerHTML = buildPending(id, amount, submittedAt);
  tbody.insertBefore(row, tbody.firstChild);
  if (source === 'manual') {
    mCount++;
    document.getElementById('m-count').textContent = mCount + ' transaction' + (mCount !== 1 ? 's' : '');
  } else {
    aCount++;
    document.getElementById('a-count').textContent = aCount + ' transaction' + (aCount !== 1 ? 's' : '');
  }
}

// SSE — single connection, routes events by source
const es = new EventSource('/events');
es.onmessage = (e) => {
  const ev = JSON.parse(e.data);
  const row = document.getElementById('tx-' + ev.transaction_id);
  if (row) {
    row.innerHTML = buildScored(ev);
    return;
  }
  // auto stream: server pushes without a pending row existing
  if (ev.source === 'auto') {
    addPendingRow(ev.transaction_id, ev.amount, ev.submitted_at, 'auto');
    const r = document.getElementById('tx-' + ev.transaction_id);
    if (r) r.innerHTML = buildScored(ev);
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
    const now = new Date().toISOString();
    addPendingRow(id, amount, now, 'manual');
    el.textContent = id.slice(0, 8) + '... submitted';
    setTimeout(() => { el.textContent = ''; }, 4000);
  } catch(e) { el.textContent = 'Error: ' + e.message; }
}

async function toggleAuto() {
  const btn = document.getElementById('auto-btn');
  const dot = document.getElementById('auto-dot');
  const st  = document.getElementById('a-status');
  const isRunning = btn.textContent === 'Stop';

  if (isRunning) {
    await fetch('/auto/stop', {method: 'POST'});
    btn.textContent = 'Start'; btn.className = 'btn btn-green';
    dot.className = 'dot'; st.textContent = 'Stopped';
  } else {
    const interval = parseFloat(document.getElementById('a-interval').value);
    const amount   = parseFloat(document.getElementById('a-amount').value);
    const res = await fetch('/auto/start?interval=' + interval + '&amount=' + amount, {method: 'POST'});
    if (!res.ok) { st.textContent = 'Error: Kafka unavailable'; return; }
    btn.textContent = 'Stop'; btn.className = 'btn btn-red';
    dot.className = 'dot dot-on'; st.textContent = 'Running';
  }
}
</script>
</body>
</html>"""
