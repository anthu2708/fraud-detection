import json
import os
import random
import threading
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, Producer
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
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

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://fraud:fraud@localhost:5432/fraud")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SCORED_TOPIC = "transactions.scored"
RAW_TOPIC = "transactions.raw"
SKIP_KAFKA = os.getenv("SKIP_KAFKA", "false").lower() == "true"

_connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=_connect_args)

_kafka_producer: Producer | None = None


class Base(DeclarativeBase):
    pass


class Transaction(Base):
    __tablename__ = "transactions"
    id = Column(Integer, primary_key=True)
    transaction_id = Column(String, unique=True, index=True)
    scored_at = Column(DateTime, server_default=func.now())
    anomaly_score = Column(Float)
    risk_score = Column(Float)
    is_fraud = Column(Boolean)
    model_version = Column(String(10))


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
            with Session(engine) as s:
                if not s.query(Transaction).filter_by(transaction_id=data["transaction_id"]).first():
                    s.add(Transaction(
                        transaction_id=data["transaction_id"],
                        anomaly_score=data["anomaly_score"],
                        risk_score=data["risk_score"],
                        is_fraud=data["is_fraud"],
                        model_version=data["model_version"],
                    ))
                    s.commit()
    finally:
        consumer.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _kafka_producer
    Base.metadata.create_all(engine)
    if not SKIP_KAFKA:
        _kafka_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
        threading.Thread(target=_consume_loop, daemon=True).start()
    yield
    if _kafka_producer:
        _kafka_producer.flush()


app = FastAPI(title="Fraud Detection Query API", lifespan=lifespan)


class SubmitRequest(BaseModel):
    amount: float
    time: float | None = None  # seconds from start of day; defaults to now


@app.post("/transactions/submit")
def submit_transaction(req: SubmitRequest):
    if _kafka_producer is None:
        raise HTTPException(503, "Kafka unavailable")

    now = datetime.now(timezone.utc)
    t = req.time if req.time is not None else (now.hour * 3600 + now.minute * 60 + now.second)
    tx_id = str(uuid.uuid4())

    tx = {f"V{i}": round(random.gauss(0, 1), 6) for i in range(1, 29)}
    tx["Amount"] = req.amount
    tx["Time"] = float(t)
    tx["transaction_id"] = tx_id
    tx["sent_at"] = now.isoformat()

    _kafka_producer.produce(RAW_TOPIC, key=tx_id, value=json.dumps(tx))
    _kafka_producer.poll(0)

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
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/transactions")
def list_transactions(limit: int = 100):
    with Session(engine) as s:
        rows = s.query(Transaction).order_by(Transaction.scored_at.desc()).limit(limit).all()
        return [
            {
                "transaction_id": r.transaction_id,
                "is_fraud": r.is_fraud,
                "risk_score": r.risk_score,
                "model_version": r.model_version,
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


@app.get("/demo", response_class=HTMLResponse)
def demo():
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Fraud Detection Demo</title>
<style>
  body { font-family: monospace; max-width: 800px; margin: 40px auto; padding: 0 20px; background: #0d1117; color: #c9d1d9; }
  h1 { color: #58a6ff; }
  h2 { color: #8b949e; font-size: 1em; margin-top: 2em; }
  input[type=number] { background: #161b22; border: 1px solid #30363d; color: #c9d1d9; padding: 8px 12px; border-radius: 6px; width: 200px; font-size: 1em; }
  button { background: #238636; border: none; color: #fff; padding: 8px 20px; border-radius: 6px; cursor: pointer; font-size: 1em; margin-left: 8px; }
  button:hover { background: #2ea043; }
  #result { margin-top: 20px; padding: 16px; border-radius: 6px; display: none; }
  .fraud { background: #3d1f1f; border: 1px solid #f85149; }
  .ok    { background: #1a2f1a; border: 1px solid #3fb950; }
  .badge-fraud { color: #f85149; font-weight: bold; }
  .badge-ok    { color: #3fb950; font-weight: bold; }
  table { width: 100%; border-collapse: collapse; margin-top: 12px; font-size: 0.9em; }
  th { color: #8b949e; text-align: left; padding: 6px 8px; border-bottom: 1px solid #30363d; }
  td { padding: 6px 8px; border-bottom: 1px solid #21262d; }
  .f { color: #f85149; }
  .label { color: #8b949e; font-size: 0.85em; }
  #status-msg { color: #8b949e; margin-top: 8px; font-size: 0.9em; }
</style>
</head>
<body>
<h1>Fraud Detection</h1>
<p class="label">Submit a transaction — scored by Isolation Forest v2 in real-time via Kafka</p>

<div style="margin-top: 24px; display: flex; align-items: center; gap: 8px; flex-wrap: wrap;">
  <div>
    <div class="label">Amount ($)</div>
    <input type="number" id="amount" value="5000" min="0" step="0.01">
  </div>
  <div style="margin-left: 16px;">
    <div class="label">Time (seconds from midnight, 0–86400)</div>
    <input type="number" id="time" placeholder="now" min="0" max="86400" step="1">
  </div>
  <div style="margin-top: 18px;">
    <button onclick="submit()">Submit Transaction</button>
  </div>
</div>

<div id="status-msg"></div>
<div id="result"></div>

<h2>RECENT TRANSACTIONS</h2>
<table id="tx-table">
  <thead><tr><th>ID</th><th>Risk Score</th><th>Status</th></tr></thead>
  <tbody id="tx-body"></tbody>
</table>

<script>
async function submit() {
  const amount = parseFloat(document.getElementById('amount').value);
  const timeVal = document.getElementById('time').value;
  const body = { amount };
  if (timeVal !== '') body.time = parseFloat(timeVal);

  document.getElementById('result').style.display = 'none';
  document.getElementById('status-msg').textContent = 'Submitting...';

  const res = await fetch('/transactions/submit', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const { transaction_id } = await res.json();
  document.getElementById('status-msg').textContent = `Submitted: ${transaction_id} — waiting for score...`;
  poll(transaction_id, 0);
}

async function poll(id, attempts) {
  if (attempts > 30) {
    document.getElementById('status-msg').textContent = 'Timeout — scoring service may be down.';
    return;
  }
  const res = await fetch(`/transactions/${id}/status`);
  const data = await res.json();
  if (data.status === 'pending') {
    setTimeout(() => poll(id, attempts + 1), 1000);
    return;
  }
  document.getElementById('status-msg').textContent = '';
  const fraud = data.is_fraud;
  const el = document.getElementById('result');
  el.className = fraud ? 'fraud' : 'ok';
  el.style.display = 'block';
  el.innerHTML = `
    <span class="${fraud ? 'badge-fraud' : 'badge-ok'}">${fraud ? '🚨 FRAUD DETECTED' : '✅ LEGITIMATE'}</span><br><br>
    <span class="label">Transaction ID:</span> ${data.transaction_id}<br>
    <span class="label">Risk Score:</span> ${data.risk_score}<br>
    <span class="label">Anomaly Score:</span> ${data.anomaly_score}<br>
    <span class="label">Model:</span> ${data.model_version}
  `;
  loadRecent();
}

async function loadRecent() {
  const res = await fetch('/transactions?limit=20');
  const rows = await res.json();
  const tbody = document.getElementById('tx-body');
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td style="font-size:0.8em">${r.transaction_id.slice(0, 8)}...</td>
      <td>${r.risk_score?.toFixed(4) ?? '-'}</td>
      <td class="${r.is_fraud ? 'f' : ''}">${r.is_fraud ? 'FRAUD' : 'ok'}</td>
    </tr>
  `).join('');
}

loadRecent();
setInterval(loadRecent, 5000);
</script>
</body>
</html>"""
