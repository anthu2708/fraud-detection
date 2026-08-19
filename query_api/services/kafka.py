import asyncio
import json
import os
import random
import threading
import time as _time
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Producer
from prometheus_client import Histogram
from sqlalchemy.orm import Session

from .db import Transaction, engine

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SCORED_TOPIC = "transactions.scored"
RAW_TOPIC = "transactions.raw"
MANUAL_TOPIC = "transactions.manual"
SKIP_KAFKA = os.getenv("SKIP_KAFKA", "false").lower() == "true"

E2E_LATENCY = Histogram(
    "pipeline_e2e_latency_seconds",
    "End-to-end latency from producer sent_at to query-api serve",
    ["source"],
)

_producer: Producer | None = None
_sse_queues: list[asyncio.Queue] = []
_dashboard_queues: list[asyncio.Queue] = []
_main_loop: asyncio.AbstractEventLoop | None = None
_consumer_thread: threading.Thread | None = None

_V_SAMPLES: dict[str, list[float]] = {}

# Aggregation state
_agg_lock = threading.Lock()
_win: deque = deque()           # (ts, risk_score, latency_s, is_fraud)
_agg_total: int = 0
_fraud_flags: deque = deque(maxlen=8)
_recent_samples: deque = deque(maxlen=10)
_rate_history: deque = deque([0] * 30, maxlen=30)
_cur_bucket: int = 0
_cur_bucket_count: int = 0

def _load_v_samples() -> None:
    p = Path(__file__).parent.parent / "data" / "v_samples.json"
    if p.exists():
        global _V_SAMPLES
        _V_SAMPLES = json.loads(p.read_text())


def push_event(payload: dict) -> None:
    if _main_loop is None:
        return
    msg = json.dumps(payload)
    for q in _sse_queues[:]:
        _main_loop.call_soon_threadsafe(q.put_nowait, msg)


def subscribe_sse() -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue()
    _sse_queues.append(q)
    return q


def unsubscribe_sse(q: asyncio.Queue) -> None:
    try:
        _sse_queues.remove(q)
    except ValueError:
        pass


def subscribe_dashboard() -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue()
    _dashboard_queues.append(q)
    return q


def unsubscribe_dashboard(q: asyncio.Queue) -> None:
    try:
        _dashboard_queues.remove(q)
    except ValueError:
        pass


def _agg_update(risk: float, lat: float | None, fraud: bool) -> None:
    global _agg_total, _cur_bucket, _cur_bucket_count
    now = _time.time()
    with _agg_lock:
        _agg_total += 1
        _win.append((now, risk, lat, fraud))
        sec = int(now)
        if sec != _cur_bucket:
            _rate_history.append(_cur_bucket_count)
            _cur_bucket = sec
            _cur_bucket_count = 1
        else:
            _cur_bucket_count += 1


def _push_dashboard_stats() -> None:
    now = _time.time()
    with _agg_lock:
        cutoff = now - 5.0
        while _win and _win[0][0] < cutoff:
            _win.popleft()

        recent = [e for e in _win if e[0] >= now - 1.0]
        rate = len(recent)
        lats = sorted(e[2] for e in recent if e[2] is not None)
        avg_lat = round(sum(lats) / len(lats) * 1000) if lats else 0
        p95_lat = round(lats[int(len(lats) * 0.95)] * 1000) if lats else 0
        risks = [e[1] for e in recent]
        n = len(risks)
        if n:
            low = round(sum(1 for r in risks if r < 0.4) / n * 100)
            med = round(sum(1 for r in risks if 0.4 <= r < 0.7) / n * 100)
            hi  = 100 - low - med
        else:
            low = med = hi = 0
        alerts = sum(1 for e in recent if e[3])
        payload = json.dumps({
            "rate": rate,
            "total": _agg_total,
            "avg_lat_ms": avg_lat,
            "p95_lat_ms": p95_lat,
            "alerts": alerts,
            "risk": {"low": low, "med": med, "hi": hi},
            "flags": list(_fraud_flags),
            "samples": list(_recent_samples),
            "history": list(_rate_history),
        })

    if _main_loop:
        for q in _dashboard_queues[:]:
            _main_loop.call_soon_threadsafe(q.put_nowait, payload)


def _stats_pusher() -> None:
    while True:
        _time.sleep(0.2)
        if _dashboard_queues:
            _push_dashboard_stats()


def produce(amount: float, time_s: float | None, source: str) -> str:
    if _producer is None:
        raise RuntimeError("Kafka unavailable")
    now = datetime.now(timezone.utc)
    t = time_s if time_s is not None else (now.hour * 3600 + now.minute * 60 + now.second)
    tx_id = str(uuid.uuid4())
    tx = {
        f"V{i}": random.choice(_V_SAMPLES[f"V{i}"]) if _V_SAMPLES.get(f"V{i}") else round(random.gauss(0, 1), 6)
        for i in range(1, 29)
    }
    tx.update({
        "Amount": amount,
        "Time": float(t),
        "transaction_id": tx_id,
        "sent_at": now.isoformat(),
        "source": source,
    })
    topic = MANUAL_TOPIC if source == "manual" else RAW_TOPIC
    _producer.produce(topic, key=tx_id, value=json.dumps(tx))
    _producer.poll(0)
    return tx_id


def _consume_once(consumer: "Consumer") -> None:
    msg = consumer.poll(0.1)
    if msg is None:
        return
    if msg.error():
        if msg.error().code() != KafkaError._PARTITION_EOF:
            print(f"[query_api] kafka error: {msg.error()}")
        return
    data = json.loads(msg.value())
    tx_id = data["transaction_id"]

    submitted_at = None
    if sent := data.get("sent_at"):
        try:
            submitted_at = datetime.fromisoformat(sent)
        except ValueError:
            pass

    source = data.get("source", "auto")
    amount = data.get("amount")

    with Session(engine) as s:
        if s.query(Transaction).filter_by(transaction_id=tx_id).first():
            return
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
        E2E_LATENCY.labels(source=source).observe(latency)

    push_event({
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

    risk = data["risk_score"]
    fraud = data["is_fraud"]
    _agg_update(risk, latency, fraud)
    with _agg_lock:
        if fraud or risk > 0.7:
            _fraud_flags.appendleft({"id": tx_id[:8], "risk": round(risk, 2), "label": "FRAUD" if fraud else "REVIEW"})
        _recent_samples.appendleft({
            "id": tx_id[:7],
            "risk": round(risk, 4),
            "lat_ms": round(latency * 1000) if latency is not None else None,
            "fraud": fraud,
        })


def _consume_loop() -> None:
    retry = 0
    while True:
        consumer = None
        try:
            consumer = Consumer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "group.id": "query-api",
                "auto.offset.reset": "latest",
            })
            consumer.subscribe([SCORED_TOPIC])
            retry = 0
            while True:
                _consume_once(consumer)
        except (RuntimeError, ValueError, OSError) as e:
            print(f"[query_api] consumer error: {e} — retry {retry}")
            retry += 1
            _time.sleep(min(2 ** retry, 30))
        finally:
            if consumer:
                consumer.close()


def consumer_alive() -> bool:
    return _consumer_thread is not None and _consumer_thread.is_alive()


def startup() -> None:
    global _producer, _main_loop, _consumer_thread
    _load_v_samples()
    _main_loop = asyncio.get_event_loop()
    threading.Thread(target=_stats_pusher, daemon=True).start()
    if not SKIP_KAFKA:
        _producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
        _consumer_thread = threading.Thread(target=_consume_loop, daemon=True)
        _consumer_thread.start()


def shutdown() -> None:
    if _producer:
        _producer.flush()
