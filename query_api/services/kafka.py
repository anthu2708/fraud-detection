import asyncio
import json
import os
import random
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Producer
from sqlalchemy.orm import Session

from .db import Transaction, engine

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SCORED_TOPIC = "transactions.scored"
RAW_TOPIC = "transactions.raw"
MANUAL_TOPIC = "transactions.manual"
SKIP_KAFKA = os.getenv("SKIP_KAFKA", "false").lower() == "true"

_producer: Producer | None = None
_sse_queues: list[asyncio.Queue] = []
_main_loop: asyncio.AbstractEventLoop | None = None
_consumer_thread: threading.Thread | None = None

_V_SAMPLES: dict[str, list[float]] = {}

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


def _consume_loop() -> None:
    import time as _time
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
    if not SKIP_KAFKA:
        _producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
        _consumer_thread = threading.Thread(target=_consume_loop, daemon=True)
        _consumer_thread.start()


def shutdown() -> None:
    if _producer:
        _producer.flush()
