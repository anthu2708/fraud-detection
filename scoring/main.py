import json
import os
import signal
import time

import joblib
import numpy as np
from confluent_kafka import Consumer, KafkaError, Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
IN_TOPIC = "transactions.raw"
MANUAL_TOPIC = "transactions.manual"
OUT_TOPIC = "transactions.scored"
MODEL_PATH = os.getenv("MODEL_PATH", "models/isolation_forest_v2.pkl")
MODEL_VERSION = os.getenv("MODEL_VERSION", "v2")

_shutdown = False


def _handle_signal(sig, frame):
    global _shutdown
    print(f"[scoring] signal {sig} received — will stop after current message")
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def load_model(path: str) -> dict:
    artifact = joblib.load(path)
    print(f"[scoring] model loaded: {path}")
    return artifact


def score(artifact: dict, tx: dict) -> dict:
    model = artifact["model"]
    features = artifact["features"]
    scaler = artifact.get("scaler")
    scale_cols = artifact.get("scale_cols", [])
    threshold = artifact.get("threshold")

    X = np.array([[tx[f] for f in features]])
    if scaler is not None and scale_cols:
        X[:, scale_cols] = scaler.transform(X[:, scale_cols])

    raw = float(model.score_samples(X)[0])
    is_fraud = (raw < threshold) if threshold is not None else (model.predict(X)[0] == -1)

    return {
        "transaction_id": tx["transaction_id"],
        "sent_at": tx.get("sent_at", ""),
        "amount": tx.get("Amount"),
        "source": tx.get("source", "auto"),
        "anomaly_score": round(raw, 6),
        "risk_score": round(float(min(max(-raw, 0), 1)), 6),
        "is_fraud": bool(is_fraud),
        "model_version": MODEL_VERSION,
    }


def _make_consumer(group_suffix: str, offset_reset: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"scoring-service-{group_suffix}",
        "auto.offset.reset": offset_reset,
        "enable.auto.commit": False,
    })


def main():
    artifact = load_model(MODEL_PATH)

    # manual topic: latest only (priority lane, no backlog replay)
    manual_consumer = _make_consumer("manual", "latest")
    manual_consumer.subscribe([MANUAL_TOPIC])

    # raw topic: earliest (replay all producer history)
    auto_consumer = _make_consumer("auto", "earliest")
    auto_consumer.subscribe([IN_TOPIC])

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    count = 0
    fraud_count = 0
    t0 = time.perf_counter()

    print(f"[scoring] consuming {MANUAL_TOPIC} (priority) + {IN_TOPIC} → {OUT_TOPIC}")

    try:
        while not _shutdown:
            # poll manual first (non-blocking) — gives priority to manual submits
            msg = manual_consumer.poll(0.0)
            active = manual_consumer
            if msg is None:
                msg = auto_consumer.poll(1.0)
                active = auto_consumer
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[scoring] error: {msg.error()}")
                continue

            t_score = time.perf_counter()
            tx = json.loads(msg.value())
            result = score(artifact, tx)
            latency_ms = (time.perf_counter() - t_score) * 1000

            producer.produce(OUT_TOPIC, key=result["transaction_id"], value=json.dumps(result))
            producer.flush()
            active.commit(asynchronous=False)

            count += 1
            if result["is_fraud"]:
                fraud_count += 1

            if count % 10_000 == 0:
                elapsed = time.perf_counter() - t0
                print(f"[scoring] {count} scored | {count/elapsed:.0f} tx/s | fraud={fraud_count} | last_latency={latency_ms:.2f}ms")

    finally:
        print(f"[scoring] shutting down cleanly — {count} messages processed")
        manual_consumer.close()
        auto_consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
