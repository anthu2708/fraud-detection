import json
import os
import time

import joblib
import numpy as np
from confluent_kafka import Consumer, KafkaError, Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
IN_TOPIC = "transactions.raw"
OUT_TOPIC = "transactions.scored"
MODEL_PATH = os.getenv("MODEL_PATH", "models/isolation_forest_v2.pkl")
MODEL_VERSION = os.getenv("MODEL_VERSION", "v2")


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
        "anomaly_score": round(raw, 6),
        "risk_score": round(float(min(max(-raw, 0), 1)), 6),
        "is_fraud": bool(is_fraud),
        "model_version": MODEL_VERSION,
    }


def main():
    artifact = load_model(MODEL_PATH)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "scoring-service",
        "auto.offset.reset": "earliest",
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    consumer.subscribe([IN_TOPIC])

    count = 0
    fraud_count = 0
    t0 = time.perf_counter()

    print(f"[scoring] consuming {IN_TOPIC} → {OUT_TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)
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
            producer.poll(0)

            count += 1
            if result["is_fraud"]:
                fraud_count += 1

            if count % 10_000 == 0:
                elapsed = time.perf_counter() - t0
                print(f"[scoring] {count} scored | {count/elapsed:.0f} tx/s | fraud={fraud_count} | last_latency={latency_ms:.2f}ms")

    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
