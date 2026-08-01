import json
import os
import threading

from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI
from contextlib import asynccontextmanager
from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, create_engine, func
from sqlalchemy.orm import DeclarativeBase, Session

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://fraud:fraud@localhost:5432/fraud")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SCORED_TOPIC = "transactions.scored"
SKIP_KAFKA = os.getenv("SKIP_KAFKA", "false").lower() == "true"

_connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=_connect_args)


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
    Base.metadata.create_all(engine)
    if not SKIP_KAFKA:
        threading.Thread(target=_consume_loop, daemon=True).start()
    yield


app = FastAPI(title="Fraud Detection Query API", lifespan=lifespan)


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
