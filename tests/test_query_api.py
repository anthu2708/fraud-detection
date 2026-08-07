import os

# Set before importing app so SQLAlchemy uses SQLite and Kafka is skipped
os.environ.setdefault("DATABASE_URL", "sqlite:///./test_fraud.db")
os.environ.setdefault("SKIP_KAFKA", "true")

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from query_api.main import app
from query_api.services.db import Base, Transaction, engine

client = TestClient(app)


@pytest.fixture(autouse=True)
def fresh_db():
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)


def test_health():
    r = client.get("/health")
    # SKIP_KAFKA=true → no consumer thread → 503 degraded
    assert r.status_code == 503
    assert r.json()["status"] == "degraded"
    assert r.json()["consumer"] == "dead"


def test_list_transactions_empty():
    r = client.get("/transactions")
    assert r.status_code == 200
    assert r.json() == []


def test_fraud_transactions_empty():
    r = client.get("/transactions/fraud")
    assert r.status_code == 200
    assert r.json() == []


def test_stats_no_data():
    r = client.get("/stats")
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 0
    assert data["fraud"] == 0


def test_list_returns_inserted_transaction():
    with Session(engine) as s:
        s.add(Transaction(transaction_id="tx-001", anomaly_score=-0.4, risk_score=0.4, is_fraud=True, model_version="v2"))
        s.commit()

    r = client.get("/transactions")
    assert r.status_code == 200
    assert len(r.json()) == 1
    assert r.json()[0]["transaction_id"] == "tx-001"


def test_fraud_endpoint_filters_correctly():
    with Session(engine) as s:
        s.add(Transaction(transaction_id="tx-fraud", anomaly_score=-0.5, risk_score=0.5, is_fraud=True, model_version="v2"))
        s.add(Transaction(transaction_id="tx-normal", anomaly_score=-0.2, risk_score=0.2, is_fraud=False, model_version="v2"))
        s.commit()

    r = client.get("/transactions/fraud")
    assert r.status_code == 200
    ids = [t["transaction_id"] for t in r.json()]
    assert "tx-fraud" in ids
    assert "tx-normal" not in ids


def test_stats_counts_correctly():
    with Session(engine) as s:
        s.add(Transaction(transaction_id="f1", anomaly_score=-0.5, risk_score=0.5, is_fraud=True, model_version="v2"))
        s.add(Transaction(transaction_id="n1", anomaly_score=-0.2, risk_score=0.2, is_fraud=False, model_version="v2"))
        s.add(Transaction(transaction_id="n2", anomaly_score=-0.1, risk_score=0.1, is_fraud=False, model_version="v2"))
        s.commit()

    r = client.get("/stats")
    assert r.json()["total"] == 3
    assert r.json()["fraud"] == 1


def test_submit_unavailable_when_kafka_skipped():
    r = client.post("/transactions/submit", json={"amount": 100.0})
    assert r.status_code == 503


def test_transaction_status_pending():
    r = client.get("/transactions/nonexistent-id/status")
    assert r.status_code == 200
    assert r.json()["status"] == "pending"


def test_transaction_status_scored():
    with Session(engine) as s:
        s.add(Transaction(transaction_id="tx-scored", anomaly_score=-0.4, risk_score=0.4, is_fraud=True, model_version="v2"))
        s.commit()

    r = client.get("/transactions/tx-scored/status")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "scored"
    assert data["is_fraud"] is True
    assert data["transaction_id"] == "tx-scored"
