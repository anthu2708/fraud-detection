from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..services import kafka
from ..services.db import Transaction, engine

router = APIRouter()


class SubmitRequest(BaseModel):
    amount: float
    time: float | None = None


@router.post("/transactions/submit")
def submit_transaction(req: SubmitRequest):
    try:
        tx_id = kafka.produce(req.amount, req.time, "manual")
    except RuntimeError:
        raise HTTPException(503, "Kafka unavailable")
    return {"transaction_id": tx_id}


@router.get("/transactions/fraud")
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


@router.get("/transactions/{transaction_id}/status")
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


@router.get("/transactions")
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


@router.get("/stats")
def stats():
    with Session(engine) as s:
        total = s.query(Transaction).count()
        fraud = s.query(Transaction).filter_by(is_fraud=True).count()
        return {"total": total, "fraud": fraud, "fraud_rate": round(fraud / total, 4) if total else 0}


@router.get("/health")
def health(response: Response):
    alive = kafka.consumer_alive()
    if not alive:
        response.status_code = 503
    return {"status": "ok" if alive else "degraded", "consumer": "alive" if alive else "dead"}
