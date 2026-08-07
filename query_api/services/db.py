import os

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
from sqlalchemy.orm import DeclarativeBase

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./fraud_local.db")

_connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=_connect_args)

_MIGRATIONS = [
    "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS submitted_at TIMESTAMP",
    "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS source VARCHAR(10) DEFAULT 'auto'",
    "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS amount FLOAT",
]


def migrate() -> None:
    """Add columns that may be missing from tables created before schema updates."""
    if DATABASE_URL.startswith("sqlite"):
        return  # SQLite re-creates fresh in tests; no migration needed
    from sqlalchemy import text
    with engine.begin() as conn:
        for sql in _MIGRATIONS:
            conn.execute(text(sql))


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
    source = Column(String(10), default="auto")
    amount = Column(Float)
