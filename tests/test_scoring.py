import joblib
import numpy as np
import pytest
from sklearn.ensemble import IsolationForest

from src.scoring import score_transaction


@pytest.fixture
def model_path(tmp_path):
    X = np.random.default_rng(42).standard_normal((200, 2))
    model = IsolationForest(contamination=0.1, random_state=42)
    model.fit(X)
    path = tmp_path / "test_model.pkl"
    joblib.dump({"model": model, "features": ["f1", "f2"]}, path)
    return str(path)


def test_returns_required_keys(model_path):
    result = score_transaction({"f1": 0.5, "f2": -0.3}, model_path=model_path)
    assert {"anomaly_score", "risk_score", "is_fraud"} <= result.keys()


def test_is_fraud_is_bool(model_path):
    result = score_transaction({"f1": 0.5, "f2": -0.3}, model_path=model_path)
    assert isinstance(result["is_fraud"], bool)


def test_obvious_anomaly_flagged(model_path):
    result = score_transaction({"f1": 999.0, "f2": 999.0}, model_path=model_path)
    assert result["is_fraud"] is True


def test_risk_score_range(model_path):
    result = score_transaction({"f1": 0.1, "f2": 0.1}, model_path=model_path)
    assert 0.0 <= result["risk_score"] <= 1.0
