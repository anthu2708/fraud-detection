import joblib
import numpy as np


def score_transaction(transaction: dict, model_path: str = "models/isolation_forest_v2.pkl") -> dict:
    artifact = joblib.load(model_path)
    model = artifact["model"]
    features = artifact["features"]
    scaler = artifact.get("scaler")
    scale_cols = artifact.get("scale_cols", [])
    threshold = artifact.get("threshold")

    X = np.array([[transaction[f] for f in features]])

    if scaler is not None and scale_cols:
        X[:, scale_cols] = scaler.transform(X[:, scale_cols])

    raw_score = float(model.score_samples(X)[0])  # more negative = more anomalous

    if threshold is not None:
        is_fraud = raw_score < threshold
    else:
        is_fraud = model.predict(X)[0] == -1

    risk_score = round(float(np.clip(-raw_score, 0, 1)), 6)

    return {
        "anomaly_score": round(raw_score, 6),
        "risk_score": risk_score,
        "is_fraud": bool(is_fraud),
    }
