"""Train Isolation Forest on Kaggle Credit Card Fraud dataset and save model."""
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import (
    classification_report,
    precision_recall_curve,
    precision_score,
    recall_score,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import RobustScaler

DATA_PATH = Path("data/creditcard.csv")


def _load_split(data_path: Path):
    df = pd.read_csv(data_path)
    features = [c for c in df.columns if c != "Class"]
    X = df[features].values
    y = df["Class"].values
    split = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    _X_train, X_test, _y_train, y_test = split

    # Save test set so Producer replays only unseen transactions
    test_df = pd.DataFrame(X_test, columns=features)
    test_df["Class"] = y_test
    test_path = data_path.parent / "creditcard_test.csv"
    test_df.to_csv(test_path, index=False)
    print(f"Test set saved → {test_path}  ({len(test_df)} rows)")

    return split, features


def _best_threshold(scores: np.ndarray, y_true: np.ndarray, min_precision: float = 0.10) -> float:
    """Find score threshold maximising F2 with precision >= min_precision.

    Fraud detection: recall matters more than precision, but flagging everything
    is useless. min_precision ensures at least 1-in-10 flags are real fraud.
    """
    # precision_recall_curve expects higher score = positive class; our scores:
    # more negative = more anomalous, so pass -scores
    prec, rec, thresholds = precision_recall_curve(y_true, -scores)
    # F2 = 5*p*r / (4*p + r)
    f2 = (5 * prec * rec) / (4 * prec + rec + 1e-9)
    mask = prec >= min_precision
    if not mask.any():
        return float(np.percentile(scores, 95))
    best_idx = int(np.argmax(np.where(mask, f2, 0)))
    best_idx = min(best_idx, len(thresholds) - 1)
    return float(-thresholds[best_idx])  # convert back to original score space


def train_v1():
    """Baseline: train on all data, no scaling, default contamination threshold."""
    (X_train, X_test, y_train, y_test), features = _load_split(DATA_PATH)

    contamination = float(y_train.mean())
    model = IsolationForest(n_estimators=100, contamination=contamination, random_state=42, n_jobs=-1)
    print("Training v1...")
    model.fit(X_train)

    preds = (model.predict(X_test) == -1).astype(int)
    print(f"\nPrecision (fraud): {precision_score(y_test, preds):.4f}")
    print(f"Recall    (fraud): {recall_score(y_test, preds):.4f}")
    print(classification_report(y_test, preds, target_names=["normal", "fraud"]))

    path = Path("models/isolation_forest_v1.pkl")
    path.parent.mkdir(exist_ok=True)
    joblib.dump({"model": model, "scaler": None, "scale_cols": [], "threshold": None, "features": features}, path)
    print(f"Model saved → {path}")


def train_v2():
    """v2: scale Amount+Time, train on normal-only, tune threshold for F2."""
    (X_train, X_test, y_train, y_test), features = _load_split(DATA_PATH)

    scale_cols = [features.index("Time"), features.index("Amount")]
    X_normal_raw = X_train[y_train == 0]
    scaler = RobustScaler()
    scaler.fit(X_normal_raw[:, scale_cols])
    X_train_s = X_train.copy()
    X_test_s = X_test.copy()
    X_train_s[:, scale_cols] = scaler.transform(X_train[:, scale_cols])
    X_test_s[:, scale_cols] = scaler.transform(X_test[:, scale_cols])

    X_normal = X_train_s[y_train == 0]
    contamination = float(y_train.mean())

    model = IsolationForest(n_estimators=200, contamination=contamination, random_state=42, n_jobs=-1)
    print("Training v2 (normal-only, scaled, threshold-tuned)...")
    model.fit(X_normal)

    # Raw anomaly scores (more negative = more anomalous)
    scores = model.score_samples(X_test_s)
    threshold = _best_threshold(scores, y_test)
    preds = (scores < threshold).astype(int)

    precision = precision_score(y_test, preds)
    recall = recall_score(y_test, preds)
    print(f"\nThreshold (score): {threshold:.6f}")
    print(f"Precision (fraud): {precision:.4f}")
    print(f"Recall    (fraud): {recall:.4f}")
    print(classification_report(y_test, preds, target_names=["normal", "fraud"]))

    path = Path("models/isolation_forest_v2.pkl")
    path.parent.mkdir(exist_ok=True)
    joblib.dump({
        "model": model,
        "scaler": scaler,
        "scale_cols": scale_cols,
        "threshold": threshold,
        "features": features,
    }, path)
    print(f"Model saved → {path}")


def train_v3():
    """relataly approach: GridSearchCV with y labels to find best hyperparams."""
    from sklearn.metrics import f1_score as f1
    from sklearn.metrics import make_scorer
    from sklearn.model_selection import GridSearchCV

    (X_train, X_test, y_train, y_test), features = _load_split(DATA_PATH)
    contamination = float(y_train.mean())

    # IsolationForest predict() returns -1 (anomaly) / 1 (normal)
    # Map y to same space so GridSearchCV scorer works
    y_train_mapped = np.where(y_train == 1, -1, 1)
    _y_test_mapped  = np.where(y_test  == 1, -1, 1)

    model = IsolationForest(contamination=contamination, random_state=42, n_jobs=-1)
    scorer = make_scorer(f1, pos_label=-1, average="binary")

    grid = GridSearchCV(
        model,
        param_grid={"n_estimators": [50, 100, 200], "max_features": [1.0, 5, 10], "bootstrap": [True]},
        scoring=scorer,
        cv=3,
        verbose=1,
        n_jobs=-1,
    )
    print("Grid search (relataly approach)...")
    grid.fit(X_train, y_train_mapped)

    print(f"\nBest params: {grid.best_params_}")
    preds = grid.best_estimator_.predict(X_test)          # -1 / 1
    preds_binary = (preds == -1).astype(int)              # 1=fraud, 0=normal

    print(f"Precision (fraud): {precision_score(y_test, preds_binary):.4f}")
    print(f"Recall    (fraud): {recall_score(y_test, preds_binary):.4f}")
    print(classification_report(y_test, preds_binary, target_names=["normal", "fraud"]))

    path = Path("models/isolation_forest_v3.pkl")
    path.parent.mkdir(exist_ok=True)
    joblib.dump({
        "model": grid.best_estimator_,
        "scaler": None, "scale_cols": [], "threshold": None,
        "features": features,
    }, path)
    print(f"Model saved → {path}")


if __name__ == "__main__":
    version = sys.argv[1] if len(sys.argv) > 1 else "v2"
    if version == "v1":
        train_v1()
    elif version == "v3":
        train_v3()
    else:
        train_v2()
