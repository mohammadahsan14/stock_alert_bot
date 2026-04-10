from __future__ import annotations

import json
from pathlib import Path

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "outputs" / "local" / "ml"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PERF_LOG_PATH = BASE_DIR / "outputs" / "local" / "logs" / "performance_log.csv"
MODEL_PATH = OUTPUT_DIR / "strategy_model.joblib"
METRICS_PATH = OUTPUT_DIR / "strategy_model_metrics.json"


def load_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Performance log not found: {path}")

    df = pd.read_csv(path)
    if df.empty:
        raise ValueError("Performance log is empty.")

    return df


def build_target(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Keep only rows with final evaluated outcomes
    valid_outcomes = {"🏆 Target Hit", "🛑 Stop Hit", "⏳ Not Hit"}
    df = df[df["outcome"].isin(valid_outcomes)].copy()

    if df.empty:
        raise ValueError("No evaluated rows found in performance log.")

    # Binary target:
    # 1 = target hit
    # 0 = everything else
    df["target_label"] = (df["outcome"] == "🏆 Target Hit").astype(int)

    return df


def select_features(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, list[str], list[str]]:
    candidate_numeric = [
        "score",
        "confidence",
        "entry_price",
        "target_price",
        "stop_loss",
    ]

    candidate_categorical = [
        "source_mode",
        "instrument_type",
        "decision",
    ]

    numeric_features = [c for c in candidate_numeric if c in df.columns]
    categorical_features = [c for c in candidate_categorical if c in df.columns]

    feature_cols = numeric_features + categorical_features
    if not feature_cols:
        raise ValueError("No usable features found in dataset.")

    x = df[feature_cols].copy()
    y = df["target_label"].copy()

    return x, y, numeric_features, categorical_features


def build_pipeline(numeric_features: list[str], categorical_features: list[str]) -> Pipeline:
    numeric_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )

    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_features),
            ("cat", categorical_transformer, categorical_features),
        ]
    )

    model = LogisticRegression(
        max_iter=2000,
        class_weight="balanced",
        random_state=42,
    )

    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )


def main() -> None:
    print("Loading data...")
    df = load_data(PERF_LOG_PATH)
    print(f"Rows loaded: {len(df)}")

    print("Building target...")
    df = build_target(df)
    print(f"Rows after outcome filter: {len(df)}")
    print("Outcome counts:")
    print(df["outcome"].value_counts(dropna=False))

    x, y, numeric_features, categorical_features = select_features(df)


    class_counts = y.value_counts().to_dict()
    print("\nTarget label counts:")
    print(class_counts)

    if y.nunique() < 2:
        print("\nNot enough class variety to train yet.")
        print("Need at least one positive and one negative example.")
        print("Collect more postmarket history, then rerun.")
        return

    print("\nNumeric features:")
    print(numeric_features)

    print("\nCategorical features:")
    print(categorical_features)

    if len(df) < 20:
        print("\nWarning: very small dataset. Model results will not be reliable yet.")

    x_train, x_test, y_train, y_test = train_test_split(
        x,
        y,
        test_size=0.3,
        random_state=42,
        stratify=y if y.nunique() > 1 else None,
    )

    pipeline = build_pipeline(numeric_features, categorical_features)

    print("\nTraining model...")
    pipeline.fit(x_train, y_train)

    print("Evaluating model...")
    y_pred = pipeline.predict(x_test)

    metrics: dict[str, object] = {
        "rows_total": int(len(df)),
        "rows_train": int(len(x_train)),
        "rows_test": int(len(x_test)),
        "positive_rate": float(y.mean()),
        "numeric_features": numeric_features,
        "categorical_features": categorical_features,
        "classification_report": classification_report(y_test, y_pred, output_dict=True),
        "confusion_matrix": confusion_matrix(y_test, y_pred).tolist(),
    }

    if len(set(y_test)) > 1:
        y_prob = pipeline.predict_proba(x_test)[:, 1]
        metrics["roc_auc"] = float(roc_auc_score(y_test, y_prob))

    print("\nClassification report:")
    print(classification_report(y_test, y_pred))

    print("Confusion matrix:")
    print(confusion_matrix(y_test, y_pred))

    if "roc_auc" in metrics:
        print(f"ROC-AUC: {metrics['roc_auc']:.4f}")

    print(f"\nSaving model to: {MODEL_PATH}")
    joblib.dump(pipeline, MODEL_PATH)

    print(f"Saving metrics to: {METRICS_PATH}")
    with open(METRICS_PATH, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    print("\nDone.")


if __name__ == "__main__":
    main()