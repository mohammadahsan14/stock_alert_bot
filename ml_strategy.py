from __future__ import annotations

from pathlib import Path
from typing import Optional

import joblib
import pandas as pd



BASE_DIR = Path(__file__).resolve().parent
MODEL_PATH = BASE_DIR / "outputs" / "local" / "ml" / "strategy_model.joblib"

_MODEL = None


def load_model():
    global _MODEL
    if _MODEL is None:
        if not MODEL_PATH.exists():
            return None
        _MODEL = joblib.load(MODEL_PATH)
    return _MODEL


def predict_win_prob(row: dict) -> Optional[float]:
    model = load_model()
    if model is None:
        return None

    features = pd.DataFrame([{
        "score": row.get("score"),
        "confidence": row.get("confidence"),
        "entry_price": row.get("current"),
        "target_price": row.get("target_price"),
        "stop_loss": row.get("stop_loss"),
        "source_mode": row.get("source_mode", "midday"),
        "instrument_type": row.get("instrument_type", "stock"),
        "decision": row.get("decision"),
    }])

    try:
        prob = model.predict_proba(features)[0][1]
        return float(prob)
    except Exception:
        return None