# llm/client.py (LOCKED + env-driven + timeout + minimal retry)
from __future__ import annotations

import os
import time
from typing import Optional

from openai import OpenAI


_client: Optional[OpenAI] = None


def _is_local() -> bool:
    # aligns with your pipeline style (IS_LOCAL exists in config, but keep client standalone)
    return os.getenv("IS_LOCAL", "0").strip().lower() in {"1", "true", "yes", "y", "on"}


def get_client() -> OpenAI:
    global _client
    if _client is not None:
        return _client

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise ValueError("OPENAI_API_KEY not found in environment variables.")

    _client = OpenAI(api_key=api_key)
    return _client


def llm_text(
    prompt: str,
    *,
    model: Optional[str] = None,
    temperature: Optional[float] = None,
    max_output_tokens: int = 400,
) -> str:
    """
    Send prompt to LLM and return plain text response.
    Locked behaviors:
      - env-driven model/temperature
      - timeout + 1 retry
      - safe empty-string fallback on failure
    """
    if not prompt or not str(prompt).strip():
        return ""

    m = (model or os.getenv("LLM_MODEL", "gpt-4.1-mini")).strip()

    # temperature is only used if provided; keep default low for stability
    # If STRICT_LLM=1, force temperature=0 for determinism.
    strict = os.getenv("STRICT_LLM", "0").strip() == "1"
    t_env = os.getenv("LLM_TEMPERATURE", "").strip()
    t_val = temperature if temperature is not None else (float(t_env) if t_env else 0.2)
    if strict:
        t_val = 0.0

    # hard clamp
    try:
        max_out = int(max_output_tokens)
    except Exception:
        max_out = 400
    max_out = max(64, min(max_out, 1200))

    client = get_client()

    # small retry to handle transient failures
    attempts = 2
    for i in range(attempts):
        try:
            response = client.responses.create(
                model=m,
                input=prompt,
                max_output_tokens=max_out,
                temperature=t_val,
                timeout=float(os.getenv("LLM_TIMEOUT_SEC", "20")),
            )
            out = (response.output_text or "").strip()
            return out
        except Exception:
            if i == attempts - 1:
                return ""
            # short backoff
            time.sleep(0.6)

    return ""