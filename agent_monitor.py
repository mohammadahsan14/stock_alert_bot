# agent_monitor.py
from __future__ import annotations

import os
from datetime import datetime, time
from zoneinfo import ZoneInfo

from performance_tracker import (
    PortfolioConfig,
    load_open_portfolio,
    save_open_portfolio,
    append_trade_history,
    update_and_close_positions,
    portfolio_summary,
)

from email_sender import send_email as _send_email
from config import (
    IS_LOCAL,
    SENDER_EMAIL,
    RECEIVER_EMAIL,
    LOCAL_RECEIVER_EMAIL,
    EMAIL_SUBJECT_PREFIX_LOCAL,
    EMAIL_SUBJECT_PREFIX_PROD,
)

# -----------------------------
# Config
# -----------------------------
LOCAL_TZ = ZoneInfo("America/Chicago")

SESSION_START = time(8, 30)
SESSION_END = time(15, 0)

PAPER_TRADING_ENABLED = os.getenv("PAPER_TRADING_ENABLED", "0") == "1"
AGENT_EMAIL_ON_CLOSE = os.getenv("AGENT_EMAIL_ON_CLOSE", "1") == "1"
EMAIL_DRY_RUN = os.getenv("EMAIL_DRY_RUN", "0") == "1"

EMAIL_SUBJECT_PREFIX = EMAIL_SUBJECT_PREFIX_LOCAL if IS_LOCAL else EMAIL_SUBJECT_PREFIX_PROD
EFFECTIVE_RECEIVER_EMAIL = (
    (LOCAL_RECEIVER_EMAIL or RECEIVER_EMAIL) if IS_LOCAL else RECEIVER_EMAIL
)


# -----------------------------
# Email helper
# -----------------------------
def send_email(subject: str, html_body: str) -> bool:
    final_subject = f"{EMAIL_SUBJECT_PREFIX} {subject}"

    if EMAIL_DRY_RUN:
        print(f"🧪 DRY RUN: would send email → {final_subject}")
        return True

    return _send_email(
        subject=final_subject,
        html_body=html_body,
        to_email=EFFECTIVE_RECEIVER_EMAIL,
        from_email=SENDER_EMAIL,
        attachment_path=None,
    )


# -----------------------------
# Agent Monitor
# -----------------------------
def run_agent_monitor(now: datetime | None = None) -> None:
    now = now or datetime.now(LOCAL_TZ)

    # Safety: only run if explicitly enabled
    if not PAPER_TRADING_ENABLED:
        print("🧪 agent_monitor: PAPER_TRADING_ENABLED=0 (skipping)")
        return

    # Only monitor during market hours (unless local)
    if not IS_LOCAL:
        if now.time() < SESSION_START or now.time() > SESSION_END:
            print("🧪 agent_monitor: outside market hours (skipping)")
            return

    cfg = PortfolioConfig()
    open_df = load_open_portfolio(cfg)

    if open_df is None or open_df.empty:
        print("🧪 agent_monitor: no open positions")
        return

    # Evaluate open positions
    remaining, closed_df = update_and_close_positions(cfg, open_df, now)
    save_open_portfolio(cfg, remaining)

    # If nothing closed
    if closed_df is None or closed_df.empty:
        print("🧪 agent_monitor: nothing closed this cycle")
        return

    # Append closed trades to history
    closed_hist = closed_df.copy()
    closed_hist["action"] = "CLOSE"
    append_trade_history(cfg, closed_hist)

    summary = portfolio_summary(remaining, closed_df)

    print("✅ agent_monitor closed positions:", len(closed_df))
    print(summary)

    # Optional email notification
    if AGENT_EMAIL_ON_CLOSE:
        rows = []
        for _, r in closed_df.iterrows():
            sym = str(r.get("symbol", ""))
            entry = r.get("entry_price", "")
            exitp = r.get("exit_price", "")
            outcome = r.get("outcome", "")
            rows.append(
                f"<li><b>{sym}</b> entry={entry} exit={exitp} {outcome}</li>"
            )

        html = f"""
        <h2>🤖 Paper Agent — Closed Positions</h2>
        <p><b>Time:</b> {now.strftime('%Y-%m-%d %H:%M:%S %Z')}</p>
        <ul>{''.join(rows)}</ul>
        <pre>{summary}</pre>
        """

        send_email("🤖 Paper Agent — Positions Closed", html)


if __name__ == "__main__":
    run_agent_monitor()