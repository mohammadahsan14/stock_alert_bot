#!/bin/zsh

cd /Users/mohammadahsan/PycharmProjects/stock_alert_bot || exit 1

echo "[$(date)] run_longterm_daily.sh started"

export APP_ENV=local
export SEND_LONGTERM_EMAIL=1

/Users/mohammadahsan/PycharmProjects/stock_alert_bot/.venv/bin/python longterm_strategy.py
/Users/mohammadahsan/PycharmProjects/stock_alert_bot/.venv/bin/python -c "from longterm_strategy import send_portfolio_alerts; print(send_portfolio_alerts())"

echo "[$(date)] run_longterm_daily.sh finished"