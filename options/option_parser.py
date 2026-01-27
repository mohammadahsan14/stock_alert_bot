from __future__ import annotations
import re
from datetime import date

# OCC format:
# ROOT(1-6) + YYMMDD + C/P + STRIKE(8 digits, strike*1000)
# Example: AAPL250117C00150000
OCC_RE = re.compile(r"^([A-Z]{1,6})(\d{2})(\d{2})(\d{2})([CP])(\d{8})$")


def parse_option_symbol(symbol: str) -> dict:
    s = (symbol or "").strip().upper()
    m = OCC_RE.match(s)
    if not m:
        raise ValueError(f"Not a valid OCC option symbol: {symbol}")

    underlying, yy, mm, dd, cp, strike8 = m.groups()

    expiry = date(2000 + int(yy), int(mm), int(dd))
    strike = int(strike8) / 1000.0

    return {
        "symbol": s,
        "underlying": underlying,
        "expiry": expiry,
        "option_type": cp,   # "C" or "P"
        "strike": strike,
    }


if __name__ == "__main__":
    tests = [
        "AAPL250117C00150000",
        "TSLA250621P00200000",
    ]
    for t in tests:
        print("\nRAW:", t)
        print(parse_option_symbol(t))