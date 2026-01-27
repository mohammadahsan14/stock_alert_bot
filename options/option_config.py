# options/option_config.py
from __future__ import annotations

import os
from typing import List


def load_occ_list() -> List[str]:
    """
    Load OCC option symbols for option analysis.

    Priority:
    1) OPTIONS_OCC_LIST from .env (explicit override / manual testing)
       Example:
         OPTIONS_OCC_LIST=AAPL260620C00250000,TSLA260620P00400000

    2) (Future) Dynamically generated list from stock picks
       - This will be injected later from option_universe.py
       - Kept empty for now to avoid breaking changes
    """

    raw = (os.getenv("OPTIONS_OCC_LIST") or "").strip()

    if raw:
        occs = [x.strip().upper() for x in raw.split(",") if x.strip()]
        print(f"🔧 Loaded {len(occs)} OCC symbols from OPTIONS_OCC_LIST")
        return occs

    # ---- FUTURE EXTENSION POINT ----
    # Example (not active yet):
    #
    # from options.option_universe import build_occ_from_stock_picks
    # return build_occ_from_stock_picks()

    print("ℹ️ OPTIONS_OCC_LIST not set — no manual option symbols loaded")
    return []