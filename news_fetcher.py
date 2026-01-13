# news_fetcher.py
from __future__ import annotations

import time
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional

import feedparser
import requests

from config import NEWS_API_KEY, FINNHUB_API_KEY

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Safari/537.36"
)

REQ_TIMEOUT = 8
LOOKBACK_DAYS_DEFAULT = 7

# In-memory cache (per process run)
# key: (symbol, max_articles) -> list[str]
_CACHE: Dict[Tuple[str, int], List[str]] = {}

# Light rate-limit to avoid bursting APIs in loops
# (Helps Railway + avoids Finnhub/NewsAPI throttling)
_LAST_CALL_TS = 0.0
MIN_SECONDS_BETWEEN_CALLS = 0.15


def _throttle():
    global _LAST_CALL_TS
    now = time.time()
    delta = now - _LAST_CALL_TS
    if delta < MIN_SECONDS_BETWEEN_CALLS:
        time.sleep(MIN_SECONDS_BETWEEN_CALLS - delta)
    _LAST_CALL_TS = time.time()


def _safe_get(url: str) -> Optional[requests.Response]:
    try:
        _throttle()
        return requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQ_TIMEOUT)
    except Exception:
        return None


def _norm_title(t: str) -> str:
    return " ".join((t or "").strip().lower().split())


def _dedupe_items(items: List[Tuple[str, str, str]]) -> List[Tuple[str, str, str]]:
    """
    items: (title, url, source)
    Removes duplicates by normalized title or url.
    """
    seen = set()
    out = []
    for title, url, source in items:
        key = (_norm_title(title), (url or "").strip())
        if key in seen:
            continue
        seen.add(key)
        out.append((title, url, source))
    return out


def _to_html(items: List[Tuple[str, str, str]]) -> List[str]:
    """
    Convert items into your existing HTML format.
    """
    out = []
    for title, link, source in items:
        title = title or "No title"
        link = link or "#"
        # include source tag for trust
        out.append(f'✅ <a href="{link}" target="_blank">{title}</a> <span style="color:#666;">({source})</span>')
    return out


# -----------------------------
# NewsAPI.org
# -----------------------------
def fetch_newsapi(symbol: str, max_articles: int = 3, lookback_days: int = LOOKBACK_DAYS_DEFAULT):
    if not NEWS_API_KEY:
        return []

    try:
        company_map = {
            "AAPL": "Apple",
            "MSFT": "Microsoft",
            "GOOGL": "Alphabet",
            "AMZN": "Amazon",
            "TSLA": "Tesla",
            "META": "Meta",
            "NVDA": "Nvidia",
        }
        query = company_map.get(symbol, symbol)

        from_dt = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat()

        url = (
            "https://newsapi.org/v2/everything"
            f"?q={query}"
            "&sortBy=publishedAt"
            f"&from={from_dt}"
            f"&apiKey={NEWS_API_KEY}"
            f"&pageSize={max_articles}"
            "&language=en"
        )

        r = _safe_get(url)
        if not r:
            return []

        data = r.json() if r.status_code == 200 else {}
        arts = data.get("articles", []) or []

        items: List[Tuple[str, str, str]] = []
        for art in arts[: max_articles * 2]:  # pull a few extra for dedupe
            title = art.get("title") or ""
            link = art.get("url") or ""
            if not title or not link:
                continue
            items.append((title, link, "NewsAPI"))

        items = _dedupe_items(items)[:max_articles]
        return _to_html(items)
    except Exception:
        return []


# -----------------------------
# Yahoo Finance RSS
# -----------------------------
def fetch_yahoo_rss(symbol: str, max_articles: int = 3):
    try:
        url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
        feed = feedparser.parse(url)

        items: List[Tuple[str, str, str]] = []
        for entry in (feed.entries or [])[: max_articles * 2]:
            title = getattr(entry, "title", "") or ""
            link = getattr(entry, "link", "") or ""
            if not title or not link:
                continue
            items.append((title, link, "Yahoo RSS"))

        items = _dedupe_items(items)[:max_articles]
        return _to_html(items)
    except Exception:
        return []


# -----------------------------
# Finnhub.io (dynamic date range)
# -----------------------------
def fetch_finnhub(symbol: str, max_articles: int = 3, lookback_days: int = LOOKBACK_DAYS_DEFAULT):
    if not FINNHUB_API_KEY:
        return []

    try:
        to_date = datetime.utcnow().date()
        from_date = to_date - timedelta(days=lookback_days)

        url = (
            "https://finnhub.io/api/v1/company-news"
            f"?symbol={symbol}"
            f"&from={from_date.isoformat()}"
            f"&to={to_date.isoformat()}"
            f"&token={FINNHUB_API_KEY}"
        )

        r = _safe_get(url)
        if not r or r.status_code != 200:
            return []

        data = r.json() if r.status_code == 200 else []
        if not isinstance(data, list):
            return []

        items: List[Tuple[str, str, str]] = []
        for item in data[: max_articles * 3]:
            title = item.get("headline") or ""
            link = item.get("url") or ""
            if not title or not link:
                continue
            items.append((title, link, "Finnhub"))

        items = _dedupe_items(items)[:max_articles]
        return _to_html(items)
    except Exception:
        return []


# -----------------------------
# Unified news fetcher
# Priority: NewsAPI -> Finnhub -> Yahoo
# Returns list[str] of HTML links (backward compatible)
# -----------------------------
def fetch_news_links(symbol: str, max_articles: int = 3) -> List[str]:
    # cache for this run (very important: scoring + email may call twice)
    key = (symbol.upper(), int(max_articles))
    if key in _CACHE:
        return _CACHE[key]

    news = fetch_newsapi(symbol, max_articles=max_articles, lookback_days=LOOKBACK_DAYS_DEFAULT)

    if not news:
        news = fetch_finnhub(symbol, max_articles=max_articles, lookback_days=LOOKBACK_DAYS_DEFAULT)

    if not news:
        news = fetch_yahoo_rss(symbol, max_articles=max_articles)

    if not news:
        news = ["No news available"]

    _CACHE[key] = news
    return news