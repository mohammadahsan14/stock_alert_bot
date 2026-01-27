# news_fetcher.py
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional
from urllib.parse import quote_plus, urlparse

import feedparser
import requests

from config import NEWS_API_KEY, FINNHUB_API_KEY

# -----------------------------
# Hard blocklist (known junk / consent / irrelevant)
# -----------------------------
BLOCKED_NEWS_DOMAINS = {
    "consent.yahoo.com",
    "languagelog.ldc.upenn.edu",
    "pypi.org",
    "globalresearch.ca",
    "tomshardware.com",
    "timesofindia.indiatimes.com",
    "nature.com",
}


def _is_blocked_url(u: str) -> bool:
    try:
        host = (urlparse(u).netloc or "").lower().replace("www.", "")
        return any(host == d or host.endswith("." + d) for d in BLOCKED_NEWS_DOMAINS)
    except Exception:
        return False


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Safari/537.36"
)

REQ_TIMEOUT = 8
LOOKBACK_DAYS_DEFAULT = 7

# Cache key includes lookback_days + max_articles
_CACHE: Dict[Tuple[str, int, int], List[str]] = {}
_CACHE_MAX = 2000  # prevent unbounded growth

_LAST_CALL_TS = 0.0
MIN_SECONDS_BETWEEN_CALLS = 0.15

DEBUG_NEWS = os.getenv("DEBUG_NEWS", "0") == "1"


# -----------------------------
# Relevance / junk filtering
# -----------------------------
_ALLOWED_NEWS_DOMAINS = {
    "reuters.com", "bloomberg.com", "wsj.com", "ft.com", "marketwatch.com",
    "cnbc.com", "finance.yahoo.com", "seekingalpha.com", "benzinga.com",
    "investors.com", "morningstar.com", "fool.com", "thestreet.com",
    "nasdaq.com", "nytimes.com", "theguardian.com", "apnews.com", "bbc.com",
    "globenewswire.com", "prnewswire.com", "businesswire.com",
}

# Extra soft-block patterns (keep small, let BLOCKED_NEWS_DOMAINS do the heavy work)
_BLOCK_URL_SUBSTRINGS = (
    "github.com/",
    "stackoverflow.com/",
)

_BLOCK_TITLE_HINTS = (
    "captcha", "recaptcha", "solver", "pypi", "package", "github", "npm",
)


def _domain(url: str) -> str:
    try:
        d = urlparse(url or "").netloc.lower()
        if d.startswith("www."):
            d = d[4:]
        return d
    except Exception:
        return ""


def _is_relevant(symbol: str, title: str, url: str) -> bool:
    sym = (symbol or "").upper().strip()
    t = (title or "").lower()
    u = (url or "").lower()

    # Hard block first
    if _is_blocked_url(url):
        return False

    # obvious garbage
    if any(s in u for s in _BLOCK_URL_SUBSTRINGS):
        return False
    if any(h in t for h in _BLOCK_TITLE_HINTS):
        return False
    # hard-block known bad domains
    if _is_blocked_url(url):
        return False
    # strong relevance: ticker appears in title or URL
    if sym:
        sym_l = sym.lower()
        if sym_l in t or f"/{sym_l}" in u or f"={sym_l}" in u:
            return True

    # fallback: trusted domains (still better than random)
    d = _domain(url)
    if d in _ALLOWED_NEWS_DOMAINS or any(d.endswith("." + x) for x in _ALLOWED_NEWS_DOMAINS):
        return True

    return False


def _filter_items(symbol: str, items: List[Tuple[str, str, str]], max_articles: int) -> List[Tuple[str, str, str]]:
    cleaned: List[Tuple[str, str, str]] = []
    for title, url, source in (items or []):
        if not title or not url:
            continue
        if _is_blocked_url(url):  # ✅ actually use the domain blocklist
            continue
        if _is_relevant(symbol, title, url):
            cleaned.append((title, url, source))
    return cleaned[:max_articles]


def _throttle():
    global _LAST_CALL_TS
    now = time.time()
    delta = now - _LAST_CALL_TS
    if delta < MIN_SECONDS_BETWEEN_CALLS:
        time.sleep(MIN_SECONDS_BETWEEN_CALLS - delta)
    _LAST_CALL_TS = time.time()


def _safe_get(url: str) -> Tuple[Optional[requests.Response], str]:
    """
    Returns: (response or None, error_reason)
    Retries once on common transient errors (429/5xx).
    """
    transient = {429, 500, 502, 503, 504}

    for attempt in range(2):
        try:
            _throttle()
            r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQ_TIMEOUT)
            if r.status_code in transient and attempt == 0:
                time.sleep(1.0)
                continue
            return r, ""
        except Exception as e:
            if attempt == 0:
                time.sleep(0.5)
                continue
            return None, f"exception:{type(e).__name__}"

    return None, "no_response"


def _norm_title(t: str) -> str:
    t = (t or "").strip().lower()
    # Normalize common suffix patterns
    for sep in [" - ", " | "]:
        if sep in t:
            t = t.split(sep)[0].strip()
    return " ".join(t.split())


def _dedupe_items(items: List[Tuple[str, str, str]]) -> List[Tuple[str, str, str]]:
    """
    items: (title, url, source)
    Removes duplicates by normalized title OR url.
    """
    seen_titles = set()
    seen_urls = set()
    out: List[Tuple[str, str, str]] = []

    for title, url, source in items:
        nt = _norm_title(title)
        u = (url or "").strip()

        if nt and nt in seen_titles:
            continue
        if u and u in seen_urls:
            continue

        if nt:
            seen_titles.add(nt)
        if u:
            seen_urls.add(u)

        out.append((title, url, source))

    return out


def _to_html(items: List[Tuple[str, str, str]]) -> List[str]:
    out = []
    for title, link, source in items:
        title = title or "No title"
        link = link or "#"
        out.append(
            f'✅ <a href="{link}" target="_blank">{title}</a> '
            f'<span style="color:#666;">({source})</span>'
        )
    return out


def _no_news_html(source: str = "none") -> List[str]:
    # Always return HTML format to keep downstream parsing consistent
    return [f'🟡 <span style="color:#666;">No recent news found ({source})</span>']


# -----------------------------
# NewsAPI.org
# -----------------------------
def fetch_newsapi(
    symbol: str,
    max_articles: int = 3,
    lookback_days: int = LOOKBACK_DAYS_DEFAULT
) -> Tuple[List[str], str]:
    if not NEWS_API_KEY:
        return [], "missing_api_key"

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
        raw_query = company_map.get(symbol.upper(), symbol.upper())
        query = quote_plus(raw_query)

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

        r, err = _safe_get(url)
        if not r:
            return [], err or "no_response"

        if r.status_code != 200:
            return [], f"http_{r.status_code}"

        data = r.json() or {}
        arts = data.get("articles", []) or []

        items: List[Tuple[str, str, str]] = []
        for art in arts[: max_articles * 3]:
            title = (art.get("title") or "").strip()
            link = (art.get("url") or "").strip()
            if not title or not link:
                continue
            items.append((title, link, "NewsAPI"))

        items = _dedupe_items(items)
        items = _filter_items(symbol, items, max_articles)
        return (_to_html(items) if items else []), ("ok" if items else "empty_or_filtered")

    except Exception:
        return [], "exception:NewsAPI"


# -----------------------------
# Yahoo Finance RSS
# -----------------------------
def fetch_yahoo_rss(symbol: str, max_articles: int = 3) -> Tuple[List[str], str]:
    try:
        url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={quote_plus(symbol)}&region=US&lang=en-US"
        feed = feedparser.parse(url)

        items: List[Tuple[str, str, str]] = []
        for entry in (feed.entries or [])[: max_articles * 3]:
            title = getattr(entry, "title", "") or ""
            link = getattr(entry, "link", "") or ""
            if not title or not link:
                continue
            items.append((title, link, "Yahoo RSS"))

        items = _dedupe_items(items)
        items = _filter_items(symbol, items, max_articles)
        return (_to_html(items) if items else []), ("ok" if items else "empty_or_filtered")
    except Exception:
        return [], "exception:YahooRSS"


# -----------------------------
# Finnhub.io
# -----------------------------
def fetch_finnhub(
    symbol: str,
    max_articles: int = 3,
    lookback_days: int = LOOKBACK_DAYS_DEFAULT
) -> Tuple[List[str], str]:
    if not FINNHUB_API_KEY:
        return [], "missing_api_key"

    try:
        to_date = datetime.utcnow().date()
        from_date = to_date - timedelta(days=lookback_days)

        url = (
            "https://finnhub.io/api/v1/company-news"
            f"?symbol={quote_plus(symbol.upper())}"
            f"&from={from_date.isoformat()}"
            f"&to={to_date.isoformat()}"
            f"&token={FINNHUB_API_KEY}"
        )

        r, err = _safe_get(url)
        if not r:
            return [], err or "no_response"

        if r.status_code != 200:
            return [], f"http_{r.status_code}"

        data = r.json()
        if not isinstance(data, list):
            return [], "bad_json_shape"

        items: List[Tuple[str, str, str]] = []
        for item in data[: max_articles * 5]:
            title = (item.get("headline") or "").strip()
            link = (item.get("url") or "").strip()
            if not title or not link:
                continue
            items.append((title, link, "Finnhub"))

        items = _dedupe_items(items)
        items = _filter_items(symbol, items, max_articles)
        return (_to_html(items) if items else []), ("ok" if items else "empty_or_filtered")

    except Exception:
        return [], "exception:Finnhub"


# -----------------------------
# Unified fetcher
# Priority: NewsAPI -> Finnhub -> Yahoo
# Returns list[str] HTML (backward compatible)
# -----------------------------
def fetch_news_links(
    symbol: str,
    max_articles: int = 3,
    lookback_days: int = LOOKBACK_DAYS_DEFAULT
) -> List[str]:
    key = (symbol.upper(), int(max_articles), int(lookback_days))

    if key in _CACHE:
        return _CACHE[key]

    # prevent unbounded growth
    if len(_CACHE) > _CACHE_MAX:
        _CACHE.clear()

    reasons = {}

    news, why = fetch_newsapi(symbol, max_articles=max_articles, lookback_days=lookback_days)
    reasons["NewsAPI"] = why
    if news:
        _CACHE[key] = news
        return news

    news, why = fetch_finnhub(symbol, max_articles=max_articles, lookback_days=lookback_days)
    reasons["Finnhub"] = why
    if news:
        _CACHE[key] = news
        return news

    news, why = fetch_yahoo_rss(symbol, max_articles=max_articles)
    reasons["YahooRSS"] = why
    if news:
        _CACHE[key] = news
        return news

    if DEBUG_NEWS:
        print(f"🗞️ news_fetcher: no news for {symbol} | reasons={reasons}")

    out = _no_news_html(source="; ".join([f"{k}:{v}" for k, v in reasons.items()]))
    _CACHE[key] = out
    return out