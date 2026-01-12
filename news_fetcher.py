# news_fetcher.py
import requests
import feedparser
from datetime import datetime, timedelta
from config import NEWS_API_KEY, FINNHUB_API_KEY

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Safari/537.36"
)

# -----------------------------
# NewsAPI.org
# -----------------------------
def fetch_newsapi(symbol, max_articles=3):
    if not NEWS_API_KEY:
        return []
    try:
        company_map = {
            "AAPL": "Apple",
            "MSFT": "Microsoft",
            "GOOGL": "Alphabet",
            "AMZN": "Amazon",
            "TSLA": "Tesla",
        }
        query = company_map.get(symbol, symbol)
        url = (
            "https://newsapi.org/v2/everything"
            f"?q={query}"
            "&sortBy=publishedAt"
            f"&apiKey={NEWS_API_KEY}"
            f"&pageSize={max_articles}"
            "&language=en"
        )
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=8)
        data = r.json()

        items = []
        for art in data.get("articles", [])[:max_articles]:
            title = art.get("title") or "No title"
            link = art.get("url") or "#"
            items.append(f'✅ <a href="{link}" target="_blank">{title}</a>')
        return items
    except Exception:
        return []

# -----------------------------
# Yahoo Finance RSS
# -----------------------------
def fetch_yahoo_rss(symbol, max_articles=3):
    try:
        url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
        feed = feedparser.parse(url)
        items = []
        for entry in feed.entries[:max_articles]:
            title = getattr(entry, "title", "No title")
            link = getattr(entry, "link", "#")
            items.append(f'✅ <a href="{link}" target="_blank">{title}</a>')
        return items
    except Exception:
        return []

# -----------------------------
# Finnhub.io (dynamic date range)
# -----------------------------
def fetch_finnhub(symbol, max_articles=3, lookback_days=7):
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
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=8)
        data = r.json() if r.status_code == 200 else []

        items = []
        for item in data[:max_articles]:
            title = item.get("headline") or "No title"
            link = item.get("url") or "#"
            items.append(f'✅ <a href="{link}" target="_blank">{title}</a>')
        return items
    except Exception:
        return []

# -----------------------------
# Unified news fetcher
# Priority: NewsAPI -> Finnhub -> Yahoo
# -----------------------------
def fetch_news_links(symbol, max_articles=3):
    news = fetch_newsapi(symbol, max_articles=max_articles)
    if not news:
        news = fetch_finnhub(symbol, max_articles=max_articles, lookback_days=7)
    if not news:
        news = fetch_yahoo_rss(symbol, max_articles=max_articles)

    if not news:
        return ["No news available"]
    return news