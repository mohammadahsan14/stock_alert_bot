# news_fetcher.py

import requests
import feedparser

# Example API Key, replace with your own in config.py if needed
NEWS_API_KEY = "d49d081e83844d1388a52bff554f6a19"

def fetch_newsapi(symbol, max_articles=3):
    """Fetch news from NewsAPI.org"""
    try:
        company_name_map = {
            "AAPL": "Apple",
            "MSFT": "Microsoft",
            "GOOGL": "Alphabet",
            "AMZN": "Amazon",
            "TSLA": "Tesla",
        }
        query = company_name_map.get(symbol, symbol)
        url = (f"https://newsapi.org/v2/everything?q={query}"
               f"&sortBy=publishedAt&apiKey={NEWS_API_KEY}&pageSize={max_articles}")
        r = requests.get(url, timeout=5).json()
        news_items = []
        for art in r.get("articles", [])[:max_articles]:
            title = art.get("title", "No title")
            link = art.get("url", "#")
            news_items.append(f'✅ <a href="{link}" target="_blank">{title}</a>')
        return news_items
    except Exception:
        return []

def fetch_yahoo_rss(symbol, max_articles=3):
    """Fetch news from Yahoo Finance RSS feed"""
    try:
        url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
        feed = feedparser.parse(url)
        news_items = []
        for entry in feed.entries[:max_articles]:
            news_items.append(f'✅ <a href="{entry.link}" target="_blank">{entry.title}</a>')
        return news_items
    except Exception:
        return []

def fetch_news_links(symbol, max_articles=3):
    """Combine multiple sources with fallback"""
    news = fetch_newsapi(symbol, max_articles)
    if not news:
        news = fetch_yahoo_rss(symbol, max_articles)
    if not news:
        news = ["No news available"]
    return news