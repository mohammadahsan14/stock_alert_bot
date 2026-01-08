# utils.py

def generate_reason(score, symbol, news_links):
    """Generate textual reason for a stock based on score and news"""
    if score >= 50:
        reason = f"Strong predictive score for {symbol}, potential upside. {', '.join(news_links)}"
    elif score >= 25:
        reason = f"Moderate predictive score for {symbol}, watch closely. {', '.join(news_links)}"
    else:
        reason = f"Low predictive score for {symbol}, caution advised. {', '.join(news_links)}"
    return reason