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
# utils.py

def get_price_category(price: float) -> str:
    """
    Categorize stock based on its price.
    """
    if price is None:
        return "Unknown"

    try:
        price = float(price)
    except (TypeError, ValueError):
        return "Unknown"

    if price < 20:
        return "1️⃣ Low"
    elif 20 <= price <= 100:
        return "2️⃣ Mid"
    else:
        return "3️⃣ High"


def get_price_category_description(category: str) -> str:
    """
    Human-readable explanation for price category.
    """
    descriptions = {
        "1️⃣ Low": "Low-priced stock (higher volatility & risk)",
        "2️⃣ Mid": "Mid-priced stock (balanced risk & reward)",
        "3️⃣ High": "High-priced stock (more stable, capital-heavy)",
        "Unknown": "Price data unavailable"
    }
    return descriptions.get(category, "N/A")