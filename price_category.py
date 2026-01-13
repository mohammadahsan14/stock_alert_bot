# price_category.py

def get_price_category(price):
    """
    Categorize stock based on price:

    - Ultra Penny ($): < $10
    - Penny ($): $10 – $50
    - Mid ($$): $50 – $150
    - Mid-High ($$$): $150 – $300
    - High ($$$$): >= $300

    Returns:
        str: price category label used across email + Excel grouping
    """
    try:
        p = float(price)
    except (TypeError, ValueError):
        return "Unknown"

    # Defensive guard (bad API data / halted tickers)
    if p <= 0:
        return "Unknown"

    if p < 10:
        return "Ultra Penny ($)"
    elif p < 50:
        return "Penny ($)"
    elif p < 150:
        return "Mid ($$)"
    elif p < 300:
        return "Mid-High ($$$)"
    else:
        return "High ($$$$)"