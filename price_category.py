# price_category.py

def get_price_category(price):
    """
    Categorize stock based on price:

    - Ultra Penny ($): < $5
    - Low ($): $5 – $20
    - Mid ($$): $20 – $100
    - Mid-High ($$$): $100 – $300
    - High ($$$$): >= $300
    """

    try:
        p = float(price)
    except (TypeError, ValueError):
        return "Unknown"

    if p <= 0:
        return "Unknown"

    if p < 5:
        return "Ultra Penny ($)"
    elif p < 20:
        return "Low ($)"
    elif p < 100:
        return "Mid ($$)"
    elif p < 300:
        return "Mid-High ($$$)"
    else:
        return "High ($$$$)"