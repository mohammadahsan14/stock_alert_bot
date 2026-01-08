import os
from flask import Flask, request, render_template_string
import pandas as pd
import requests
from top_movers import fetch_top_50_tickers, calculate_top_movers
from market_direction import get_market_direction
from yahooquery import Ticker as YQTicker
import matplotlib.pyplot as plt
import io
import base64
import openai
from dotenv import load_dotenv

# -----------------------------
# LOAD ENV VARIABLES
# -----------------------------
load_dotenv()
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY

# -----------------------------
# CONFIG
# -----------------------------
TOP_N = 10
app = Flask(__name__)

# -----------------------------
# Color mappings
# -----------------------------
RISK_COLORS = {"Low": "#d4edda", "Medium": "#fff3cd", "High": "#f8d7da"}
DECISION_COLORS = {"Buy": "#d4edda", "Sell": "#f8d7da", "Hold": "#fff3cd"}

# -----------------------------
# Helpers
# -----------------------------
def fetch_combined_news(ticker):
    news_items = []
    try:
        t = YQTicker(ticker)
        yq_news = t.news or []
        news_items += [n['title'] for n in yq_news[:5]]
    except:
        pass
    try:
        url = f"https://newsapi.org/v2/everything?q={ticker}&apiKey={NEWSAPI_KEY}&sortBy=publishedAt&pageSize=5"
        r = requests.get(url).json()
        if r.get("articles"):
            news_items += [a["title"] for a in r["articles"]]
    except:
        pass
    news_items = list(dict.fromkeys(news_items))  # Remove duplicates
    return news_items[:10] if news_items else ["No news available."]

def generate_price_chart(ticker):
    try:
        df = YQTicker(ticker).history(period="10d")
        if df.empty:
            return None
        plt.figure(figsize=(3,1.5))
        plt.plot(df['close'], marker="o")
        plt.title(f"{ticker} Price")
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format="png")
        plt.close()
        buf.seek(0)
        return base64.b64encode(buf.getvalue()).decode("utf-8")
    except:
        return None

def get_top_movers():
    tickers = fetch_top_50_tickers()
    movers = calculate_top_movers(tickers, TOP_N)
    return pd.DataFrame(movers)

def fetch_quarterly_income(ticker):
    """Return latest quarterly revenue, fallback N/A"""
    try:
        t = YQTicker(ticker)
        rev = t.financial_data[ticker].get("totalRevenue", None)
        return f"${rev:,}" if rev else "N/A"
    except:
        return "N/A"

def fallback_response(user_input):
    """Generate fallback response and render HTML table if a ticker is found"""
    user_input = user_input.lower()
    df = get_top_movers()
    market = get_market_direction()

    if "top mover" in user_input:
        top = df[['symbol','pct_change','decision']].head(5)
        lines = [f"{row['symbol']}: {row['pct_change']:.2f}% ({row['decision']})" for _,row in top.iterrows()]
        return "<pre>Top Movers:\n" + "\n".join(lines) + "</pre>"

    # Check if user asked about a specific ticker
    ticker_list = [t for t in df['symbol'] if t.lower() in user_input]
    if ticker_list:
        ticker = ticker_list[0]
        row = df[df['symbol']==ticker].iloc[0]
        chart_img = generate_price_chart(ticker)
        news = fetch_combined_news(ticker)
        quarterly_income = fetch_quarterly_income(ticker)

        # Static suggestions; can later integrate GPT dynamic content
        why_buy = f"Consider buying {ticker} if growth trends continue and market conditions are favorable."
        why_not_buy = f"Avoid {ticker} if volatility is high or risk tolerance is low."

        # Build HTML table
        table_html = f"""
        <table border="1" cellpadding="5" style="border-collapse:collapse; margin-top:10px;">
            <tr>
                <th>Symbol</th><th>Price</th><th>Decision</th><th>Risk</th><th>Quarterly Income</th>
                <th>News</th><th>Why Buy</th><th>Why Not Buy</th><th>Chart</th>
            </tr>
            <tr>
                <td>{ticker}</td>
                <td>${row['current']:.2f}</td>
                <td style="background-color:{DECISION_COLORS.get(row['decision'],'#fff')};">{row['decision']}</td>
                <td style="background-color:{RISK_COLORS.get(row['risk'],'#fff')};">{row['risk']}</td>
                <td>{quarterly_income}</td>
                <td>{"<br>".join(news)}</td>
                <td>{why_buy}</td>
                <td>{why_not_buy}</td>
                <td>{f'<img src="data:image/png;base64,{chart_img}">' if chart_img else 'N/A'}</td>
            </tr>
        </table>
        """
        return table_html

    return ("Sorry, I didn't understand. You can ask about 'top movers', 'market direction', "
            "buy/sell stocks, sector stocks, or a specific ticker like 'AAPL', 'MSFT'.")

def chatbot_response(user_input):
    if not OPENAI_API_KEY:
        return fallback_response(user_input)
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role":"user","content":user_input}]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"⚠️ OpenAI unavailable: {str(e)}<br>" + fallback_response(user_input)

# -----------------------------
# HTML template
# -----------------------------
HTML_TEMPLATE = """
<!doctype html>
<html>
<head>
<title>Stock ChatBot</title>
<style>
body { font-family: Arial; max-width: 1000px; margin: auto; }
.chat { border:1px solid #ccc; padding:10px; height:600px; overflow-y:auto; }
input[type=text] { width:80%; padding:5px; }
input[type=submit] { padding:5px; }
.message.user { color: blue; }
.message.bot { color: green; }
table { width:100%; border-collapse:collapse; margin-top:10px; }
th, td { border:1px solid #ccc; padding:5px; text-align:left; vertical-align:top; }
img { max-width:150px; }
</style>
</head>
<body>
<h2>Stock ChatBot</h2>
<div class="chat">
{% for m in messages %}
<div class="message {{ m.role }}">
    <strong>{{ m.role|capitalize }}:</strong> {{ m.text|safe }}
</div>
{% endfor %}
</div>
<form method="post">
<input type="text" name="user_input" placeholder="Ask about stocks..." required>
<input type="submit" value="Send">
</form>
</body>
</html>
"""

# -----------------------------
# Flask routes
# -----------------------------
messages = []

@app.route("/", methods=["GET","POST"])
def dashboard():
    global messages
    if request.method=="POST":
        user_input = request.form["user_input"]
        messages.append({"role":"user","text":user_input})
        bot_text = chatbot_response(user_input)
        messages.append({"role":"bot","text":bot_text})
    return render_template_string(HTML_TEMPLATE, messages=messages)

# -----------------------------
# Open browser automatically
# -----------------------------
def open_browser():
    import webbrowser
    webbrowser.open("http://127.0.0.1:5000")

# -----------------------------
# Run Flask
# -----------------------------
if __name__=="__main__":
    import threading
    threading.Timer(1.5, open_browser).start()
    app.run(debug=True)