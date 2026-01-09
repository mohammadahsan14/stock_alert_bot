import os
import smtplib
from email.message import EmailMessage
import logging

logging.basicConfig(level=logging.INFO)

SENDER_EMAIL = os.getenv("SENDER_EMAIL")
APP_PASSWORD = os.getenv("APP_PASSWORD")
RECEIVER_EMAIL = os.getenv("RECEIVER_EMAIL")

msg = EmailMessage()
msg["Subject"] = "🚀 Test Email from Railway"
msg["From"] = SENDER_EMAIL
msg["To"] = RECEIVER_EMAIL
msg.set_content("This is a test email from your Railway deployment.")

try:
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(SENDER_EMAIL, APP_PASSWORD)
        smtp.send_message(msg)
        logging.info("✅ Test email sent successfully!")
except Exception as e:
    logging.error(f"❌ Test email failed: {e}")