# test_email.py
from config import SENDER_EMAIL, APP_PASSWORD, RECEIVER_EMAIL
import smtplib
from email.message import EmailMessage

def send_test_email():
    msg = EmailMessage()
    msg["Subject"] = "✅ Test Email from Stock Alert Bot"
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECEIVER_EMAIL
    msg.set_content("This is a test email to confirm your Railway deployment can send emails.")

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(SENDER_EMAIL, APP_PASSWORD)
            smtp.send_message(msg)
            print("✅ Test email sent successfully")
    except Exception as e:
        print("❌ Test email failed:", e)

if __name__ == "__main__":
    send_test_email()