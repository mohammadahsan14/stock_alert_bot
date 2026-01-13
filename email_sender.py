# email_sender.py
import os
import base64
import mimetypes
import smtplib
from email.message import EmailMessage
from typing import Any, Optional, cast

import resend


def _send_resend(
    subject: str,
    html_body: str,
    to_email: str,
    from_email: str,
    attachment_path: Optional[str] = None,
) -> bool:
    api_key = os.getenv("RESEND_API_KEY")
    if not api_key:
        raise RuntimeError("Missing RESEND_API_KEY in environment variables.")

    resend.api_key = api_key

    params = cast(dict[str, Any], {
        "from": from_email,
        "to": [to_email],
        "subject": subject,
        "html": html_body,
    })

    if attachment_path:
        mime_type, _ = mimetypes.guess_type(attachment_path)
        if not mime_type:
            mime_type = "application/octet-stream"

        with open(attachment_path, "rb") as f:
            content_b64 = base64.b64encode(f.read()).decode("utf-8")

        params["attachments"] = [{
            "filename": os.path.basename(attachment_path),
            "content": content_b64,
            "type": mime_type,  # helps some clients
        }]

    try:
        r = resend.Emails.send(params)
        print("✅ Resend Email sent:", r)
        return True
    except Exception as e:
        print("❌ Resend Email failed:", e)
        return False


def _send_gmail_smtp(
    subject: str,
    html_body: str,
    to_email: str,
    from_email: str,
    attachment_path: Optional[str] = None,
) -> bool:
    """
    Gmail SMTP sender (TLS / port 587).
    Requires:
      - GMAIL_USER (usually same as from_email)
      - GMAIL_APP_PASSWORD (Google App Password)
    """
    gmail_user = os.getenv("GMAIL_USER") or from_email
    gmail_app_password = os.getenv("GMAIL_APP_PASSWORD") or os.getenv("APP_PASSWORD")

    if not gmail_user:
        raise RuntimeError("Missing GMAIL_USER (or from_email) for Gmail SMTP.")
    if not gmail_app_password:
        raise RuntimeError("Missing GMAIL_APP_PASSWORD (or APP_PASSWORD) for Gmail SMTP.")

    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email

    # Plaintext fallback + HTML
    msg.set_content("This email contains HTML content. Please view it in an HTML-capable email client.")
    msg.add_alternative(html_body, subtype="html")

    if attachment_path:
        mime_type, _ = mimetypes.guess_type(attachment_path)
        if not mime_type:
            mime_type = "application/octet-stream"
        maintype, subtype = mime_type.split("/", 1)

        with open(attachment_path, "rb") as f:
            data = f.read()

        msg.add_attachment(
            data,
            maintype=maintype,
            subtype=subtype,
            filename=os.path.basename(attachment_path),
        )

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(gmail_user, gmail_app_password)
            server.send_message(msg)

        print("✅ Gmail SMTP Email sent")
        return True
    except Exception as e:
        print("❌ Gmail SMTP Email failed:", e)
        return False


def send_email(
    subject: str,
    html_body: str,
    to_email: str,
    from_email: str,
    attachment_path: Optional[str] = None,
) -> bool:
    """
    Unified sender.
    Controlled by:
      EMAIL_PROVIDER = "resend" (default) or "gmail"
      EMAIL_FALLBACK_TO_GMAIL = "1" to fallback if Resend fails
    """
    provider = (os.getenv("EMAIL_PROVIDER") or "resend").strip().lower()
    fallback = (os.getenv("EMAIL_FALLBACK_TO_GMAIL") == "1")

    if provider == "gmail":
        return _send_gmail_smtp(subject, html_body, to_email, from_email, attachment_path)

    # default: resend
    ok = _send_resend(subject, html_body, to_email, from_email, attachment_path)
    if ok:
        return True

    if fallback:
        print("↩️ Falling back to Gmail SMTP...")
        return _send_gmail_smtp(subject, html_body, to_email, from_email, attachment_path)

    return False