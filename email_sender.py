# email_sender.py
from __future__ import annotations

import os
import time
import base64
import mimetypes
import smtplib
from email.message import EmailMessage
from typing import Any, Optional, cast, List, Tuple

import resend

# -----------------------------
# Tunables
# -----------------------------
SMTP_TIMEOUT_SECONDS = int((os.getenv("SMTP_TIMEOUT_SECONDS") or "25").strip())
RESEND_MIN_SECONDS_BETWEEN_CALLS = float((os.getenv("RESEND_MIN_SECONDS_BETWEEN_CALLS") or "0.15").strip())
DEBUG_EMAIL = (os.getenv("DEBUG_EMAIL") or "").strip() == "1"

_LAST_RESEND_CALL_TS = 0.0


def _throttle_resend() -> None:
    global _LAST_RESEND_CALL_TS
    now = time.time()
    delta = now - _LAST_RESEND_CALL_TS
    if delta < RESEND_MIN_SECONDS_BETWEEN_CALLS:
        time.sleep(RESEND_MIN_SECONDS_BETWEEN_CALLS - delta)
    _LAST_RESEND_CALL_TS = time.time()


def _normalize_recipients(to_email: str) -> List[str]:
    """
    Accepts:
      - single email: "a@b.com"
      - comma-separated: "a@b.com, c@d.com"
    Returns a clean list.
    """
    if not to_email:
        return []
    parts = [p.strip() for p in str(to_email).split(",")]
    return [p for p in parts if p]


def _format_from(from_email: str) -> str:
    """
    Resend often expects: "Name <email@domain.com>".
    If FROM_NAME is set, we format it that way.
    """
    name = (os.getenv("FROM_NAME") or "").strip()
    if name and from_email and "<" not in from_email:
        return f"{name} <{from_email}>"
    return from_email


def _safe_attachment_payload(attachment_path: str) -> Optional[dict[str, Any]]:
    """
    Returns Resend attachment dict if attachment exists, else None.
    """
    try:
        if not attachment_path:
            return None

        if (not os.path.exists(attachment_path)) or (not os.path.isfile(attachment_path)):
            print(f"⚠️ Attachment not found or not a file: {attachment_path}")
            return None

        mime_type, _ = mimetypes.guess_type(attachment_path)
        if not mime_type:
            mime_type = "application/octet-stream"

        with open(attachment_path, "rb") as f:
            content_b64 = base64.b64encode(f.read()).decode("utf-8")

        return {
            "filename": os.path.basename(attachment_path),
            "content": content_b64,
            "type": mime_type,
        }
    except Exception as e:
        print("⚠️ Failed to read attachment:", e)
        return None


def _guess_maintype_subtype(path: str) -> Tuple[str, str]:
    mime_type, _ = mimetypes.guess_type(path)
    if not mime_type:
        return ("application", "octet-stream")

    if "/" not in mime_type:
        return ("application", "octet-stream")

    maintype, subtype = mime_type.split("/", 1)
    maintype = maintype or "application"
    subtype = subtype or "octet-stream"
    return (maintype, subtype)


def _send_resend(
    subject: str,
    html_body: str,
    to_email: str,
    from_email: str,
    attachment_path: Optional[str] = None,
    reply_to: Optional[str] = None,
) -> bool:
    api_key = (os.getenv("RESEND_API_KEY") or "").strip()
    if not api_key:
        print("❌ Resend Email failed: Missing RESEND_API_KEY")
        return False

    resend.api_key = api_key

    recipients = _normalize_recipients(to_email)
    if not recipients:
        print("❌ Resend Email failed: Missing to_email recipient(s)")
        return False
    if not from_email:
        print("❌ Resend Email failed: Missing from_email")
        return False

    params = cast(dict[str, Any], {
        "from": _format_from(from_email),
        "to": recipients,
        "subject": subject or "(no subject)",
        "html": html_body or "",
    })

    params["reply_to"] = (reply_to or from_email)

    if attachment_path:
        att = _safe_attachment_payload(attachment_path)
        if att:
            params["attachments"] = [att]

    try:
        _throttle_resend()
        r = resend.Emails.send(params)

        if DEBUG_EMAIL:
            print("✅ Resend Email sent (debug):", r)
        else:
            rid = None
            try:
                rid = r.get("id") if isinstance(r, dict) else None
            except Exception:
                rid = None
            print("✅ Resend Email sent" + (f" id={rid}" if rid else ""))

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
    reply_to: Optional[str] = None,
) -> bool:
    """
    Gmail SMTP sender (TLS / port 587).
    Requires:
      - GMAIL_USER (usually same as from_email)
      - GMAIL_APP_PASSWORD (Google App Password) OR APP_PASSWORD
    """
    recipients = _normalize_recipients(to_email)
    if not recipients:
        print("❌ Gmail SMTP Email failed: Missing to_email recipient(s)")
        return False
    if not from_email:
        print("❌ Gmail SMTP Email failed: Missing from_email")
        return False

    gmail_user = (os.getenv("GMAIL_USER") or from_email).strip()
    gmail_app_password = (os.getenv("GMAIL_APP_PASSWORD") or os.getenv("APP_PASSWORD") or "").strip()

    if not gmail_user:
        print("❌ Gmail SMTP Email failed: Missing GMAIL_USER (or from_email)")
        return False
    if not gmail_app_password:
        print("❌ Gmail SMTP Email failed: Missing GMAIL_APP_PASSWORD (or APP_PASSWORD)")
        return False

    smtp_host = (os.getenv("SMTP_HOST") or "smtp.gmail.com").strip()
    smtp_port = int((os.getenv("SMTP_PORT") or "587").strip())

    msg = EmailMessage()
    msg["Subject"] = subject or "(no subject)"
    msg["From"] = _format_from(from_email)
    msg["Reply-To"] = (reply_to or from_email)
    msg["To"] = ", ".join(recipients)

    msg.set_content("This email contains HTML content. Please view it in an HTML-capable email client.")
    msg.add_alternative(html_body or "", subtype="html")

    if attachment_path:
        if os.path.exists(attachment_path) and os.path.isfile(attachment_path):
            maintype, subtype = _guess_maintype_subtype(attachment_path)
            try:
                with open(attachment_path, "rb") as f:
                    data = f.read()

                msg.add_attachment(
                    data,
                    maintype=maintype,
                    subtype=subtype,
                    filename=os.path.basename(attachment_path),
                )
            except Exception as e:
                print("⚠️ Gmail SMTP: failed to attach file:", e)
        else:
            print(f"⚠️ Gmail SMTP: attachment not found: {attachment_path}")

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=SMTP_TIMEOUT_SECONDS) as server:
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
    reply_to: Optional[str] = None,
) -> bool:
    """
    Unified sender.
    Controlled by:
      EMAIL_PROVIDER = "resend" (default) or "gmail"
      EMAIL_FALLBACK_TO_GMAIL = "1" to fallback if Resend fails
      EMAIL_DRY_RUN = "1" to simulate send (local testing)
      FROM_NAME = "Your Bot Name" (optional) -> formats "Name <email>"
      DEBUG_EMAIL = "1" -> prints more response detail
    """
    provider = (os.getenv("EMAIL_PROVIDER") or "resend").strip().lower()
    fallback = (os.getenv("EMAIL_FALLBACK_TO_GMAIL") == "1")

    if (os.getenv("EMAIL_DRY_RUN") or "").strip() == "1":
        print("🧪 EMAIL_DRY_RUN=1 (not sending)")
        print("provider:", provider)
        print("to:", _normalize_recipients(to_email))
        print("from:", _format_from(from_email))
        print("reply_to:", reply_to or from_email)
        print("subject:", subject or "(no subject)")
        print("attachment:", attachment_path or "None")
        return True

    if provider == "gmail":
        return _send_gmail_smtp(subject, html_body, to_email, from_email, attachment_path, reply_to=reply_to)

    ok = _send_resend(subject, html_body, to_email, from_email, attachment_path, reply_to=reply_to)
    if ok:
        return True

    if fallback:
        print("↩️ Falling back to Gmail SMTP...")
        return _send_gmail_smtp(subject, html_body, to_email, from_email, attachment_path, reply_to=reply_to)

    return False