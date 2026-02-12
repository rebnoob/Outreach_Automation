from __future__ import annotations

import os
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Dict, List

from .database import get_due_email_actions, mark_action_status


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _smtp_settings() -> Dict[str, str]:
    return {
        "host": os.getenv("SMTP_HOST", ""),
        "port": os.getenv("SMTP_PORT", "587"),
        "username": os.getenv("SMTP_USER", ""),
        "password": os.getenv("SMTP_PASS", ""),
        "from_email": os.getenv("SMTP_FROM", ""),
        "use_tls": os.getenv("SMTP_TLS", "true").lower() != "false",
    }


def _send_email_live(to_email: str, subject: str, body: str) -> None:
    settings = _smtp_settings()
    missing = [k for k in ("host", "username", "password", "from_email") if not settings[k]]
    if missing:
        raise RuntimeError(f"Missing SMTP settings: {', '.join(missing)}")

    msg = EmailMessage()
    msg["From"] = settings["from_email"]
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(settings["host"], int(settings["port"])) as server:
        if settings["use_tls"]:
            server.starttls()
        server.login(settings["username"], settings["password"])
        server.send_message(msg)


def send_due_emails(conn, action_date: str, limit: int, live: bool) -> Dict[str, int]:
    rows = get_due_email_actions(conn, action_date, limit)
    sent = 0
    failed = 0
    skipped = 0

    for row in rows:
        action_id = int(row["id"])
        to_email = (row["primary_email"] or "").strip()
        if not to_email:
            mark_action_status(conn, action_id, status="skipped", error="Missing destination email")
            skipped += 1
            continue

        try:
            if live:
                _send_email_live(to_email, row["subject"], row["body"])
                mark_action_status(conn, action_id, status="sent", sent_at=_utc_now())
            else:
                mark_action_status(conn, action_id, status="simulated", sent_at=_utc_now())
            sent += 1
        except Exception as exc:
            mark_action_status(conn, action_id, status="failed", error=str(exc))
            failed += 1

    return {
        "processed": len(rows),
        "sent_or_simulated": sent,
        "failed": failed,
        "skipped": skipped,
    }
