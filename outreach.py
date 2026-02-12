from __future__ import annotations

from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

from .database import get_primary_contact, insert_outreach_action


DEFAULT_VALUE_PROP = (
    "We reduce manual interventions in high-mix machine tending by combining "
    "deterministic control with VLA-based exception recovery."
)


def _sequence_for_channel(channel: str) -> List[Tuple[int, str, str]]:
    # day_offset, step_name, channel
    if channel == "email":
        return [
            (0, "intro_email", "email"),
            (3, "followup_email_1", "email"),
            (7, "call_followup", "phone"),
            (12, "followup_email_2", "email"),
        ]
    if channel == "phone":
        return [
            (0, "intro_call", "phone"),
            (2, "email_after_call", "email"),
            (6, "call_followup", "phone"),
        ]
    if channel == "contact_form":
        return [
            (0, "contact_form_intro", "contact_form"),
            (4, "phone_followup", "phone"),
            (8, "email_followup", "email"),
        ]
    if channel == "linkedin":
        return [
            (0, "linkedin_connect", "linkedin"),
            (5, "linkedin_followup", "linkedin"),
        ]
    return [(0, "manual_research", "research")]


def _email_subject(step_name: str, company_name: str) -> str:
    if step_name == "intro_email":
        return f"Idea to reduce machine-tending interventions at {company_name}"
    if step_name == "followup_email_1":
        return f"Quick follow-up on machine-tending exceptions at {company_name}"
    if step_name == "followup_email_2":
        return f"Should I close this out for {company_name}?"
    if step_name == "email_after_call":
        return f"Recap and pilot outline for {company_name}"
    return f"Automation opportunity for {company_name}"


def _email_body(
    step_name: str,
    company_name: str,
    domain: str,
    value_prop: str,
) -> str:
    if step_name == "intro_email":
        return (
            f"Hi {company_name} team,\n\n"
            f"I work on robotic manipulation for high-mix manufacturing cells. {value_prop}\n\n"
            "If useful, I can run a scoped pilot focused on one cell with clear KPIs: "
            "interventions/shift, changeover time, and throughput.\n\n"
            "Would you be open to a 20-minute call next week?\n\n"
            "Best,\n"
            "<Your Name>"
        )

    if step_name == "followup_email_1":
        return (
            f"Hi {company_name} team,\n\n"
            "Following up in case this is relevant for your CNC/assembly operations. "
            "We usually start with one process where hard-coded automation struggles on "
            "exceptions, then compare against baseline performance.\n\n"
            "If you share one bottleneck process, I can send a one-page pilot plan.\n\n"
            "Best,\n"
            "<Your Name>"
        )

    if step_name == "followup_email_2":
        return (
            f"Hi {company_name} team,\n\n"
            "I have not heard back, so I will close this out for now. If reducing "
            "manual interventions or changeover engineering time is still a priority, "
            "reply with the right contact and I will send a concise pilot proposal.\n\n"
            "Best,\n"
            "<Your Name>"
        )

    if step_name == "email_after_call":
        return (
            f"Hi {company_name} team,\n\n"
            "Thanks for the call. As discussed, I can scope a 4-6 week pilot with "
            "baseline vs hybrid-VLA comparison and clear go/no-go thresholds.\n\n"
            "If you share a part family + current cycle baseline, I will send draft SOW.\n\n"
            "Best,\n"
            "<Your Name>"
        )

    return (
        f"Hi {company_name} team,\n\n"
        f"I am reaching out because {domain} looks like a strong fit for machine-tending automation.\n\n"
        "Best,\n"
        "<Your Name>"
    )


def _non_email_message(step_name: str, company_name: str, value_prop: str) -> str:
    if step_name.startswith("intro_call"):
        return (
            f"Call script: 20-second intro + ask if they are open to a pilot for one high-mix "
            f"cell. Message: {value_prop}"
        )
    if "contact_form" in step_name:
        return (
            f"Contact form message: We help {company_name} reduce machine-tending exceptions "
            "in high-mix cells. Open to a 20-minute pilot scoping call?"
        )
    if "linkedin" in step_name:
        return (
            "LinkedIn note: Working on high-mix robotic manipulation that cuts intervention "
            "load in machine tending. Open to a short conversation?"
        )
    return f"Manual follow-up for {company_name}."


def plan_outreach(
    conn,
    companies,
    start_date: date,
    value_prop: str = DEFAULT_VALUE_PROP,
) -> int:
    planned = 0

    for row in companies:
        company_id = int(row["id"])
        company_name = row["name"] or row["domain"].split(".")[0].title()
        domain = row["domain"]
        best_channel = row["best_channel"] or "research"

        contact = get_primary_contact(conn, company_id)
        contact_id = int(contact["id"]) if contact else None

        for day_offset, step_name, channel in _sequence_for_channel(best_channel):
            scheduled_for = (start_date + timedelta(days=day_offset)).isoformat()

            if channel == "email":
                subject = _email_subject(step_name, company_name)
                body = _email_body(step_name, company_name, domain, value_prop)
            else:
                subject = f"{step_name} for {company_name}"
                body = _non_email_message(step_name, company_name, value_prop)

            insert_outreach_action(
                conn,
                company_id=company_id,
                contact_id=contact_id,
                step_name=step_name,
                channel=channel,
                subject=subject,
                body=body,
                scheduled_for=scheduled_for,
            )
            planned += 1

    return planned
