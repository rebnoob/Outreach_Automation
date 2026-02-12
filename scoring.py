from __future__ import annotations

from typing import Dict, List, Tuple

from .database import get_company_text, update_company_scoring


FIT_KEYWORDS: Dict[str, int] = {
    "cnc": 6,
    "machining": 6,
    "machine shop": 7,
    "precision": 3,
    "fabrication": 5,
    "metal": 3,
    "tooling": 3,
    "assembly": 4,
    "contract manufacturing": 6,
    "injection molding": 6,
    "job shop": 6,
    "high mix": 7,
    "low volume": 5,
    "prototype": 4,
    "production": 3,
    "automation": 2,
}

NEGATIVE_KEYWORDS: Dict[str, int] = {
    "digital marketing": -8,
    "seo agency": -8,
    "web design": -7,
    "law firm": -8,
    "real estate": -7,
    "staffing agency": -7,
}


def _calculate_fit_score(text: str) -> Tuple[float, List[str]]:
    lower = text.lower()
    raw_score = 0
    hits: List[str] = []

    for keyword, weight in FIT_KEYWORDS.items():
        if keyword in lower:
            raw_score += weight
            hits.append(keyword)

    for keyword, weight in NEGATIVE_KEYWORDS.items():
        if keyword in lower:
            raw_score += weight

    normalized = max(0.0, min(100.0, raw_score * 2.2))
    return normalized, hits


def _calculate_contact_score(row) -> Tuple[float, str]:
    score = 0.0
    reasons: List[str] = []

    primary_email = (row["primary_email"] or "").lower()
    phone = row["phone"]
    contact_form = row["contact_form_url"]
    linkedin = row["linkedin_url"]

    if primary_email:
        score += 45
        reasons.append("email")
        if any(hint in primary_email for hint in ("operations", "plant", "engineering", "automation")):
            score += 15
            reasons.append("role_email")
        if any(primary_email.startswith(prefix) for prefix in ("info@", "contact@", "sales@")):
            score -= 5

    if phone:
        score += 20
        reasons.append("phone")

    if contact_form:
        score += 15
        reasons.append("contact_form")

    if linkedin:
        score += 10
        reasons.append("linkedin")

    return max(0.0, min(100.0, score)), ", ".join(reasons)


def _best_channel(row) -> Tuple[str, str]:
    email = row["primary_email"]
    phone = row["phone"]
    contact_form = row["contact_form_url"]
    linkedin = row["linkedin_url"]

    if email:
        if any(h in email.lower() for h in ("operations", "plant", "engineering", "automation")):
            return "email", "Role-specific email found"
        return "email", "Email inbox found and fastest to test response"
    if phone and contact_form:
        return "phone", "Phone plus form provides fast qualification"
    if phone:
        return "phone", "Phone available, use short qualification call"
    if contact_form:
        return "contact_form", "Only website form available"
    if linkedin:
        return "linkedin", "LinkedIn is the only discovered channel"
    return "research", "No reliable outreach channel discovered"


def score_companies(conn, companies) -> int:
    scored = 0
    for row in companies:
        text = get_company_text(conn, int(row["id"]))
        fit_score, hits = _calculate_fit_score(text)
        contact_score, contact_reason = _calculate_contact_score(row)
        outreach_score = max(0.0, min(100.0, fit_score * 0.7 + contact_score * 0.3))
        best_channel, channel_reason = _best_channel(row)

        hit_summary = ", ".join(hits[:8]) if hits else "No clear manufacturing keyword hits"
        if contact_reason:
            channel_reason = f"{channel_reason}. Signals: {contact_reason}. Keywords: {hit_summary}"
        else:
            channel_reason = f"{channel_reason}. Keywords: {hit_summary}"

        update_company_scoring(
            conn,
            int(row["id"]),
            fit_score=fit_score,
            contact_score=contact_score,
            outreach_score=outreach_score,
            best_channel=best_channel,
            channel_reason=channel_reason,
        )
        scored += 1
    return scored
