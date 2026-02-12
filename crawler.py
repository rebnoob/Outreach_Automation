from __future__ import annotations

import re
from typing import Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from .config import ToolkitConfig
from .database import (
    upsert_contact,
    upsert_page,
    update_company_enrichment,
    utc_now_iso,
)


EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PHONE_RE = re.compile(
    r"(?:\+1[-.\s]?)?\(?\b\d{3}\b\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"
)
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.IGNORECASE | re.DOTALL)
LINK_RE = re.compile(r"<a[^>]+href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", re.IGNORECASE | re.DOTALL)

CONTACT_LINK_HINTS = (
    "contact",
    "about",
    "team",
    "leadership",
    "company",
    "locations",
)

ROLE_EMAIL_HINTS = ("operations", "plant", "manufacturing", "engineering", "automation")


def _fetch_html(url: str, config: ToolkitConfig) -> Optional[str]:
    request = Request(url, headers={"User-Agent": config.user_agent})
    try:
        with urlopen(request, timeout=config.timeout_seconds) as response:
            content_type = response.headers.get("Content-Type", "")
            if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
                return None
            return response.read().decode("utf-8", errors="ignore")
    except Exception:
        return None


def _strip_tags(raw_html: str) -> str:
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", raw_html)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _extract_title(raw_html: str) -> str:
    match = TITLE_RE.search(raw_html)
    if match:
        return _strip_tags(match.group(1))[:240]
    h1_match = H1_RE.search(raw_html)
    if h1_match:
        return _strip_tags(h1_match.group(1))[:240]
    return ""


def _extract_links(raw_html: str, base_url: str) -> List[Tuple[str, str]]:
    links: List[Tuple[str, str]] = []
    for href, text in LINK_RE.findall(raw_html):
        href = href.strip()
        if not href or href.startswith("#"):
            continue
        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            continue
        cleaned_text = _strip_tags(text).lower()
        links.append((absolute, cleaned_text))
    return links


def _candidate_subpages(raw_html: str, base_url: str, domain: str) -> List[str]:
    candidates: List[str] = []
    for url, text in _extract_links(raw_html, base_url):
        parsed = urlparse(url)
        netloc = parsed.netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        if netloc != domain:
            continue

        path_lower = parsed.path.lower()
        if any(hint in path_lower for hint in CONTACT_LINK_HINTS) or any(
            hint in text for hint in CONTACT_LINK_HINTS
        ):
            candidates.append(url)

    unique: List[str] = []
    seen: Set[str] = set()
    for c in candidates:
        if c not in seen:
            unique.append(c)
            seen.add(c)
    return unique


def _pick_primary_email(emails: Sequence[str]) -> Optional[str]:
    if not emails:
        return None

    normalized = []
    for e in emails:
        lower = e.lower().strip(" .,;:\")\'")
        if lower.endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")):
            continue
        if "@example." in lower:
            continue
        if any(token in lower for token in ("@mysite.com", "@yourdomain.com", "no-reply@", "noreply@")):
            continue
        normalized.append(lower)

    if not normalized:
        return None

    # Role-specific mailboxes are usually better than generic inboxes.
    role_first = sorted(
        normalized,
        key=lambda e: (
            0 if any(h in e for h in ROLE_EMAIL_HINTS) else 1,
            1 if any(g in e for g in ("info@", "contact@", "sales@")) else 0,
            len(e),
        ),
    )
    return role_first[0]


def _extract_linkedin(links: Sequence[Tuple[str, str]]) -> Optional[str]:
    for url, _ in links:
        if "linkedin.com/company" in url or "linkedin.com/in/" in url:
            return url
    return None


def _find_contact_form_url(raw_html: str, page_url: str) -> Optional[str]:
    html_lower = raw_html.lower()
    if "<form" not in html_lower:
        return None
    if "contact" in html_lower or "message" in html_lower or "inquiry" in html_lower:
        return page_url
    return None


def _guess_company_name(home_title: str, domain: str) -> str:
    if home_title:
        parts = re.split(r"\||-", home_title)
        candidate = parts[0].strip()
        if 2 <= len(candidate) <= 80:
            return candidate
    label = domain.split(".")[0]
    return label.replace("-", " ").replace("_", " ").title()


def enrich_company(conn, company_row, config: ToolkitConfig, max_pages: Optional[int] = None) -> Dict[str, object]:
    company_id = int(company_row["id"])
    domain = company_row["domain"]
    base_url = company_row["url"] or f"https://{domain}"
    max_pages = max_pages or config.max_pages_per_company
    crawl_time = utc_now_iso()

    homepage = _fetch_html(base_url, config)
    if not homepage and base_url.startswith("https://"):
        fallback = "http://" + base_url[len("https://") :]
        homepage = _fetch_html(fallback, config)
        if homepage:
            base_url = fallback

    if not homepage:
        update_company_enrichment(
            conn,
            company_id,
            {
                "status": "enriched",
                "last_crawled_at": crawl_time,
                "notes": "Could not fetch site",
            },
        )
        return {"updated": False, "reason": "unreachable"}

    home_title = _extract_title(homepage)
    home_text = _strip_tags(homepage)
    upsert_page(conn, company_id, base_url, home_title, home_text)

    links = _extract_links(homepage, base_url)
    pages_to_visit = _candidate_subpages(homepage, base_url, domain)[:max_pages]

    all_emails: Set[str] = set(EMAIL_RE.findall(homepage))
    all_phones: Set[str] = {p.strip() for p in PHONE_RE.findall(homepage)}
    linkedin_url = _extract_linkedin(links)
    contact_form_url = _find_contact_form_url(homepage, base_url)

    crawled_count = 1
    for page_url in pages_to_visit:
        raw_html = _fetch_html(page_url, config)
        if not raw_html:
            continue
        crawled_count += 1
        title = _extract_title(raw_html)
        text = _strip_tags(raw_html)
        upsert_page(conn, company_id, page_url, title, text)

        all_emails.update(EMAIL_RE.findall(raw_html))
        all_phones.update(p.strip() for p in PHONE_RE.findall(raw_html))

        page_links = _extract_links(raw_html, page_url)
        if not linkedin_url:
            linkedin_url = _extract_linkedin(page_links)
        if not contact_form_url:
            contact_form_url = _find_contact_form_url(raw_html, page_url)

    primary_email = _pick_primary_email(sorted(all_emails))
    phone = sorted(all_phones)[0] if all_phones else None
    name = _guess_company_name(home_title, domain)

    notes = f"Crawled {crawled_count} page(s)"
    update_company_enrichment(
        conn,
        company_id,
        {
            "name": name,
            "status": "enriched",
            "phone": phone,
            "contact_form_url": contact_form_url,
            "linkedin_url": linkedin_url,
            "primary_email": primary_email,
            "last_crawled_at": crawl_time,
            "notes": notes,
        },
    )

    if primary_email or phone or linkedin_url:
        upsert_contact(
            conn,
            company_id=company_id,
            email=primary_email,
            phone=phone,
            linkedin_url=linkedin_url,
            source_url=base_url,
            confidence=0.9,
            name=None,
            title=None,
        )

    return {
        "updated": True,
        "company_id": company_id,
        "emails_found": len(all_emails),
        "phones_found": len(all_phones),
        "pages_crawled": crawled_count,
    }
