from __future__ import annotations

import re
from typing import List, Optional, Sequence
from urllib.parse import parse_qs, quote_plus, unquote, urlparse
from urllib.request import Request, urlopen

from .config import ToolkitConfig
from .database import upsert_company


RESULT_LINK_RE = re.compile(r'class="result__a"[^>]*href="([^"]+)"', re.IGNORECASE)

EXCLUDED_DOMAINS = {
    "duckduckgo.com",
    "google.com",
    "bing.com",
    "mfg.com",
    "thomasnet.com",
    "linkedin.com",
    "facebook.com",
    "instagram.com",
    "youtube.com",
    "x.com",
    "twitter.com",
    "wikipedia.org",
    "indeed.com",
    "ziprecruiter.com",
    "glassdoor.com",
    "mapquest.com",
    "yelp.com",
    "yellowpages.com",
    "dnb.com",
    "zoominfo.com",
}


def normalize_domain(url: str) -> Optional[str]:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return None
    netloc = parsed.netloc.lower().strip()
    if not netloc:
        return None
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return netloc


def _unwrap_duckduckgo_link(raw_href: str) -> str:
    href = unquote(raw_href).replace("&amp;", "&")
    if href.startswith("//"):
        href = "https:" + href

    if "duckduckgo.com/l/?" in href:
        parsed = urlparse(href)
        params = parse_qs(parsed.query)
        uddg = params.get("uddg", [None])[0]
        if uddg:
            return unquote(uddg)

    if "duckduckgo.com/y.js" in href:
        parsed = urlparse(href)
        params = parse_qs(parsed.query)
        ad_domain = params.get("ad_domain", [None])[0]
        if ad_domain:
            cleaned = ad_domain.strip().lower()
            if cleaned and "." in cleaned:
                return f"https://{cleaned}"

    return href


def _is_excluded(domain: str) -> bool:
    return any(domain == d or domain.endswith("." + d) for d in EXCLUDED_DOMAINS)


def search_duckduckgo(query: str, max_results: int, config: ToolkitConfig) -> List[str]:
    encoded = quote_plus(query)
    url = config.search_endpoint.format(query=encoded)
    request = Request(url, headers={"User-Agent": config.user_agent})
    with urlopen(request, timeout=config.timeout_seconds) as response:
        html = response.read().decode("utf-8", errors="ignore")

    urls: List[str] = []
    seen_domains = set()
    for href in RESULT_LINK_RE.findall(html):
        target = _unwrap_duckduckgo_link(href)
        if not target.startswith("http"):
            continue

        domain = normalize_domain(target)
        if not domain or _is_excluded(domain):
            continue

        if domain in seen_domains:
            continue
        seen_domains.add(domain)
        urls.append(target)
        if len(urls) >= max_results:
            break
    return urls


def discover_companies(
    conn,
    queries: Sequence[str],
    max_results_per_query: int,
    segment: str,
    config: ToolkitConfig,
) -> int:
    inserted = 0
    for query in queries:
        found_urls = search_duckduckgo(query, max_results_per_query, config)
        for url in found_urls:
            domain = normalize_domain(url)
            if not domain:
                continue
            if _is_excluded(domain):
                continue

            row_id_before = conn.execute(
                "SELECT id FROM companies WHERE domain = ?", (domain,)
            ).fetchone()
            upsert_company(
                conn=conn,
                domain=domain,
                url=url,
                segment=segment,
                source_query=query,
            )
            if row_id_before is None:
                inserted += 1
    return inserted


def default_manufacturing_queries(states: Optional[Sequence[str]] = None) -> List[str]:
    if not states:
        states = ["california", "texas", "illinois", "ohio", "michigan", "indiana"]

    queries: List[str] = []
    for state in states:
        queries.extend(
            [
                f"CNC machine shop {state}",
                f"high mix low volume manufacturing {state}",
                f"precision machining company {state}",
                f"contract manufacturing assembly {state}",
                f"injection molding manufacturer {state}",
            ]
        )
    return queries


def load_queries_from_file(path: str) -> List[str]:
    queries: List[str] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            cleaned = line.strip()
            if cleaned and not cleaned.startswith("#"):
                queries.append(cleaned)
    return queries
