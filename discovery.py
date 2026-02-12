from __future__ import annotations

import re
from typing import Dict, List, Optional, Sequence
from urllib.parse import parse_qs, quote_plus, unquote, urlparse
from urllib.request import Request, urlopen

from .config import ToolkitConfig
from .database import upsert_company


RESULT_LINK_RE = re.compile(r'<a[^>]+href="([^"]+)"', re.IGNORECASE)
MARKDOWN_LINK_RE = re.compile(r"\[[^\]]+\]\((https?://[^)\s]+)\)")
DEFAULT_SEARCH_ENDPOINTS = (
    "https://lite.duckduckgo.com/lite/?q={query}",
    "https://html.duckduckgo.com/html/?q={query}",
    "https://duckduckgo.com/html/?q={query}",
    "https://r.jina.ai/http://duckduckgo.com/html/?q={query}",
)

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


def _search_endpoints(config: ToolkitConfig) -> List[str]:
    endpoints: List[str] = []
    configured = (config.search_endpoint or "").strip()
    if configured:
        endpoints.append(configured)
    for endpoint in DEFAULT_SEARCH_ENDPOINTS:
        if endpoint not in endpoints:
            endpoints.append(endpoint)
    return endpoints


def search_duckduckgo(query: str, max_results: int, config: ToolkitConfig) -> List[str]:
    encoded = quote_plus(query)
    for endpoint in _search_endpoints(config):
        try:
            url = endpoint.format(query=encoded)
            request = Request(url, headers={"User-Agent": config.user_agent})
            with urlopen(request, timeout=config.timeout_seconds) as response:
                html = response.read().decode("utf-8", errors="ignore")
        except Exception:
            continue

        urls: List[str] = []
        seen_domains = set()
        candidate_links = RESULT_LINK_RE.findall(html) + MARKDOWN_LINK_RE.findall(html)
        for href in candidate_links:
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

        if urls:
            return urls

    return []


def discover_companies_with_stats(
    conn,
    queries: Sequence[str],
    max_results_per_query: int,
    segment: str,
    config: ToolkitConfig,
) -> Dict[str, int]:
    stats = {
        "queries": len(queries),
        "queries_with_results": 0,
        "results_found": 0,
        "unique_domains_found": 0,
        "inserted": 0,
        "existing": 0,
        "skipped_invalid": 0,
        "skipped_excluded": 0,
    }
    unique_domains = set()

    for query in queries:
        found_urls = search_duckduckgo(query, max_results_per_query, config)
        if found_urls:
            stats["queries_with_results"] += 1
        stats["results_found"] += len(found_urls)

        for url in found_urls:
            domain = normalize_domain(url)
            if not domain:
                stats["skipped_invalid"] += 1
                continue
            if _is_excluded(domain):
                stats["skipped_excluded"] += 1
                continue

            unique_domains.add(domain)
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
                stats["inserted"] += 1
            else:
                stats["existing"] += 1

    stats["unique_domains_found"] = len(unique_domains)
    return stats


def discover_companies(
    conn,
    queries: Sequence[str],
    max_results_per_query: int,
    segment: str,
    config: ToolkitConfig,
) -> int:
    stats = discover_companies_with_stats(
        conn=conn,
        queries=queries,
        max_results_per_query=max_results_per_query,
        segment=segment,
        config=config,
    )
    return int(stats["inserted"])


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
