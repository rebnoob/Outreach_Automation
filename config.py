from dataclasses import dataclass
import os


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


@dataclass
class ToolkitConfig:
    timeout_seconds: int = 12
    user_agent: str = DEFAULT_USER_AGENT
    max_pages_per_company: int = 4
    search_endpoint: str = "https://duckduckgo.com/html/?q={query}"


def load_config() -> ToolkitConfig:
    timeout = int(os.getenv("OUTREACH_TIMEOUT_SECONDS", "12"))
    max_pages = int(os.getenv("OUTREACH_MAX_PAGES", "4"))
    user_agent = os.getenv("OUTREACH_USER_AGENT", DEFAULT_USER_AGENT)
    return ToolkitConfig(
        timeout_seconds=timeout,
        user_agent=user_agent,
        max_pages_per_company=max_pages,
    )
