import re
import os
import json
from datetime import datetime
from typing import Optional


# ── Text utilities ────────────────────────────────────────────────────────────

def clean_html(text: str) -> str:
    """Strip HTML tags and normalise whitespace."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def build_article_text(title: str, description: str) -> str:
    """Combine title and description into a single string for LLM input."""
    parts = [p.strip() for p in (title, description) if p and p.strip()]
    return " ".join(parts)


# ── JSON / file utilities ─────────────────────────────────────────────────────

def safe_json_load(path: str) -> dict | list | None:
    """Load a JSON file, returning None on any error."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[helpers] Could not load {path}: {e}")
        return None


def ensure_dir(path: str) -> None:
    """Create directory (and parents) if it doesn't exist."""
    os.makedirs(path, exist_ok=True)


# ── Timestamp ─────────────────────────────────────────────────────────────────

def now_iso() -> str:
    """Return current UTC time as an ISO-8601 string."""
    return datetime.utcnow().isoformat() + "Z"


# ── Deduplication ─────────────────────────────────────────────────────────────

def deduplicate(items: list[dict], key: str) -> list[dict]:
    """
    Remove duplicate dicts from a list based on a single key.
    Preserves first occurrence. Items where the key is None or empty
    are always kept (they can't be meaningfully deduplicated by URL).
    """
    seen = set()
    result = []
    for item in items:
        val = item.get(key)
        if not val:
            # No key value — keep it, can't deduplicate
            result.append(item)
            continue
        if val not in seen:
            seen.add(val)
            result.append(item)
    return result


# ── Google News URL resolver ──────────────────────────────────────────────────

def resolve_article_url(url: str, use_redirect: bool = False) -> str:
    """
    For Google News RSS URLs: the real article URL should be captured at
    fetch time from entry.source.href (see rss_fetcher._get_real_url).
    This function is a no-op fallback for when we only have the encoded link.

    Google News RSS encoded URLs (news.google.com/rss/articles/CBMi...)
    cannot be reliably decoded without JavaScript rendering or the gnews
    package. The correct approach is to read entry.source.href at fetch time.

    Args:
        url:          The URL to resolve (may be encoded Google News URL)
        use_redirect: Ignored — kept for API compatibility

    Returns:
        The original URL unchanged. Real URLs are captured at fetch time.
    """
    if not url or "news.google.com" not in url:
        return url
    # Can't reliably decode Google News URLs without JavaScript rendering.
    # The real URL is captured at fetch time from entry.source.href.
    return url


def resolve_article_urls_batch(
    articles: list[dict],
    url_key: str = "url",
    use_redirect: bool = False,
) -> list[dict]:
    """
    Resolve Google News URLs for a batch of articles in-place.

    Adds a 'article_url' key with the resolved real URL.
    The original 'url' key is preserved unchanged.

    Args:
        articles:     List of article dicts
        url_key:      Key containing the URL to resolve (default: 'url')
        use_redirect: Ignored — kept for API compatibility

    Returns:
        The same list with 'article_url' added to each dict.
    """
    for article in articles:
        raw_url = article.get(url_key, "")
        article["article_url"] = resolve_article_url(raw_url, use_redirect)
    return articles
