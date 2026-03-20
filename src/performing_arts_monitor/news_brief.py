from __future__ import annotations

import hashlib
import html
import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import quote, quote_plus, urlparse

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

from performing_arts_monitor.config import AppConfig
from performing_arts_monitor.models import CollectedItem

GOOGLE_NEWS_SEARCH = "https://news.google.com/rss/search?q={query}&hl=ko&gl=KR&ceid=KR:ko"
NEWS_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/133.0.0.0 Safari/537.36"
)
ARTICLE_BODY_SELECTORS = (
    "article",
    "#dic_area",
    ".article_view",
    ".article_body",
    ".news_view",
    ".story-body",
    ".article-body",
    ".main_contents",
    ".news_end",
    ".post-content",
    ".article_txt",
)

PERFORMANCE_CONTEXT_TERMS = (
    "뮤지컬",
    "연극",
    "공연예술",
    "오페라",
    "오디션",
    "캐스팅",
    "개막",
    "초연",
    "배우",
)
NEWS_PRIORITY_TERMS = (
    "오디션",
    "캐스팅",
    "합류",
    "개막",
    "초연",
    "내한",
    "지원사업",
    "창작지원",
    "예술인지원",
    "청년예술지원사업",
    "라인업",
    "복귀",
    "인터뷰",
    "수상",
)
NEWS_PENALTY_TERMS = (
    "포토",
    "화보",
    "굿즈",
    "클립",
    "숏폼",
    "영상",
    "팬미팅",
    "OST",
)
NEWS_EXCLUDE_TERMS = (
    "bts",
    "k-pop",
    "케이팝",
    "아이돌",
    "콘서트",
    "월드투어",
    "앨범",
    "드라마",
    "예능",
)
LOW_SIGNAL_DOMAINS = (
    "youtube.com",
    "youtu.be",
    "blog",
    "tistory.com",
    "instagram.com",
    "x.com",
)
DOMAIN_BOOSTS = {
    "playdb.co.kr": 6.0,
    "yna.co.kr": 6.0,
    "newsis.com": 5.0,
    "mk.co.kr": 4.0,
    "hankyung.com": 4.0,
    "sedaily.com": 4.0,
    "nocutnews.co.kr": 4.0,
    "newsculture.press": 4.0,
    "xportsnews.com": 3.0,
    "sportsseoul.com": 3.0,
    "sportsworldi.com": 3.0,
}


@dataclass(frozen=True, slots=True)
class NewsQuery:
    key: str
    label: str
    query: str


def collect_keyword_news(
    *,
    config: AppConfig,
    start_utc: datetime,
    now: datetime,
) -> list[CollectedItem]:
    queries = _build_queries(config, hours_back=max(24, math.ceil((now - start_utc).total_seconds() / 3600.0)))
    collected: dict[str, CollectedItem] = {}
    resolved_url_cache: dict[str, str] = {}
    article_cache: dict[str, tuple[str, str, str | None]] = {}

    for query in queries:
        url = GOOGLE_NEWS_SEARCH.format(query=quote_plus(query.query))
        try:
            response = requests.get(
                url,
                timeout=config.request_timeout_seconds,
                headers={"User-Agent": NEWS_USER_AGENT, "Accept": "application/rss+xml"},
            )
            response.raise_for_status()
        except requests.RequestException:
            continue

        parsed = feedparser.parse(response.text)
        for entry in parsed.entries[: config.news_per_query_limit * 2]:
            published_at = _parse_published(entry)
            if published_at is None or published_at < start_utc:
                continue

            raw_title = _normalize_whitespace(str(entry.get("title") or ""))
            title, source_name = _extract_title_and_source(entry, raw_title)
            link = _normalize_whitespace(str(entry.get("link") or ""))
            summary = _clean_html(str(entry.get("summary") or ""))
            combined = _normalize_whitespace(" ".join(value for value in (title, summary, source_name) if value))
            matched_people = _find_mentions(combined, config.tracked_people)
            matched_keywords = _find_mentions(combined, config.tracked_keywords)
            if not _looks_relevant(combined, matched_people, matched_keywords):
                continue

            resolved_url = resolved_url_cache.get(link)
            if resolved_url is None:
                resolved_url = _resolve_google_news_url(link, timeout=config.request_timeout_seconds) or link
                resolved_url_cache[link] = resolved_url
            article_payload = article_cache.get(resolved_url)
            if article_payload is None:
                article_payload = _fetch_article_detail(resolved_url, timeout=config.request_timeout_seconds)
                article_cache[resolved_url] = article_payload
            article_summary, article_body, article_url = article_payload
            effective_url = article_url or resolved_url
            if article_summary:
                summary = article_summary
            if article_body:
                combined = _normalize_whitespace(" ".join(value for value in (title, summary, article_body, source_name) if value))
                matched_people = _find_mentions(combined, config.tracked_people)
                matched_keywords = _find_mentions(combined, config.tracked_keywords)
                if not _looks_relevant(combined, matched_people, matched_keywords):
                    continue

            fingerprint = _fingerprint(title, source_name, effective_url)
            source_weight = _source_weight(source_name, effective_url, matched_people, matched_keywords, combined)
            collected.setdefault(
                fingerprint,
                CollectedItem(
                    source_key=f"news_{query.key}",
                    source_label=f"키워드 뉴스/{query.label}",
                    site_name=source_name or "Google News",
                    source_kind="news_search",
                    title=title,
                    url=effective_url,
                    published_at=published_at,
                    summary=summary,
                    body_text=article_body or summary,
                    attachments=[],
                    external_urls=[],
                    source_weight=source_weight,
                    fingerprint=fingerprint,
                    metadata={
                        "query_key": query.key,
                        "query_label": query.label,
                        "matched_people": matched_people,
                        "matched_keywords": matched_keywords,
                        "source_url": link,
                    },
                ),
            )

    return sorted(
        collected.values(),
        key=lambda item: (item.published_at, item.source_weight),
        reverse=True,
    )


def _build_queries(config: AppConfig, *, hours_back: int) -> list[NewsQuery]:
    day_window = max(1, min(7, math.ceil(hours_back / 24)))
    context = '"뮤지컬" OR "연극" OR "공연예술" OR "공연" OR "오페라"'
    news_focus = '"캐스팅" OR "개막" OR "오디션" OR "지원사업" OR "창작지원" OR "인터뷰"'

    queries = [
        NewsQuery(
            key="industry",
            label="공연 업계",
            query=f'({context}) ({news_focus}) when:{day_window}d',
        ),
    ]

    keyword_terms = [keyword for keyword in config.tracked_keywords if keyword not in {"뮤지컬"}]
    for index, chunk in enumerate(_chunks(keyword_terms, 3), start=1):
        quoted = " OR ".join(f'"{term}"' for term in chunk)
        queries.append(
            NewsQuery(
                key=f"keyword_{index}",
                label=f"키워드 {index}",
                query=f"({quoted}) ({context}) when:{day_window}d",
            )
        )

    for index, chunk in enumerate(_chunks(list(config.tracked_people), 4), start=1):
        quoted = " OR ".join(f'"{term}"' for term in chunk)
        queries.append(
            NewsQuery(
                key=f"people_{index}",
                label=f"인물 {index}",
                query=f"({quoted}) ({context} OR \"캐스팅\" OR \"인터뷰\") when:{day_window}d",
            )
        )

    return queries


def _chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _parse_published(entry: feedparser.FeedParserDict) -> datetime | None:
    for field in ("published", "updated", "pubDate"):
        raw = entry.get(field)
        if not raw:
            continue
        try:
            parsed = date_parser.parse(str(raw))
        except (ValueError, TypeError, OverflowError):
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return None


def _extract_title_and_source(entry: feedparser.FeedParserDict, raw_title: str) -> tuple[str, str]:
    if " - " in raw_title:
        maybe_title, maybe_source = raw_title.rsplit(" - ", 1)
        if maybe_title.strip() and maybe_source.strip():
            return maybe_title.strip(), maybe_source.strip()
    source = ""
    if "source" in entry and getattr(entry.source, "title", None):
        source = _normalize_whitespace(str(entry.source.title))
    return raw_title, source or "Google News"


def _clean_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value)
    text = html.unescape(text)
    return _normalize_whitespace(text)[:800]


def _resolve_google_news_url(source_url: str, *, timeout: int) -> str | None:
    try:
        parsed = urlparse(source_url)
        parts = [part for part in parsed.path.split("/") if part]
        if parsed.netloc != "news.google.com" or len(parts) < 2 or parts[-2] not in {"articles", "rss", "read"}:
            return source_url
        base64_str = parts[-1]
        timestamp_match = None
        signature_match = None
        for article_page_url in (
            f"https://news.google.com/articles/{base64_str}",
            f"https://news.google.com/rss/articles/{base64_str}",
        ):
            article_page = requests.get(
                article_page_url,
                timeout=timeout,
                headers={"User-Agent": NEWS_USER_AGENT, "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"},
            )
            if article_page.status_code >= 400:
                continue
            timestamp_value = re.search(r'data-n-a-ts="([^"]+)"', article_page.text)
            signature_value = re.search(r'data-n-a-sg="([^"]+)"', article_page.text)
            if timestamp_value and signature_value:
                timestamp_match = timestamp_value.group(1)
                signature_match = signature_value.group(1)
                break
        if not timestamp_match or not signature_match:
            return None

        payload = [
            "Fbv4je",
            (
                f'["garturlreq",[["X","X",["X","X"],null,null,1,1,"KR:ko",null,1,null,null,null,null,null,0,1],'
                f'"X","X",1,[1,1,1],1,1,null,0,0,null,0],"{base64_str}",{timestamp_match},"{signature_match}"]'
            ),
        ]
        response = requests.post(
            "https://news.google.com/_/DotsSplashUi/data/batchexecute",
            timeout=timeout,
            headers={
                "User-Agent": NEWS_USER_AGENT,
                "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
            },
            data="f.req=" + quote(json.dumps([[payload]])),
        )
        response.raise_for_status()
        if "\n\n" not in response.text:
            return None
        parsed_payload = json.loads(response.text.split("\n\n", 1)[1])[:-2]
        return str(json.loads(parsed_payload[0][2])[1]).strip() or None
    except Exception:
        return None


def _fetch_article_detail(url: str, *, timeout: int) -> tuple[str, str, str | None]:
    try:
        response = requests.get(
            url,
            timeout=timeout,
            headers={"User-Agent": NEWS_USER_AGENT, "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"},
        )
        response.raise_for_status()
        content_type = (response.headers.get("content-type") or "").lower()
        if "text/html" not in content_type:
            return "", "", response.url
        soup = BeautifulSoup(response.text[:900000], "html.parser")
        summary = (
            _extract_meta_content(soup, "og:description", "property")
            or _extract_meta_content(soup, "description", "name")
            or _extract_meta_content(soup, "twitter:description", "name")
        )
        body_text = ""
        for selector in ARTICLE_BODY_SELECTORS:
            node = soup.select_one(selector)
            if node is None:
                continue
            text = _normalize_whitespace(node.get_text(" ", strip=True))
            if len(text) >= 120:
                body_text = text[:3200]
                break
        if not body_text:
            body_text = _normalize_whitespace(soup.get_text(" ", strip=True))[:3200]
        if (not summary or len(summary) < 20 or summary.lower() in {"msn", "google 뉴스", "google news"}) and body_text:
            summary = _first_sentences(body_text)
        return summary, body_text, response.url
    except Exception:
        return "", "", None


def _extract_meta_content(soup: BeautifulSoup, key: str, attribute: str) -> str:
    tag = soup.find("meta", attrs={attribute: key})
    if tag and tag.get("content"):
        return _normalize_whitespace(html.unescape(str(tag.get("content"))))
    return ""


def _looks_relevant(text: str, matched_people: list[str], matched_keywords: list[str]) -> bool:
    lowered = text.lower()
    has_context = any(term.lower() in lowered for term in PERFORMANCE_CONTEXT_TERMS)
    if not has_context:
        return False
    if any(term in lowered for term in NEWS_EXCLUDE_TERMS) and not matched_people:
        return False
    if any(term.lower() in lowered for term in NEWS_PENALTY_TERMS) and not matched_people:
        return False
    if matched_people or matched_keywords:
        return True
    has_strict_context = any(term in lowered for term in ("뮤지컬", "연극", "오페라", "공연예술"))
    return has_strict_context and any(term.lower() in lowered for term in NEWS_PRIORITY_TERMS)


def _find_mentions(text: str, values: tuple[str, ...]) -> list[str]:
    lowered = text.lower()
    return [value for value in values if value.lower() in lowered]


def _extract_domain(url: str) -> str:
    host = urlparse(url).netloc.lower().strip()
    if host.startswith("www."):
        host = host[4:]
    return host


def _source_weight(
    source_name: str,
    url: str,
    matched_people: list[str],
    matched_keywords: list[str],
    combined_text: str,
) -> float:
    score = 6.0
    domain = _extract_domain(url)
    for suffix, boost in DOMAIN_BOOSTS.items():
        if domain.endswith(suffix):
            score += boost
            break
    if any(signal in domain for signal in LOW_SIGNAL_DOMAINS):
        score -= 4.0
    score += min(len(matched_people) * 2.5, 6.0)
    score += min(len(matched_keywords) * 1.2, 4.0)
    score += min(sum(1 for term in NEWS_PRIORITY_TERMS if term.lower() in combined_text.lower()) * 1.2, 4.0)
    return round(max(0.0, min(score, 18.0)), 1)


def _fingerprint(title: str, source_name: str, url: str) -> str:
    payload = f"{_normalize_whitespace(title.lower())}|{_normalize_whitespace(source_name.lower())}|{url}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _first_sentences(text: str, *, max_chars: int = 240) -> str:
    cleaned = _normalize_whitespace(text)
    cleaned = re.sub(r"^\[[^\]]{0,80}\]\s*", "", cleaned)
    cleaned = re.sub(r"^[가-힣A-Za-z0-9·\s]+?\|\s*[가-힣A-Za-z0-9·\s]{0,40}기자\s*", "", cleaned)
    if len(cleaned) <= max_chars:
        return cleaned
    match = re.match(r"(.{40,240}?[.!?])(?:\s|$)", cleaned)
    if match:
        return match.group(1).strip()
    return cleaned[: max_chars - 1].rstrip() + "…"
