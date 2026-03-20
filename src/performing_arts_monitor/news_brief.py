from __future__ import annotations

import hashlib
import html
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import quote_plus, urlparse

import feedparser
import requests
from dateutil import parser as date_parser

from performing_arts_monitor.config import AppConfig
from performing_arts_monitor.models import CollectedItem

GOOGLE_NEWS_SEARCH = "https://news.google.com/rss/search?q={query}&hl=ko&gl=KR&ceid=KR:ko"
NEWS_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/133.0.0.0 Safari/537.36"
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

            fingerprint = _fingerprint(title, source_name, link)
            source_weight = _source_weight(source_name, link, matched_people, matched_keywords, combined)
            collected.setdefault(
                fingerprint,
                CollectedItem(
                    source_key=f"news_{query.key}",
                    source_label=f"키워드 뉴스/{query.label}",
                    site_name=source_name or "Google News",
                    source_kind="news_search",
                    title=title,
                    url=link,
                    published_at=published_at,
                    summary=summary,
                    body_text=summary,
                    attachments=[],
                    external_urls=[],
                    source_weight=source_weight,
                    fingerprint=fingerprint,
                    metadata={
                        "query_key": query.key,
                        "query_label": query.label,
                        "matched_people": matched_people,
                        "matched_keywords": matched_keywords,
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
