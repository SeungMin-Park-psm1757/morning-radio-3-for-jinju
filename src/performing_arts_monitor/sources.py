from __future__ import annotations

import hashlib
import html
import json
import re
import time as time_module
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urljoin, urlparse

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

from performing_arts_monitor.config import AppConfig
from performing_arts_monitor.models import CollectedItem, SourceDefinition

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/133.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}
BLOCKED_MARKERS = (
    "access denied",
    "forbidden",
    "verify you are human",
    "just a moment",
    "cf-browser-verification",
    "attention required",
    "/cdn-cgi/challenge-platform/",
    "cf-chl-",
)

OD_NOTICE_URL = "https://www.odmusical.com/kor/audition/notice"
OD_NEWS_URL = "https://www.odmusical.com/kor/audition/notice2"
EMK_AUDITION_FEED = "https://emkmusical.com/notice_audition/feed/"
EMK_NOTICE_FEED = "https://emkmusical.com/notice/feed/"
EMK_NEWS_FEED = "https://emkmusical.com/news/feed/"
EMK_RECRUIT_FEED = "https://emkmusical.com/recruit/feed/"
OTR_AUDITION_FEED = "https://otr.co.kr/audition/feed/"
OTR_NOTICE_FEED = "https://otr.co.kr/notice/feed/"
SHOWNOTE_NOTICE_URL = "https://www.shownote.com/Community/BoardNotice/Index"
SNCO_AUDITION_URL = "https://sncokorea.com/audition"
SNCO_NEWS_URL = "https://sncokorea.com/news"
ISEENSEE_NOTICE_JSON = "https://m.iseensee.com/JsonData/Article/ArticleList.aspx"
ACOM_NOTICE_URL = "http://www.acommusical.com/default/community/community01.php?sub=01"
CJENM_NEWS_URL = "https://www.cjenm.com/ko/news/filter/"
CJENM_INCLUDE_TERMS = (
    "뮤지컬",
    "연극",
    "오페라",
    "공연예술",
    "performing arts",
    "캐스팅",
    "오디션",
    "개막",
    "초연",
    "웨스트엔드",
    "브로드웨이",
    "라이프 오브 파이",
    "위키드",
    "프로즌",
    "오페라의 유령",
    "헬스키친",
)
CJENM_EXCLUDE_TERMS = (
    "mnet",
    "엠카운트다운",
    "k-pop",
    "케이팝",
    "팬터랙티브",
    "가입자",
    "플랫폼",
    "tvn",
    "티빙",
    "드라마",
    "예능",
)

ATTACHMENT_EXTENSIONS = (
    ".pdf",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".hwp",
    ".hwpx",
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".zip",
)

BODY_SELECTORS = (
    ".board_view",
    ".view_text",
    ".txt_area",
    ".read_con",
    ".read_cont",
    ".board_contents",
    ".entry-content",
    ".post-content",
    ".view_cont",
    ".boardPostContents",
    ".sub_content",
    "#content",
    "article",
    ".contents",
    ".cont",
)

SOURCES: tuple[SourceDefinition, ...] = (
    SourceDefinition("emk_audition", "EMK 오디션", "EMK", "official_audition", 18.0, EMK_AUDITION_FEED),
    SourceDefinition("emk_notice", "EMK 공지", "EMK", "official_notice", 15.0, EMK_NOTICE_FEED),
    SourceDefinition("emk_news", "EMK 뉴스", "EMK", "official_news", 12.0, EMK_NEWS_FEED),
    SourceDefinition("emk_recruit", "EMK 채용", "EMK", "official_notice", 12.0, EMK_RECRUIT_FEED),
    SourceDefinition("otr_audition", "OTR 오디션", "OTR", "community_board", 8.0, OTR_AUDITION_FEED),
    SourceDefinition("otr_notice", "OTR 공지", "OTR", "community_board", 7.0, OTR_NOTICE_FEED),
    SourceDefinition("od_notice", "OD 오디션", "OD", "official_audition", 18.0, OD_NOTICE_URL),
    SourceDefinition("od_news", "OD 뉴스", "OD", "official_news", 12.0, OD_NEWS_URL),
    SourceDefinition("shownote_notice", "Shownote 공지", "SHOWNOTE", "official_notice", 15.0, SHOWNOTE_NOTICE_URL),
    SourceDefinition("snco_audition", "SNCO 오디션", "SNCO", "official_audition", 18.0, SNCO_AUDITION_URL),
    SourceDefinition("snco_news", "SNCO 뉴스", "SNCO", "official_news", 12.0, SNCO_NEWS_URL),
    SourceDefinition("iseensee_notice", "신시컴퍼니 공지", "SHINSEE", "official_notice", 14.0, ISEENSEE_NOTICE_JSON),
    SourceDefinition("cjenm_news", "CJ ENM 뉴스", "CJ ENM", "corporate_newsroom", 10.0, CJENM_NEWS_URL),
    SourceDefinition("acomm_notice", "ACOM 공지", "ACOM", "official_notice", 15.0, ACOM_NOTICE_URL),
)


@dataclass(slots=True)
class DetailPayload:
    published_at: datetime | None
    summary: str
    body_text: str
    attachments: list[str]
    external_urls: list[str]
    resolved_url: str | None


def collect_items(
    *,
    config: AppConfig,
    start_utc: datetime | None = None,
    now: datetime | None = None,
) -> tuple[list[CollectedItem], dict[str, str]]:
    reference_time = now or datetime.now(tz=UTC)
    cutoff = start_utc or config.collection_window_start(reference_time)
    collected: list[CollectedItem] = []
    errors: dict[str, str] = {}

    for source in SOURCES:
        try:
            items = _collect_source(source, config=config, now=reference_time, cutoff=cutoff)
            collected.extend(items)
        except Exception as exc:  # pragma: no cover - resilience path
            errors[source.key] = str(exc)

    deduped: dict[str, CollectedItem] = {}
    for item in collected:
        deduped.setdefault(item.fingerprint, item)
    items = sorted(
        deduped.values(),
        key=lambda item: (item.published_at, item.source_weight),
        reverse=True,
    )
    return items, errors


def _collect_source(
    source: SourceDefinition,
    *,
    config: AppConfig,
    now: datetime,
    cutoff: datetime,
) -> list[CollectedItem]:
    if source.key.startswith("emk_") or source.key.startswith("otr_"):
        return _collect_feed_source(source, config=config, now=now, cutoff=cutoff)
    if source.key.startswith("od_"):
        return _collect_od_source(source, config=config, now=now, cutoff=cutoff)
    if source.key == "shownote_notice":
        return _collect_shownote_notice(source, config=config, now=now, cutoff=cutoff)
    if source.key.startswith("snco_"):
        return _collect_snco_source(source, config=config, now=now, cutoff=cutoff)
    if source.key == "iseensee_notice":
        return _collect_iseensee_notice(source, config=config, now=now, cutoff=cutoff)
    if source.key == "cjenm_news":
        return _collect_cjenm_news(source, config=config, now=now, cutoff=cutoff)
    if source.key == "acomm_notice":
        return _collect_acomm_notice(source, config=config, now=now, cutoff=cutoff)
    return []


def _collect_feed_source(
    source: SourceDefinition,
    *,
    config: AppConfig,
    now: datetime,
    cutoff: datetime,
) -> list[CollectedItem]:
    response = _get(source.url, config)
    parsed = feedparser.parse(response.text)
    channel_title = _normalize_whitespace(str(parsed.feed.get("title") or ""))
    if not parsed.entries or channel_title.startswith("댓글:"):
        return _collect_feed_fallback_table(source, config=config, now=now, cutoff=cutoff)

    items: list[CollectedItem] = []
    for entry in parsed.entries[: config.max_source_items * 2]:
        published_at = _parse_datetime(str(entry.get("published") or entry.get("updated") or ""), config)
        if published_at is None or published_at < cutoff:
            continue

        title = _extract_feed_title(entry)
        if not title:
            continue
        raw_url = str(entry.get("link") or "").strip()
        if not raw_url:
            continue

        detail = _detail_from_url(raw_url, source.key, config)
        body_text = detail.body_text or _clean_html(entry.get("summary", ""))
        summary = detail.summary or _first_sentences(body_text or title)
        resolved_url = detail.resolved_url or raw_url
        effective_published_at = _resolved_published_at(published_at, detail.published_at, cutoff=cutoff, now=now)
        url = resolved_url
        if source.site_name == "OTR":
            promoted = _promote_external_url(detail.external_urls)
            if promoted:
                url = promoted

        items.append(
            CollectedItem(
                source_key=source.key,
                source_label=source.label,
                site_name=source.site_name,
                source_kind=source.source_kind,
                title=title,
                url=url,
                published_at=effective_published_at,
                summary=summary,
                body_text=body_text,
                attachments=detail.attachments,
                external_urls=detail.external_urls,
                source_weight=source.source_weight,
                fingerprint=_fingerprint(title, url, source.key),
                metadata={"raw_url": raw_url},
            )
        )
        if len(items) >= config.max_source_items:
            break
    return items


def _collect_feed_fallback_table(
    source: SourceDefinition,
    *,
    config: AppConfig,
    now: datetime,
    cutoff: datetime,
) -> list[CollectedItem]:
    list_url = _feed_listing_url(source.url)
    soup = _get_soup(list_url, config)

    items: list[CollectedItem] = []
    for row in soup.select("table tbody tr"):
        anchor = row.select_one("td a[href]")
        cells = row.select("td")
        if anchor is None or len(cells) < 2:
            continue

        title = _normalize_whitespace(anchor.get_text(" ", strip=True))
        if len(title) < 4 or re.fullmatch(r"\d+", title):
            continue
        raw_url = urljoin(list_url, anchor.get("href", "").strip())
        published_at = _extract_board_row_datetime(cells, row.get_text(" ", strip=True), source.key, config)
        if not title or not raw_url or published_at is None or published_at < cutoff:
            continue

        detail = _detail_from_url(raw_url, source.key, config)
        body_text = detail.body_text
        summary = detail.summary or _first_sentences(body_text or title)
        resolved_url = detail.resolved_url or raw_url
        effective_published_at = _resolved_published_at(published_at, detail.published_at, cutoff=cutoff, now=now)
        url = resolved_url
        if source.site_name == "OTR":
            promoted = _promote_external_url(detail.external_urls)
            if promoted:
                url = promoted

        items.append(
            CollectedItem(
                source_key=source.key,
                source_label=source.label,
                site_name=source.site_name,
                source_kind=source.source_kind,
                title=title,
                url=url,
                published_at=effective_published_at,
                summary=summary,
                body_text=body_text,
                attachments=detail.attachments,
                external_urls=detail.external_urls,
                source_weight=source.source_weight,
                fingerprint=_fingerprint(title, url, source.key),
                metadata={"raw_url": raw_url},
            )
        )
        if len(items) >= config.max_source_items:
            break
    return items


def _collect_od_source(
    source: SourceDefinition,
    *,
    config: AppConfig,
    now: datetime,
    cutoff: datetime,
) -> list[CollectedItem]:
    soup = _get_soup(source.url, config)
    items: list[CollectedItem] = []

    for row in soup.select("table.list_tbl tbody tr"):
        anchor = row.select_one("td.sbj a")
        date_cell = row.select("td")
        if anchor is None or len(date_cell) < 4:
            continue
        title = _normalize_whitespace(anchor.get_text(" ", strip=True))
        detail_url = urljoin(source.url, anchor.get("href", "").strip())
        published_at = _parse_datetime(date_cell[3].get_text(" ", strip=True), config)
        if not title or not detail_url or published_at is None or published_at < cutoff:
            continue

        detail = _detail_from_url(detail_url, source.key, config)
        effective_published_at = _resolved_published_at(published_at, detail.published_at, cutoff=cutoff, now=now)
        items.append(
            CollectedItem(
                source_key=source.key,
                source_label=source.label,
                site_name=source.site_name,
                source_kind=source.source_kind,
                title=title,
                url=detail.resolved_url or detail_url,
                published_at=effective_published_at,
                summary=detail.summary or _first_sentences(detail.body_text or title),
                body_text=detail.body_text,
                attachments=detail.attachments,
                external_urls=detail.external_urls,
                source_weight=source.source_weight,
                fingerprint=_fingerprint(title, detail.resolved_url or detail_url, source.key),
            )
        )
        if len(items) >= config.max_source_items:
            break
    return items


def _collect_shownote_notice(
    source: SourceDefinition,
    *,
    config: AppConfig,
    now: datetime,
    cutoff: datetime,
) -> list[CollectedItem]:
    response = _get(source.url, config)
    article_payloads = _extract_angular_json_array(response.text, "$scope.Articles")

    items: list[CollectedItem] = []
    for payload in article_payloads[: config.max_source_items * 2]:
        title = _normalize_whitespace(str(payload.get("Title") or ""))
        article_id = str(payload.get("ArticleId") or "").strip()
        if not title or not article_id:
            continue
        published_at = _parse_datetime(str(payload.get("DisplayWritingTime") or payload.get("RegisterDate") or ""), config)
        if published_at is None or published_at < cutoff:
            continue
        detail_url = f"https://www.shownote.com/Community/BoardNotice/Details?articleId={article_id}"
        detail = _detail_from_url(detail_url, source.key, config)
        effective_published_at = _resolved_published_at(published_at, detail.published_at, cutoff=cutoff, now=now)
        items.append(
            CollectedItem(
                source_key=source.key,
                source_label=source.label,
                site_name=source.site_name,
                source_kind=source.source_kind,
                title=title,
                url=detail.resolved_url or detail_url,
                published_at=effective_published_at,
                summary=detail.summary or _first_sentences(detail.body_text or title),
                body_text=detail.body_text,
                attachments=detail.attachments,
                external_urls=detail.external_urls,
                source_weight=source.source_weight,
                fingerprint=_fingerprint(title, detail.resolved_url or detail_url, source.key),
            )
        )
        if len(items) >= config.max_source_items:
            break
    return items


def _collect_snco_source(
    source: SourceDefinition,
    *,
    config: AppConfig,
    now: datetime,
    cutoff: datetime,
) -> list[CollectedItem]:
    soup = _get_soup(source.url, config)
    items: list[CollectedItem] = []

    for wrapper in soup.select(".boardPostWrapper"):
        anchor = wrapper.select_one(".boardPostTitle a[href*='/boardPost/']")
        date_node = wrapper.select_one(".boardPostCreateDate")
        if anchor is None or date_node is None:
            continue
        title = _normalize_whitespace(anchor.get_text(" ", strip=True))
        detail_url = urljoin(source.url, anchor.get("href", "").strip())
        published_at = _parse_datetime(date_node.get_text(" ", strip=True), config)
        if not title or not detail_url or published_at is None or published_at < cutoff:
            continue
        detail = _detail_from_url(detail_url, source.key, config)
        effective_published_at = _resolved_published_at(published_at, detail.published_at, cutoff=cutoff, now=now)
        items.append(
            CollectedItem(
                source_key=source.key,
                source_label=source.label,
                site_name=source.site_name,
                source_kind=source.source_kind,
                title=title,
                url=detail.resolved_url or detail_url,
                published_at=effective_published_at,
                summary=detail.summary or _first_sentences(detail.body_text or title),
                body_text=detail.body_text,
                attachments=detail.attachments,
                external_urls=detail.external_urls,
                source_weight=source.source_weight,
                fingerprint=_fingerprint(title, detail.resolved_url or detail_url, source.key),
            )
        )
        if len(items) >= config.max_source_items:
            break
    return items


def _collect_iseensee_notice(
    source: SourceDefinition,
    *,
    config: AppConfig,
    now: datetime,
    cutoff: datetime,
) -> list[CollectedItem]:
    response = _get(
        source.url,
        config,
        params={"boardid": 1, "curPage": 1, "pageSize": config.max_source_items, "searchText": ""},
    )
    payload = json.loads(response.text)

    items: list[CollectedItem] = []
    for row in payload.get("items", []):
        title = _normalize_whitespace(str(row.get("title") or ""))
        article_id = str(row.get("id") or "").strip()
        if not title or not article_id:
            continue
        published_at = _parse_datetime(str(row.get("regdate") or ""), config)
        if published_at is None or published_at < cutoff:
            continue
        detail_url = f"https://m.iseensee.com/Community/NoticeRead.aspx?page=1&id={article_id}"
        detail = _detail_from_url(detail_url, source.key, config)
        effective_published_at = _resolved_published_at(published_at, detail.published_at, cutoff=cutoff, now=now)
        items.append(
            CollectedItem(
                source_key=source.key,
                source_label=source.label,
                site_name=source.site_name,
                source_kind=source.source_kind,
                title=title,
                url=detail.resolved_url or detail_url,
                published_at=effective_published_at,
                summary=detail.summary or _first_sentences(detail.body_text or title),
                body_text=detail.body_text,
                attachments=detail.attachments,
                external_urls=detail.external_urls,
                source_weight=source.source_weight,
                fingerprint=_fingerprint(title, detail.resolved_url or detail_url, source.key),
            )
        )
        if len(items) >= config.max_source_items:
            break
    return items


def _collect_cjenm_news(
    source: SourceDefinition,
    *,
    config: AppConfig,
    now: datetime,
    cutoff: datetime,
) -> list[CollectedItem]:
    response = _get(source.url, config)
    soup = BeautifulSoup(response.text, "html.parser")
    next_data = soup.find("script", id="__NEXT_DATA__")
    if next_data is None or not next_data.string:
        return []
    payload = json.loads(next_data.string)
    entries = _find_cjenm_news_entries(payload)

    items: list[CollectedItem] = []
    for entry in entries:
        title = _normalize_whitespace(str(entry.get("bbscTit") or ""))
        body_html = str(entry.get("htmlCnts") or "")
        detail_path = str(entry.get("frontDetailUrlAddr") or "").strip().lstrip("/")
        if not detail_path:
            continue
        body_text = _clean_html(body_html)
        if not _matches_cjenm_keywords(title=title, body_text=body_text, detail_path=detail_path, config=config):
            continue
        putup_dt = entry.get("putupDt")
        if not putup_dt:
            continue
        published_at = datetime.fromtimestamp(int(putup_dt) / 1000, tz=UTC)
        if published_at < cutoff:
            continue
        detail_url = urljoin("https://www.cjenm.com/ko/", detail_path)
        external_urls = []
        ext_url = str(entry.get("extnlLinkUrlAddr") or "").strip()
        if ext_url:
            external_urls.append(ext_url)
        file_path = str(entry.get("filePathAddr") or "").strip()
        attachments = [urljoin("https://www.cjenm.com", file_path)] if file_path else []
        items.append(
            CollectedItem(
                source_key=source.key,
                source_label=source.label,
                site_name=source.site_name,
                source_kind=source.source_kind,
                title=title,
                url=detail_url,
                published_at=published_at,
                summary=_first_sentences(body_text or title),
                body_text=body_text,
                attachments=attachments,
                external_urls=external_urls,
                source_weight=source.source_weight,
                fingerprint=_fingerprint(title, detail_url, source.key),
            )
        )
        if len(items) >= config.max_source_items:
            break
    return items


def _collect_acomm_notice(
    source: SourceDefinition,
    *,
    config: AppConfig,
    now: datetime,
    cutoff: datetime,
) -> list[CollectedItem]:
    soup = _get_soup(source.url, config)
    items: list[CollectedItem] = []

    for row in soup.select("tr[onclick*='com_board_basic=read_form']"):
        anchor = row.select_one("a[href*='com_board_basic=read_form']")
        date_node = row.select_one(".bbsetc_dateof_write")
        if anchor is None or date_node is None:
            continue
        title = _normalize_whitespace(anchor.get_text(" ", strip=True))
        detail_url = urljoin(source.url, anchor.get("href", "").strip())
        published_at = _parse_datetime(date_node.get_text(" ", strip=True), config)
        if not title or not detail_url or published_at is None or published_at < cutoff:
            continue
        detail = _detail_from_url(detail_url, source.key, config)
        effective_published_at = _resolved_published_at(published_at, detail.published_at, cutoff=cutoff, now=now)
        items.append(
            CollectedItem(
                source_key=source.key,
                source_label=source.label,
                site_name=source.site_name,
                source_kind=source.source_kind,
                title=title,
                url=detail.resolved_url or detail_url,
                published_at=effective_published_at,
                summary=detail.summary or _first_sentences(detail.body_text or title),
                body_text=detail.body_text,
                attachments=detail.attachments,
                external_urls=detail.external_urls,
                source_weight=source.source_weight,
                fingerprint=_fingerprint(title, detail.resolved_url or detail_url, source.key),
            )
        )
        if len(items) >= config.max_source_items:
            break
    return items


def _detail_from_url(url: str, source_key: str, config: AppConfig) -> DetailPayload:
    if source_key == "shownote_notice":
        return _shownote_detail(url, config)
    if source_key.startswith("otr_"):
        return _otr_detail(url, config)
    if source_key.startswith("snco_"):
        return _snco_detail(url, config)
    return _generic_detail(url, config)


def _generic_detail(url: str, config: AppConfig) -> DetailPayload:
    response = _get(url, config)
    soup = BeautifulSoup(response.text[:700000], "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    attachments = _extract_attachment_urls(soup, response.url)
    external_urls = _extract_external_urls(soup, response.url)
    summary = (
        _extract_meta_content(soup, "og:description", "property")
        or _extract_meta_content(soup, "description", "name")
        or _extract_meta_content(soup, "twitter:description", "name")
    )
    body_text = ""
    for selector in BODY_SELECTORS:
        node = soup.select_one(selector)
        if node is None:
            continue
        text = _node_text(node)
        if len(text) >= 80:
            body_text = text
            break
    if not body_text:
        body_text = _crop_body(_node_text(soup), max_chars=2200)
    if not summary:
        summary = _first_sentences(body_text)
    published_at = _extract_first_datetime(_node_text(soup), config)
    return DetailPayload(
        published_at=published_at,
        summary=summary,
        body_text=body_text,
        attachments=attachments,
        external_urls=external_urls,
        resolved_url=response.url,
    )


def _otr_detail(url: str, config: AppConfig) -> DetailPayload:
    response = _get(url, config)
    soup = BeautifulSoup(response.text[:700000], "html.parser")
    body_node = soup.select_one(".table-view") or soup.select_one(".board-view") or soup.select_one(".mb-board")
    body_text = _node_text(body_node) if body_node is not None else ""
    body_text = body_text.split("목록 댓글", 1)[0].strip()
    body_text = body_text.split("Powered by MangBoard", 1)[0].strip()
    attachments = _extract_attachment_urls(soup, response.url)
    external_urls = _extract_external_urls(soup, response.url)
    published_text = _extract_meta_content(soup, "article:published_time", "property")
    published_at = _parse_datetime(published_text, config) or _extract_first_datetime(body_text, config)
    summary = _first_sentences(body_text)
    return DetailPayload(
        published_at=published_at,
        summary=summary,
        body_text=body_text,
        attachments=attachments,
        external_urls=external_urls,
        resolved_url=response.url,
    )


def _shownote_detail(url: str, config: AppConfig) -> DetailPayload:
    response = _get(url, config)
    match = re.search(
        r"\$scope\.Article\s*=\s*angular\.fromJson\((\{.*?\})\);",
        response.text,
        flags=re.S,
    )
    if not match:
        return _generic_detail(url, config)
    payload = json.loads(match.group(1))
    contents_html = html.unescape(str(payload.get("Contents") or ""))
    summary = _first_sentences(_clean_html(contents_html))
    files = payload.get("Files") or []
    attachments = []
    for file_info in files:
        file_url = str(file_info.get("FileDownloadUrl") or "").strip()
        if file_url:
            attachments.append(urljoin(response.url, file_url))
    if not attachments:
        attachments = re.findall(r"https?://[^\s\"']+/Down/Board/[^\s\"']+", response.text)
    body_text = _clean_html(contents_html)
    published_at = _parse_datetime(str(payload.get("DisplayWritingTime") or payload.get("RegisterDate") or ""), config)
    return DetailPayload(
        published_at=published_at,
        summary=summary,
        body_text=body_text,
        attachments=attachments,
        external_urls=[],
        resolved_url=response.url,
    )


def _snco_detail(url: str, config: AppConfig) -> DetailPayload:
    response = _get(url, config)
    soup = BeautifulSoup(response.text[:700000], "html.parser")
    summary = (
        _extract_meta_content(soup, "og:description", "property")
        or _extract_meta_content(soup, "description", "name")
    )
    title = _extract_meta_content(soup, "og:title", "property")
    combined_text = _node_text(soup)
    published_at = _extract_first_datetime(combined_text, config)
    external_urls = _extract_external_urls(soup, response.url)
    attachments = _extract_attachment_urls(soup, response.url)
    body_text = summary or _first_sentences(combined_text)
    return DetailPayload(
        published_at=published_at,
        summary=summary or _first_sentences(body_text or title),
        body_text=body_text,
        attachments=attachments,
        external_urls=external_urls,
        resolved_url=response.url,
    )


def _find_cjenm_news_entries(payload: object) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []

    def visit(node: object) -> None:
        if isinstance(node, dict):
            if "bbscTit" in node and "frontDetailUrlAddr" in node:
                entries.append(node)
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for item in node:
                visit(item)

    visit(payload)
    return entries


def _matches_cjenm_keywords(
    *,
    title: str,
    body_text: str,
    detail_path: str,
    config: AppConfig,
) -> bool:
    lowered = f"{title} {body_text}".lower()
    detail_lower = detail_path.lower()
    has_tracked_person = any(person.lower() in lowered for person in config.tracked_people)
    has_positive_signal = "performing-arts" in detail_lower or any(
        keyword.lower() in lowered for keyword in CJENM_INCLUDE_TERMS
    )
    if not (has_tracked_person or has_positive_signal):
        return False
    if not has_tracked_person and any(keyword.lower() in lowered for keyword in CJENM_EXCLUDE_TERMS):
        return False
    return True


def _extract_feed_title(entry: feedparser.FeedParserDict) -> str:
    title = _normalize_whitespace(str(entry.get("title") or ""))
    if " - " in title:
        maybe_title, maybe_source = title.rsplit(" - ", 1)
        if maybe_title and maybe_source and len(maybe_source) <= 40:
            return maybe_title.strip()
    return title


def _feed_listing_url(url: str) -> str:
    return re.sub(r"/feed/?$", "/", url)


def _extract_board_row_datetime(
    cells: list[object],
    row_text: str,
    source_key: str,
    config: AppConfig,
) -> datetime | None:
    cell_texts = [_normalize_whitespace(getattr(cell, "get_text")(" ", strip=True)) for cell in cells]
    if source_key.startswith("emk_") and len(cell_texts) >= 4:
        return _parse_datetime(cell_texts[3], config)
    if source_key == "otr_notice" and len(cell_texts) >= 4:
        return _parse_datetime(cell_texts[3], config)
    if source_key == "otr_audition" and len(cell_texts) >= 6:
        return _parse_datetime(cell_texts[5], config)
    return _extract_first_datetime(_normalize_whitespace(row_text), config)


def _extract_angular_json_array(text: str, scope_name: str) -> list[dict[str, object]]:
    pattern = rf"{re.escape(scope_name)}\s*=\s*angular\.fromJson\((\[.*?\])\);"
    match = re.search(pattern, text, flags=re.S)
    if not match:
        return []
    return json.loads(match.group(1))


def _extract_attachment_urls(soup: BeautifulSoup, base_url: str) -> list[str]:
    attachments: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = urljoin(base_url, anchor.get("href", "").strip())
        lowered = href.lower()
        if any(lowered.endswith(ext) for ext in ATTACHMENT_EXTENSIONS) or "/down/" in lowered:
            attachments.append(href)
    return list(dict.fromkeys(attachments))


def _extract_external_urls(soup: BeautifulSoup, base_url: str) -> list[str]:
    base_domain = _extract_domain(base_url)
    urls: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = urljoin(base_url, anchor.get("href", "").strip())
        domain = _extract_domain(href)
        if not domain or domain == base_domain:
            continue
        if any(skip in domain for skip in ("facebook.com", "instagram.com", "twitter.com", "youtube.com", "youtu.be", "mangboard.com")):
            continue
        urls.append(href)
    return list(dict.fromkeys(urls))


def _promote_external_url(urls: list[str]) -> str | None:
    for url in urls:
        domain = _extract_domain(url)
        if domain and not any(skip in domain for skip in ("google.com", "facebook.com", "instagram.com", "twitter.com", "mangboard.com")):
            return url
    return None


def _extract_meta_content(soup: BeautifulSoup, key: str, attribute: str) -> str:
    tag = soup.find("meta", attrs={attribute: key})
    if tag and tag.get("content"):
        return _normalize_whitespace(html.unescape(str(tag.get("content"))))
    return ""


def _extract_first_datetime(text: str, config: AppConfig) -> datetime | None:
    patterns = (
        r"20\d{2}[./-]\d{2}[./-]\d{2}\s+\d{2}:\d{2}",
        r"20\d{2}[./-]\d{2}[./-]\d{2}",
        r"\d{2}[./]\d{2}[./]\d{2}\s+\d{2}:\d{2}",
        r"\d{2}[./]\d{2}[./]\d{2}",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            parsed = _parse_datetime(match.group(0), config)
            if parsed is not None:
                return parsed
    return None


def _parse_datetime(value: str, config: AppConfig) -> datetime | None:
    text = _normalize_whitespace(value)
    if not text:
        return None
    text = text.replace("작성일", "").strip()
    if not any(separator in text for separator in ("-", ".", "/")):
        return None
    match = re.fullmatch(r"(\d{2})[./](\d{2})[./](\d{2})(?:\s+(\d{2}:\d{2}))?", text)
    if match:
        year, month, day, time_part = match.groups()
        text = f"20{year}-{month}-{day}"
        if time_part:
            text += f" {time_part}"
    text = text.replace(".", "-").replace("/", "-")
    try:
        parsed = date_parser.parse(text, fuzzy=True)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=config.timezone)
    return parsed.astimezone(UTC)


def _resolved_published_at(
    list_published_at: datetime,
    detail_published_at: datetime | None,
    *,
    cutoff: datetime,
    now: datetime,
) -> datetime:
    if detail_published_at is None:
        return list_published_at
    if detail_published_at < cutoff:
        return list_published_at
    if detail_published_at > now:
        return list_published_at
    return detail_published_at


def _node_text(node: BeautifulSoup) -> str:
    text = node.get_text(" ", strip=True)
    return _crop_body(text)


def _crop_body(text: str, *, max_chars: int = 3000) -> str:
    cleaned = _normalize_whitespace(text)
    if len(cleaned) <= max_chars:
        return cleaned
    return cleaned[:max_chars].rstrip() + "..."


def _first_sentences(text: str, count: int = 2, max_chars: int = 240) -> str:
    cleaned = _normalize_whitespace(text)
    if not cleaned:
        return ""
    parts = re.split(r"(?<=[.!?])\s+|(?<=다\.)\s+", cleaned)
    summary = " ".join(part.strip() for part in parts[:count] if part.strip())
    summary = _normalize_whitespace(summary or cleaned)
    if len(summary) <= max_chars:
        return summary
    return summary[: max_chars - 1].rstrip() + "…"


def _clean_html(value: object) -> str:
    if value is None:
        return ""
    text = re.sub(r"<[^>]+>", " ", str(value))
    return _normalize_whitespace(html.unescape(text))


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _extract_domain(url: str) -> str:
    host = urlparse(url).netloc.lower().strip()
    if host.startswith("www."):
        host = host[4:]
    return host


def _fingerprint(title: str, url: str, source_key: str) -> str:
    normalized_title = re.sub(r"\s+", " ", title.lower()).strip()
    normalized_title = re.sub(r"[\"'`“”‘’]", "", normalized_title)
    payload = f"{source_key}|{normalized_title}|{url.strip().lower()}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _get(url: str, config: AppConfig, **kwargs: object) -> requests.Response:
    last_error: Exception | None = None
    candidate_urls = _candidate_urls(url)
    attempts_per_candidate = 1 if len(candidate_urls) > 1 else 3
    for candidate_url in candidate_urls:
        for attempt in range(attempts_per_candidate):
            try:
                response = requests.get(
                    candidate_url,
                    headers=REQUEST_HEADERS,
                    timeout=_request_timeout_seconds(candidate_url, config),
                    **kwargs,
                )
                response.raise_for_status()
                response.encoding = response.apparent_encoding or response.encoding
                content_type = (response.headers.get("content-type") or "").lower()
                if "text/html" in content_type:
                    lowered = response.text[:12000].lower()
                    if any(marker in lowered for marker in BLOCKED_MARKERS):
                        raise ValueError(f"Blocked or challenge page detected for {candidate_url}")
                return response
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
                last_error = exc
                if attempt == attempts_per_candidate - 1:
                    break
                time_module.sleep(1.5 * (attempt + 1))
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Failed to fetch {url}")


def _get_soup(url: str, config: AppConfig) -> BeautifulSoup:
    response = _get(url, config)
    return BeautifulSoup(response.text[:700000], "html.parser")


def _candidate_urls(url: str) -> list[str]:
    parsed = urlparse(url)
    candidates = [url]
    if parsed.netloc not in {"www.odmusical.com", "odmusical.com"}:
        return candidates

    path_variants = [parsed.path]
    if parsed.path.startswith("/kor/"):
        path_variants.append(parsed.path[4:] or "/")
    else:
        path_variants.append("/kor" + parsed.path)

    for path in path_variants:
        normalized_path = path or "/"
        for host in ("www.odmusical.com", "odmusical.com"):
            candidate = parsed._replace(netloc=host, scheme="https", path=normalized_path).geturl()
            if candidate not in candidates:
                candidates.append(candidate)
    return candidates


def _request_timeout_seconds(url: str, config: AppConfig) -> int:
    host = urlparse(url).netloc.lower().strip()
    if host in {"www.odmusical.com", "odmusical.com"}:
        return max(config.request_timeout_seconds, 40)
    return config.request_timeout_seconds
