from __future__ import annotations

import html
import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from performing_arts_monitor.config import AppConfig
from performing_arts_monitor.gemini import GeminiTriage
from performing_arts_monitor.models import (
    CATEGORY_LABELS,
    CATEGORY_ORDER,
    CollectedItem,
    DigestRun,
    DigestSection,
    TriagedItem,
)
from performing_arts_monitor.news_brief import collect_keyword_news
from performing_arts_monitor.sources import collect_items
from performing_arts_monitor.telegram import send_digest

SUPPORT_TERMS = (
    "지원사업",
    "창작지원",
    "예술인지원",
    "청년예술지원사업",
    "공모",
    "공모전",
    "선정작",
    "레지던시",
    "펀드",
    "grant",
)
AUDITION_TERMS = (
    "오디션",
    "지원서",
    "지원자",
    "모집",
    "서류",
    "합격자",
    "최종 결과",
    "1차 합격",
    "2차 합격",
    "2차 실기",
)
WORKS_TERMS = (
    "캐스팅",
    "출연",
    "합류",
    "복귀",
    "개막",
    "초연",
    "재연",
    "라인업",
    "티켓 오픈",
    "티켓오픈",
    "공연",
    "상영",
    "웨스트엔드",
    "브로드웨이",
)
PEOPLE_TERMS = (
    "배우",
    "프로듀서",
    "연출",
    "작곡",
    "작사가",
    "인터뷰",
    "수상",
    "복귀",
    "주연",
)
COMPANY_TERMS = (
    "채용",
    "인턴",
    "회사",
    "공지",
    "런칭",
    "멤버십",
    "파트너십",
    "업무",
    "시즌",
)
LOW_SIGNAL_TERMS = (
    "예매마감",
    "티켓오픈",
    "티켓 오픈",
    "이벤트",
    "굿즈",
    "md",
    "포토",
    "영상 공개",
    "mv",
    "membership",
)
STATUS_TERMS = (
    "종료",
    "완료",
    "마감",
    "마감안내",
    "closed",
)
ORGANIZATION_TERMS = (
    "emk",
    "오디컴퍼니",
    "쇼노트",
    "에스앤코",
    "신시컴퍼니",
    "cj enm",
    "acom",
    "국립정동극장",
)
GENERIC_WORK_TITLES = {
    "공연정보",
    "할인정보",
    "공연소개",
    "캐스팅",
    "시놉시스",
    "공지",
}
SOFT_SOURCE_ERROR_KEYS = {
    "od_notice",
    "od_news",
}


def run_pipeline(config: AppConfig) -> Path:
    now_utc = datetime.now(tz=UTC)
    start_utc = config.collection_window_start(now_utc)
    run_dir = config.output_dir / now_utc.strftime("%Y%m%d-%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    raw_items, source_errors = collect_items(config=config, start_utc=start_utc, now=now_utc)
    triaged_items = _triage_items(config, raw_items, now_utc)
    selected_items = _select_items(config, triaged_items)
    sections = _build_sections(selected_items)

    news_raw_items: list[CollectedItem] = []
    try:
        news_raw_items = collect_keyword_news(config=config, start_utc=start_utc, now=now_utc)
    except Exception as exc:  # pragma: no cover - resilience path
        source_errors["keyword_news"] = str(exc)
    news_triaged_items = _triage_items(config, news_raw_items, now_utc)
    news_selected_items = _select_news_items(config, news_triaged_items)

    digest = _build_digest(config, sections, news_selected_items, now_utc, source_errors)
    message_digest = _render_message_digest(digest, config.timezone, source_errors)

    _write_json(
        run_dir / "raw_items.json",
        {"generated_at": now_utc.isoformat(), "items": [item.to_dict() for item in raw_items]},
    )
    _write_json(
        run_dir / "triaged_items.json",
        {"generated_at": now_utc.isoformat(), "items": [item.to_dict() for item in triaged_items]},
    )
    _write_json(
        run_dir / "selected_items.json",
        {"generated_at": now_utc.isoformat(), "items": [item.to_dict() for item in selected_items]},
    )
    _write_json(
        run_dir / "news_raw_items.json",
        {"generated_at": now_utc.isoformat(), "items": [item.to_dict() for item in news_raw_items]},
    )
    _write_json(
        run_dir / "news_triaged_items.json",
        {"generated_at": now_utc.isoformat(), "items": [item.to_dict() for item in news_triaged_items]},
    )
    _write_json(
        run_dir / "news_selected_items.json",
        {"generated_at": now_utc.isoformat(), "items": [item.to_dict() for item in news_selected_items]},
    )
    _write_json(run_dir / "digest.json", digest.to_dict())
    (run_dir / "message_digest.md").write_text(message_digest, encoding="utf-8")

    telegram_metadata: dict[str, Any] = {"sent": False}
    if config.telegram_enabled:
        try:
            public_links = _public_links(config, run_dir)
            telegram_metadata = send_digest(
                config=config,
                digest_markdown=message_digest,
                public_links=public_links,
            )
        except Exception as exc:  # pragma: no cover - resilience path
            telegram_metadata = {"sent": False, "error": str(exc)}

    summary = _render_summary(
        config=config,
        start_utc=start_utc,
        end_utc=now_utc,
        raw_items=raw_items,
        triaged_items=triaged_items,
        selected_items=selected_items,
        news_raw_items=news_raw_items,
        news_triaged_items=news_triaged_items,
        news_selected_items=news_selected_items,
        sections=sections,
        source_errors=source_errors,
        telegram_metadata=telegram_metadata,
        run_dir=run_dir,
    )
    (run_dir / "summary.md").write_text(summary, encoding="utf-8")

    _write_run_archive_page(run_dir, digest, config.timezone)
    _write_archive_index(config.output_dir, config.archive_limit)
    _write_json(
        run_dir / "run_metadata.json",
        {
            "generated_at": now_utc.isoformat(),
            "start_utc": start_utc.isoformat(),
            "end_utc": now_utc.isoformat(),
            "llm_enabled": config.llm_enabled,
            "telegram": telegram_metadata,
            "source_errors": source_errors,
            "raw_item_count": len(raw_items),
            "triaged_item_count": len(triaged_items),
            "selected_item_count": len(selected_items),
            "news_raw_item_count": len(news_raw_items),
            "news_triaged_item_count": len(news_triaged_items),
            "news_selected_item_count": len(news_selected_items),
        },
    )
    return run_dir


def _triage_items(
    config: AppConfig,
    items: list[CollectedItem],
    now_utc: datetime,
) -> list[TriagedItem]:
    llm_payloads: dict[str, dict[str, Any]] = {}
    if config.llm_enabled and items:
        try:
            llm_payloads = GeminiTriage(config).triage_items(items)
        except Exception:
            llm_payloads = {}

    triaged: list[TriagedItem] = []
    for item in items:
        local = _local_assessment(item, config, now_utc)
        llm = llm_payloads.get(item.fingerprint)
        merged = _merge_assessment(item, local, llm)
        triaged.append(merged)

    grouped: dict[str, list[TriagedItem]] = {}
    for item in triaged:
        if not item.keep:
            continue
        grouped.setdefault(item.duplicate_key, []).append(item)

    deduped: list[TriagedItem] = []
    for cluster in grouped.values():
        representative = max(
            cluster,
            key=lambda item: (
                item.final_score,
                item.source_weight,
                len(item.body_text),
                item.published_at,
            ),
        )
        representative.duplicate_group_size = len(cluster)
        deduped.append(representative)

    rejected = [item for item in triaged if not item.keep]
    all_items = sorted(
        deduped + rejected,
        key=lambda item: (item.keep, item.final_score, item.published_at),
        reverse=True,
    )
    return all_items


def _merge_assessment(
    item: CollectedItem,
    local: dict[str, Any],
    llm: dict[str, Any] | None,
) -> TriagedItem:
    llm_keep = bool(llm.get("keep")) if llm else local["keep"]
    llm_relevant = bool(llm.get("relevant")) if llm else local["keep"]
    category = str(llm.get("category") or local["category"]) if llm else local["category"]
    if category not in CATEGORY_LABELS:
        category = local["category"]
    canonical_url = _canonical_url_for_item(item)
    llm_score = float(llm.get("importance") or 0.0) if llm else 0.0
    final_score = local["local_score"]
    local_news_override = _allow_local_news_override(item, local, llm)
    if llm:
        final_score = round((local["local_score"] * 0.6) + (llm_score * 0.4), 1)
    if local_news_override:
        final_score = max(final_score, local["local_score"])
    keep = bool(final_score >= local["candidate_threshold"] and ((llm_keep and llm_relevant) or local_news_override))
    secondary_tags = (
        [str(tag).strip() for tag in llm.get("secondary_tags", []) if str(tag).strip()]
        if llm
        else local["secondary_tags"]
    )
    mentioned_people = (
        [str(value).strip() for value in llm.get("mentioned_people", []) if str(value).strip()]
        if llm
        else local["mentioned_people"]
    )
    if not mentioned_people:
        mentioned_people = local["mentioned_people"]
    mentioned_works = (
        [str(value).strip() for value in llm.get("mentioned_works", []) if str(value).strip()]
        if llm
        else local["mentioned_works"]
    )
    if not mentioned_works:
        mentioned_works = local["mentioned_works"]
    mentioned_orgs = (
        [str(value).strip() for value in llm.get("mentioned_organizations", []) if str(value).strip()]
        if llm
        else local["mentioned_organizations"]
    )
    if not mentioned_orgs:
        mentioned_orgs = local["mentioned_organizations"]
    duplicate_key = str(llm.get("duplicate_key") or local["duplicate_key"]) if llm else local["duplicate_key"]
    if not duplicate_key:
        duplicate_key = local["duplicate_key"]

    one_line_summary = str(llm.get("one_line_summary") or "").strip() if llm else ""
    if not one_line_summary:
        one_line_summary = local["one_line_summary"]
    watch_point = str(llm.get("watch_point") or "").strip() if llm else ""
    if not watch_point:
        watch_point = local["watch_point"]

    exclude_reason = ""
    if not keep:
        exclude_reason = str(llm.get("exclude_reason") or "").strip() if llm else ""
        if not exclude_reason:
            exclude_reason = local["exclude_reason"]

    return TriagedItem(
        source_key=item.source_key,
        source_label=item.source_label,
        site_name=item.site_name,
        source_kind=item.source_kind,
        title=item.title,
        url=item.url,
        canonical_url=canonical_url,
        published_at=item.published_at,
        summary=item.summary,
        body_text=item.body_text,
        attachments=item.attachments,
        external_urls=item.external_urls,
        source_weight=item.source_weight,
        fingerprint=item.fingerprint,
        category=category,
        secondary_tags=secondary_tags,
        keep=keep,
        exclude_reason=exclude_reason,
        duplicate_key=duplicate_key,
        one_line_summary=one_line_summary,
        watch_point=watch_point,
        local_score=local["local_score"],
        llm_score=llm_score,
        final_score=final_score,
        relevance_confidence=float(llm.get("relevance_confidence") or local["relevance_confidence"]) if llm else local["relevance_confidence"],
        mentioned_people=mentioned_people,
        mentioned_works=mentioned_works,
        mentioned_organizations=mentioned_orgs,
    )


def _allow_local_news_override(
    item: CollectedItem,
    local: dict[str, Any],
    llm: dict[str, Any] | None,
) -> bool:
    if llm is None or item.source_kind != "news_search":
        return False
    if local["keep"] is not True:
        return False
    if local["local_score"] < max(55.0, local["score_threshold"]):
        return False
    if len(_normalize_whitespace(" ".join(value for value in (item.summary, item.body_text) if value))) < 80:
        return False
    if bool(llm.get("keep")) and bool(llm.get("relevant")):
        return False
    return True


def _local_assessment(
    item: CollectedItem,
    config: AppConfig,
    now_utc: datetime,
) -> dict[str, Any]:
    headline_text = " ".join(value for value in (item.title, item.summary) if value)
    text = _combined_text(item)
    headline_people = _find_mentions(headline_text, config.tracked_people)
    body_people = _find_mentions(item.body_text, config.tracked_people)
    matched_people = list(dict.fromkeys([*headline_people, *body_people]))
    headline_keywords = _find_mentions(headline_text, config.tracked_keywords)
    body_keywords = _find_mentions(item.body_text, config.tracked_keywords)
    matched_keywords = list(dict.fromkeys([*headline_keywords, *body_keywords]))
    mentioned_works = _extract_work_titles(text)
    mentioned_orgs = _extract_organizations(text, item.site_name)
    secondary_tags = _secondary_tags(headline_text)
    category = _heuristic_category(
        item=item,
        headline_text=headline_text,
        full_text=text,
        headline_people=headline_people,
        matched_people=matched_people,
    )
    watch_point = _watch_point_from_tags(secondary_tags)
    one_line_summary = _heuristic_summary(item)

    age_hours = max((now_utc - item.published_at).total_seconds() / 3600.0, 0.0)
    recency_score = max(0.0, 15.0 - (age_hours * 0.45))
    actionability = 0.0
    if category in {"audition", "support"}:
        actionability += 10.0
        if item.source_kind.startswith("official_"):
            actionability += 3.0
        if "마감임박" in secondary_tags:
            actionability += 3.0
        if item.attachments:
            actionability += 2.5
        if item.external_urls:
            actionability += 2.5
        if any(term in text.lower() for term in ("지원서", "지원 방법", "첨부파일", "forms.gle", "구글폼")):
            actionability += 2.0
    elif "채용" in secondary_tags or category == "company_news":
        actionability += 7.0
    elif category == "works_casting":
        actionability += 6.0

    impact = 0.0
    if headline_people:
        impact += min(len(headline_people) * 5.0, 10.0)
    elif matched_people:
        impact += min(len(matched_people) * 1.5, 4.5)
    if matched_people and item.source_kind in {"official_news", "news_search"}:
        impact += 3.0
    if mentioned_works:
        impact += min(len(mentioned_works) * 2.0, 6.0)
    if any(term in text.lower() for term in ("브로드웨이", "웨스트엔드", "국립정동극장", "초연", "30주년", "월드")):
        impact += 4.0

    keyword_boost = min((len(headline_keywords) * 2.0) + (max(0, len(matched_keywords) - len(headline_keywords)) * 1.0), 6.0)
    if item.source_kind == "news_search" and matched_keywords:
        keyword_boost = min(keyword_boost + 2.0, 8.0)
    content_richness = min(len(item.body_text) / 700.0, 1.0) * 4.0
    headline_low_signal_hits = _count_hits(headline_text, LOW_SIGNAL_TERMS)
    body_low_signal_hits = max(0, _count_hits(text, LOW_SIGNAL_TERMS) - headline_low_signal_hits)
    low_signal_penalty = min((headline_low_signal_hits * 8.0) + (body_low_signal_hits * 2.0), 18.0)
    if "티켓오픈" in secondary_tags:
        low_signal_penalty = min(24.0, low_signal_penalty + (2.0 if headline_people else 6.0))
    status_penalty = min(_count_hits(headline_text, STATUS_TERMS) * 3.0, 9.0)
    if "합격발표" in secondary_tags:
        status_penalty = max(0.0, status_penalty - 4.0)

    local_score = round(
        max(
            0.0,
            min(
                100.0,
                22.0
                + item.source_weight
                + recency_score
                + actionability
                + impact
                + keyword_boost
                + content_richness
                - low_signal_penalty
                - status_penalty,
            ),
        ),
        1,
    )

    candidate_threshold = max(40.0, config.score_threshold - 10.0)
    keep = bool(category in CATEGORY_LABELS and local_score >= candidate_threshold)
    exclude_reason = ""
    if category == "company_news" and not (headline_keywords or headline_people or mentioned_works):
        keep = False
        exclude_reason = "산업 관련성이 약함"
    elif "티켓오픈" in secondary_tags and local_score < config.score_threshold and not headline_people:
        keep = False
        exclude_reason = "티켓 오픈 단순 공지"
    elif not keep:
        exclude_reason = "로컬 기준 점수 미달"

    relevance_confidence = round(min(1.0, 0.35 + (local_score / 100.0) * 0.65), 2)
    return {
        "category": category,
        "secondary_tags": secondary_tags,
        "mentioned_people": matched_people,
        "mentioned_works": mentioned_works,
        "mentioned_organizations": mentioned_orgs,
        "watch_point": watch_point,
        "one_line_summary": one_line_summary,
        "local_score": local_score,
        "score_threshold": config.score_threshold,
        "candidate_threshold": candidate_threshold,
        "keep": keep,
        "exclude_reason": exclude_reason,
        "relevance_confidence": relevance_confidence,
        "duplicate_key": _duplicate_key_fallback(item.title, category, matched_people, mentioned_works, secondary_tags),
    }


def _select_items(config: AppConfig, triaged_items: list[TriagedItem]) -> list[TriagedItem]:
    strict_candidates = [
        item
        for item in triaged_items
        if item.keep and item.final_score >= config.score_threshold and item.category in CATEGORY_LABELS
    ]
    strict_candidates.sort(key=lambda item: (item.final_score, item.published_at), reverse=True)

    by_category = {key: [] for key in CATEGORY_ORDER}
    for item in strict_candidates:
        by_category.setdefault(item.category, []).append(item)

    selected: list[TriagedItem] = []
    selected_ids: set[str] = set()

    for category in CATEGORY_ORDER:
        category_items = by_category.get(category, [])
        if not category_items:
            continue
        item = category_items[0]
        selected.append(item)
        selected_ids.add(item.fingerprint)
        if len(selected) >= config.max_total_items:
            return selected

    for item in strict_candidates:
        if item.fingerprint in selected_ids:
            continue
        category_items = [selected_item for selected_item in selected if selected_item.category == item.category]
        if len(category_items) >= config.max_items_per_category:
            continue
        selected.append(item)
        selected_ids.add(item.fingerprint)
        if len(selected) >= config.max_total_items:
            break

    if selected:
        return sorted(selected, key=lambda item: (CATEGORY_ORDER.index(item.category), -item.final_score))

    fallback_threshold = max(45.0, config.score_threshold - 8.0)
    fallback_candidates = [
        item
        for item in triaged_items
        if item.keep
        and item.final_score >= fallback_threshold
        and item.category in {"audition", "support", "people", "works_casting"}
        and ("티켓오픈" not in item.secondary_tags or bool(item.mentioned_people))
    ]
    fallback_candidates.sort(
        key=lambda item: (
            item.category in {"audition", "support"},
            bool(item.external_urls or item.attachments),
            bool(item.mentioned_people),
            item.final_score,
            item.published_at,
        ),
        reverse=True,
    )

    fallback_selected: list[TriagedItem] = []
    category_counts: dict[str, int] = {}
    for item in fallback_candidates:
        category_count = category_counts.get(item.category, 0)
        if category_count >= min(2, config.max_items_per_category):
            continue
        if item.watch_point:
            if "원문 확인" not in item.watch_point:
                item.watch_point = f"{item.watch_point}; 원문 확인 권장"
        else:
            item.watch_point = "원문 확인 권장"
        fallback_selected.append(item)
        category_counts[item.category] = category_count + 1
        if len(fallback_selected) >= min(3, config.max_total_items):
            break

    return sorted(fallback_selected, key=lambda item: (CATEGORY_ORDER.index(item.category), -item.final_score))


def _select_news_items(config: AppConfig, triaged_items: list[TriagedItem]) -> list[TriagedItem]:
    strict_candidates = [
        item
        for item in triaged_items
        if item.keep
        and item.final_score >= config.news_score_threshold
        and item.category in {"audition", "support", "works_casting", "people", "company_news"}
        and ("티켓오픈" not in item.secondary_tags or bool(item.mentioned_people))
    ]
    strict_candidates.sort(
        key=lambda item: (
            bool(item.mentioned_people),
            item.category in {"people", "works_casting", "support"},
            item.final_score,
            item.published_at,
        ),
        reverse=True,
    )

    selected: list[TriagedItem] = []
    category_counts: dict[str, int] = {}
    for item in strict_candidates:
        category_count = category_counts.get(item.category, 0)
        if category_count >= 2:
            continue
        selected.append(item)
        category_counts[item.category] = category_count + 1
        if len(selected) >= config.max_news_items:
            break
    if selected:
        return selected

    fallback_threshold = max(44.0, config.news_score_threshold - 4.0)
    fallback_candidates = [
        item
        for item in triaged_items
        if item.keep
        and item.final_score >= fallback_threshold
        and item.category in {"support", "works_casting", "people"}
    ]
    fallback_candidates.sort(key=lambda item: (bool(item.mentioned_people), item.final_score, item.published_at), reverse=True)
    return fallback_candidates[: config.max_news_items]


def _build_sections(items: list[TriagedItem]) -> list[DigestSection]:
    sections: list[DigestSection] = []
    for category in CATEGORY_ORDER:
        category_items = [item for item in items if item.category == category]
        if not category_items:
            continue
        category_items.sort(key=lambda item: (item.final_score, item.published_at), reverse=True)
        sections.append(
            DigestSection(
                key=category,
                label=CATEGORY_LABELS[category],
                items=category_items,
            )
        )
    return sections


def _build_digest(
    config: AppConfig,
    sections: list[DigestSection],
    news_items: list[TriagedItem],
    now_utc: datetime,
    source_errors: dict[str, str],
) -> DigestRun:
    local_now = now_utc.astimezone(config.timezone)
    title = f"{local_now.strftime('%Y-%m-%d')} 한국 뮤지컬/공연예술 모니터"
    intro = _build_intro(sections, news_items, source_errors)
    return DigestRun(
        title=title,
        intro=intro,
        sections=sections,
        news_items=news_items,
        generated_at=now_utc,
    )


def _build_intro(
    sections: list[DigestSection],
    news_items: list[TriagedItem],
    source_errors: dict[str, str],
) -> str:
    visible_source_errors = _visible_source_errors(
        source_errors,
        has_selected_content=bool(sections or news_items),
    )
    monitor_total = sum(len(section.items) for section in sections)
    news_total = len(news_items)
    if monitor_total == 0 and news_total == 0:
        if visible_source_errors:
            return "오늘은 선별 항목이 없었고 일부 소스 수집에 실패했습니다."
        return "오늘은 기준 점수를 넘는 공식 공지나 업계 동향이 많지 않았습니다."

    parts: list[str] = []
    if monitor_total:
        monitor_parts = [f"{section.label} {len(section.items)}건" for section in sections]
        parts.append(f"공식 모니터에서 {', '.join(monitor_parts)}")
    if news_total:
        parts.append(f"키워드 뉴스 {news_total}건")
    joined = parts[0] if len(parts) == 1 else f"{parts[0]}과 {parts[1]}"
    if visible_source_errors:
        return f"오늘은 {joined}을 추렸고 일부 소스 수집 오류가 있었습니다."
    return f"오늘은 {joined}을 추렸습니다."


def _render_message_digest(
    digest: DigestRun,
    timezone: ZoneInfo,
    source_errors: dict[str, str],
) -> str:
    visible_source_errors = _visible_source_errors(
        source_errors,
        has_selected_content=bool(digest.sections or digest.news_items),
    )
    lines = [f"# {digest.title}", "", digest.intro]
    lines.append("")
    lines.append("## 챕터 1. 공식 모니터")
    if not digest.sections:
        lines.append("- 선별 항목 없음")
    for section in digest.sections:
        lines.append("")
        lines.append(f"### {section.label}")
        for item in section.items:
            lines.append(f"- **{item.title}**")
            lines.append(f"  요약: {item.one_line_summary}")
            if item.watch_point:
                lines.append(f"  체크: {item.watch_point}")
            lines.append(
                f"  출처: {item.site_name} | {item.published_at.astimezone(timezone).strftime('%Y-%m-%d')}"
            )
            lines.append(f"  링크: {item.canonical_url}")
            lines.append("")
    if digest.news_items:
        lines.append("")
        lines.append("## 챕터 2. 키워드 뉴스 브리프")
        for item in digest.news_items:
            lines.append(f"- **{item.title}**")
            lines.append(f"  요약: {item.one_line_summary}")
            if item.watch_point:
                lines.append(f"  체크: {item.watch_point}")
            lines.append(
                f"  출처: {item.site_name} | {item.published_at.astimezone(timezone).strftime('%Y-%m-%d')}"
            )
            lines.append(f"  링크: {item.canonical_url}")
            lines.append("")
    if visible_source_errors:
        lines.append("")
        lines.append("## 수집 상태")
        for key in sorted(visible_source_errors):
            lines.append(f"- {key}: 수집 실패")
    return "\n".join(lines).strip() + "\n"


def _render_summary(
    *,
    config: AppConfig,
    start_utc: datetime,
    end_utc: datetime,
    raw_items: list[CollectedItem],
    triaged_items: list[TriagedItem],
    selected_items: list[TriagedItem],
    news_raw_items: list[CollectedItem],
    news_triaged_items: list[TriagedItem],
    news_selected_items: list[TriagedItem],
    sections: list[DigestSection],
    source_errors: dict[str, str],
    telegram_metadata: dict[str, Any],
    run_dir: Path,
) -> str:
    lines = [
        f"# {end_utc.astimezone(config.timezone).strftime('%Y-%m-%d')} 한국 뮤지컬/공연예술 모니터",
        "",
        f"- 실행 디렉터리: `{run_dir}`",
        f"- 시간 범위(UTC): `{start_utc.isoformat()}` ~ `{end_utc.isoformat()}`",
        f"- LLM 사용: `{config.llm_enabled}`",
        f"- 텔레그램 전송: `{telegram_metadata.get('sent', False)}`",
        f"- 챕터 1 원시 아이템: `{len(raw_items)}`",
        f"- 챕터 1 분류 아이템: `{len(triaged_items)}`",
        f"- 챕터 1 선별 아이템: `{len(selected_items)}`",
        f"- 챕터 2 뉴스 원시 아이템: `{len(news_raw_items)}`",
        f"- 챕터 2 뉴스 분류 아이템: `{len(news_triaged_items)}`",
        f"- 챕터 2 뉴스 선별 아이템: `{len(news_selected_items)}`",
        "",
        "## 챕터 1 현황",
    ]
    for section in sections:
        lines.append(f"- {section.label}: {len(section.items)}건")
    if not sections:
        lines.append("- 선별된 항목 없음")

    lines.append("")
    lines.append("## 챕터 2 현황")
    if news_selected_items:
        for item in news_selected_items:
            lines.append(f"- {item.category_label}: {item.title}")
    else:
        lines.append("- 선별된 뉴스 없음")

    lines.append("")
    lines.append("## 수집 오류")
    if source_errors:
        for key, message in source_errors.items():
            lines.append(f"- {key}: {message}")
    else:
        lines.append("- 없음")

    lines.append("")
    lines.append("## 산출물")
    for filename in (
        "raw_items.json",
        "triaged_items.json",
        "selected_items.json",
        "news_raw_items.json",
        "news_triaged_items.json",
        "news_selected_items.json",
        "digest.json",
        "message_digest.md",
        "summary.md",
        "index.html",
        "run_metadata.json",
    ):
        lines.append(f"- `{filename}`")
    return "\n".join(lines).strip() + "\n"


def _combined_text(item: CollectedItem) -> str:
    return " ".join(
        value
        for value in (
            item.title,
            item.summary,
            item.body_text,
            " ".join(item.attachments),
            " ".join(item.external_urls),
        )
        if value
    )


def _find_mentions(text: str, values: tuple[str, ...]) -> list[str]:
    lowered = text.lower()
    matches = [value for value in values if value.lower() in lowered]
    return list(dict.fromkeys(matches))


def _extract_work_titles(text: str) -> list[str]:
    titles = re.findall(r"<([^<>]{1,40})>", text)
    cleaned = [_normalize_whitespace(title) for title in titles if title.strip()]
    filtered = [
        title
        for title in cleaned
        if len(title) >= 2 and title not in GENERIC_WORK_TITLES and not title.endswith("정보")
    ]
    return list(dict.fromkeys(filtered))[:6]


def _extract_organizations(text: str, site_name: str) -> list[str]:
    lowered = text.lower()
    matches = [value for value in ORGANIZATION_TERMS if value.lower() in lowered]
    canonical = [site_name]
    canonical.extend(matches)
    return list(dict.fromkeys(_normalize_whitespace(value) for value in canonical if value))


def _secondary_tags(text: str) -> list[str]:
    tags: list[str] = []
    lowered = text.lower()
    rules = (
        ("마감", "마감임박"),
        ("합격", "합격발표"),
        ("티켓오픈", "티켓오픈"),
        ("티켓 오픈", "티켓오픈"),
        ("캐스팅변경", "캐스팅변경"),
        ("채용", "채용"),
        ("개막", "공연개막"),
        ("인터뷰", "인터뷰"),
        ("수상", "수상"),
    )
    for needle, tag in rules:
        if needle in lowered:
            tags.append(tag)
    return list(dict.fromkeys(tags))


def _watch_point_from_tags(tags: list[str]) -> str:
    if "마감임박" in tags:
        return "마감 일정과 제출 요건 확인 필요"
    if "합격발표" in tags:
        return "후속 일정과 개별 안내 여부 확인 필요"
    if "채용" in tags:
        return "지원 자격과 마감 시점 확인 필요"
    if "캐스팅변경" in tags:
        return "공연 일정과 변경 배역 확인 필요"
    if "티켓오픈" in tags:
        return "오픈 일정과 캐스팅 공지 함께 확인 필요"
    if "공연개막" in tags:
        return "개막 일정과 주요 캐스팅 확인 필요"
    return ""


def _heuristic_category(
    *,
    item: CollectedItem,
    headline_text: str,
    full_text: str,
    headline_people: list[str],
    matched_people: list[str],
) -> str:
    headline_lower = headline_text.lower()
    full_lower = full_text.lower()
    headline_has_support = any(term.lower() in headline_lower for term in SUPPORT_TERMS)
    headline_has_audition = any(term.lower() in headline_lower for term in AUDITION_TERMS)
    headline_has_works = any(term.lower() in headline_lower for term in WORKS_TERMS)
    headline_has_people_terms = any(term.lower() in headline_lower for term in PEOPLE_TERMS)
    full_has_support = any(term.lower() in full_lower for term in SUPPORT_TERMS)
    full_has_audition = any(term.lower() in full_lower for term in AUDITION_TERMS)
    full_has_works = any(term.lower() in full_lower for term in WORKS_TERMS)
    full_has_people_terms = any(term.lower() in full_lower for term in PEOPLE_TERMS)

    if headline_has_support:
        return "support"
    if headline_has_works and not headline_has_audition:
        return "works_casting"
    if item.source_kind == "official_audition" and (headline_has_audition or full_has_audition):
        return "audition"
    if headline_has_audition:
        return "audition"
    if headline_people and headline_has_people_terms:
        return "people"
    if headline_has_works or full_has_works:
        return "works_casting"
    if full_has_support:
        return "support"
    if matched_people and (headline_has_people_terms or full_has_people_terms):
        return "people"
    if full_has_audition and item.source_kind in {"official_audition", "community_board"}:
        return "audition"
    if matched_people:
        return "people"
    return "company_news"


def _heuristic_summary(item: CollectedItem) -> str:
    title = _normalize_whitespace(item.title)
    compact_title = title.replace(" ", "")
    title_work_titles = _extract_work_titles(title)
    if "티켓오픈" in compact_title:
        if title_work_titles:
            return f"{title_work_titles[0]} 티켓 오픈 일정과 공연 기본 정보를 담은 공식 공지입니다."
        return "티켓 오픈 일정과 공연 기본 정보를 담은 공식 공지입니다."
    if "오디션" in compact_title:
        if title_work_titles:
            return f"{title_work_titles[0]} 관련 지원 일정과 제출 요건을 담은 공식 공고입니다."
        return "지원 일정과 제출 요건을 담은 공식 공고입니다."
    candidates = [item.summary, item.body_text]
    for candidate in candidates:
        cleaned = _normalize_whitespace(candidate)
        cleaned = re.sub(r"^[※•*\-]+\s*", "", cleaned)
        if cleaned.startswith("제목 ") or ("카테고리" in cleaned and "작성자" in cleaned):
            if any(term in compact_title for term in ("모집", "구인", "채용")):
                return f"{title} 관련 지원 정보를 담은 모집 공고입니다."
            return _first_sentence(title)
        if len(cleaned) >= 24 and cleaned.count(" - ") < 3 and cleaned.count("|") < 3:
            return _first_sentence(cleaned)
    return _first_sentence(item.title)


def _duplicate_key_fallback(
    title: str,
    category: str,
    mentioned_people: list[str],
    mentioned_works: list[str],
    secondary_tags: list[str],
) -> str:
    parts = [category]
    if mentioned_works:
        parts.append(mentioned_works[0].lower())
    if mentioned_people:
        parts.append(mentioned_people[0].lower())
    title_key = re.sub(r"[^0-9a-z가-힣]+", "-", title.lower()).strip("-")
    if title_key and not (mentioned_works or mentioned_people):
        parts.append(title_key[:64])
    if secondary_tags:
        parts.append(secondary_tags[0].lower())
    if len(parts) == 1:
        parts.append(title_key[:64])
    return "|".join(parts)


def _canonical_url_for_item(item: CollectedItem) -> str:
    if item.source_kind == "community_board" and item.external_urls:
        return item.external_urls[0]
    return item.url


def _count_hits(text: str, terms: tuple[str, ...]) -> int:
    lowered = text.lower()
    return sum(1 for term in terms if term.lower() in lowered)


def _first_sentence(text: str) -> str:
    cleaned = _normalize_whitespace(text)
    match = re.match(r"(.+?[.!?])(?:\s|$)", cleaned)
    if match:
        return match.group(1)
    if len(cleaned) <= 220:
        return cleaned
    return cleaned[:219].rstrip() + "…"


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _visible_source_errors(
    source_errors: dict[str, str],
    *,
    has_selected_content: bool,
) -> dict[str, str]:
    if not has_selected_content:
        return source_errors
    return {
        key: message
        for key, message in source_errors.items()
        if key not in SOFT_SOURCE_ERROR_KEYS
    }


def _public_links(config: AppConfig, run_dir: Path) -> dict[str, str] | None:
    base_url = (config.public_archive_base_url or "").strip()
    if not base_url:
        return None

    base = base_url.rstrip("/") + "/"
    run_prefix = f"{run_dir.name}/"
    return {
        "archive": urljoin(base, f"{run_prefix}index.html"),
        "summary": urljoin(base, f"{run_prefix}summary.md"),
        "digest": urljoin(base, f"{run_prefix}message_digest.md"),
    }


def _write_run_archive_page(run_dir: Path, digest: DigestRun, timezone: ZoneInfo) -> None:
    sections: list[str] = [
        "<!doctype html>",
        "<html lang='ko'>",
        "<head>",
        "<meta charset='utf-8'>",
        f"<title>{html.escape(digest.title)}</title>",
        "<meta name='viewport' content='width=device-width, initial-scale=1'>",
        "<style>",
        "body{font-family:Segoe UI,Apple SD Gothic Neo,sans-serif;max-width:960px;margin:40px auto;padding:0 20px;line-height:1.6;background:#f7f8fb;color:#101828;}",
        "main{background:#fff;border:1px solid #e4e7ec;border-radius:18px;padding:28px 32px;box-shadow:0 10px 30px rgba(16,24,40,.06);}",
        "h1,h2{margin-top:0;}",
        ".story{border-top:1px solid #eaecf0;padding-top:16px;margin-top:16px;}",
        "a{color:#1d4ed8;text-decoration:none;}",
        "</style>",
        "</head>",
        "<body><main>",
        f"<h1>{html.escape(digest.title)}</h1>",
        f"<p>{html.escape(digest.intro)}</p>",
        "<p><a href='summary.md'>summary.md</a> | <a href='message_digest.md'>message_digest.md</a> | <a href='selected_items.json'>selected_items.json</a></p>",
    ]

    for section in digest.sections:
        sections.append(f"<section class='story'><h2>{html.escape(section.label)}</h2><ul>")
        for item in section.items:
            sections.append("<li>")
            sections.append(f"<strong><a href='{html.escape(item.canonical_url)}'>{html.escape(item.title)}</a></strong><br>")
            sections.append(f"{html.escape(item.one_line_summary)}<br>")
            meta = f"{item.site_name} | {item.published_at.astimezone(timezone).strftime('%Y-%m-%d')}"
            sections.append(f"<small>{html.escape(meta)}</small>")
            sections.append("</li>")
        sections.append("</ul></section>")

    sections.append("</main></body></html>")
    (run_dir / "index.html").write_text("\n".join(sections), encoding="utf-8")


def _write_archive_index(output_dir: Path, limit: int) -> None:
    run_dirs = sorted(
        [path for path in output_dir.iterdir() if path.is_dir() and re.fullmatch(r"\d{8}-\d{6}", path.name)],
        key=lambda path: path.name,
        reverse=True,
    )[:limit]
    sections = [
        "<!doctype html>",
        "<html lang='ko'>",
        "<head><meta charset='utf-8'><title>Performing Arts Monitor Archive</title>",
        "<meta name='viewport' content='width=device-width, initial-scale=1'>",
        "<style>body{font-family:Segoe UI,Apple SD Gothic Neo,sans-serif;max-width:960px;margin:40px auto;padding:0 20px;background:#f7f8fb;color:#101828;}main{background:#fff;border:1px solid #e4e7ec;border-radius:18px;padding:28px 32px;}li{margin:12px 0;}a{color:#1d4ed8;text-decoration:none;}</style>",
        "</head><body><main><h1>Performing Arts Monitor Archive</h1><ul>",
    ]
    for run_dir in run_dirs:
        summary_path = run_dir / "summary.md"
        title = run_dir.name
        if summary_path.exists():
            first_line = summary_path.read_text(encoding="utf-8").splitlines()[0].replace("# ", "").strip()
            if first_line:
                title = first_line
        sections.append(
            f"<li><a href='{html.escape(run_dir.name)}/index.html'>{html.escape(title)}</a> <small>({html.escape(run_dir.name)})</small></li>"
        )
    sections.append("</ul></main></body></html>")
    (output_dir / "index.html").write_text("\n".join(sections), encoding="utf-8")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
