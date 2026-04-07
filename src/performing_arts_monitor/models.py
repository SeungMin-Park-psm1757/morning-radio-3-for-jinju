from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

CATEGORY_LABELS = {
    "audition": "오디션",
    "support": "지원사업",
    "works_casting": "작품·캐스팅",
    "people": "배우·인물 동향",
    "company_news": "제작사 소식",
}

CATEGORY_ORDER = (
    "audition",
    "support",
    "works_casting",
    "people",
    "company_news",
)


@dataclass(frozen=True, slots=True)
class SourceDefinition:
    key: str
    label: str
    site_name: str
    source_kind: str
    source_weight: float
    url: str


@dataclass(slots=True)
class CollectedItem:
    source_key: str
    source_label: str
    site_name: str
    source_kind: str
    title: str
    url: str
    published_at: datetime
    summary: str
    body_text: str
    attachments: list[str]
    external_urls: list[str]
    source_weight: float
    fingerprint: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["published_at"] = self.published_at.isoformat()
        return data


@dataclass(slots=True)
class TriagedItem:
    source_key: str
    source_label: str
    site_name: str
    source_kind: str
    title: str
    url: str
    canonical_url: str
    published_at: datetime
    summary: str
    body_text: str
    attachments: list[str]
    external_urls: list[str]
    source_weight: float
    fingerprint: str
    category: str
    secondary_tags: list[str]
    keep: bool
    exclude_reason: str
    duplicate_key: str
    one_line_summary: str
    watch_point: str
    local_score: float
    llm_score: float
    final_score: float
    relevance_confidence: float
    duplicate_group_size: int = 1
    mentioned_people: list[str] = field(default_factory=list)
    mentioned_works: list[str] = field(default_factory=list)
    mentioned_organizations: list[str] = field(default_factory=list)
    detail_bullets: list[str] = field(default_factory=list)

    @property
    def category_label(self) -> str:
        return CATEGORY_LABELS.get(self.category, self.category)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["published_at"] = self.published_at.isoformat()
        data["category_label"] = self.category_label
        return data


@dataclass(slots=True)
class DigestSection:
    key: str
    label: str
    items: list[TriagedItem]

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "items": [item.to_dict() for item in self.items],
        }


@dataclass(slots=True)
class DigestRun:
    title: str
    intro: str
    sections: list[DigestSection]
    generated_at: datetime
    news_items: list[TriagedItem] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "intro": self.intro,
            "generated_at": self.generated_at.isoformat(),
            "sections": [section.to_dict() for section in self.sections],
            "news_items": [item.to_dict() for item in self.news_items],
        }
