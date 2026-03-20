from __future__ import annotations

import json
import re
from typing import Any

from google import genai
from google.genai import types

from performing_arts_monitor.config import AppConfig
from performing_arts_monitor.models import CollectedItem


def _extract_text(response: Any) -> str:
    text = getattr(response, "text", None)
    if text:
        return text

    candidates = getattr(response, "candidates", None) or []
    parts: list[str] = []
    for candidate in candidates:
        content = getattr(candidate, "content", None)
        for part in getattr(content, "parts", []) or []:
            value = getattr(part, "text", None)
            if value:
                parts.append(value)
    return "\n".join(parts).strip()


def _extract_json_payload(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```json\s*", "", cleaned)
        cleaned = re.sub(r"^```\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1:
        raise ValueError("Model did not return a JSON object.")
    return json.loads(cleaned[start : end + 1])


def _json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)


def _truncate(text: str, limit: int) -> str:
    stripped = re.sub(r"\s+", " ", text).strip()
    if len(stripped) <= limit:
        return stripped
    return stripped[: limit - 1].rstrip() + "…"


class GeminiTriage:
    def __init__(self, config: AppConfig) -> None:
        if not config.gemini_api_key:
            raise ValueError("GEMINI_API_KEY is required for GeminiTriage.")
        self.config = config
        self.client = genai.Client(api_key=config.gemini_api_key)

    def _generate_json(
        self,
        *,
        system_instruction: str,
        prompt: str,
    ) -> dict[str, Any]:
        response = self.client.models.generate_content(
            model=self.config.triage_model,
            contents=prompt,
            config=types.GenerateContentConfig(
                systemInstruction=system_instruction,
                temperature=0.2,
                maxOutputTokens=self.config.max_output_tokens,
                responseMimeType="application/json",
            ),
        )
        return _extract_json_payload(_extract_text(response))

    def triage_items(self, items: list[CollectedItem]) -> dict[str, dict[str, Any]]:
        results: dict[str, dict[str, Any]] = {}
        for batch in _batched(items, 18):
            payload = self._triage_batch(batch)
            for item in payload.get("items", []):
                item_id = str(item.get("id") or "").strip()
                if item_id:
                    results[item_id] = item
        return results

    def _triage_batch(self, items: list[CollectedItem]) -> dict[str, Any]:
        serializable_items = [
            {
                "id": item.fingerprint,
                "site_name": item.site_name,
                "source_label": item.source_label,
                "source_kind": item.source_kind,
                "title": item.title,
                "url": item.url,
                "published_at": item.published_at.isoformat(),
                "summary": _truncate(item.summary, 300),
                "body_text": _truncate(item.body_text, 900),
                "attachments": item.attachments[:4],
                "external_urls": item.external_urls[:4],
            }
            for item in items
        ]
        tracked_people = list(self.config.tracked_people)
        tracked_keywords = list(self.config.tracked_keywords)

        system_instruction = (
            "You are a careful Korean performing arts industry desk editor. "
            "Use only the supplied metadata. "
            "Your job is to filter relevance, classify the item, detect duplicates across sites, "
            "judge importance, and write one-line summaries without hype. "
            "Tracked people are priority signals, not automatic inclusion. "
            "Do not invent missing facts, dates, quotes, or casting details."
        )

        prompt = f"""
Return exactly one JSON object with this shape:
{{
  "items": [
    {{
      "id": "string",
      "relevant": true,
      "category": "audition | support | works_casting | people | company_news",
      "secondary_tags": ["string"],
      "importance": 0,
      "relevance_confidence": 0.0,
      "duplicate_key": "short string",
      "keep": true,
      "one_line_summary": "1 Korean sentence",
      "watch_point": "short Korean sentence or empty string",
      "mentioned_people": ["string"],
      "mentioned_works": ["string"],
      "mentioned_organizations": ["string"],
      "exclude_reason": "short Korean string or empty string"
    }}
  ]
}}

Rules:
- `category` must be one of: audition, support, works_casting, people, company_news.
- Mark `relevant=false` and `keep=false` for unrelated or very low-signal items.
- For `source_kind = news_search`, keep only items that are clearly tied to the Korean musical or performing arts industry and the tracked keywords or tracked people.
- Use `support` only for actual grants, support programs, open calls, creation support, or public arts funding.
- Use `audition` for auditions, casting calls, applicant notices, pass/fail notices, and application schedules.
- Use `works_casting` for casting news, production lineups, opening schedules, ticket opening only when the item signals a meaningful production development, or major work movement.
- Use `people` for concrete actor or creator movement, awards, interviews with real industry significance, notable comments, or tracked-person developments.
- Use `company_news` for company announcements, hiring notices, partnerships, launches, and official corporate updates.
- Ticket opening alone is lower-signal unless it indicates a major production milestone or tracked person relevance, and it should usually rank below actionable audition or support items.
- Completed or closed notices can still be relevant if they are final results for a major audition or a notable production update.
- `duplicate_key` should be the same when multiple items describe the same underlying event.
- `importance` is 0-100 and should reflect practical importance for a daily industry monitor.
- `secondary_tags` can include values like 마감임박, 합격발표, 티켓오픈, 캐스팅변경, 채용, 공연개막, 인터뷰, 수상.
- `one_line_summary` must be concise Korean, fact-based, and messenger-friendly.
- `watch_point` should be short and optional.
- Never copy the title verbatim as the full summary if a shorter substantive sentence is possible.

Tracked people:
{_json_dumps(tracked_people)}

Tracked keywords:
{_json_dumps(tracked_keywords)}

Items:
{_json_dumps(serializable_items)}
""".strip()

        return self._generate_json(
            system_instruction=system_instruction,
            prompt=prompt,
        )


def _batched(items: list[CollectedItem], size: int) -> list[list[CollectedItem]]:
    return [items[index : index + size] for index in range(0, len(items), size)]
