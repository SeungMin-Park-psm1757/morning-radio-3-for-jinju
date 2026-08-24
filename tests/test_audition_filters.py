from __future__ import annotations

import unittest
from datetime import UTC, datetime

from performing_arts_monitor.models import CollectedItem
from performing_arts_monitor.pipeline import (
    _audition_profile_exclusion_reason,
    _extract_work_titles,
    _heuristic_category,
)


def _item(title: str, body_text: str = "") -> CollectedItem:
    return CollectedItem(
        source_key="test",
        source_label="Test",
        site_name="Test",
        source_kind="community_board",
        title=title,
        url="https://example.com",
        published_at=datetime.now(tz=UTC),
        summary="",
        body_text=body_text,
        attachments=[],
        external_urls=[],
        source_weight=0.0,
        fingerprint=title,
    )


class AuditionFilterTests(unittest.TestCase):
    def test_generic_artist_recruitment_is_not_an_audition(self) -> None:
        item = _item("공연예술 지원사업 참여 예술가 모집")
        category = _heuristic_category(
            item=item,
            headline_text=item.title,
            full_text=item.title,
            headline_people=[],
            matched_people=[],
        )
        self.assertNotEqual(category, "audition")

    def test_female_actor_recruitment_is_an_audition(self) -> None:
        item = _item("창작 뮤지컬 여성 배우 모집")
        category = _heuristic_category(
            item=item,
            headline_text=item.title,
            full_text=item.title,
            headline_people=[],
            matched_people=[],
        )
        self.assertEqual(category, "audition")
        self.assertEqual(_audition_profile_exclusion_reason(item, category), "")

    def test_child_role_is_excluded(self) -> None:
        item = _item("뮤지컬 여성 아역 배우 모집")
        self.assertIn("아역", _audition_profile_exclusion_reason(item, "audition"))

    def test_mixed_gender_notice_is_excluded(self) -> None:
        item = _item("뮤지컬 남성 및 여성 배우 모집")
        self.assertIn("남성", _audition_profile_exclusion_reason(item, "audition"))

    def test_kpop_notice_is_excluded(self) -> None:
        item = _item("K-pop 걸그룹 여성 멤버 모집")
        self.assertIn("K-pop", _audition_profile_exclusion_reason(item, "audition"))

    def test_opening_title_beats_support_term_in_body(self) -> None:
        item = _item(
            "뮤지컬 ‘다산, 물 위의 별’ 9월 12일 초연",
            "지역 공연예술 지원사업을 통해 제작된 작품입니다.",
        )
        category = _heuristic_category(
            item=item,
            headline_text=item.title,
            full_text=f"{item.title} {item.body_text}",
            headline_people=[],
            matched_people=[],
        )
        self.assertEqual(category, "works_casting")

    def test_work_titles_support_common_quote_styles(self) -> None:
        self.assertIn("네로", _extract_work_titles("뮤지컬 ‘네로’ 런던 개막"))
        self.assertIn("다산, 물 위의 별", _extract_work_titles("뮤지컬 '다산, 물 위의 별' 초연"))


if __name__ == "__main__":
    unittest.main()
