from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

DEFAULT_TRACKED_PEOPLE: tuple[str, ...] = ()

DEFAULT_TRACKED_KEYWORDS: tuple[str, ...] = (
    "뮤지컬",
    "오디션",
    "오디션공고",
    "모집요강",
    "지원자격",
    "지원서",
    "서류전형",
    "실기전형",
    "창작지원",
    "예술인지원",
    "청년예술지원사업",
    "뮤지컬오디션",
    "연극오디션",
    "배우오디션",
    "오페라오디션",
    "통합오디션",
    "여배우모집",
    "여성배역",
    "성인오디션",
    "서울오디션",
    "대학로오디션",
    "개막",
    "초연",
    "재연",
    "캐스팅",
    "라인업",
)


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _csv_env(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    value = (os.getenv(name) or "").strip()
    if not value:
        return default
    items = [item.strip() for item in value.split(",") if item.strip()]
    return tuple(dict.fromkeys(items))


def _env_optional_int(name: str) -> int | None:
    value = (os.getenv(name) or "").strip()
    if not value:
        return None
    return int(value)


@dataclass(slots=True)
class AppConfig:
    gemini_api_key: str | None
    telegram_bot_token: str | None
    telegram_chat_id: str | None
    telegram_thread_id: str | None
    telegram_silent: bool
    hours_back: int | None
    output_dir: Path
    timezone_name: str
    schedule_hour_local: int
    weekdays_only: bool
    triage_model: str
    max_output_tokens: int
    archive_limit: int
    public_archive_base_url: str | None
    max_total_items: int
    max_items_per_category: int
    score_threshold: float
    max_news_items: int
    news_score_threshold: float
    news_per_query_limit: int
    max_source_items: int
    request_timeout_seconds: int
    skip_llm: bool
    tracked_people: tuple[str, ...]
    tracked_keywords: tuple[str, ...]

    @property
    def timezone(self) -> ZoneInfo:
        return ZoneInfo(self.timezone_name)

    @property
    def llm_enabled(self) -> bool:
        return bool(self.gemini_api_key) and not self.skip_llm

    @property
    def telegram_enabled(self) -> bool:
        return bool(self.telegram_bot_token and self.telegram_chat_id)

    def collection_window_start(self, now_utc: datetime) -> datetime:
        if self.hours_back is not None:
            return now_utc - timedelta(hours=self.hours_back)

        local_now = now_utc.astimezone(self.timezone)
        current_slot = self._latest_scheduled_slot(local_now)
        previous_slot = self._previous_scheduled_slot(current_slot)
        return previous_slot.astimezone(UTC)

    def _latest_scheduled_slot(self, local_now: datetime) -> datetime:
        candidate_date = local_now.date()
        current_clock = (
            local_now.hour,
            local_now.minute,
            local_now.second,
            local_now.microsecond,
        )
        scheduled_clock = (self.schedule_hour_local, 0, 0, 0)
        if current_clock < scheduled_clock:
            candidate_date -= timedelta(days=1)
        while self.weekdays_only and candidate_date.weekday() >= 5:
            candidate_date -= timedelta(days=1)
        return datetime(
            candidate_date.year,
            candidate_date.month,
            candidate_date.day,
            self.schedule_hour_local,
            tzinfo=self.timezone,
        )

    def _previous_scheduled_slot(self, slot_local: datetime) -> datetime:
        candidate_date = slot_local.date() - timedelta(days=1)
        while self.weekdays_only and candidate_date.weekday() >= 5:
            candidate_date -= timedelta(days=1)
        return datetime(
            candidate_date.year,
            candidate_date.month,
            candidate_date.day,
            self.schedule_hour_local,
            tzinfo=self.timezone,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a Korean musical and performing arts digest from recent official sources.",
    )
    parser.add_argument(
        "--hours-back",
        type=int,
        default=_env_optional_int("PERFORMING_ARTS_MONITOR_HOURS_BACK"),
        help="Optional override for the collection window in hours. Defaults to the previous scheduled write slot.",
    )
    parser.add_argument(
        "--output-dir",
        default=os.getenv("PERFORMING_ARTS_MONITOR_OUTPUT_DIR", "output"),
        help="Directory where generated files will be written.",
    )
    parser.add_argument(
        "--skip-llm",
        action="store_true",
        help="Skip Gemini calls and use fallback heuristic triage instead.",
    )
    return parser


def load_config(args: argparse.Namespace) -> AppConfig:
    _load_dotenv(Path(".env"))
    return AppConfig(
        gemini_api_key=os.getenv("GEMINI_API_KEY"),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID"),
        telegram_thread_id=os.getenv("TELEGRAM_THREAD_ID"),
        telegram_silent=_env_bool("PERFORMING_ARTS_MONITOR_TELEGRAM_SILENT", False),
        hours_back=args.hours_back,
        output_dir=Path(args.output_dir),
        timezone_name=os.getenv("PERFORMING_ARTS_MONITOR_TIMEZONE", "Asia/Seoul"),
        schedule_hour_local=int(os.getenv("PERFORMING_ARTS_MONITOR_SCHEDULE_HOUR_LOCAL", "9")),
        weekdays_only=_env_bool("PERFORMING_ARTS_MONITOR_WEEKDAYS_ONLY", True),
        triage_model=os.getenv("PERFORMING_ARTS_MONITOR_TRIAGE_MODEL", "gemini-2.5-flash-lite"),
        max_output_tokens=int(os.getenv("PERFORMING_ARTS_MONITOR_MAX_OUTPUT_TOKENS", "8192")),
        archive_limit=int(os.getenv("PERFORMING_ARTS_MONITOR_ARCHIVE_LIMIT", "20")),
        public_archive_base_url=os.getenv("PERFORMING_ARTS_MONITOR_PUBLIC_ARCHIVE_BASE_URL"),
        max_total_items=int(os.getenv("PERFORMING_ARTS_MONITOR_MAX_TOTAL_ITEMS", "8")),
        max_items_per_category=int(os.getenv("PERFORMING_ARTS_MONITOR_MAX_ITEMS_PER_CATEGORY", "3")),
        score_threshold=float(os.getenv("PERFORMING_ARTS_MONITOR_SCORE_THRESHOLD", "55")),
        max_news_items=int(os.getenv("PERFORMING_ARTS_MONITOR_MAX_NEWS_ITEMS", "4")),
        news_score_threshold=float(os.getenv("PERFORMING_ARTS_MONITOR_NEWS_SCORE_THRESHOLD", "48")),
        news_per_query_limit=int(os.getenv("PERFORMING_ARTS_MONITOR_NEWS_PER_QUERY_LIMIT", "8")),
        max_source_items=int(os.getenv("PERFORMING_ARTS_MONITOR_MAX_SOURCE_ITEMS", "20")),
        request_timeout_seconds=int(os.getenv("PERFORMING_ARTS_MONITOR_REQUEST_TIMEOUT_SECONDS", "30")),
        skip_llm=args.skip_llm,
        tracked_people=_csv_env(
            "PERFORMING_ARTS_MONITOR_TRACKED_PEOPLE",
            DEFAULT_TRACKED_PEOPLE,
        ),
        tracked_keywords=_csv_env(
            "PERFORMING_ARTS_MONITOR_TRACKED_KEYWORDS",
            DEFAULT_TRACKED_KEYWORDS,
        ),
    )
