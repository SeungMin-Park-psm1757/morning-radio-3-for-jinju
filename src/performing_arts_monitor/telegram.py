from __future__ import annotations

import html
import re
from typing import Any

import requests

from performing_arts_monitor.config import AppConfig

TELEGRAM_MAX_MESSAGE = 3500


def send_digest(
    *,
    config: AppConfig,
    digest_markdown: str,
    public_links: dict[str, str] | None = None,
) -> dict[str, Any]:
    chat_info = _get_chat_info(config)
    text = _prepare_single_text_message(digest_markdown, public_links)
    message_id = _send_text_message(config, text)
    return {
        "sent": True,
        "message_id": message_id,
        "target_type": chat_info.get("type"),
        "target_title": chat_info.get("title"),
        "target_username": chat_info.get("username"),
        "thread_id": config.telegram_thread_id,
        "public_links": public_links or {},
    }


def _get_chat_info(config: AppConfig) -> dict[str, Any]:
    response = requests.post(
        _telegram_url(config, "getChat"),
        data={"chat_id": config.telegram_chat_id},
        timeout=20,
    )
    response.raise_for_status()
    data = response.json()
    if not data.get("ok"):
        raise ValueError(f"Telegram getChat failed: {data}")

    result = data["result"]
    title = (
        result.get("title")
        or " ".join(part for part in [result.get("first_name"), result.get("last_name")] if part).strip()
        or None
    )
    return {
        "id": result.get("id"),
        "type": result.get("type"),
        "title": title,
        "username": result.get("username"),
    }


def _send_text_message(config: AppConfig, text: str) -> int:
    payload: dict[str, Any] = {
        "chat_id": config.telegram_chat_id,
        "text": text,
        "disable_web_page_preview": True,
        "disable_notification": config.telegram_silent,
        "parse_mode": "HTML",
    }
    if config.telegram_thread_id:
        payload["message_thread_id"] = config.telegram_thread_id
    response = requests.post(
        _telegram_url(config, "sendMessage"),
        data=payload,
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    if not data.get("ok"):
        raise ValueError(f"Telegram sendMessage failed: {data}")
    return int(data["result"]["message_id"])


def _prepare_single_text_message(markdown: str, public_links: dict[str, str] | None) -> str:
    text = _markdown_to_telegram_html(markdown)
    text = _append_public_links(text, public_links)
    if len(text) <= TELEGRAM_MAX_MESSAGE:
        return text
    compact_text = _truncate_html_message(text, TELEGRAM_MAX_MESSAGE)
    return compact_text


def _append_public_links(text: str, public_links: dict[str, str] | None) -> str:
    if not public_links:
        return text

    labels = (
        ("archive", "아카이브 보기"),
        ("summary", "실행 요약"),
        ("digest", "메시지 요약"),
    )
    link_lines = ["<b>바로가기</b>"]
    for key, label in labels:
        url = (public_links.get(key) or "").strip()
        if not url:
            continue
        link_lines.append(
            f"- <a href=\"{html.escape(url, quote=True)}\">{html.escape(label)}</a>"
        )

    if len(link_lines) == 1:
        return text
    return f"{text}\n\n" + "\n".join(link_lines)


def _markdown_to_telegram_html(markdown: str) -> str:
    lines: list[str] = []
    for raw_line in markdown.splitlines():
        line = raw_line.rstrip()
        if line.startswith("# "):
            lines.append(f"<b>{html.escape(line[2:])}</b>")
            continue
        if line.startswith("## "):
            lines.append("")
            lines.append(f"<b>{html.escape(line[3:])}</b>")
            continue
        if line.startswith("- **") and line.endswith("**"):
            title = line[4:-2]
            lines.append(f"- <b>{html.escape(title)}</b>")
            continue
        if line.startswith("  링크:"):
            url = line.split(":", 1)[1].strip()
            if url:
                escaped = html.escape(url, quote=True)
                lines.append(f"<a href=\"{escaped}\">원문 보기</a>")
            continue
        if line.startswith("  "):
            lines.append(_inline_markdown_to_html(line.strip()))
            continue
        lines.append(html.escape(line))
    return "\n".join(lines).strip()


def _inline_markdown_to_html(text: str) -> str:
    escaped = html.escape(text)
    escaped = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", escaped)
    url_match = re.search(r"(https?://\S+)", text)
    if url_match and text.startswith("링크:"):
        url = html.escape(url_match.group(1), quote=True)
        return f'<a href="{url}">원문 보기</a>'
    return escaped


def _truncate_html_message(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text

    plain = re.sub(r"<[^>]+>", "", text)
    plain = html.unescape(plain)
    trimmed = plain[: max(0, limit - 1)].rstrip()
    if trimmed.endswith("…"):
        return html.escape(trimmed)
    return html.escape(trimmed + "…")


def _telegram_url(config: AppConfig, method: str) -> str:
    return f"https://api.telegram.org/bot{config.telegram_bot_token}/{method}"
