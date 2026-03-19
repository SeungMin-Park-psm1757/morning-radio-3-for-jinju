# Performing Arts Monitor

Performing Arts Monitor builds a daily Korean musical and performing arts digest and ships it as:

- a collected-item archive
- a triaged and deduplicated digest
- a Telegram text message
- a simple HTML archive page

## What It Does

- Collects official notices, auditions, company updates, and industry items from Korean musical and performing arts sites.
- Uses RSS first where available, then falls back to HTML parsing.
- Applies local scoring for source trust, recency, actionability, and tracked people or keyword hits.
- Uses Gemini for relevance filtering, category classification, semantic deduplication, importance judgment, and one-line summaries.
- Writes per-run output plus an HTML archive index.

## Quick Start

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e .
copy .env.example .env
```

Set `GEMINI_API_KEY` in `.env`, then run:

```bash
performing-arts-monitor
```

For a no-API smoke test:

```bash
performing-arts-monitor --skip-llm
```

## Main Outputs

Each run writes to `output/YYYYMMDD-HHMMSS/`.

- `raw_items.json`: all collected raw items
- `triaged_items.json`: Gemini or heuristic triage results
- `selected_items.json`: final selected representatives
- `digest.json`: machine-readable digest
- `message_digest.md`: Telegram-friendly digest
- `summary.md`: run summary
- `index.html`: run-level archive page
- `run_metadata.json`: machine-readable run metadata

The root `output/index.html` file lists recent runs as a lightweight archive page.

## Key Environment Variables

- `GEMINI_API_KEY`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `TELEGRAM_THREAD_ID`
- `PERFORMING_ARTS_MONITOR_TELEGRAM_SILENT`
- `PERFORMING_ARTS_MONITOR_OUTPUT_DIR`
- `PERFORMING_ARTS_MONITOR_TIMEZONE`
- `PERFORMING_ARTS_MONITOR_SCHEDULE_HOUR_LOCAL`
- `PERFORMING_ARTS_MONITOR_WEEKDAYS_ONLY`
- `PERFORMING_ARTS_MONITOR_HOURS_BACK` (optional override)
- `PERFORMING_ARTS_MONITOR_TRIAGE_MODEL`
- `PERFORMING_ARTS_MONITOR_MAX_TOTAL_ITEMS`
- `PERFORMING_ARTS_MONITOR_MAX_ITEMS_PER_CATEGORY`
- `PERFORMING_ARTS_MONITOR_SCORE_THRESHOLD`
- `PERFORMING_ARTS_MONITOR_PUBLIC_ARCHIVE_BASE_URL`
- `PERFORMING_ARTS_MONITOR_TRACKED_PEOPLE`
- `PERFORMING_ARTS_MONITOR_TRACKED_KEYWORDS`

## Sources

The initial MVP collects from:

- EMK
- OTR
- OD Musical
- Shownote
- SNCO
- Shinsee Company
- CJ ENM Performing Arts and Newsroom
- ACOM

## GitHub Actions

The workflow is defined in `.github/workflows/daily-monitor.yml`.

- Schedule: weekdays `09:00 KST`
- Default collection window: from the previous scheduled write slot to the current run time
- Manual `workflow_dispatch` runs default to a `72h` override for easier validation
- Telegram delivery is enabled when Telegram secrets are present
- Manual runs are supported with `workflow_dispatch`
