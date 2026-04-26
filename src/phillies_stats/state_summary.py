from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

import duckdb


DEFAULT_PROMPT_VERSION = "state-summary-v1"
DEFAULT_TONE_LABEL = "Team Snapshot"


class StateSummaryValidationError(ValueError):
    """Raised when a generated team state summary payload cannot be stored."""


def upsert_team_state_summary(
    conn: duckdb.DuckDBPyConnection,
    payload: dict[str, Any],
    *,
    season: int | None = None,
) -> dict[str, Any]:
    row = normalize_team_state_summary_payload(payload, season=season)
    conn.execute(
        """
        DELETE FROM team_state_summaries
        WHERE season = ?
          AND as_of_date = ?
        """,
        [row["season"], row["as_of_date"]],
    )
    conn.execute(
        """
        INSERT INTO team_state_summaries (
            season,
            as_of_date,
            headline,
            summary_text,
            tone_label,
            key_stats_json,
            sources_json,
            generated_at,
            prompt_version
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            row["season"],
            row["as_of_date"],
            row["headline"],
            row["summary_text"],
            row["tone_label"],
            row["key_stats_json"],
            row["sources_json"],
            row["generated_at"],
            row["prompt_version"],
        ],
    )
    return row


def normalize_team_state_summary_payload(payload: dict[str, Any], *, season: int | None = None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise StateSummaryValidationError("Summary payload must be a JSON object.")

    normalized_season = _parse_season(payload.get("season", season))
    as_of_date = _parse_date(payload.get("as_of_date"))
    headline = _required_text(payload, "headline")
    summary_text = _required_text(payload, "summary_text")
    tone_label = _optional_text(payload.get("tone_label")) or DEFAULT_TONE_LABEL
    generated_at = _parse_datetime(payload.get("generated_at"))
    prompt_version = _optional_text(payload.get("prompt_version")) or DEFAULT_PROMPT_VERSION
    key_stats = payload.get("key_stats", payload.get("stat_facts", []))
    sources = payload.get("sources", [])

    return {
        "season": normalized_season,
        "as_of_date": as_of_date,
        "headline": headline,
        "summary_text": summary_text,
        "tone_label": tone_label,
        "key_stats_json": _json_text(_normalize_key_stats(key_stats)),
        "sources_json": _json_text(_normalize_sources(sources)),
        "generated_at": generated_at,
        "prompt_version": prompt_version,
    }


def _parse_season(value: object) -> int:
    if value is None:
        raise StateSummaryValidationError("Summary payload is missing season.")
    try:
        season = int(value)
    except (TypeError, ValueError) as exc:
        raise StateSummaryValidationError("Summary season must be an integer.") from exc
    if season < 1900 or season > 2100:
        raise StateSummaryValidationError("Summary season is outside the supported range.")
    return season


def _parse_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or not value.strip():
        raise StateSummaryValidationError("Summary payload is missing as_of_date.")
    try:
        return date.fromisoformat(value.strip())
    except ValueError as exc:
        raise StateSummaryValidationError("Summary as_of_date must use YYYY-MM-DD format.") from exc


def _parse_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return _drop_timezone(value)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if value is None or value == "":
        return datetime.now(timezone.utc).replace(tzinfo=None)
    if not isinstance(value, str):
        raise StateSummaryValidationError("Summary generated_at must be an ISO datetime string.")
    cleaned = value.strip().replace("Z", "+00:00")
    try:
        return _drop_timezone(datetime.fromisoformat(cleaned))
    except ValueError as exc:
        raise StateSummaryValidationError("Summary generated_at must be an ISO datetime string.") from exc


def _drop_timezone(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _required_text(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    text = _optional_text(value)
    if not text:
        raise StateSummaryValidationError(f"Summary payload is missing {key}.")
    return text


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_key_stats(value: object) -> list[dict[str, str]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise StateSummaryValidationError("Summary key_stats must be a list.")

    rows: list[dict[str, str]] = []
    for item in value:
        if isinstance(item, str):
            text = item.strip()
            if text:
                rows.append({"label": text, "value": ""})
            continue
        if isinstance(item, dict):
            label = _optional_text(item.get("label"))
            raw_value = item.get("value")
            stat_value = "" if raw_value is None else str(raw_value).strip()
            if label:
                rows.append({"label": label, "value": stat_value})
            continue
        raise StateSummaryValidationError("Each key stat must be a string or object.")
    return rows


def _normalize_sources(value: object) -> list[dict[str, str]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise StateSummaryValidationError("Summary sources must be a list.")

    rows: list[dict[str, str]] = []
    for item in value:
        if isinstance(item, str):
            url = item.strip()
            if url:
                rows.append({"label": _source_label_from_url(url), "url": _validate_source_url(url)})
            continue
        if isinstance(item, dict):
            url = _required_source_url(item.get("url"))
            label = _optional_text(item.get("label")) or _optional_text(item.get("title")) or _source_label_from_url(url)
            rows.append({"label": label, "url": url})
            continue
        raise StateSummaryValidationError("Each source must be a URL string or object.")
    return rows


def _required_source_url(value: object) -> str:
    url = _optional_text(value)
    if not url:
        raise StateSummaryValidationError("Source entries must include a URL.")
    return _validate_source_url(url)


def _validate_source_url(url: str) -> str:
    if not url.startswith(("http://", "https://")):
        raise StateSummaryValidationError("Source URLs must start with http:// or https://.")
    return url


def _source_label_from_url(url: str) -> str:
    cleaned = url.replace("https://", "").replace("http://", "").split("/", 1)[0]
    return cleaned.removeprefix("www.") or "Source"


def _json_text(value: object) -> str:
    try:
        return json.dumps(value, ensure_ascii=True, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise StateSummaryValidationError("Summary payload contains non-serializable JSON data.") from exc
