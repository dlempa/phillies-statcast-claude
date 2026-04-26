from __future__ import annotations

import json
import mimetypes
import re
from base64 import b64encode
from datetime import date, datetime
from html import escape
from pathlib import Path
from zoneinfo import ZoneInfo

import altair as alt
import pandas as pd
import streamlit as st

from phillies_stats.display import format_metric_value

PHILLIES_RED = "#E81828"
PHILLIES_RED_DARK = "#C1121F"
PHILLIES_TEXT = "#1F2933"
PHILLIES_MUTED = "#6B7280"
PHILLIES_LOGO_PATH = Path(__file__).resolve().parent / "assets" / "primary_logo.png"
EASTERN_TZ = ZoneInfo("America/New_York")
UTC_TZ = ZoneInfo("UTC")

APP_CSS = """
<style>
:root {
    --ph-bg: #ffffff;
    --ph-bg-alt: #f6f8fa;
    --ph-panel: #ffffff;
    --ph-panel-soft: #f8fafc;
    --ph-border: #e5e7eb;
    --ph-border-strong: rgba(232, 24, 40, 0.34);
    --ph-text: #1f2933;
    --ph-muted: #6b7280;
    --ph-muted-soft: #9ca3af;
    --ph-accent: #e81828;
    --ph-accent-dark: #c1121f;
    --ph-accent-soft: rgba(232, 24, 40, 0.08);
    --ph-shadow: 0 8px 22px rgba(31, 41, 51, 0.07);
}

.stApp {
    background: var(--ph-bg);
    color: var(--ph-text);
}

[data-testid="stAppViewContainer"] > .main,
[data-testid="stAppViewContainer"] > .main > div {
    background: var(--ph-bg);
}

header[data-testid="stHeader"] {
    background: transparent;
    border-bottom: 0;
    height: 0;
    pointer-events: none;
}

header[data-testid="stHeader"] * {
    pointer-events: auto;
}

div[data-testid="stDecoration"],
footer {
    display: none;
}

div[data-testid="stToolbar"] {
    visibility: visible;
    pointer-events: auto;
}

button[title*="sidebar"],
button[title*="Sidebar"],
button[aria-label*="sidebar"],
button[aria-label*="Sidebar"] {
    visibility: visible !important;
    opacity: 1 !important;
    pointer-events: auto !important;
    color: var(--ph-accent-dark) !important;
    background: #ffffff !important;
    border: 1px solid var(--ph-border) !important;
    border-radius: 8px !important;
}

.block-container {
    padding-top: 1.25rem;
    padding-bottom: 2.25rem;
    max-width: 1360px;
}

section[data-testid="stSidebar"] {
    background: #fbfcfd;
    border-right: 1px solid var(--ph-border);
}

section[data-testid="stSidebar"] [data-testid="stSidebarNav"] {
    padding-top: 0.75rem;
}

section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a {
    border-radius: 8px;
    margin-bottom: 0.2rem;
    color: var(--ph-text);
    font-weight: 600;
}

section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a:hover {
    background: var(--ph-panel-soft);
}

section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a[aria-current="page"] {
    background: var(--ph-accent-soft);
    border: 1px solid var(--ph-border-strong);
    color: var(--ph-accent-dark);
}

section[data-testid="stSidebar"] [data-testid="stSidebarNav"] > div > p {
    color: var(--ph-muted);
    font-size: 0.76rem;
    letter-spacing: 0;
    text-transform: uppercase;
    margin-top: 1rem;
}

div[data-testid="stVerticalBlockBorderWrapper"] {
    background: var(--ph-panel);
    border: 1px solid var(--ph-border) !important;
    border-radius: 8px;
    box-shadow: 0 1px 2px rgba(31, 41, 51, 0.04);
}

div[data-testid="stVerticalBlockBorderWrapper"] > div {
    padding: 0.15rem 0.2rem;
}

div[data-testid="stMetric"] {
    background: var(--ph-panel);
    border: 1px solid var(--ph-border);
    border-radius: 8px;
    padding: 0.5rem 0.75rem;
    box-shadow: 0 1px 2px rgba(31, 41, 51, 0.04);
}

div[data-testid="stMetricLabel"] p {
    color: var(--ph-muted);
}

div[data-testid="metric-container"] [data-testid="stMetricValue"] {
    color: var(--ph-text);
}

label[data-testid="stWidgetLabel"] p {
    color: var(--ph-muted);
    font-size: 0.82rem;
    letter-spacing: 0;
    text-transform: uppercase;
}

div[data-baseweb="select"] > div,
div[data-baseweb="base-input"] > div {
    background: #ffffff;
    border: 1px solid var(--ph-border);
    border-radius: 8px;
    min-height: 2.7rem;
}

div[data-baseweb="select"] svg {
    color: var(--ph-muted);
}

div.stButton > button {
    min-height: 2.75rem;
    border-radius: 8px;
    background: var(--ph-accent);
    border: 1px solid var(--ph-accent);
    color: white;
    font-weight: 600;
    box-shadow: 0 4px 10px rgba(232, 24, 40, 0.14);
}

div.stButton > button:hover {
    background: var(--ph-accent-dark);
    border-color: var(--ph-accent-dark);
    color: white;
}

.page-hero {
    background: var(--ph-panel);
    border: 1px solid var(--ph-border);
    border-left: 5px solid var(--ph-accent);
    border-radius: 8px;
    padding: 1.15rem 1.25rem;
    margin-bottom: 1rem;
    box-shadow: var(--ph-shadow);
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
}

.page-hero-main {
    min-width: 0;
}

.page-eyebrow {
    color: var(--ph-muted);
    font-size: 0.76rem;
    letter-spacing: 0;
    text-transform: uppercase;
    margin-bottom: 0.45rem;
    font-weight: 700;
}

.page-title {
    color: var(--ph-text);
    font-family: "Trebuchet MS", "Segoe UI", sans-serif;
    font-size: 2.35rem;
    font-weight: 700;
    line-height: 1.08;
    margin: 0;
}

.page-subtitle {
    color: var(--ph-muted);
    font-size: 1rem;
    line-height: 1.55;
    margin-top: 0.55rem;
    max-width: 56rem;
}

.page-meta {
    color: var(--ph-accent-dark);
    margin-top: 0.85rem;
    display: inline-flex;
    gap: 0.35rem;
    align-items: center;
    font-size: 0.92rem;
    padding: 0.38rem 0.65rem;
    background: var(--ph-accent-soft);
    border: 1px solid var(--ph-border-strong);
    border-radius: 8px;
    font-weight: 600;
}

.state-summary-panel {
    position: relative;
    overflow: hidden;
    background: linear-gradient(135deg, #ffffff 0%, #ffffff 58%, #fff6f7 100%);
    border: 1px solid var(--ph-border);
    border-left: 5px solid var(--ph-accent);
    border-radius: 8px;
    padding: 1.05rem 1.15rem 1.1rem;
    margin: 0 0 1rem;
    box-shadow: var(--ph-shadow);
}

.state-summary-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 1.25rem;
    margin-bottom: 0.7rem;
}

.state-summary-heading {
    min-width: 0;
}

.state-summary-title {
    color: var(--ph-text);
    font-family: "Trebuchet MS", "Segoe UI", sans-serif;
    font-size: 1.42rem;
    line-height: 1.12;
    font-weight: 700;
    margin: 0;
}

.state-summary-headline {
    color: var(--ph-accent-dark);
    font-size: 0.98rem;
    line-height: 1.35;
    font-weight: 700;
    margin-top: 0.35rem;
    max-width: 54rem;
}

.state-summary-meta {
    display: flex;
    align-items: center;
    gap: 0.8rem;
    flex: 0 0 auto;
}

.state-summary-visual {
    width: 3.25rem;
    height: 3.25rem;
    flex: 0 0 auto;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    border: 1px solid var(--ph-border-strong);
    border-radius: 8px;
    background: rgba(255, 255, 255, 0.82);
    box-shadow: 0 5px 14px rgba(31, 41, 51, 0.08);
}

.state-summary-visual .phillies-logo-mark {
    width: 2.65rem;
    height: 2.65rem;
}

.state-summary-visual .phillies-mark {
    width: 2.7rem;
    height: 2.7rem;
    font-size: 1.9rem;
}

.state-summary-date {
    color: var(--ph-muted);
    font-size: 0.82rem;
    line-height: 1.4;
    text-align: right;
    max-width: 11rem;
}

.state-summary-copy {
    color: var(--ph-text);
    font-size: 1rem;
    line-height: 1.58;
    margin: 0.15rem 0 0;
    max-width: 68rem;
}

.state-stat-strip {
    display: flex;
    flex-wrap: wrap;
    gap: 0.45rem;
    margin-top: 0.85rem;
}

.state-stat-pill {
    display: inline-flex;
    align-items: center;
    gap: 0.38rem;
    min-height: 2rem;
    border: 1px solid var(--ph-border);
    border-radius: 8px;
    background: var(--ph-panel-soft);
    padding: 0.35rem 0.55rem;
    color: var(--ph-text);
}

.state-stat-pill--tone {
    background: var(--ph-accent-soft);
    border-color: var(--ph-border-strong);
    color: var(--ph-accent-dark);
}

.state-stat-pill--tone::before {
    content: "";
    width: 0.48rem;
    height: 0.48rem;
    border-radius: 999px;
    background: var(--ph-accent);
    box-shadow: 0 0 0 3px rgba(232, 24, 40, 0.12);
}

.state-stat-label {
    color: var(--ph-muted);
    font-size: 0.75rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0;
}

.state-stat-value {
    color: var(--ph-text);
    font-size: 0.92rem;
    font-weight: 700;
}

.state-source-row {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.45rem;
    margin-top: 0.8rem;
    color: var(--ph-muted);
    font-size: 0.84rem;
}

.state-source-label {
    color: var(--ph-muted);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0;
    font-size: 0.74rem;
}

.state-source-row a {
    color: var(--ph-accent-dark);
    text-decoration: none;
    font-weight: 600;
}

.state-source-row a:hover {
    text-decoration: underline;
}

.phillies-mark {
    width: 4.6rem;
    height: 4.6rem;
    flex: 0 0 auto;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    border: 2px solid var(--ph-accent);
    border-radius: 8px;
    color: var(--ph-accent);
    background: #ffffff;
    font-family: Georgia, "Times New Roman", serif;
    font-size: 3.3rem;
    font-weight: 800;
    line-height: 1;
}

.phillies-logo-mark {
    width: 5rem;
    height: 5rem;
    flex: 0 0 auto;
    object-fit: contain;
}

.section-heading {
    margin-bottom: 0.55rem;
}

.section-heading h2 {
    color: var(--ph-text);
    font-size: 1.04rem;
    font-weight: 700;
    margin: 0;
}

.section-heading p {
    color: var(--ph-muted);
    font-size: 0.92rem;
    line-height: 1.45;
    margin: 0.3rem 0 0;
}

.filter-caption {
    color: var(--ph-muted);
    font-size: 0.82rem;
    letter-spacing: 0;
    text-transform: uppercase;
    margin-bottom: 0.35rem;
    font-weight: 700;
}

.stat-card {
    background: var(--ph-panel);
    border: 1px solid var(--ph-border);
    border-radius: 8px;
    padding: 0.75rem 0.8rem;
    min-height: 6.45rem;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    box-shadow: 0 1px 2px rgba(31, 41, 51, 0.04);
}

.stat-card--accent {
    border-color: var(--ph-border-strong);
    background: var(--ph-accent-soft);
}

.stat-label {
    color: var(--ph-muted);
    font-size: 0.82rem;
    letter-spacing: 0;
    text-transform: uppercase;
    font-weight: 700;
}

.stat-value {
    color: var(--ph-text);
    font-family: "Trebuchet MS", "Segoe UI", sans-serif;
    font-size: 1.7rem;
    font-weight: 700;
    line-height: 1.12;
    margin-top: 0.45rem;
}

.stat-helper {
    color: var(--ph-muted);
    font-size: 0.92rem;
    margin-top: 0.65rem;
}

.profile-header {
    display: flex;
    justify-content: space-between;
    gap: 1rem;
    align-items: end;
}

.profile-title {
    color: var(--ph-text);
    font-family: "Trebuchet MS", "Segoe UI", sans-serif;
    font-size: 1.75rem;
    font-weight: 700;
    margin: 0;
}

.profile-subtitle {
    color: var(--ph-muted);
    margin-top: 0.35rem;
    line-height: 1.6;
}

.profile-chip {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    padding: 0.4rem 0.65rem;
    border-radius: 8px;
    border: 1px solid var(--ph-border-strong);
    background: var(--ph-accent-soft);
    color: var(--ph-accent-dark);
    font-size: 0.84rem;
    letter-spacing: 0;
    text-transform: uppercase;
    font-weight: 700;
}

.stTabs [data-baseweb="tab-list"] {
    gap: 0.25rem;
    background: var(--ph-accent-dark);
    border: 1px solid var(--ph-accent-dark);
    border-radius: 8px;
    padding: 0.3rem;
    margin: 0.25rem 0 1rem;
    box-shadow: 0 4px 12px rgba(193, 18, 31, 0.16);
}

.stTabs [data-baseweb="tab"] {
    border-radius: 6px;
    color: rgba(255, 255, 255, 0.82);
    min-height: 2.65rem;
    padding-left: 1rem;
    padding-right: 1rem;
    font-weight: 700;
}

.stTabs [aria-selected="true"] {
    background: #ffffff !important;
    color: var(--ph-accent-dark) !important;
    box-shadow: 0 2px 6px rgba(31, 41, 51, 0.14);
}

.stTabs [data-baseweb="tab"]:hover {
    background: rgba(255, 255, 255, 0.12);
    color: #ffffff;
}

.stAlert {
    border-radius: 8px;
    border: 1px solid var(--ph-border);
    background: var(--ph-panel-soft);
    color: var(--ph-text);
}

.phillies-table-wrap {
    overflow-x: auto;
}

table.phillies-table {
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    background: #ffffff;
    border: 1px solid var(--ph-border);
    border-radius: 8px;
}

table.phillies-table thead th {
    color: #ffffff;
    font-size: 0.75rem;
    letter-spacing: 0;
    text-transform: uppercase;
    font-weight: 700;
    text-align: center;
    background: var(--ph-accent-dark);
    padding: 0.6rem 0.7rem;
    border-bottom: 1px solid var(--ph-accent-dark);
}

table.phillies-table thead th:first-child {
    border-top-left-radius: 8px;
}

table.phillies-table thead th:last-child {
    border-top-right-radius: 8px;
}

table.phillies-table tbody td {
    color: var(--ph-text);
    font-size: 0.92rem;
    text-align: center;
    vertical-align: middle;
    padding: 0.62rem 0.7rem;
    border-bottom: 1px solid var(--ph-border);
}

table.phillies-table tbody tr:last-child td {
    border-bottom: none;
}

table.phillies-table tbody tr:hover td {
    background: #fff8f8;
}

table.phillies-table .cell-emphasis {
    color: var(--ph-text);
    font-weight: 700;
}

table.phillies-table .cell-secondary {
    color: var(--ph-muted);
}

table.phillies-table .cell-rank {
    color: var(--ph-accent-dark);
    font-weight: 700;
}

@media (max-width: 760px) {
    .block-container {
        padding-left: 1rem;
        padding-right: 1rem;
    }

    .page-hero {
        align-items: flex-start;
        flex-wrap: wrap;
    }

    .state-summary-header {
        display: block;
    }

    .state-summary-meta {
        justify-content: space-between;
        margin-top: 0.7rem;
    }

    .state-summary-date {
        text-align: left;
        max-width: none;
    }

}
</style>
"""


def apply_app_theme() -> None:
    st.markdown(APP_CSS, unsafe_allow_html=True)


def _logo_data_uri() -> str | None:
    if not PHILLIES_LOGO_PATH.exists():
        return None

    mime_type = mimetypes.guess_type(PHILLIES_LOGO_PATH)[0] or "image/png"
    encoded_logo = b64encode(PHILLIES_LOGO_PATH.read_bytes()).decode("ascii")
    return f"data:{mime_type};base64,{encoded_logo}"


def render_logo_mark(label: str = "P") -> str:
    logo_src = _logo_data_uri()
    if logo_src:
        return f"<img class='phillies-logo-mark' alt='Phillies logo' src='{logo_src}' />"

    return f"<div class='phillies-mark' aria-label='Phillies visual mark'>{escape(label)}</div>"


def render_page_header(
    title: str,
    subtitle: str,
    *,
    eyebrow: str | None = None,
    meta: str | None = None,
    show_mark: bool = False,
) -> None:
    eyebrow_html = f"<div class='page-eyebrow'>{escape(eyebrow)}</div>" if eyebrow else ""
    meta_html = f"<div class='page-meta'>{escape(meta)}</div>" if meta else ""
    mark_html = render_logo_mark() if show_mark else ""
    st.html(
        f"""
        <section class="page-hero">
            <div class="page-hero-main">
                {eyebrow_html}
                <h1 class="page-title">{escape(title)}</h1>
                <p class="page-subtitle">{escape(subtitle)}</p>
                {meta_html}
            </div>
            {mark_html}
        </section>
        """,
    )


def render_section_heading(title: str, subtitle: str | None = None) -> None:
    subtitle_html = f"<p>{escape(subtitle)}</p>" if subtitle else ""
    st.html(
        f"""
        <div class="section-heading">
            <h2>{escape(title)}</h2>
            {subtitle_html}
        </div>
        """,
    )


def render_team_state_summary(summary: dict[str, object] | None) -> None:
    if not summary:
        return

    card_title = "State of the Phillies"
    headline = _clean_state_summary_text(summary.get("headline"))
    summary_text = _clean_state_summary_text(summary.get("summary_text"))
    if not summary_text:
        return

    tone_label = _clean_state_summary_text(summary.get("tone_label")) or "Team Snapshot"
    updated = format_timestamp(summary.get("as_of_date")) or format_timestamp(summary.get("generated_at"))
    date_html = f"<div class='state-summary-date'>{escape(updated)}</div>" if updated else ""
    headline_html = ""
    if headline and headline.casefold() != card_title.casefold():
        headline_html = f"<div class='state-summary-headline'>{escape(headline)}</div>"
    stat_html = _state_summary_stats_html(_json_list(summary.get("key_stats_json")), tone_label=tone_label)
    source_html = _state_summary_sources_html(_json_list(summary.get("sources_json")))
    visual_html = f"<div class='state-summary-visual' aria-hidden='true'>{render_logo_mark()}</div>"

    st.html(
        f"""
        <section class="state-summary-panel">
            <div class="state-summary-header">
                <div class="state-summary-heading">
                    <h2 class="state-summary-title">{escape(card_title)}</h2>
                    {headline_html}
                </div>
                <div class="state-summary-meta">
                    {date_html}
                    {visual_html}
                </div>
            </div>
            <p class="state-summary-copy">{escape(summary_text)}</p>
            {stat_html}
            {source_html}
        </section>
        """,
    )


def render_filter_caption(text: str) -> None:
    st.html(f"<div class='filter-caption'>{escape(text)}</div>")


def render_profile_header(name: str, subtitle: str, chip: str | None = None) -> None:
    chip_html = f"<div class='profile-chip'>{escape(chip)}</div>" if chip else ""
    st.html(
        f"""
        <div class="profile-header">
            <div>
                <h2 class="profile-title">{escape(name)}</h2>
                <div class="profile-subtitle">{escape(subtitle)}</div>
            </div>
            {chip_html}
        </div>
        """,
    )


def render_stat_cards(cards: list[dict[str, str | None]], *, columns: int | None = None) -> None:
    if not cards:
        return
    row_size = columns or len(cards)
    for start in range(0, len(cards), row_size):
        row_cards = cards[start : start + row_size]
        card_columns = st.columns(len(row_cards))
        for column, card in zip(card_columns, row_cards):
            tone = card.get("tone") or "default"
            helper = card.get("helper")
            helper_html = f"<div class='stat-helper'>{escape(str(helper))}</div>" if helper else ""
            with column:
                st.html(
                    f"""
                    <div class="stat-card stat-card--{escape(str(tone))}">
                        <div>
                            <div class="stat-label">{escape(str(card.get("label", "")))}</div>
                            <div class="stat-value">{escape(str(card.get("value", "")))}</div>
                        </div>
                        {helper_html}
                    </div>
                    """,
                )


def style_chart(chart: alt.Chart, *, height: int = 320) -> alt.Chart:
    return (
        chart.properties(height=height, background="transparent")
        .configure_view(strokeWidth=0)
        .configure_axis(
            labelColor=PHILLIES_MUTED,
            titleColor=PHILLIES_MUTED,
            domainColor="#D1D5DB",
            gridColor="#EEF2F7",
            tickColor="#E5E7EB",
        )
        .configure_title(color=PHILLIES_TEXT, fontSize=16)
        .configure_legend(
            titleColor=PHILLIES_MUTED,
            labelColor=PHILLIES_MUTED,
            orient="bottom",
            padding=12,
            fillColor="transparent",
            strokeColor="transparent",
        )
        .configure_range(category=[PHILLIES_RED, PHILLIES_TEXT, PHILLIES_MUTED, "#9CA3AF", PHILLIES_RED_DARK])
        .configure_header(labelColor=PHILLIES_MUTED, titleColor=PHILLIES_TEXT)
    )


def format_card(value: object, suffix: str = "") -> str:
    formatted = format_metric_value(value)
    if formatted == "No data":
        return formatted
    return f"{formatted}{suffix}"


def format_timestamp(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC_TZ)
        eastern_value = value.astimezone(EASTERN_TZ)
        return eastern_value.strftime("%b %d, %Y %I:%M %p ET")
    if isinstance(value, date):
        return value.strftime("%b %d, %Y")
    return str(value)


def _json_list(value: object) -> list[object]:
    if isinstance(value, list):
        return value
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _clean_state_summary_text(value: object) -> str:
    text = str(value or "").strip()
    return re.sub(r"(?<=[A-Za-z0-9])\?(?=[A-Za-z])", "'", text)


def _state_summary_stats_html(stats: list[object], *, tone_label: str | None = None) -> str:
    stat_items = []
    tone_text = _clean_state_summary_text(tone_label)
    if tone_text:
        stat_items.append(
            "<div class='state-stat-pill state-stat-pill--tone'>"
            f"<span class='state-stat-value'>{escape(tone_text)}</span>"
            "</div>"
        )
    for item in stats[:5]:
        if not isinstance(item, dict):
            continue
        label = _clean_state_summary_text(item.get("label"))
        value = _clean_state_summary_text(item.get("value"))
        if not label and not value:
            continue
        if value:
            stat_items.append(
                "<div class='state-stat-pill'>"
                f"<span class='state-stat-label'>{escape(label)}</span>"
                f"<span class='state-stat-value'>{escape(value)}</span>"
                "</div>"
            )
        else:
            stat_items.append(
                "<div class='state-stat-pill'>"
                f"<span class='state-stat-value'>{escape(label)}</span>"
                "</div>"
            )
    if not stat_items:
        return ""
    return f"<div class='state-stat-strip'>{''.join(stat_items)}</div>"


def _state_summary_sources_html(sources: list[object]) -> str:
    source_links = []
    for source in sources[:5]:
        if not isinstance(source, dict):
            continue
        label = _clean_state_summary_text(source.get("label") or "Source")
        url = str(source.get("url") or "").strip()
        if not label or not url.startswith(("http://", "https://")):
            continue
        source_links.append(f"<a href='{escape(url, quote=True)}' target='_blank'>{escape(label)}</a>")
    if not source_links:
        return ""
    return (
        "<div class='state-source-row'>"
        "<span class='state-source-label'>Sources</span>"
        f"{' | '.join(source_links)}"
        "</div>"
    )
