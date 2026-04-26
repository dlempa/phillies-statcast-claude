from __future__ import annotations

from html import escape
import re
import unicodedata
from datetime import date, datetime

import pandas as pd


def format_player_name(name: object) -> object:
    if not isinstance(name, str):
        return name
    clean_name = _reorder_comma_name(" ".join(name.split()))
    if not clean_name:
        return None

    pieces = []
    for token in clean_name.split(" "):
        if re.fullmatch(r"(?:[a-z]\.){1,4}", token.lower()):
            pieces.append(token.upper())
        else:
            pieces.append(token.title())
    return " ".join(pieces)


def normalize_player_key(name: object) -> str | None:
    formatted = format_player_name(name)
    if not isinstance(formatted, str):
        return None
    ascii_name = unicodedata.normalize("NFKD", formatted)
    ascii_name = "".join(char for char in ascii_name if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]", "", ascii_name.lower())


def format_display_dataframe(frame: pd.DataFrame) -> pd.DataFrame:
    display = frame.copy()
    for column in display.columns:
        lowered = column.lower()
        if "player" in lowered or lowered.endswith("_name") or lowered == "name":
            display[column] = display[column].map(format_player_name)
            continue
        display[column] = display[column].map(_format_value)
    return display


def render_centered_table(frame: pd.DataFrame) -> str:
    formatted = format_display_dataframe(frame)
    normalized_emphasis: set[str] = set()
    normalized_secondary: set[str] = set()
    return _render_html_table(formatted, normalized_emphasis, normalized_secondary)


def render_highlight_table(
    frame: pd.DataFrame,
    *,
    emphasis_columns: list[str] | None = None,
    secondary_columns: list[str] | None = None,
) -> str:
    formatted = format_display_dataframe(frame)
    normalized_emphasis = {_normalize_column_name(column) for column in emphasis_columns or []}
    normalized_secondary = {_normalize_column_name(column) for column in secondary_columns or []}
    return _render_html_table(formatted, normalized_emphasis, normalized_secondary)


def format_metric_value(value: object) -> str:
    formatted = _format_value(value)
    return "No data" if formatted is None else str(formatted)


def _format_value(value: object) -> object:
    if value is None or (isinstance(value, float) and pd.isna(value)) or value is pd.NaT:
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%m-%d-%Y")
    if isinstance(value, (datetime, date)):
        return value.strftime("%m-%d-%Y")
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else f"{value:.2f}"
    return value


def _reorder_comma_name(name: str) -> str:
    if "," not in name:
        return name
    last_name, first_name = [piece.strip() for piece in name.split(",", 1)]
    if not first_name:
        return last_name
    return f"{first_name} {last_name}"


def _normalize_column_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _render_html_table(
    frame: pd.DataFrame,
    emphasis_columns: set[str],
    secondary_columns: set[str],
) -> str:
    header_html = []
    for column in frame.columns:
        header_html.append(f"<th>{escape(str(column))}</th>")

    body_rows = []
    for _, row in frame.iterrows():
        row_cells = []
        for column, value in row.items():
            cell_classes = []
            normalized = _normalize_column_name(str(column))
            if normalized in emphasis_columns:
                cell_classes.append("cell-emphasis")
            if normalized in secondary_columns:
                cell_classes.append("cell-secondary")
            if normalized in {"rank", "hr", "hrnumber"}:
                cell_classes.append("cell-rank")
            class_attr = f" class='{' '.join(cell_classes)}'" if cell_classes else ""
            display_value = "&mdash;" if value is None or value is pd.NA else escape(str(value))
            row_cells.append(f"<td{class_attr}>{display_value}</td>")
        body_rows.append(f"<tr>{''.join(row_cells)}</tr>")

    return (
        "<div class='phillies-table-wrap'>"
        "<table class='phillies-table'>"
        f"<thead><tr>{''.join(header_html)}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table>"
        "</div>"
    )
