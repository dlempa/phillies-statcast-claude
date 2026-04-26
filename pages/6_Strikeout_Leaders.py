from __future__ import annotations

import sys
from pathlib import Path

import altair as alt
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from phillies_stats.config import get_config
from phillies_stats.database import get_connection, initialize_database
from phillies_stats.display import format_metric_value, render_highlight_table
from phillies_stats.queries import (
    get_pitcher_options,
    get_pitcher_strikeout_leaders,
    get_pitcher_strikeouts_by_month,
    get_pitcher_strikeouts_by_opponent,
)
from phillies_stats.ui import PHILLIES_RED, apply_app_theme, render_filter_caption, render_page_header, render_section_heading, render_stat_cards, style_chart


config = get_config()
conn = get_connection(config.db_path)
initialize_database(conn)
apply_app_theme()

render_page_header(
    "Strikeout Leaders",
    "A cleaner leaderboard for Phillies strikeout production, with supporting monthly and opponent views for any pitcher on the staff.",
    eyebrow="Pitcher Stats",
)

leaders = get_pitcher_strikeout_leaders(conn, limit=20)
pitchers = get_pitcher_options(conn)

selected_pitcher = None
if pitchers:
    with st.container(border=True):
        render_section_heading("Select a Pitcher", "Choose a pitcher to update the supporting split views below.")
        render_filter_caption("Pitcher selector")
        selected_pitcher = st.selectbox("Pitcher", options=pitchers, label_visibility="collapsed", key="strikeout_leaders_pitcher_selector")

if leaders.empty:
    st.info("No Phillies pitcher strikeout data is available yet.")
else:
    strikeout_rate_leader = leaders.sort_values(
        ["strikeouts_per_appearance", "strikeouts"], ascending=[False, False]
    ).iloc[0]
    render_stat_cards(
        [
            {
                "label": "Strikeout leader",
                "value": format_metric_value(leaders.iloc[0]["strikeouts"]),
                "helper": str(leaders.iloc[0]["player_name"]),
                "tone": "accent",
            },
            {
                "label": "Best K per appearance",
                "value": format_metric_value(strikeout_rate_leader["strikeouts_per_appearance"]),
                "helper": str(strikeout_rate_leader["player_name"]),
            },
            {
                "label": "Pitchers tracked",
                "value": format_metric_value(leaders["player_name"].nunique()),
            },
            {
                "label": "Total strikeouts",
                "value": format_metric_value(leaders["strikeouts"].sum()),
            },
        ]
    )

    with st.container(border=True):
        render_section_heading("Strikeout Leaderboard")
        st.html(
            render_highlight_table(
                leaders.rename(
                    columns={
                        "player_name": "Pitcher",
                        "position": "Role",
                        "strikeouts": "Strikeouts",
                        "appearances": "Appearances",
                        "strikeouts_per_appearance": "K per Appearance",
                        "walks_issued": "Walks",
                        "home_runs_allowed": "HR Allowed",
                    }
                ),
                emphasis_columns=["Pitcher", "Strikeouts"],
                secondary_columns=["Role", "Appearances", "K per Appearance", "Walks", "HR Allowed"],
            ),
        )

if selected_pitcher:
    split_left, split_right = st.columns([1.15, 1])

    with split_left:
        with st.container(border=True):
            render_section_heading("Strikeouts By Month", "A seasonal rhythm view for the selected pitcher.")
            by_month = get_pitcher_strikeouts_by_month(conn, selected_pitcher)
            if by_month.empty:
                st.info("No monthly strikeout breakdown is available for this pitcher yet.")
            else:
                month_chart = by_month.copy()
                chart = style_chart(
                    alt.Chart(month_chart)
                    .mark_bar(size=44, color=PHILLIES_RED)
                    .encode(
                        x=alt.X("month_start:T", title="Month"),
                        y=alt.Y("strikeouts:Q", title="Strikeouts"),
                        tooltip=[
                            alt.Tooltip("player_name:N", title="Pitcher"),
                            alt.Tooltip("strikeouts:Q", title="Strikeouts"),
                            alt.Tooltip("month_start:T", title="Month", format="%b %Y"),
                        ],
                    ),
                    height=320,
                )
                st.altair_chart(chart, use_container_width=True)

    with split_right:
        with st.container(border=True):
            render_section_heading("Strikeouts By Opponent", "Which clubs the selected pitcher has struck out the most.")
            by_opponent = get_pitcher_strikeouts_by_opponent(conn, selected_pitcher)
            if by_opponent.empty:
                st.info("No opponent strikeout breakdown is available for this pitcher yet.")
            else:
                st.html(
                    render_highlight_table(
                        by_opponent.rename(columns={"opponent": "Opponent", "strikeouts": "Strikeouts"}),
                        emphasis_columns=["Opponent", "Strikeouts"],
                    ),
                )
