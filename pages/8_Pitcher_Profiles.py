from __future__ import annotations

import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from phillies_stats.config import get_config
from phillies_stats.database import get_connection, initialize_database
from phillies_stats.display import render_highlight_table
from phillies_stats.queries import get_pitcher_options, get_pitcher_profile
from phillies_stats.ui import (
    PHILLIES_RED,
    apply_app_theme,
    format_card,
    render_filter_caption,
    render_page_header,
    render_profile_header,
    render_section_heading,
    render_stat_cards,
    style_chart,
)


config = get_config()
conn = get_connection(config.db_path)
initialize_database(conn)
apply_app_theme()

render_page_header(
    "Pitcher Profiles",
    "A cleaner pitcher profile built around role, season totals, velocity, outcomes, and supporting split views.",
    eyebrow="Pitcher Stats",
)

pitchers = get_pitcher_options(conn)
if not pitchers:
    st.info("No Phillies pitcher profiles are available yet.")
else:
    with st.container(border=True):
        render_section_heading("Select a Pitcher", "Choose a Phillies pitcher to update the profile below.")
        render_filter_caption("Pitcher selector")
        selected_pitcher = st.selectbox("Pitcher", options=pitchers, label_visibility="collapsed", key="pitcher_profile_selector")

    profile = get_pitcher_profile(conn, selected_pitcher)
    summary = profile["summary"]
    league_context = profile["league_context"]

    if not summary:
        with st.container(border=True):
            render_profile_header(
                selected_pitcher,
                "Current-season MLB context for the selected Phillies pitcher.",
                chip="Pitcher",
            )
            render_section_heading("League Context Ratings", "Current-season run prevention and strikeout rates compared with MLB role baselines.")
            st.html(
                render_highlight_table(
                    league_context,
                    emphasis_columns=["Stat", "Rating Tier"],
                    secondary_columns=["League Percentile", "MLB Qualified?"],
                ),
            )
    else:
        (
            wins,
            losses,
            innings_pitched,
            strikeouts,
            walks_issued,
            home_runs_allowed,
            era,
            whip,
            max_velocity,
            avg_fastball_velocity,
            whiffs,
            hardest_hit_allowed,
            appearances,
            games_started,
            saves,
            position,
        ) = summary

        with st.container(border=True):
            render_profile_header(
                selected_pitcher,
                "Season totals, velocity context, and split views for Phillies pitching performance.",
                chip=str(position) if position else "Pitcher",
            )
            render_stat_cards(
                [
                    {"label": "Wins", "value": format_card(wins), "tone": "accent"},
                    {"label": "Innings pitched", "value": format_card(innings_pitched)},
                    {"label": "Strikeouts", "value": format_card(strikeouts)},
                    {"label": "ERA", "value": format_card(era)},
                ]
            )

        with st.container(border=True):
            render_section_heading("League Context Ratings", "Current-season run prevention and strikeout rates compared with MLB role baselines.")
            st.html(
                render_highlight_table(
                    league_context,
                    emphasis_columns=["Stat", "Rating Tier"],
                    secondary_columns=["League Percentile", "MLB Qualified?"],
                ),
            )

        overview_tab, velocity_tab, outcomes_tab, splits_tab = st.tabs(["Overview", "Velocity", "Outcomes", "Splits"])

        with overview_tab:
            overview_left, overview_right = st.columns([1.05, 1])

            with overview_left:
                with st.container(border=True):
                    render_section_heading("Overview", "The core season numbers that define the profile.")
                    render_stat_cards(
                        [
                            {"label": "Losses", "value": format_card(losses)},
                            {"label": "WHIP", "value": format_card(whip)},
                            {"label": "Walks", "value": format_card(walks_issued)},
                            {"label": "HR allowed", "value": format_card(home_runs_allowed)},
                            {"label": "Games started", "value": format_card(games_started)},
                            {"label": "Appearances", "value": format_card(appearances)},
                            {"label": "Saves", "value": format_card(saves)},
                            {"label": "Role", "value": str(position) if position else "No data"},
                        ],
                        columns=4,
                    )

            with overview_right:
                with st.container(border=True):
                    render_section_heading("Pitch Usage Mix", "How the selected pitcher distributes the arsenal.")
                    pitch_usage = profile["pitch_usage"]
                    if pitch_usage.empty:
                        st.info("Pitch usage data is not available for this pitcher yet.")
                    else:
                        usage_chart = style_chart(
                            alt.Chart(pitch_usage)
                            .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6, color=PHILLIES_RED)
                            .encode(
                                x=alt.X("pitch_name:N", title="Pitch Type", sort="-y"),
                                y=alt.Y("usage_pct:Q", title="Usage %"),
                                tooltip=[
                                    alt.Tooltip("pitch_name:N", title="Pitch Type"),
                                    alt.Tooltip("pitch_count:Q", title="Pitch Count"),
                                    alt.Tooltip("usage_pct:Q", title="Usage %", format=".2f"),
                                ],
                            ),
                            height=300,
                        )
                        st.altair_chart(usage_chart, use_container_width=True)
                        st.html(
                            render_highlight_table(
                                pitch_usage.rename(
                                    columns={
                                        "pitch_name": "Pitch Type",
                                        "pitch_count": "Pitch Count",
                                        "usage_pct": "Usage %",
                                    }
                                ),
                                emphasis_columns=["Pitch Type", "Usage %"],
                                secondary_columns=["Pitch Count"],
                            ),
                        )

        with velocity_tab:
            render_stat_cards(
                [
                    {"label": "Max velocity", "value": format_card(max_velocity, " mph"), "tone": "accent"},
                    {"label": "Avg fastball", "value": format_card(avg_fastball_velocity, " mph")},
                    {"label": "Whiffs", "value": format_card(whiffs)},
                    {"label": "Games started", "value": format_card(games_started)},
                ]
            )

            with st.container(border=True):
                render_section_heading("Fastest Pitches", "The highest velocity pitches from this pitcher so far.")
                fastest_pitches = profile["fastest_pitches"]
                if fastest_pitches.empty:
                    st.info("No pitch-level velocity data is available for this pitcher yet.")
                else:
                    st.html(
                        render_highlight_table(
                            fastest_pitches.rename(
                                columns={
                                    "game_date": "Date",
                                    "opponent": "Opponent",
                                    "pitch_name": "Pitch Type",
                                    "release_speed": "Velocity (mph)",
                                }
                            ),
                            emphasis_columns=["Date", "Velocity (mph)"],
                            secondary_columns=["Opponent", "Pitch Type"],
                        ),
                    )

        with outcomes_tab:
            render_stat_cards(
                [
                    {"label": "Walks", "value": format_card(walks_issued), "tone": "accent"},
                    {"label": "HR allowed", "value": format_card(home_runs_allowed)},
                    {"label": "ERA", "value": format_card(era)},
                    {"label": "WHIP", "value": format_card(whip)},
                    {"label": "Whiffs", "value": format_card(whiffs)},
                    {"label": "Hardest-hit allowed", "value": format_card(hardest_hit_allowed, " mph")},
                ],
                columns=3,
            )

            with st.container(border=True):
                render_section_heading("Outcome Snapshot", "The main run-prevention and contact-quality outcomes for this pitcher.")
                outcome_table = pd.DataFrame(
                    [
                    {"Metric": "Wins", "Value": format_card(wins)},
                    {"Metric": "Losses", "Value": format_card(losses)},
                    {"Metric": "Innings Pitched", "Value": format_card(innings_pitched)},
                    {"Metric": "Strikeouts", "Value": format_card(strikeouts)},
                    {"Metric": "Walks", "Value": format_card(walks_issued)},
                    {"Metric": "Home Runs Allowed", "Value": format_card(home_runs_allowed)},
                    {"Metric": "ERA", "Value": format_card(era)},
                    {"Metric": "WHIP", "Value": format_card(whip)},
                    {"Metric": "Saves", "Value": format_card(saves)},
                    ]
                )
                st.html(
                    render_highlight_table(
                        outcome_table,
                        emphasis_columns=["Metric", "Value"],
                    ),
                )

        with splits_tab:
            split_left, split_right = st.columns([1.15, 1])

            with split_left:
                with st.container(border=True):
                    render_section_heading("Strikeouts By Month", "A month-by-month strikeout progression for this pitcher.")
                    strikeouts_by_month = profile["strikeouts_by_month"]
                    if strikeouts_by_month.empty:
                        st.info("No monthly strikeout data is available for this pitcher yet.")
                    else:
                        month_chart = style_chart(
                            alt.Chart(strikeouts_by_month)
                            .mark_bar(size=42, color=PHILLIES_RED)
                            .encode(
                                x=alt.X("month_start:T", title="Month"),
                                y=alt.Y("strikeouts:Q", title="Strikeouts"),
                                tooltip=[
                                    alt.Tooltip("month_start:T", title="Month", format="%b %Y"),
                                    alt.Tooltip("strikeouts:Q", title="Strikeouts"),
                                ],
                            ),
                            height=320,
                        )
                        st.altair_chart(month_chart, use_container_width=True)

            with split_right:
                with st.container(border=True):
                    render_section_heading("Strikeouts By Opponent", "How the selected pitcher's strikeouts stack up by opponent.")
                    strikeouts_by_opponent = profile["strikeouts_by_opponent"]
                    if strikeouts_by_opponent.empty:
                        st.info("No opponent strikeout data is available for this pitcher yet.")
                    else:
                        st.html(
                            render_highlight_table(
                                strikeouts_by_opponent.rename(columns={"opponent": "Opponent", "strikeouts": "Strikeouts"}),
                                emphasis_columns=["Opponent", "Strikeouts"],
                            ),
                        )
