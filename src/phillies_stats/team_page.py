from __future__ import annotations

import altair as alt
import streamlit as st

from phillies_stats.config import get_config
from phillies_stats.database import get_connection, initialize_database
from phillies_stats.display import render_highlight_table
from phillies_stats.queries import (
    get_last_updated,
    get_nl_east_standings,
    get_phillies_team_rankings,
    get_team_local_summary,
    get_team_recent_results,
    get_team_run_differential_trend,
)
from phillies_stats.ui import (
    PHILLIES_RED,
    apply_app_theme,
    format_card,
    format_timestamp,
    render_page_header,
    render_section_heading,
    render_skeleton_block,
    render_stat_cards,
    style_chart,
)


def render_team_stats_page() -> None:
    config = get_config()
    conn = get_connection(config.db_path)
    initialize_database(conn)
    apply_app_theme()

    local_summary = get_team_local_summary(conn)
    last_updated = get_last_updated(conn)

    render_page_header(
        "Team Stats",
        "Current Phillies team performance, NL East standings, and league ranking context in one daily snapshot.",
        eyebrow=f"Phillies {config.season}",
        meta=f"Last updated {format_timestamp(last_updated)}" if last_updated else None,
        show_mark=True,
    )

    render_stat_cards(
        [
            {
                "label": "Record",
                "value": str(local_summary["record"]),
                "tone": "accent",
            },
            {
                "label": "Current streak",
                "value": str(local_summary["streak"]),
            },
            {
                "label": "Run differential",
                "value": _signed_number(local_summary["run_differential"]),
            },
            {
                "label": "Games played",
                "value": format_card(int(local_summary["wins"]) + int(local_summary["losses"])),
            },
        ]
    )

    standings = get_nl_east_standings(conn, season=config.season)
    rankings = get_phillies_team_rankings(conn, season=config.season)
    recent_results = get_team_recent_results(conn, limit=10)
    run_diff_trend = get_team_run_differential_trend(conn)

    standings_col, ranks_col = st.columns([1.1, 1])

    with standings_col:
        with st.container(border=True):
            render_section_heading("NL East Standings")
            if standings.empty:
                render_skeleton_block(
                    "NL East standings will appear after the team context refresh runs", kind="table"
                )
            else:
                display = standings.rename(
                    columns={
                        "division_rank": "Rank",
                        "team_abbr": "Team",
                        "team_name": "Club",
                        "wins": "W",
                        "losses": "L",
                        "winning_percentage": "PCT",
                        "games_back": "GB",
                        "runs_scored": "RS",
                        "runs_allowed": "RA",
                        "run_differential": "Diff",
                        "streak": "Streak",
                    }
                )
                st.html(
                    render_highlight_table(
                        display[["Rank", "Team", "Club", "W", "L", "PCT", "GB", "Diff", "Streak"]],
                        emphasis_columns=["Rank", "Team", "W", "L", "GB"],
                        secondary_columns=["PCT", "Diff", "Streak"],
                    )
                )

    with ranks_col:
        with st.container(border=True):
            render_section_heading("Phillies League Ranks")
            if rankings.empty:
                st.info("Team rankings will appear after the team context refresh runs.")
            else:
                st.html(
                    render_highlight_table(
                        rankings,
                        emphasis_columns=["Stat", "Value"],
                        secondary_columns=["NL Rank", "MLB Rank"],
                    )
                )

    trend_col, results_col = st.columns([1.15, 1])

    with trend_col:
        with st.container(border=True):
            render_section_heading("Run Differential Trend", "Cumulative run differential across completed Phillies games.")
            if run_diff_trend.empty:
                st.info("Run differential trend will appear after game data is loaded.")
            else:
                chart = style_chart(
                    alt.Chart(run_diff_trend)
                    .mark_line(point=True, strokeWidth=2.5, color=PHILLIES_RED)
                    .encode(
                        x=alt.X("game_date:T", title="Date"),
                        y=alt.Y("cumulative_run_differential:Q", title="Cumulative Run Differential"),
                        tooltip=[
                            alt.Tooltip("game_date:T", title="Date", format="%m-%d-%Y"),
                            alt.Tooltip("opponent:N", title="Opponent"),
                            alt.Tooltip("result_text:N", title="Result"),
                            alt.Tooltip("game_run_differential:Q", title="Game Diff"),
                            alt.Tooltip("cumulative_run_differential:Q", title="Season Diff"),
                        ],
                    ),
                    height=340,
                )
                st.altair_chart(chart, use_container_width=True)

    with results_col:
        with st.container(border=True):
            render_section_heading("Recent Results", "The latest completed games from the local DuckDB game log.")
            if recent_results.empty:
                st.info("Recent results will appear after game data is loaded.")
            else:
                display = recent_results.rename(
                    columns={
                        "game_date": "Date",
                        "opponent": "Opponent",
                        "result_text": "Result",
                        "runs_for": "RF",
                        "runs_against": "RA",
                    }
                )
                st.html(
                    render_highlight_table(
                        display,
                        emphasis_columns=["Date", "Opponent", "Result"],
                        secondary_columns=["RF", "RA"],
                    )
                )


def _signed_number(value: object) -> str:
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return "No data"
    return f"+{numeric}" if numeric > 0 else str(numeric)
