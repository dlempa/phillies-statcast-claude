from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from phillies_stats.config import get_config
from phillies_stats.database import get_connection, initialize_database
from phillies_stats.queries import (
    get_last_updated,
    get_latest_team_state_summary,
    get_team_local_summary,
    get_team_rolling_run_trend,
)
from phillies_stats.ui import (
    PHILLIES_RED,
    PHILLIES_TEXT,
    apply_app_theme,
    format_timestamp,
    render_page_header,
    render_section_heading,
    render_skeleton_block,
    render_stat_cards,
    render_team_state_summary,
    style_chart,
)


def render_home(team_stats_page, batter_stats_page, pitcher_stats_page) -> None:
    config = get_config()
    conn = get_connection(config.db_path)
    initialize_database(conn)
    apply_app_theme()

    last_updated = get_last_updated(conn)
    render_page_header(
        "Phillies Statcast 2026",
        "A Phillies stats app for checking the team picture, digging into hitters, and exploring the pitching staff with public baseball data that updates automatically each day.",
        eyebrow=f"{config.season} Season",
        meta=f"Last updated {format_timestamp(last_updated)}" if last_updated else None,
        show_mark=True,
    )

    state_summary = get_latest_team_state_summary(conn, season=config.season)

    if not last_updated:
        render_skeleton_block(
            "Statcast data will appear once the daily refresh runs", kind="cards"
        )
        render_skeleton_block("Team form chart loading", kind="chart")
    else:
        render_team_state_summary(state_summary, as_hero=True)
        _render_hero_metric_cards(conn)
        _render_team_form_chart(conn)

    render_section_heading("Explore The Data")
    team_col, batter_col, pitcher_col = st.columns(3)

    with team_col:
        with st.container(border=True):
            render_section_heading(
                "Team Stats",
                "Record, streak, NL East standings, team ranks, recent results, and run differential.",
            )
            if st.button("Open Team Stats", key="home_team_stats", use_container_width=True):
                st.switch_page(team_stats_page)

    with batter_col:
        with st.container(border=True):
            render_section_heading(
                "Batter Stats",
                "Home run leaderboards, power stats, hitter profiles, and game-level offensive notes.",
            )
            if st.button("Open Batter Stats", key="home_batter_stats", use_container_width=True):
                st.switch_page(batter_stats_page)

    with pitcher_col:
        with st.container(border=True):
            render_section_heading(
                "Pitcher Stats",
                "Strikeouts, velocity, pitcher profiles, outcomes, and season pitching context.",
            )
            if st.button("Open Pitcher Stats", key="home_pitcher_stats", use_container_width=True):
                st.switch_page(pitcher_stats_page)

    st.divider()

    about_col, pipeline_col, tools_col = st.columns([1.25, 1.1, 0.95])

    with about_col:
        with st.container(border=True):
            render_section_heading("About This Page")
            st.caption(
                "This started in March 2026 as a fun side project and a way to experiment with AI tools while building something I would actually want to use. "
                "It brings together a few interests that tend to overlap for me: the Phillies, sports stats, product thinking, and building small apps that get better over time."
            )

    with pipeline_col:
        with st.container(border=True):
            render_section_heading("Data Pipeline")
            st.caption(
                "The app pulls public MLB data, reshapes it into Phillies-focused tables, and stores the results in DuckDB. "
                "A scheduled GitHub Actions job refreshes the database daily, so the Streamlit app can read from a ready-to-query local data store."
            )

    with tools_col:
        with st.container(border=True):
            render_section_heading("Tools Used")
            st.caption("MLB Stats API · DuckDB · Streamlit · Python · Claude Code · GitHub Actions")


def _render_hero_metric_cards(conn) -> None:
    summary = get_team_local_summary(conn)
    record = summary.get("record") or "0-0"
    streak = summary.get("streak") or "—"
    run_diff = summary.get("run_differential")
    if isinstance(run_diff, (int, float)):
        run_diff_value = f"{int(run_diff):+d}"
    else:
        run_diff_value = "—"
    runs_for = summary.get("runs_for")
    runs_against = summary.get("runs_against")
    helper = (
        f"{runs_for} for vs {runs_against} against"
        if isinstance(runs_for, (int, float)) and isinstance(runs_against, (int, float))
        else "Run differential"
    )
    render_stat_cards(
        [
            {"label": "Record", "value": str(record), "tone": "accent", "helper": "Season W-L"},
            {"label": "Streak", "value": str(streak), "tone": "accent", "helper": "Current run of results"},
            {"label": "Run Differential", "value": run_diff_value, "tone": "accent", "helper": helper},
        ],
        columns=3,
    )


def _render_team_form_chart(conn, *, window: int = 10) -> None:
    frame = get_team_rolling_run_trend(conn, window=window)
    with st.container(border=True):
        render_section_heading(
            f"Last {window} Games — Team Form",
            f"Rolling {window}-game average of runs scored and runs allowed.",
        )
        if frame.empty or frame[["rolling_runs_for", "rolling_runs_against"]].dropna(how="all").empty:
            render_skeleton_block(
                f"Team form chart will appear after {window} games are logged", kind="chart"
            )
            return

        long = pd.melt(
            frame,
            id_vars=["game_date"],
            value_vars=["rolling_runs_for", "rolling_runs_against"],
            var_name="metric_key",
            value_name="rolling_runs",
        )
        long["metric"] = long["metric_key"].map(
            {"rolling_runs_for": "Runs scored", "rolling_runs_against": "Runs allowed"}
        )
        long = long.dropna(subset=["rolling_runs"])
        chart = style_chart(
            alt.Chart(long)
            .mark_line(point=True, strokeWidth=3)
            .encode(
                x=alt.X("game_date:T", title="Game date"),
                y=alt.Y("rolling_runs:Q", title=f"Rolling {window}-game runs / game"),
                color=alt.Color(
                    "metric:N",
                    scale=alt.Scale(
                        domain=["Runs scored", "Runs allowed"],
                        range=[PHILLIES_RED, PHILLIES_TEXT],
                    ),
                    title=None,
                ),
                tooltip=[
                    alt.Tooltip("game_date:T", title="Date"),
                    alt.Tooltip("metric:N", title="Metric"),
                    alt.Tooltip("rolling_runs:Q", title="Rolling avg", format=".2f"),
                ],
            ),
            height=240,
        )
        st.altair_chart(chart, use_container_width=True)
