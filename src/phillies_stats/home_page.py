from __future__ import annotations

import streamlit as st

from phillies_stats.config import get_config
from phillies_stats.database import get_connection, initialize_database
from phillies_stats.queries import get_last_updated, get_latest_team_state_summary
from phillies_stats.ui import (
    apply_app_theme,
    format_timestamp,
    render_page_header,
    render_section_heading,
    render_team_state_summary,
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

    if not last_updated:
        st.info("No Statcast data is loaded yet. Run the bootstrap script to backfill the season.")

    render_team_state_summary(get_latest_team_state_summary(conn, season=config.season))

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
            st.caption("MLB Stats API · DuckDB · Streamlit · Python · ChatGPT Codex · GitHub Actions")
