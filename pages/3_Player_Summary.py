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
from phillies_stats.display import render_highlight_table
from phillies_stats.queries import get_player_options, get_player_summary
from phillies_stats.ui import (
    PHILLIES_RED,
    apply_app_theme,
    format_card,
    render_filter_caption,
    render_page_header,
    render_profile_header,
    render_section_heading,
    render_skeleton_block,
    render_stat_cards,
    style_chart,
)


config = get_config()
conn = get_connection(config.db_path)
initialize_database(conn)
apply_app_theme()

render_page_header(
    "Hitter Profiles",
    "Choose a Phillies hitter to view a cleaner player profile with home run logs, power context, and seasonal splits.",
    eyebrow="Hitter Stats",
)

players = get_player_options(conn)
if not players:
    render_skeleton_block(
        "Hitter profiles will appear once Statcast data is loaded", kind="cards"
    )
else:
    with st.container(border=True):
        render_section_heading("Select a Hitter", "Pick a Phillies hitter to switch the profile view.")
        render_filter_caption("Profile selector")
        selected_player = st.selectbox("Hitter", options=players, label_visibility="collapsed", key="hitter_profile_selector")

    player_summary = get_player_summary(conn, selected_player)
    summary = player_summary["summary"]
    monthly = player_summary["monthly"]
    home_runs = player_summary["home_runs"]
    league_context = player_summary["league_context"]
    mlbam_id = player_summary.get("mlbam_id")

    if not summary:
        with st.container(border=True):
            render_profile_header(
                selected_player,
                "Current-season MLB context for the selected Phillies hitter.",
                chip="Hitter Profile",
                mlbam_id=mlbam_id,
            )
            render_section_heading("League Context Ratings", "Current-season production compared with MLB hitter baselines.")
            st.html(
                render_highlight_table(
                    league_context,
                    emphasis_columns=["Stat", "Rating Tier"],
                    secondary_columns=["League Percentile", "MLB Qualified?"],
                ),
            )
    else:
        hr_count, longest_hr, average_hr, hardest_hit_ball = summary

        with st.container(border=True):
            render_profile_header(
                selected_player,
                "Home run production, distance, and quality of contact for the current season.",
                chip="Hitter Profile",
                mlbam_id=mlbam_id,
            )
            render_stat_cards(
                [
                    {"label": "Home runs", "value": format_card(hr_count), "tone": "accent"},
                    {"label": "Longest HR", "value": format_card(longest_hr, " ft")},
                    {"label": "Average HR distance", "value": format_card(average_hr, " ft")},
                    {"label": "Hardest-hit ball", "value": format_card(hardest_hit_ball, " mph")},
                ]
            )

        with st.container(border=True):
            render_section_heading("League Context Ratings", "Current-season production compared with MLB hitter baselines.")
            st.html(
                render_highlight_table(
                    league_context,
                    emphasis_columns=["Stat", "Rating Tier"],
                    secondary_columns=["League Percentile", "MLB Qualified?"],
                ),
            )

        overview_tab, home_runs_tab, power_tab, splits_tab = st.tabs(["Overview", "Home Runs", "Power", "Splits"])

        with overview_tab:
            left_panel, right_panel = st.columns(2)

            with left_panel:
                with st.container(border=True):
                    render_section_heading("Recent Home Runs", "The latest balls this hitter has put over the wall.")
                    if home_runs.empty:
                        st.info("No home runs are available for this player yet.")
                    else:
                        recent_home_runs = (
                            home_runs.sort_values(["home_run_number"], ascending=False)
                            .head(5)
                            .rename(
                                columns={
                                    "home_run_number": "HR #",
                                    "game_date": "Date",
                                    "opponent": "Opponent",
                                    "distance_ft": "Distance (ft)",
                                    "exit_velocity_mph": "Exit Velocity (mph)",
                                }
                            )
                        )
                        st.html(
                            render_highlight_table(
                                recent_home_runs[["HR #", "Date", "Opponent", "Distance (ft)", "Exit Velocity (mph)"]],
                                emphasis_columns=["HR #", "Distance (ft)"],
                                secondary_columns=["Date", "Opponent"],
                            ),
                        )

            with right_panel:
                with st.container(border=True):
                    render_section_heading("Season Power Snapshot", "This player's biggest home runs by distance.")
                    if home_runs.empty:
                        st.info("No power snapshot is available for this player yet.")
                    else:
                        power_snapshot = (
                            home_runs.sort_values(["distance_ft", "exit_velocity_mph"], ascending=[False, False])
                            .head(5)
                            .rename(
                                columns={
                                    "home_run_number": "HR #",
                                    "game_date": "Date",
                                    "distance_ft": "Distance (ft)",
                                    "exit_velocity_mph": "Exit Velocity (mph)",
                                    "launch_angle": "Launch Angle",
                                }
                            )
                        )
                        st.html(
                            render_highlight_table(
                                power_snapshot[["HR #", "Date", "Distance (ft)", "Exit Velocity (mph)", "Launch Angle"]],
                                emphasis_columns=["Distance (ft)", "Exit Velocity (mph)"],
                                secondary_columns=["Launch Angle", "Date"],
                            ),
                        )

        with home_runs_tab:
            with st.container(border=True):
                render_section_heading("All Home Runs", "Ordered by the sequence in which this player hit them during the season.")
                if home_runs.empty:
                    st.info("No home runs are available for this player yet.")
                else:
                    display_home_runs = home_runs.rename(
                        columns={
                            "home_run_number": "HR #",
                            "game_date": "Date",
                            "opponent": "Opponent",
                            "venue_name": "Ballpark",
                            "distance_ft": "Distance (ft)",
                            "exit_velocity_mph": "Exit Velocity (mph)",
                            "launch_angle": "Launch Angle",
                        }
                    )
                    st.html(
                        render_highlight_table(
                            display_home_runs,
                            emphasis_columns=["HR #", "Date", "Distance (ft)"],
                            secondary_columns=["Exit Velocity (mph)", "Launch Angle"],
                        ),
                    )

        with power_tab:
            power_cards = [
                {"label": "Longest HR", "value": format_card(longest_hr, " ft"), "tone": "accent"},
                {"label": "Average HR distance", "value": format_card(average_hr, " ft")},
                {"label": "Hardest-hit ball", "value": format_card(hardest_hit_ball, " mph")},
            ]
            render_stat_cards(power_cards, columns=3)

            power_left, power_right = st.columns(2)

            with power_left:
                with st.container(border=True):
                    render_section_heading("Longest Home Runs", "The biggest measured shots from this hitter.")
                    if home_runs.empty:
                        st.info("No home run data is available yet.")
                    else:
                        longest_home_runs = (
                            home_runs.sort_values(["distance_ft", "exit_velocity_mph"], ascending=[False, False])
                            .head(8)
                            .rename(
                                columns={
                                    "home_run_number": "HR #",
                                    "game_date": "Date",
                                    "opponent": "Opponent",
                                    "distance_ft": "Distance (ft)",
                                    "exit_velocity_mph": "Exit Velocity (mph)",
                                }
                            )
                        )
                        st.html(
                            render_highlight_table(
                                longest_home_runs[["HR #", "Date", "Opponent", "Distance (ft)", "Exit Velocity (mph)"]],
                                emphasis_columns=["Distance (ft)", "HR #"],
                                secondary_columns=["Date", "Opponent"],
                            ),
                        )

            with power_right:
                with st.container(border=True):
                    render_section_heading("Hardest-hit Home Runs", "The home runs this hitter hit the hardest.")
                    if home_runs.empty:
                        st.info("No home run data is available yet.")
                    else:
                        hardest_home_runs = (
                            home_runs.sort_values(["exit_velocity_mph", "distance_ft"], ascending=[False, False])
                            .head(8)
                            .rename(
                                columns={
                                    "home_run_number": "HR #",
                                    "game_date": "Date",
                                    "opponent": "Opponent",
                                    "distance_ft": "Distance (ft)",
                                    "exit_velocity_mph": "Exit Velocity (mph)",
                                }
                            )
                        )
                        st.html(
                            render_highlight_table(
                                hardest_home_runs[["HR #", "Date", "Opponent", "Exit Velocity (mph)", "Distance (ft)"]],
                                emphasis_columns=["Exit Velocity (mph)", "HR #"],
                                secondary_columns=["Date", "Opponent"],
                            ),
                        )

        with splits_tab:
            split_left, split_right = st.columns([1.2, 1])

            with split_left:
                with st.container(border=True):
                    render_section_heading("Monthly Home Run Breakdown", "Month-by-month home run totals for this hitter.")
                    if monthly.empty:
                        st.info("No monthly home run totals are available yet.")
                    else:
                        monthly_chart = monthly.copy()
                        chart = style_chart(
                            alt.Chart(monthly_chart)
                            .mark_bar(size=42, color=PHILLIES_RED)
                            .encode(
                                x=alt.X("month_name:N", title="Month", sort=monthly_chart["month_name"].tolist()),
                                y=alt.Y("home_run_count:Q", title="Home Runs"),
                                tooltip=[
                                    alt.Tooltip("month_name:N", title="Month"),
                                    alt.Tooltip("home_run_count:Q", title="Home Runs"),
                                ],
                            ),
                            height=320,
                        )
                        st.altair_chart(chart, use_container_width=True)

            with split_right:
                with st.container(border=True):
                    render_section_heading("Monthly Table", "The same split view in a compact table.")
                    if monthly.empty:
                        st.info("No monthly breakdown is available yet.")
                    else:
                        chart_data = monthly.rename(columns={"month_name": "Month", "home_run_count": "Home Runs"})[
                            ["Month", "Home Runs"]
                        ]
                        st.html(
                            render_highlight_table(
                                chart_data,
                                emphasis_columns=["Month", "Home Runs"],
                            ),
                        )
