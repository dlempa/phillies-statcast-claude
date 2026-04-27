from __future__ import annotations

import calendar
import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from phillies_stats.config import get_config
from phillies_stats.database import get_connection, initialize_database
from phillies_stats.display import format_metric_value, render_highlight_table
from phillies_stats.queries import get_month_options, get_player_options, get_top_longest_home_runs
from phillies_stats.ui import (
    apply_app_theme,
    render_filter_caption,
    render_leader_headshot_card,
    render_page_header,
    render_section_heading,
    render_skeleton_block,
    render_stat_cards,
)


config = get_config()
conn = get_connection(config.db_path)
initialize_database(conn)
apply_app_theme()

render_page_header(
    "Longest Home Runs",
    "The flagship leaderboard is generated live from stored Statcast events, so fresh data can move the top 10 automatically.",
    eyebrow="Hitter Stats",
)

players = get_player_options(conn)
months = get_month_options(conn)
month_options = {"All months": None, **{calendar.month_name[month]: month for month in months}}

with st.container(border=True):
    render_section_heading("Filters", "Refine the live leaderboard by hitter, month, or home and away split.")
    render_filter_caption("Live leaderboard filters")
    filter_col_1, filter_col_2, filter_col_3 = st.columns(3)
    player_filter = filter_col_1.selectbox("Player", options=["All players"] + players, key="longest_hr_player_filter")
    month_filter = filter_col_2.selectbox("Month", options=list(month_options.keys()), key="longest_hr_month_filter")
    home_away_filter = filter_col_3.selectbox("Home / Away", options=["All", "Home", "Away"], key="longest_hr_home_away_filter")

results = get_top_longest_home_runs(
    conn,
    limit=10,
    player=None if player_filter == "All players" else player_filter,
    month=month_options[month_filter],
    home_away=None if home_away_filter == "All" else home_away_filter,
)

if results.empty:
    render_skeleton_block(
        "No home runs match the current filters yet — leaderboard preview", kind="table"
    )
else:
    leader = results.iloc[0]
    cutline = results["distance_ft"].min()
    average_distance = results["distance_ft"].mean()
    represented_players = results["player_name"].nunique()

    leader_mlbam_id = None
    leader_batter_id = leader.get("batter_id") if hasattr(leader, "get") else None
    if leader_batter_id is not None and not (isinstance(leader_batter_id, float) and leader_batter_id != leader_batter_id):
        try:
            leader_mlbam_id = int(leader_batter_id)
        except (TypeError, ValueError):
            leader_mlbam_id = None

    render_leader_headshot_card(
        mlbam_id=leader_mlbam_id,
        eyebrow="Current #1",
        name=str(leader["player_name"]),
        line=f"{format_metric_value(leader['distance_ft'])} ft · {format_metric_value(leader['exit_velocity_mph'])} mph",
    )

    render_stat_cards(
        [
            {
                "label": "Current #1",
                "value": f"{format_metric_value(leader['distance_ft'])} ft",
                "helper": str(leader["player_name"]),
                "tone": "accent",
            },
            {
                "label": "Top 10 cutline",
                "value": f"{format_metric_value(cutline)} ft",
                "helper": "Current distance to stay on the board",
            },
            {
                "label": "Average distance",
                "value": f"{format_metric_value(average_distance)} ft",
                "helper": "Across the current filtered leaderboard",
            },
            {
                "label": "Players represented",
                "value": format_metric_value(represented_players),
                "helper": "Different Phillies hitters on the board",
            },
        ]
    )

    with st.container(border=True):
        render_section_heading(
            "Top 10 Leaderboard",
            "Rank, player, and distance are visually prioritized, while exit velocity and launch angle stay secondary.",
        )
        display = results.drop(columns=["batter_id"], errors="ignore").rename(
            columns={
                "rank": "Rank",
                "player_name": "Player",
                "game_date": "Date",
                "opponent": "Opponent",
                "venue_name": "Ballpark",
                "home_away": "Home/Away",
                "distance_ft": "Distance (ft)",
                "exit_velocity_mph": "Exit Velocity (mph)",
                "launch_angle": "Launch Angle",
            }
        )
        st.html(
            render_highlight_table(
                display,
                emphasis_columns=["Rank", "Player", "Distance (ft)"],
                secondary_columns=["Exit Velocity (mph)", "Launch Angle", "Home/Away"],
            ),
        )
