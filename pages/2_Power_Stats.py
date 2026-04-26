from __future__ import annotations

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
from phillies_stats.queries import (
    get_hardest_hit_balls,
    get_hardest_hit_home_runs,
    get_player_hr_distance_stats,
    get_shortest_home_runs,
)
from phillies_stats.ui import apply_app_theme, render_page_header, render_section_heading, render_stat_cards


config = get_config()
conn = get_connection(config.db_path)
initialize_database(conn)
apply_app_theme()

render_page_header(
    "Power Stats",
    "A polished look at the loudest contact, shortest wall-scrapers, and the hitters driving Phillies power this season.",
    eyebrow="Hitter Stats",
)

hardest_home_runs = get_hardest_hit_home_runs(conn).head(8)
shortest_home_runs = get_shortest_home_runs(conn).head(8)
distance_stats = get_player_hr_distance_stats(conn).head(8)
hardest_balls = get_hardest_hit_balls(conn).head(10)

card_data: list[dict[str, str]] = []
if not hardest_home_runs.empty:
    top_hard_hr = hardest_home_runs.iloc[0]
    card_data.append(
        {
            "label": "Hardest-hit HR",
            "value": f"{format_metric_value(top_hard_hr['exit_velocity_mph'])} mph",
            "helper": str(top_hard_hr["player_name"]),
            "tone": "accent",
        }
    )
if not shortest_home_runs.empty:
    wall_scraper = shortest_home_runs.iloc[0]
    card_data.append(
        {
            "label": "Shortest HR",
            "value": f"{format_metric_value(wall_scraper['distance_ft'])} ft",
            "helper": str(wall_scraper["player_name"]),
        }
    )
if not distance_stats.empty:
    distance_leader = distance_stats.iloc[0]
    card_data.append(
        {
            "label": "Top average HR distance",
            "value": f"{format_metric_value(distance_leader['avg_hr_distance_ft'])} ft",
            "helper": str(distance_leader["player_name"]),
        }
    )
if not hardest_balls.empty:
    hardest_ball = hardest_balls.iloc[0]
    card_data.append(
        {
            "label": "Hardest ball overall",
            "value": f"{format_metric_value(hardest_ball['exit_velocity_mph'])} mph",
            "helper": str(hardest_ball["player_name"]),
        }
    )

if card_data:
    render_stat_cards(card_data)

top_left, top_right = st.columns(2)

with top_left:
    with st.container(border=True):
        render_section_heading("Hardest-hit Home Runs", "The most violent Phillies home run contact this season.")
        if hardest_home_runs.empty:
            st.info("No home run data is available yet.")
        else:
            st.html(
                render_highlight_table(
                    hardest_home_runs.rename(
                        columns={
                            "player_name": "Player",
                            "game_date": "Date",
                            "opponent": "Opponent",
                            "venue_name": "Ballpark",
                            "distance_ft": "Distance (ft)",
                            "exit_velocity_mph": "Exit Velocity (mph)",
                            "launch_angle": "Launch Angle",
                        }
                    ),
                    emphasis_columns=["Player", "Exit Velocity (mph)"],
                    secondary_columns=["Launch Angle", "Ballpark"],
                ),
            )

with top_right:
    with st.container(border=True):
        render_section_heading("Shortest HRs", "Wall-scrapers and just-enough power shots.")
        if shortest_home_runs.empty:
            st.info("No home run data is available yet.")
        else:
            st.html(
                render_highlight_table(
                    shortest_home_runs.rename(
                        columns={
                            "player_name": "Player",
                            "game_date": "Date",
                            "opponent": "Opponent",
                            "venue_name": "Ballpark",
                            "distance_ft": "Distance (ft)",
                            "exit_velocity_mph": "Exit Velocity (mph)",
                            "launch_angle": "Launch Angle",
                        }
                    ),
                    emphasis_columns=["Player", "Distance (ft)"],
                    secondary_columns=["Launch Angle", "Ballpark"],
                ),
            )

bottom_left, bottom_right = st.columns(2)

with bottom_left:
    with st.container(border=True):
        render_section_heading("Player Distance Leaders", "Average and max home run distance by hitter.")
        if distance_stats.empty:
            st.info("Player distance summaries will appear after home run data is loaded.")
        else:
            st.html(
                render_highlight_table(
                    distance_stats.rename(
                        columns={
                            "player_name": "Player",
                            "home_run_count": "HRs",
                            "avg_hr_distance_ft": "Average HR Distance (ft)",
                            "max_hr_distance_ft": "Max HR Distance (ft)",
                        }
                    ),
                    emphasis_columns=["Player", "Max HR Distance (ft)"],
                    secondary_columns=["Average HR Distance (ft)"],
                ),
            )

with bottom_right:
    with st.container(border=True):
        render_section_heading("Hardest-hit Balls Overall", "Even outs count here, so the loudest contact still gets surfaced.")
        if hardest_balls.empty:
            st.info("Hard-hit ball data will appear once events are ingested.")
        else:
            st.html(
                render_highlight_table(
                    hardest_balls.rename(
                        columns={
                            "player_name": "Player",
                            "game_date": "Date",
                            "opponent": "Opponent",
                            "venue_name": "Ballpark",
                            "outcome": "Outcome",
                            "exit_velocity_mph": "Exit Velocity (mph)",
                            "launch_angle": "Launch Angle",
                            "distance_ft": "Distance (ft)",
                        }
                    ),
                    emphasis_columns=["Player", "Exit Velocity (mph)"],
                    secondary_columns=["Outcome", "Launch Angle", "Distance (ft)"],
                ),
            )
