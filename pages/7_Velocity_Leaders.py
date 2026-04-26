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
from phillies_stats.queries import get_fastest_pitches, get_pitcher_velocity_summary
from phillies_stats.ui import PHILLIES_RED, apply_app_theme, render_page_header, render_section_heading, render_stat_cards, style_chart


config = get_config()
conn = get_connection(config.db_path)
initialize_database(conn)
apply_app_theme()

render_page_header(
    "Velocity Leaders",
    "The pitching counterpart to the home run leaderboard: fastest pitches first, supported by broader max and average fastball velocity context.",
    eyebrow="Pitcher Stats",
)

fastest_pitches = get_fastest_pitches(conn, limit=10)
velocity_summary = get_pitcher_velocity_summary(conn, limit=20)

if fastest_pitches.empty and velocity_summary.empty:
    st.info("No Phillies pitch velocity data is available yet.")
else:
    cards: list[dict[str, str]] = []
    if not fastest_pitches.empty:
        fastest = fastest_pitches.iloc[0]
        cards.append(
            {
                "label": "Fastest pitch",
                "value": f"{format_metric_value(fastest['release_speed'])} mph",
                "helper": str(fastest["player_name"]),
                "tone": "accent",
            }
        )
    if not velocity_summary.empty:
        max_velocity_leader = velocity_summary.iloc[0]
        avg_fastball_leader = velocity_summary.sort_values(
            ["avg_fastball_velocity_mph", "max_velocity_mph"], ascending=[False, False]
        ).iloc[0]
        cards.extend(
            [
                {
                    "label": "Max velo leader",
                    "value": f"{format_metric_value(max_velocity_leader['max_velocity_mph'])} mph",
                    "helper": str(max_velocity_leader["player_name"]),
                },
                {
                    "label": "Avg fastball leader",
                    "value": f"{format_metric_value(avg_fastball_leader['avg_fastball_velocity_mph'])} mph",
                    "helper": str(avg_fastball_leader["player_name"]),
                },
                {
                    "label": "Pitchers 98+",
                    "value": format_metric_value((velocity_summary["max_velocity_mph"] >= 98).sum()),
                },
            ]
        )
    if cards:
        render_stat_cards(cards)

if not fastest_pitches.empty:
    with st.container(border=True):
        render_section_heading(
            "Top 10 Fastest Pitches",
        )
        st.html(
            render_highlight_table(
                fastest_pitches.rename(
                    columns={
                        "player_name": "Pitcher",
                        "game_date": "Date",
                        "opponent": "Opponent",
                        "pitch_name": "Pitch Type",
                        "release_speed": "Velocity (mph)",
                    }
                ),
                emphasis_columns=["Pitcher", "Velocity (mph)"],
                secondary_columns=["Date", "Opponent", "Pitch Type"],
            ),
        )

summary_left, summary_right = st.columns([1.15, 1])

with summary_left:
    with st.container(border=True):
        render_section_heading("Velocity Leaders Chart", "Maximum velocity by pitcher, sorted from top of the board downward.")
        if velocity_summary.empty:
            st.info("Pitcher velocity summaries are not available yet.")
        else:
            chart = style_chart(
                alt.Chart(velocity_summary)
                .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6, color=PHILLIES_RED)
                .encode(
                    x=alt.X("player_name:N", sort="-y", title="Pitcher"),
                    y=alt.Y("max_velocity_mph:Q", title="Max Velocity (mph)"),
                    tooltip=[
                        alt.Tooltip("player_name:N", title="Pitcher"),
                        alt.Tooltip("max_velocity_mph:Q", title="Max Velocity (mph)", format=".2f"),
                        alt.Tooltip(
                            "avg_fastball_velocity_mph:Q",
                            title="Average Fastball Velocity (mph)",
                            format=".2f",
                        ),
                    ],
                ),
                height=340,
            )
            st.altair_chart(chart, use_container_width=True)

with summary_right:
    with st.container(border=True):
        render_section_heading("Velocity Summary")
        if velocity_summary.empty:
            st.info("Pitcher velocity summaries are not available yet.")
        else:
            display = velocity_summary.rename(
                columns={
                    "player_name": "Pitcher",
                    "position": "Role",
                    "max_velocity_mph": "Max Velocity (mph)",
                    "avg_fastball_velocity_mph": "Average Fastball Velocity (mph)",
                }
            )
            st.html(
                render_highlight_table(
                    display,
                    emphasis_columns=["Pitcher", "Max Velocity (mph)"],
                    secondary_columns=["Average Fastball Velocity (mph)", "Role"],
                ),
            )
