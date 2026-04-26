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
from phillies_stats.queries import get_game_log
from phillies_stats.ui import apply_app_theme, render_filter_caption, render_page_header, render_section_heading, render_stat_cards


config = get_config()
conn = get_connection(config.db_path)
initialize_database(conn)
apply_app_theme()

render_page_header(
    "Game Log",
    "One row per Phillies game, with a cleaner season view of results, home run output, and the hardest-hit ball from each matchup.",
    eyebrow="Overview",
)

game_log = get_game_log(conn)
if game_log.empty:
    st.info("The game log will appear after Statcast game data is loaded.")
else:
    with st.container(border=True):
        render_section_heading("Filters", "Keep the table compact by filtering by opponent, result, or ballpark.")
        render_filter_caption("Game log filters")
        filter_col_1, filter_col_2, filter_col_3 = st.columns(3)
        opponent_filter = filter_col_1.selectbox(
            "Opponent",
            options=["All opponents"] + sorted(game_log["opponent"].dropna().unique().tolist()),
            key="game_log_opponent_filter",
        )
        result_filter = filter_col_2.selectbox(
            "Result",
            options=["All results"] + sorted(game_log["result_text"].dropna().unique().tolist()),
            key="game_log_result_filter",
        )
        venue_filter = filter_col_3.selectbox(
            "Ballpark",
            options=["All ballparks"] + sorted(game_log["venue_name"].dropna().unique().tolist()),
            key="game_log_venue_filter",
        )

    filtered = game_log.copy()
    if opponent_filter != "All opponents":
        filtered = filtered.loc[filtered["opponent"] == opponent_filter]
    if result_filter != "All results":
        filtered = filtered.loc[filtered["result_text"] == result_filter]
    if venue_filter != "All ballparks":
        filtered = filtered.loc[filtered["venue_name"] == venue_filter]

    if filtered.empty:
        st.info("No games match the current filters.")
    else:
        render_stat_cards(
            [
                {
                    "label": "Games shown",
                    "value": format_metric_value(len(filtered)),
                    "tone": "accent",
                },
                {
                    "label": "Total HRs",
                    "value": format_metric_value(filtered["hr_count"].fillna(0).sum()),
                },
                {
                    "label": "Longest HR",
                    "value": f"{format_metric_value(filtered['longest_hr_ft'].max())} ft",
                },
                {
                    "label": "Hardest-hit ball",
                    "value": f"{format_metric_value(filtered['hardest_hit_ball_mph'].max())} mph",
                },
            ]
        )

        with st.container(border=True):
            render_section_heading("Season Game Log", "Sorted newest to oldest for a quick read on the recent run of games.")
            st.html(
                render_highlight_table(
                    filtered.rename(
                        columns={
                            "game_date": "Date",
                            "opponent": "Opponent",
                            "venue_name": "Ballpark",
                            "result_text": "Result",
                            "hr_count": "HRs",
                            "longest_hr_ft": "Longest HR (ft)",
                            "hardest_hit_ball_mph": "Hardest-hit Ball (mph)",
                        }
                    ),
                    emphasis_columns=["Date", "Opponent", "Result", "HRs"],
                    secondary_columns=["Ballpark", "Longest HR (ft)", "Hardest-hit Ball (mph)"],
                ),
            )
