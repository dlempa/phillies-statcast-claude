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
from phillies_stats.queries import (
    get_fastest_pitches,
    get_last_updated,
    get_pitcher_home_run_allowed_leaders,
    get_pitcher_strikeout_leaders,
    get_pitcher_walks_leaders,
    get_pitcher_wins_leaders,
    get_pitching_dashboard_metrics,
    get_team_pitching_run_prevention_trend,
)
from phillies_stats.ui import (
    apply_app_theme,
    format_card,
    format_timestamp,
    render_page_header,
    render_section_heading,
    render_skeleton_block,
    render_stat_cards,
    style_chart,
)


def build_metric_card(label: str, result: tuple[object, object] | None, suffix: str, tone: str = "default") -> dict[str, str]:
    if not result:
        return {"label": label, "value": "No data", "helper": "", "tone": tone}
    player_name, value = result
    return {"label": label, "value": format_card(value, suffix), "helper": str(player_name), "tone": tone}


config = get_config()
conn = get_connection(config.db_path)
initialize_database(conn)
apply_app_theme()

metrics = get_pitching_dashboard_metrics(conn)
last_updated = get_last_updated(conn)
strikeout_leaders = get_pitcher_strikeout_leaders(conn, limit=6)
wins_leaders = get_pitcher_wins_leaders(conn, limit=6)
walks_leaders = get_pitcher_walks_leaders(conn, limit=6)
home_run_allowed = get_pitcher_home_run_allowed_leaders(conn, limit=6)
fastest_pitches = get_fastest_pitches(conn, limit=6)
run_prevention_trend = get_team_pitching_run_prevention_trend(conn)

render_page_header(
    "Pitching Dashboard",
    "A premium season snapshot of Phillies pitching, built around strikeouts, velocity, wins, innings, and run-prevention pressure points.",
    eyebrow="Pitcher Stats",
    meta=f"Last updated {format_timestamp(last_updated)}" if last_updated else None,
)

render_stat_cards(
    [
        build_metric_card("Strikeout leader", metrics["strikeout_leader"], " K", tone="accent"),
        build_metric_card("Fastest pitch", metrics["fastest_pitch"], " mph"),
        build_metric_card("Wins leader", metrics["wins_leader"], " W"),
        build_metric_card("Innings pitched leader", metrics["innings_leader"], " IP"),
    ]
)

primary_col, side_col = st.columns([1.35, 1])

with primary_col:
    with st.container(border=True):
        render_section_heading(
            "Staff Run Prevention Trend",
            "Runs allowed by game with a 5-game staff RA/G trend and season-to-date RA/G reference.",
        )
        if run_prevention_trend.empty:
            render_skeleton_block(
                "Run prevention trend will appear once completed Phillies games are loaded",
                kind="chart",
            )
        else:
            chart_data = run_prevention_trend.copy()
            latest_season_ra = chart_data["season_ra_per_game"].iloc[-1]
            base = alt.Chart(chart_data).encode(
                x=alt.X("game_date:T", title="Date"),
                tooltip=[
                    alt.Tooltip("game_date:T", title="Date", format="%m-%d-%Y"),
                    alt.Tooltip("opponent:N", title="Opponent"),
                    alt.Tooltip("result_text:N", title="Result"),
                    alt.Tooltip("runs_allowed:Q", title="Runs Allowed"),
                    alt.Tooltip("strikeouts:Q", title="Strikeouts"),
                    alt.Tooltip("walks:Q", title="Walks"),
                    alt.Tooltip("home_runs_allowed:Q", title="HR Allowed"),
                    alt.Tooltip("rolling_5_ra_per_game:Q", title="5-Game RA/G", format=".2f"),
                    alt.Tooltip("season_ra_per_game:Q", title="Season RA/G", format=".2f"),
                ],
            )
            bars = base.mark_bar(size=14, color="#9CA3AF", opacity=0.42).encode(
                y=alt.Y("runs_allowed:Q", title="Runs Allowed"),
            )
            rolling_line = base.mark_line(point=True, strokeWidth=3, color="#E81828").encode(
                y=alt.Y("rolling_5_ra_per_game:Q", title="Runs Allowed / RA per Game"),
            )
            season_rule = (
                alt.Chart({"values": [{"season_ra_per_game": latest_season_ra}]})
                .mark_rule(strokeDash=[6, 5], strokeWidth=2, color="#1F2933", opacity=0.7)
                .encode(
                    y=alt.Y("season_ra_per_game:Q"),
                    tooltip=[alt.Tooltip("season_ra_per_game:Q", title="Season RA/G", format=".2f")],
                )
            )
            chart = style_chart(
                (bars + rolling_line + season_rule).resolve_scale(y="shared").interactive(),
                height=380,
            )
            st.altair_chart(chart, use_container_width=True)

with side_col:
    with st.container(border=True):
        render_section_heading("Leaderboard Preview", "Strikeouts and premium velocity side by side.")
        strikeout_tab, velocity_tab = st.tabs(["Strikeouts", "Fastest Pitches"])

        with strikeout_tab:
            if strikeout_leaders.empty:
                st.info("Pitcher strikeout data is not available yet.")
            else:
                st.html(
                    render_highlight_table(
                        strikeout_leaders.rename(
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

        with velocity_tab:
            if fastest_pitches.empty:
                st.info("Velocity data is not available yet.")
            else:
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

bottom_left, bottom_right = st.columns(2)

with bottom_left:
    with st.container(border=True):
        render_section_heading("Wins Leaders", "A clean season view of wins, innings, and run prevention.")
        if wins_leaders.empty:
            st.info("Pitcher season summary data is not available yet.")
        else:
            st.html(
                render_highlight_table(
                    wins_leaders.rename(
                        columns={
                            "player_name": "Pitcher",
                            "wins": "Wins",
                            "losses": "Losses",
                            "innings_pitched": "Innings Pitched",
                            "era": "ERA",
                            "whip": "WHIP",
                        }
                    ),
                    emphasis_columns=["Pitcher", "Wins", "Innings Pitched"],
                    secondary_columns=["ERA", "WHIP", "Losses", "Position"],
                ),
            )

with bottom_right:
    with st.container(border=True):
        render_section_heading("Secondary Panel", "Walk pressure and home run damage allowed, without crowding the main dashboard.")
        walks_tab, damage_tab = st.tabs(["Walks Issued", "Home Runs Allowed"])

        with walks_tab:
            if walks_leaders.empty:
                st.info("Walk data is not available yet.")
            else:
                st.html(
                    render_highlight_table(
                        walks_leaders.rename(
                            columns={
                                "player_name": "Pitcher",
                                "walks_issued": "Walks",
                                "strikeouts": "Strikeouts",
                                "appearances": "Appearances",
                            }
                        ),
                        emphasis_columns=["Pitcher", "Walks"],
                        secondary_columns=["Strikeouts", "Appearances", "Position"],
                    ),
                )

        with damage_tab:
            if home_run_allowed.empty:
                st.info("Home run allowed data is not available yet.")
            else:
                st.html(
                    render_highlight_table(
                        home_run_allowed.rename(
                            columns={
                                "player_name": "Pitcher",
                                "home_runs_allowed": "HR Allowed",
                                "walks_issued": "Walks",
                                "strikeouts": "Strikeouts",
                            }
                        ),
                        emphasis_columns=["Pitcher", "HR Allowed"],
                        secondary_columns=["Walks", "Strikeouts", "Position"],
                    ),
                )
