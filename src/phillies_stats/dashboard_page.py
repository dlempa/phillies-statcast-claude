from __future__ import annotations

import altair as alt
import streamlit as st

from phillies_stats.config import get_config
from phillies_stats.database import get_connection, initialize_database
from phillies_stats.display import format_metric_value, format_player_name, render_highlight_table
from phillies_stats.queries import (
    get_dashboard_metrics,
    get_hardest_hit_home_runs,
    get_hr_distance_over_time,
    get_last_updated,
    get_player_hr_distance_stats,
    get_top_longest_home_runs,
)
from phillies_stats.ui import apply_app_theme, format_card, format_timestamp, render_page_header, render_section_heading, render_stat_cards, style_chart


def _format_metric(result, unit: str, fallback: str = "No data yet") -> tuple[str, str]:
    if not result:
        return fallback, ""
    player_name, value = result
    if value is None:
        return fallback, ""
    return f"{format_metric_value(value)} {unit}", player_name


def render_hitter_dashboard() -> None:
    config = get_config()
    conn = get_connection(config.db_path)
    initialize_database(conn)
    apply_app_theme()

    metrics = get_dashboard_metrics(conn)
    last_updated = get_last_updated(conn)
    top_10 = get_top_longest_home_runs(conn, limit=10)
    distance_over_time = get_hr_distance_over_time(conn)
    hardest_home_runs = get_hardest_hit_home_runs(conn, limit=5)
    player_distance = get_player_hr_distance_stats(conn).head(6)

    render_page_header(
        "Hitter Dashboard",
        f"Phillies home runs and power stats for the {config.season} season, organized around the season’s biggest swings.",
        eyebrow="Hitter Stats",
        meta=f"Last updated {format_timestamp(last_updated)}" if last_updated else None,
    )

    if not last_updated:
        st.info("No Statcast data is loaded yet. Run the bootstrap script to backfill the season.")

    render_stat_cards(
        [
            _build_metric_card("Longest HR of season", metrics["longest_hr"], " ft", tone="accent"),
            _build_metric_card("Most HRs by a Phillie", metrics["most_hrs"], " HR"),
            _build_metric_card("Hardest-hit HR", metrics["hardest_hr"], " mph"),
            _build_metric_card("Hardest-hit ball overall", metrics["hardest_ball"], " mph"),
        ]
    )

    feature_col, leaderboard_col = st.columns([1.35, 1])

    with feature_col:
        with st.container(border=True):
            render_section_heading(
                "Featured Chart",
                "Every Phillies home run from the season plotted by date and distance for a quick read on power spikes.",
            )
            if distance_over_time.empty:
                st.info("The chart will appear after Phillies home run events are loaded.")
            else:
                chart_data = distance_over_time.copy()
                chart_data["player_name"] = chart_data["player_name"].map(format_player_name)
                chart = style_chart(
                    alt.Chart(chart_data)
                    .mark_circle(size=110, opacity=0.88)
                    .encode(
                        x=alt.X("game_date:T", title="Date"),
                        y=alt.Y("distance_ft:Q", title="Distance (ft)"),
                        color=alt.Color("player_name:N", title="Player"),
                        tooltip=[
                            alt.Tooltip("player_name:N", title="Player"),
                            alt.Tooltip("distance_ft:Q", title="Distance (ft)", format=".0f"),
                            alt.Tooltip("opponent:N", title="Opponent"),
                            alt.Tooltip("venue_name:N", title="Ballpark"),
                            alt.Tooltip("game_date:T", title="Date", format="%m-%d-%Y"),
                        ],
                    )
                    .interactive(),
                    height=390,
                )
                st.altair_chart(chart, use_container_width=True)

    with leaderboard_col:
        with st.container(border=True):
            render_section_heading(
                "Featured Leaderboard",
            )
            if top_10.empty:
                st.info("No home run data is available yet.")
            else:
                preview = top_10.rename(
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
                        preview,
                        emphasis_columns=["Rank", "Player", "Distance (ft)"],
                        secondary_columns=["Exit Velocity (mph)", "Launch Angle", "Home/Away"],
                    ),
                )

    secondary_left, secondary_right = st.columns(2)

    with secondary_left:
        with st.container(border=True):
            render_section_heading("Hardest-hit Home Runs")
            if hardest_home_runs.empty:
                st.info("Hard-hit home run data is not available yet.")
            else:
                st.html(
                    render_highlight_table(
                        hardest_home_runs.rename(
                            columns={
                                "player_name": "Player",
                                "game_date": "Date",
                                "opponent": "Opponent",
                                "distance_ft": "Distance (ft)",
                                "exit_velocity_mph": "Exit Velocity (mph)",
                            }
                        ),
                        emphasis_columns=["Player", "Exit Velocity (mph)"],
                        secondary_columns=["Date", "Opponent"],
                    ),
                )

    with secondary_right:
        with st.container(border=True):
            render_section_heading("Power By Hitter")
            if player_distance.empty:
                st.info("Player power summaries will appear after home run data is loaded.")
            else:
                st.html(
                    render_highlight_table(
                        player_distance.rename(
                            columns={
                                "player_name": "Player",
                                "home_run_count": "HRs",
                                "avg_hr_distance_ft": "Average Distance (ft)",
                                "max_hr_distance_ft": "Longest HR (ft)",
                            }
                        ),
                        emphasis_columns=["Player", "Longest HR (ft)"],
                        secondary_columns=["Average Distance (ft)"],
                    ),
                )


def render_dashboard() -> None:
    render_hitter_dashboard()


def _build_metric_card(label: str, result: tuple[object, object] | None, suffix: str, tone: str = "default") -> dict[str, str]:
    if not result:
        return {"label": label, "value": "No data", "helper": "", "tone": tone}
    player_name, value = result
    return {
        "label": label,
        "value": format_card(value, suffix),
        "helper": str(player_name) if player_name else "",
        "tone": tone,
    }
