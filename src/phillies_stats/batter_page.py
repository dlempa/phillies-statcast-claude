from __future__ import annotations

import runpy
from pathlib import Path

import streamlit as st

from phillies_stats.dashboard_page import render_hitter_dashboard

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def render_batter_stats_page() -> None:
    overview_tab, longest_tab, power_tab, profiles_tab, games_tab = st.tabs(
        ["Overview", "Longest HRs", "Power", "Profiles", "Game Log"]
    )

    with overview_tab:
        render_hitter_dashboard()
    with longest_tab:
        _run_page("1_Longest_Home_Runs.py")
    with power_tab:
        _run_page("2_Power_Stats.py")
    with profiles_tab:
        _run_page("3_Player_Summary.py")
    with games_tab:
        _run_page("4_Game_Log.py")


def _run_page(filename: str) -> None:
    runpy.run_path(str(PROJECT_ROOT / "pages" / filename), run_name=f"__{filename}")
