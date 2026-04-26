from __future__ import annotations

import runpy
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def render_pitcher_stats_page() -> None:
    overview_tab, strikeouts_tab, velocity_tab, profiles_tab = st.tabs(
        ["Overview", "Strikeouts", "Velocity", "Profiles"]
    )

    with overview_tab:
        _run_page("5_Pitching_Dashboard.py")
    with strikeouts_tab:
        _run_page("6_Strikeout_Leaders.py")
    with velocity_tab:
        _run_page("7_Velocity_Leaders.py")
    with profiles_tab:
        _run_page("8_Pitcher_Profiles.py")


def _run_page(filename: str) -> None:
    runpy.run_path(str(PROJECT_ROOT / "pages" / filename), run_name=f"__{filename}")
