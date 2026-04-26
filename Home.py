from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from phillies_stats.batter_page import render_batter_stats_page
from phillies_stats.home_page import render_home
from phillies_stats.pitcher_page import render_pitcher_stats_page
from phillies_stats.team_page import render_team_stats_page


st.set_page_config(
    page_title="Phillies Statcast 2026",
    page_icon=":baseball:",
    layout="wide",
    initial_sidebar_state="expanded",
)

batter_stats_page = st.Page(
    render_batter_stats_page,
    title="Batter Stats",
    icon=":material/dashboard:",
)
pitcher_stats_page = st.Page(
    render_pitcher_stats_page,
    title="Pitcher Stats",
    icon=":material/sports_baseball:",
)
team_stats_page = st.Page(
    render_team_stats_page,
    title="Team Stats",
    icon=":material/groups:",
)
home_page = st.Page(
    lambda: render_home(team_stats_page, batter_stats_page, pitcher_stats_page),
    title="Home",
    icon=":material/home:",
    default=True,
)

navigation = st.navigation(
    [home_page, team_stats_page, batter_stats_page, pitcher_stats_page],
    position="sidebar",
    expanded=True,
)

navigation.run()
