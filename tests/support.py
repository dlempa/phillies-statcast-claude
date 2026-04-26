from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from phillies_stats.database import initialize_database


def sample_event(
    *,
    event_id: str,
    game_pk: int,
    game_date_value: date,
    batter_name: str,
    batter_id: int,
    pitcher_name: str = "Opposing Pitcher",
    pitcher_id: int = 9001,
    hit_distance_sc: float | None = None,
    launch_speed: float | None = None,
    launch_angle: float | None = None,
    events: str = "home_run",
    description: str = "hit_into_play_score",
    is_home_run: bool = True,
    is_strikeout: bool = False,
    is_in_play: bool = True,
    release_speed: float | None = 95.0,
    opponent: str = "ATL",
    home_team: str = "PHI",
    away_team: str = "ATL",
    inning_topbot: str = "Bot",
    post_home_score: int = 5,
    post_away_score: int = 3,
    phillies_role: str = "batting",
    is_phillies_batter: bool = True,
    is_phillies_pitcher: bool = False,
    batting_team: str = "PHI",
    fielding_team: str = "ATL",
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "season": 2026,
        "game_pk": game_pk,
        "game_date": game_date_value,
        "game_datetime": pd.Timestamp(game_date_value),
        "inning": 1,
        "inning_topbot": inning_topbot,
        "at_bat_number": 1,
        "pitch_number": 1,
        "phillies_role": phillies_role,
        "is_phillies_batter": is_phillies_batter,
        "is_phillies_pitcher": is_phillies_pitcher,
        "batting_team": batting_team,
        "fielding_team": fielding_team,
        "home_team": home_team,
        "away_team": away_team,
        "opponent": opponent,
        "venue_name": "Citizens Bank Park",
        "batter_id": batter_id,
        "batter_name": batter_name,
        "pitcher_id": pitcher_id,
        "pitcher_name": pitcher_name,
        "stand": "L",
        "p_throws": "R",
        "events": events,
        "description": description,
        "bb_type": "fly_ball",
        "pitch_type": "FF",
        "pitch_name": "4-Seam Fastball",
        "release_speed": release_speed,
        "effective_speed": release_speed,
        "launch_speed": launch_speed,
        "launch_angle": launch_angle,
        "hit_distance_sc": hit_distance_sc,
        "zone": 5,
        "balls": 1,
        "strikes": 1,
        "outs_when_up": 0,
        "estimated_ba_using_speedangle": 0.999 if is_home_run else 0.100,
        "estimated_woba_using_speedangle": 1.900 if is_home_run else 0.120,
        "woba_value": 2.0 if is_home_run else 0.0,
        "delta_home_win_exp": 0.1,
        "post_away_score": post_away_score,
        "post_home_score": post_home_score,
        "is_home_run": is_home_run,
        "is_strikeout": is_strikeout,
        "is_in_play": is_in_play,
    }


def sample_events_frame(*rows: dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


class TempDatabase:
    def __enter__(self):
        self.conn = duckdb.connect(":memory:")
        initialize_database(self.conn)
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        self.conn.close()
