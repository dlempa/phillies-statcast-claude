from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

TEAM_CODE = "PHI"
BALLPARK_BY_TEAM = {
    "ARI": "Chase Field",
    "ATL": "Truist Park",
    "BAL": "Oriole Park at Camden Yards",
    "BOS": "Fenway Park",
    "CHC": "Wrigley Field",
    "CIN": "Great American Ball Park",
    "CLE": "Progressive Field",
    "COL": "Coors Field",
    "CWS": "Rate Field",
    "DET": "Comerica Park",
    "HOU": "Daikin Park",
    "KC": "Kauffman Stadium",
    "LAA": "Angel Stadium",
    "LAD": "Dodger Stadium",
    "MIA": "loanDepot park",
    "MIL": "American Family Field",
    "MIN": "Target Field",
    "METS": "Citi Field",
    "NYY": "Yankee Stadium",
    "ATH": "Sutter Health Park",
    "PHI": "Citizens Bank Park",
    "PIT": "PNC Park",
    "SD": "Petco Park",
    "SEA": "T-Mobile Park",
    "SF": "Oracle Park",
    "STL": "Busch Stadium",
    "TB": "George M. Steinbrenner Field",
    "TEX": "Globe Life Field",
    "TOR": "Rogers Centre",
    "WSH": "Nationals Park",
}


@dataclass(frozen=True)
class AppConfig:
    season: int = int(os.getenv("PHILLIES_STATCAST_SEASON", "2026"))
    team_code: str = TEAM_CODE
    project_root: Path = Path(__file__).resolve().parents[2]

    @property
    def data_dir(self) -> Path:
        return self.project_root / "data"

    @property
    def db_path(self) -> Path:
        return self.data_dir / f"phillies_{self.season}.duckdb"

    @property
    def season_start(self) -> date:
        return date(self.season, 1, 1)

    @property
    def season_end(self) -> date:
        return date(self.season, 12, 31)


def get_config(season: int | None = None) -> AppConfig:
    if season is None:
        return AppConfig()
    return AppConfig(season=season)
