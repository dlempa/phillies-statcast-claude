from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol

import duckdb
import pandas as pd
import requests

from phillies_stats.config import TEAM_CODE

NL_EAST_DIVISION_ID = 204
NL_LEAGUE_ID = 104
AL_LEAGUE_ID = 103
NL_EAST_TEAM_CODES = {"ATL", "MIA", "NYM", "PHI", "WSH"}

STAT_GROUP_HITTING = "hitting"
STAT_GROUP_PITCHING = "pitching"


@dataclass(frozen=True)
class TeamDefinition:
    team_id: int
    team_abbr: str
    team_name: str
    league: str | None
    division: str | None


class TeamContextProvider(Protocol):
    def fetch_teams(self, season: int) -> pd.DataFrame:
        ...

    def fetch_team_stats(self, season: int, teams: pd.DataFrame) -> pd.DataFrame:
        ...

    def fetch_division_standings(self, season: int, division_id: int = NL_EAST_DIVISION_ID) -> pd.DataFrame:
        ...


class MlbStatsApiTeamContextProvider:
    BASE_URL = "https://statsapi.mlb.com/api/v1"

    def fetch_teams(self, season: int) -> pd.DataFrame:
        response = requests.get(
            f"{self.BASE_URL}/teams",
            params={"sportId": "1", "season": str(season), "hydrate": "league,division"},
            timeout=30,
        )
        response.raise_for_status()
        rows = []
        for team in response.json().get("teams", []):
            if not isinstance(team, dict):
                continue
            league = team.get("league", {})
            division = team.get("division", {})
            rows.append(
                {
                    "team_id": team.get("id"),
                    "team_abbr": team.get("abbreviation"),
                    "team_name": team.get("name"),
                    "league": _league_label(league.get("id") if isinstance(league, dict) else None),
                    "division": division.get("name") if isinstance(division, dict) else None,
                }
            )
        return pd.DataFrame(rows)

    def fetch_team_stats(self, season: int, teams: pd.DataFrame) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        for team in normalize_teams(teams).itertuples(index=False):
            response = requests.get(
                f"{self.BASE_URL}/teams/{int(team.team_id)}/stats",
                params={"stats": "season", "group": "hitting,pitching", "season": str(season), "gameType": "R"},
                timeout=30,
            )
            response.raise_for_status()
            rows.extend(_team_stats_payload_rows(response.json(), team))
        return pd.DataFrame(rows)

    def fetch_division_standings(self, season: int, division_id: int = NL_EAST_DIVISION_ID) -> pd.DataFrame:
        response = requests.get(
            f"{self.BASE_URL}/standings",
            params={
                "leagueId": str(NL_LEAGUE_ID),
                "divisionId": str(division_id),
                "season": str(season),
                "standingsTypes": "regularSeason",
                "hydrate": "team",
            },
            timeout=30,
        )
        response.raise_for_status()
        rows: list[dict[str, object]] = []
        for record in response.json().get("records", []):
            if not isinstance(record, dict):
                continue
            division = record.get("division", {})
            league = record.get("league", {})
            for team_record in record.get("teamRecords", []):
                if not isinstance(team_record, dict):
                    continue
                team = team_record.get("team", {})
                if not isinstance(team, dict):
                    continue
                rows.append(
                    {
                        "team_id": team.get("id"),
                        "team_abbr": team.get("abbreviation"),
                        "team_name": team.get("name"),
                        "league": _league_label(league.get("id") if isinstance(league, dict) else None),
                        "division": division.get("name") if isinstance(division, dict) else "National League East",
                        "division_rank": team_record.get("divisionRank"),
                        "wins": team_record.get("wins"),
                        "losses": team_record.get("losses"),
                        "winning_percentage": team_record.get("winningPercentage"),
                        "games_back": team_record.get("gamesBack"),
                        "runs_scored": team_record.get("runsScored"),
                        "runs_allowed": team_record.get("runsAllowed"),
                        "run_differential": team_record.get("runDifferential"),
                        "streak": _streak_text(team_record.get("streak")),
                    }
                )
        return pd.DataFrame(rows)


def refresh_team_context(
    conn: duckdb.DuckDBPyConnection,
    *,
    season: int,
    as_of_date: date | None = None,
    provider: TeamContextProvider | None = None,
) -> dict[str, int]:
    as_of = as_of_date or date.today()
    data_provider = provider or MlbStatsApiTeamContextProvider()

    teams = data_provider.fetch_teams(season)
    raw_team_stats = data_provider.fetch_team_stats(season, teams)
    raw_standings = data_provider.fetch_division_standings(season, NL_EAST_DIVISION_ID)

    team_stats = normalize_team_stats(raw_team_stats, season=season, as_of_date=as_of)
    standings = normalize_division_standings(raw_standings, season=season, as_of_date=as_of)
    standings = standings.loc[standings["team_abbr"].isin(NL_EAST_TEAM_CODES)].copy()
    _replace_team_context_rows(conn, season=season, as_of_date=as_of, team_stats=team_stats, standings=standings)
    return {"team_stat_rows": len(team_stats), "standing_rows": len(standings)}


def normalize_teams(raw_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["team_id", "team_abbr", "team_name", "league", "division"]
    if raw_df.empty:
        return pd.DataFrame(columns=columns)
    normalized = pd.DataFrame(index=raw_df.index)
    normalized["team_id"] = pd.to_numeric(_pick_series(raw_df, ("team_id", "id")), errors="coerce")
    normalized["team_abbr"] = _pick_series(raw_df, ("team_abbr", "abbreviation", "Team")).astype(str).str.upper()
    normalized["team_name"] = _pick_series(raw_df, ("team_name", "name", "Name"))
    normalized["league"] = _pick_series(raw_df, ("league",), default=None).map(_league_label)
    normalized["division"] = _pick_series(raw_df, ("division",), default=None)
    return normalized.dropna(subset=["team_id", "team_abbr", "team_name"])


def normalize_team_stats(raw_df: pd.DataFrame, *, season: int, as_of_date: date) -> pd.DataFrame:
    columns = [
        "season",
        "as_of_date",
        "team_id",
        "team_abbr",
        "team_name",
        "league",
        "division",
        "stat_group",
        "games",
        "runs",
        "runs_allowed",
        "home_runs",
        "batting_average",
        "on_base_percentage",
        "slugging_percentage",
        "ops",
        "era",
        "whip",
        "strikeouts",
        "walks",
        "home_runs_allowed",
        "created_at",
    ]
    if raw_df.empty:
        return pd.DataFrame(columns=columns)

    normalized = pd.DataFrame(index=raw_df.index)
    normalized["season"] = season
    normalized["as_of_date"] = as_of_date
    normalized["team_id"] = pd.to_numeric(_pick_series(raw_df, ("team_id", "id")), errors="coerce")
    normalized["team_abbr"] = _pick_series(raw_df, ("team_abbr", "Team", "abbreviation")).astype(str).str.upper()
    normalized["team_name"] = _pick_series(raw_df, ("team_name", "Name", "name"))
    normalized["league"] = _pick_series(raw_df, ("league",), default=None).map(_league_label)
    normalized["division"] = _pick_series(raw_df, ("division",), default=None)
    normalized["stat_group"] = _pick_series(raw_df, ("stat_group", "group"), default="").astype(str).str.lower()
    normalized["games"] = _number_from_aliases(raw_df, ("games", "gamesPlayed", "G"))
    normalized["runs"] = _number_from_aliases(raw_df, ("runs", "runsScored", "R"))
    normalized["runs_allowed"] = _number_from_aliases(raw_df, ("runs_allowed", "runsAllowed", "RA"))
    normalized["home_runs"] = _number_from_aliases(raw_df, ("home_runs", "homeRuns", "HR"))
    normalized["batting_average"] = _number_from_aliases(raw_df, ("batting_average", "avg", "AVG"))
    normalized["on_base_percentage"] = _number_from_aliases(raw_df, ("on_base_percentage", "obp", "OBP"))
    normalized["slugging_percentage"] = _number_from_aliases(raw_df, ("slugging_percentage", "slg", "SLG"))
    normalized["ops"] = _number_from_aliases(raw_df, ("ops", "OPS"))
    normalized["era"] = _number_from_aliases(raw_df, ("era", "ERA"))
    normalized["whip"] = _number_from_aliases(raw_df, ("whip", "WHIP"))
    normalized["strikeouts"] = _number_from_aliases(raw_df, ("strikeouts", "strikeOuts", "SO"))
    normalized["walks"] = _number_from_aliases(raw_df, ("walks", "baseOnBalls", "BB"))
    normalized["home_runs_allowed"] = _number_from_aliases(raw_df, ("home_runs_allowed", "homeRunsAllowed", "HRA"))
    pitching_rows = normalized["stat_group"].eq(STAT_GROUP_PITCHING)
    missing_hra = pitching_rows & normalized["home_runs_allowed"].isna()
    normalized.loc[missing_hra, "home_runs_allowed"] = normalized.loc[missing_hra, "home_runs"]
    normalized["created_at"] = datetime.now()
    normalized = normalized.loc[normalized["stat_group"].isin([STAT_GROUP_HITTING, STAT_GROUP_PITCHING])]
    return normalized.dropna(subset=["team_id", "team_abbr", "team_name"])


def normalize_division_standings(raw_df: pd.DataFrame, *, season: int, as_of_date: date) -> pd.DataFrame:
    columns = [
        "season",
        "as_of_date",
        "team_id",
        "team_abbr",
        "team_name",
        "league",
        "division",
        "division_rank",
        "wins",
        "losses",
        "winning_percentage",
        "games_back",
        "runs_scored",
        "runs_allowed",
        "run_differential",
        "streak",
        "created_at",
    ]
    if raw_df.empty:
        return pd.DataFrame(columns=columns)

    normalized = pd.DataFrame(index=raw_df.index)
    normalized["season"] = season
    normalized["as_of_date"] = as_of_date
    normalized["team_id"] = pd.to_numeric(_pick_series(raw_df, ("team_id", "id")), errors="coerce")
    normalized["team_abbr"] = _pick_series(raw_df, ("team_abbr", "Team", "abbreviation")).astype(str).str.upper()
    normalized["team_name"] = _pick_series(raw_df, ("team_name", "Name", "name"))
    normalized["league"] = _pick_series(raw_df, ("league",), default="National League").map(_league_label)
    normalized["division"] = _pick_series(raw_df, ("division",), default="National League East")
    normalized.loc[normalized["team_abbr"].isin(NL_EAST_TEAM_CODES), "division"] = "National League East"
    normalized["division_rank"] = _number_from_aliases(raw_df, ("division_rank", "divisionRank", "Rank"))
    normalized["wins"] = _number_from_aliases(raw_df, ("wins", "W"))
    normalized["losses"] = _number_from_aliases(raw_df, ("losses", "L"))
    normalized["winning_percentage"] = _number_from_aliases(raw_df, ("winning_percentage", "winningPercentage", "PCT"))
    normalized["games_back"] = _pick_series(raw_df, ("games_back", "gamesBack", "GB"), default=None)
    normalized["runs_scored"] = _number_from_aliases(raw_df, ("runs_scored", "runsScored", "RS"))
    normalized["runs_allowed"] = _number_from_aliases(raw_df, ("runs_allowed", "runsAllowed", "RA"))
    normalized["run_differential"] = _number_from_aliases(raw_df, ("run_differential", "runDifferential", "DIFF"))
    normalized["streak"] = _pick_series(raw_df, ("streak", "Streak"), default=None).map(_streak_text)
    normalized["created_at"] = datetime.now()
    return normalized.dropna(subset=["team_id", "team_abbr", "team_name"])


def _replace_team_context_rows(
    conn: duckdb.DuckDBPyConnection,
    *,
    season: int,
    as_of_date: date,
    team_stats: pd.DataFrame,
    standings: pd.DataFrame,
) -> None:
    conn.execute("DELETE FROM team_season_stats WHERE season = ? AND as_of_date = ?", [season, as_of_date])
    conn.execute("DELETE FROM division_standings WHERE season = ? AND as_of_date = ?", [season, as_of_date])

    if not team_stats.empty:
        conn.register("team_stats_stage", team_stats)
        try:
            conn.execute(
                """
                INSERT INTO team_season_stats
                SELECT * FROM team_stats_stage
                """
            )
        finally:
            conn.unregister("team_stats_stage")

    if not standings.empty:
        conn.register("division_standings_stage", standings)
        try:
            conn.execute(
                """
                INSERT INTO division_standings
                SELECT * FROM division_standings_stage
                """
            )
        finally:
            conn.unregister("division_standings_stage")


def _team_stats_payload_rows(payload: dict[str, object], team: object) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for stat_block in payload.get("stats", []):
        if not isinstance(stat_block, dict):
            continue
        group = stat_block.get("group", {})
        group_name = group.get("displayName") if isinstance(group, dict) else None
        normalized_group = str(group_name or "").lower()
        if normalized_group not in {STAT_GROUP_HITTING, STAT_GROUP_PITCHING}:
            continue
        splits = stat_block.get("splits", [])
        if not isinstance(splits, list) or not splits:
            continue
        stats = splits[0].get("stat", {}) if isinstance(splits[0], dict) else {}
        if not isinstance(stats, dict):
            continue
        row = {
            "team_id": getattr(team, "team_id"),
            "team_abbr": getattr(team, "team_abbr"),
            "team_name": getattr(team, "team_name"),
            "league": getattr(team, "league"),
            "division": getattr(team, "division"),
            "stat_group": normalized_group,
            **stats,
        }
        rows.append(row)
    return rows


def _pick_series(raw_df: pd.DataFrame, aliases: tuple[str, ...], default=None) -> pd.Series:
    for alias in aliases:
        if alias in raw_df.columns:
            return raw_df[alias]
    return pd.Series([default] * len(raw_df), index=raw_df.index)


def _number_from_aliases(raw_df: pd.DataFrame, aliases: tuple[str, ...]) -> pd.Series:
    raw = _pick_series(raw_df, aliases)
    as_text = raw.astype(str).str.replace("%", "", regex=False).str.replace(",", "", regex=False)
    return pd.to_numeric(as_text, errors="coerce")


def _league_label(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value)
    if text in {str(NL_LEAGUE_ID), "NL", "National League"}:
        return "National League"
    if text in {str(AL_LEAGUE_ID), "AL", "American League"}:
        return "American League"
    return text


def _streak_text(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, dict):
        streak_code = value.get("streakCode")
        if streak_code:
            return str(streak_code)
        code = value.get("streakType")
        number = value.get("streakNumber")
        return f"{code}{number}" if code and number is not None else None
    text = str(value).strip()
    return text or None
