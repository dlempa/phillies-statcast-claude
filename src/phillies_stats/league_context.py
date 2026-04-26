from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol

import duckdb
import pandas as pd
import requests

from phillies_stats.display import format_player_name, normalize_player_key

PLAYER_GROUP_HITTER = "hitter"
PLAYER_GROUP_STARTER = "starter"
PLAYER_GROUP_RELIEVER = "reliever"
PLAYER_GROUP_CLOSER = "closer"

DIRECTION_HIGHER = "higher"
DIRECTION_LOWER = "lower"

PERCENTILE_CUTOFFS = (15, 40, 60, 75, 90, 95)


@dataclass(frozen=True)
class StatDefinition:
    key: str
    label: str
    aliases: tuple[str, ...]
    direction: str
    display_format: str


HITTER_STAT_DEFINITIONS: tuple[StatDefinition, ...] = (
    StatDefinition("ba", "BA", ("BA", "AVG"), DIRECTION_HIGHER, "rate3"),
    StatDefinition("obp", "OBP", ("OBP",), DIRECTION_HIGHER, "rate3"),
    StatDefinition("slg", "SLG", ("SLG",), DIRECTION_HIGHER, "rate3"),
    StatDefinition("ops", "OPS", ("OPS",), DIRECTION_HIGHER, "rate3"),
    StatDefinition("iso", "ISO", ("ISO",), DIRECTION_HIGHER, "rate3"),
    StatDefinition("bb_pct", "BB%", ("BB%", "BB_pct", "BB Pct"), DIRECTION_HIGHER, "pct1"),
    StatDefinition("k_pct", "K%", ("K%", "SO%", "K_pct", "K Pct"), DIRECTION_LOWER, "pct1"),
    StatDefinition("hr", "HR", ("HR",), DIRECTION_HIGHER, "int"),
)

PITCHER_STAT_DEFINITIONS: tuple[StatDefinition, ...] = (
    StatDefinition("era", "ERA", ("ERA",), DIRECTION_LOWER, "decimal2"),
    StatDefinition("whip", "WHIP", ("WHIP",), DIRECTION_LOWER, "decimal2"),
    StatDefinition("fip", "FIP", ("FIP",), DIRECTION_LOWER, "decimal2"),
    StatDefinition("k_per_9", "K/9", ("K/9", "K9"), DIRECTION_HIGHER, "decimal2"),
    StatDefinition("bb_per_9", "BB/9", ("BB/9", "BB9"), DIRECTION_LOWER, "decimal2"),
    StatDefinition("hr_per_9", "HR/9", ("HR/9", "HR9"), DIRECTION_LOWER, "decimal2"),
)


class LeagueContextProvider(Protocol):
    def fetch_hitter_stats(self, season: int, *, start_date: date | None = None, end_date: date | None = None) -> pd.DataFrame:
        ...

    def fetch_pitcher_stats(self, season: int, *, start_date: date | None = None, end_date: date | None = None) -> pd.DataFrame:
        ...


class PybaseballLeagueContextProvider:
    def fetch_hitter_stats(self, season: int, *, start_date: date | None = None, end_date: date | None = None) -> pd.DataFrame:
        try:
            from pybaseball import batting_stats
        except ImportError as exc:
            raise RuntimeError("pybaseball is required for league context refresh.") from exc

        frame = batting_stats(season, season, qual=0)
        return pd.DataFrame() if frame is None else frame

    def fetch_pitcher_stats(self, season: int, *, start_date: date | None = None, end_date: date | None = None) -> pd.DataFrame:
        try:
            from pybaseball import pitching_stats
        except ImportError as exc:
            raise RuntimeError("pybaseball is required for league context refresh.") from exc

        frame = pitching_stats(season, season, qual=0)
        return pd.DataFrame() if frame is None else frame


class MlbStatsApiLeagueContextProvider:
    BASE_URL = "https://statsapi.mlb.com/api/v1/stats"

    def fetch_hitter_stats(self, season: int, *, start_date: date | None = None, end_date: date | None = None) -> pd.DataFrame:
        return self._fetch_stats(season, group="hitting", start_date=start_date, end_date=end_date)

    def fetch_pitcher_stats(self, season: int, *, start_date: date | None = None, end_date: date | None = None) -> pd.DataFrame:
        frame = self._fetch_stats(season, group="pitching", start_date=start_date, end_date=end_date)
        if frame.empty:
            return frame
        return _add_calculated_fip(frame)

    def _fetch_stats(
        self,
        season: int,
        *,
        group: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        params = {
            "stats": "byDateRange" if start_date or end_date else "season",
            "group": group,
            "season": str(season),
            "playerPool": "ALL",
            "sportIds": "1",
            "gameType": "R",
            "limit": "5000",
            "hydrate": "team",
        }
        if start_date:
            params["startDate"] = start_date.strftime("%m/%d/%Y")
        if end_date:
            params["endDate"] = end_date.strftime("%m/%d/%Y")

        response = requests.get(self.BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        stats = payload.get("stats", [])
        if not stats:
            return pd.DataFrame()
        splits = stats[0].get("splits", [])
        return _stats_api_splits_to_frame(splits, group=group)


class DefaultLeagueContextProvider:
    def __init__(self) -> None:
        self.pybaseball_provider = PybaseballLeagueContextProvider()
        self.mlb_stats_api_provider = MlbStatsApiLeagueContextProvider()

    def fetch_hitter_stats(self, season: int, *, start_date: date | None = None, end_date: date | None = None) -> pd.DataFrame:
        if start_date or end_date:
            return self.mlb_stats_api_provider.fetch_hitter_stats(season, start_date=start_date, end_date=end_date)
        try:
            return self.pybaseball_provider.fetch_hitter_stats(season)
        except Exception:
            return self.mlb_stats_api_provider.fetch_hitter_stats(season)

    def fetch_pitcher_stats(self, season: int, *, start_date: date | None = None, end_date: date | None = None) -> pd.DataFrame:
        if start_date or end_date:
            return self.mlb_stats_api_provider.fetch_pitcher_stats(season, start_date=start_date, end_date=end_date)
        try:
            return self.pybaseball_provider.fetch_pitcher_stats(season)
        except Exception:
            return self.mlb_stats_api_provider.fetch_pitcher_stats(season)


def refresh_league_context(
    conn: duckdb.DuckDBPyConnection,
    *,
    season: int,
    team_code: str = "PHI",
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    provider: LeagueContextProvider | None = None,
) -> dict[str, int]:
    as_of = as_of_date or end_date or date.today()
    data_provider = provider or DefaultLeagueContextProvider()
    team_games_played = get_team_games_played(conn, season=season)

    raw_hitters = data_provider.fetch_hitter_stats(season, start_date=start_date, end_date=end_date)
    raw_pitchers = data_provider.fetch_pitcher_stats(season, start_date=start_date, end_date=end_date)
    hitters = normalize_hitter_stats(raw_hitters)
    pitchers = normalize_pitcher_stats(raw_pitchers)

    if team_games_played <= 0:
        team_games_played = _infer_team_games_played(hitters, pitchers, team_code=team_code)

    cutoff_rows, pools = build_league_cutoff_rows(
        hitters,
        pitchers,
        season=season,
        as_of_date=as_of,
        team_games_played=team_games_played,
    )
    rating_rows = build_player_rating_rows(
        hitters,
        pitchers,
        pools=pools,
        season=season,
        as_of_date=as_of,
        team_code=team_code,
        team_games_played=team_games_played,
    )

    if not cutoff_rows and not rating_rows:
        return {"cutoff_rows": 0, "rating_rows": 0}

    _replace_league_context_rows(conn, season=season, as_of_date=as_of, cutoff_rows=cutoff_rows, rating_rows=rating_rows)
    return {"cutoff_rows": len(cutoff_rows), "rating_rows": len(rating_rows)}


def get_team_games_played(conn: duckdb.DuckDBPyConnection, *, season: int) -> int:
    row = conn.execute(
        """
        SELECT COUNT(DISTINCT game_pk)
        FROM games
        WHERE season = ?
        """,
        [season],
    ).fetchone()
    return int(row[0] or 0) if row else 0


def normalize_hitter_stats(raw_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["player_name", "player_key", "team", "pa", "games"] + [definition.key for definition in HITTER_STAT_DEFINITIONS]
    if raw_df.empty:
        return pd.DataFrame(columns=columns)

    normalized = pd.DataFrame(index=raw_df.index)
    normalized["player_name"] = _pick_series(raw_df, ("Name", "PlayerName", "player_name")).map(format_player_name)
    normalized["player_key"] = normalized["player_name"].map(normalize_player_key)
    normalized["team"] = _pick_series(raw_df, ("Team", "team"), default="").astype(str).str.upper()
    normalized["pa"] = _numeric_from_aliases(raw_df, ("PA", "Plate Appearances"))
    normalized["games"] = _numeric_from_aliases(raw_df, ("G", "Games"))
    for definition in HITTER_STAT_DEFINITIONS:
        normalized[definition.key] = _numeric_from_aliases(
            raw_df,
            definition.aliases,
            is_percentage=definition.display_format == "pct1",
        )
    return normalized.dropna(subset=["player_name", "player_key"])


def _stats_api_splits_to_frame(splits: list[dict[str, object]], *, group: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for split in splits:
        if not isinstance(split, dict):
            continue
        player = split.get("player", {})
        team = split.get("team", {})
        stats = split.get("stat", {})
        if not isinstance(player, dict) or not isinstance(team, dict) or not isinstance(stats, dict):
            continue

        row: dict[str, object] = {
            "Name": player.get("fullName"),
            "Team": team.get("abbreviation"),
            "G": stats.get("gamesPlayed", stats.get("gamesPitched")),
        }
        if group == "hitting":
            row.update(
                {
                    "PA": stats.get("plateAppearances"),
                    "AVG": stats.get("avg"),
                    "OBP": stats.get("obp"),
                    "SLG": stats.get("slg"),
                    "OPS": stats.get("ops"),
                    "ISO": _subtract_stats(stats.get("slg"), stats.get("avg")),
                    "BB%": _rate_per_100(stats.get("baseOnBalls"), stats.get("plateAppearances")),
                    "K%": _rate_per_100(stats.get("strikeOuts"), stats.get("plateAppearances")),
                    "HR": stats.get("homeRuns"),
                }
            )
        else:
            row.update(
                {
                    "IP": stats.get("inningsPitched"),
                    "GS": stats.get("gamesStarted"),
                    "SV": stats.get("saves"),
                    "ERA": stats.get("era"),
                    "WHIP": stats.get("whip"),
                    "FIP": stats.get("FIP"),
                    "K/9": stats.get("strikeoutsPer9Inn"),
                    "BB/9": stats.get("walksPer9Inn"),
                    "HR/9": stats.get("homeRunsPer9"),
                    "HR": stats.get("homeRuns"),
                    "BB": stats.get("baseOnBalls"),
                    "HBP": stats.get("hitBatsmen"),
                    "SO": stats.get("strikeOuts"),
                    "ER": stats.get("earnedRuns"),
                }
            )
        rows.append(row)
    return pd.DataFrame(rows)


def normalize_pitcher_stats(raw_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "player_name",
        "player_key",
        "team",
        "ip",
        "games",
        "games_started",
        "saves",
        "player_group",
    ] + [definition.key for definition in PITCHER_STAT_DEFINITIONS]
    if raw_df.empty:
        return pd.DataFrame(columns=columns)

    normalized = pd.DataFrame(index=raw_df.index)
    normalized["player_name"] = _pick_series(raw_df, ("Name", "PlayerName", "player_name")).map(format_player_name)
    normalized["player_key"] = normalized["player_name"].map(normalize_player_key)
    normalized["team"] = _pick_series(raw_df, ("Team", "team"), default="").astype(str).str.upper()
    normalized["ip"] = _numeric_from_aliases(raw_df, ("IP", "Innings Pitched")).map(_parse_innings_pitched)
    normalized["games"] = _numeric_from_aliases(raw_df, ("G", "Games"))
    normalized["games_started"] = _numeric_from_aliases(raw_df, ("GS", "Games Started", "games_started"))
    normalized["saves"] = _numeric_from_aliases(raw_df, ("SV", "Saves", "saves"))
    for definition in PITCHER_STAT_DEFINITIONS:
        normalized[definition.key] = _numeric_from_aliases(raw_df, definition.aliases)
    normalized["player_group"] = normalized.apply(
        lambda row: derive_pitcher_group(row.get("games_started"), row.get("games"), row.get("saves")),
        axis=1,
    )
    return normalized.dropna(subset=["player_name", "player_key"])


def derive_pitcher_group(games_started: object, games: object = None, saves: object = None) -> str:
    if pd.notna(saves) and float(saves) > 0:
        return PLAYER_GROUP_CLOSER
    if pd.notna(games_started) and float(games_started) > 0:
        return PLAYER_GROUP_STARTER
    return PLAYER_GROUP_RELIEVER


def pitcher_position_to_group(position: object) -> str:
    normalized = str(position or "").strip().lower()
    if normalized == "closer":
        return PLAYER_GROUP_CLOSER
    if normalized == "starter":
        return PLAYER_GROUP_STARTER
    if normalized == "reliever":
        return PLAYER_GROUP_RELIEVER
    return PLAYER_GROUP_RELIEVER


def build_league_cutoff_rows(
    hitters: pd.DataFrame,
    pitchers: pd.DataFrame,
    *,
    season: int,
    as_of_date: date,
    team_games_played: int,
) -> tuple[list[dict[str, object]], dict[tuple[str, str], pd.Series]]:
    pools: dict[tuple[str, str], pd.Series] = {}
    rows: list[dict[str, object]] = []

    hitter_minimum = max(20, _round_half_up(1.5 * team_games_played))
    hitter_pool = hitters.loc[pd.to_numeric(hitters.get("pa"), errors="coerce").fillna(-1) >= hitter_minimum].copy()
    rows.extend(
        _cutoff_rows_for_group(
            hitter_pool,
            stat_definitions=HITTER_STAT_DEFINITIONS,
            pools=pools,
            season=season,
            as_of_date=as_of_date,
            player_group=PLAYER_GROUP_HITTER,
            pool_minimum_metric="PA",
            pool_minimum_value=hitter_minimum,
        )
    )

    pitcher_minimums = {
        PLAYER_GROUP_STARTER: max(8, _round_half_up(0.7 * team_games_played)),
        PLAYER_GROUP_RELIEVER: max(5, _round_half_up(0.3 * team_games_played)),
        PLAYER_GROUP_CLOSER: max(5, _round_half_up(0.3 * team_games_played)),
    }
    for player_group, minimum in pitcher_minimums.items():
        pitcher_pool = pitchers.loc[
            pitchers.get("player_group").eq(player_group)
            & (pd.to_numeric(pitchers.get("ip"), errors="coerce").fillna(-1) >= minimum)
        ].copy()
        rows.extend(
            _cutoff_rows_for_group(
                pitcher_pool,
                stat_definitions=PITCHER_STAT_DEFINITIONS,
                pools=pools,
                season=season,
                as_of_date=as_of_date,
                player_group=player_group,
                pool_minimum_metric="IP",
                pool_minimum_value=minimum,
            )
        )

    return rows, pools


def build_player_rating_rows(
    hitters: pd.DataFrame,
    pitchers: pd.DataFrame,
    *,
    pools: dict[tuple[str, str], pd.Series],
    season: int,
    as_of_date: date,
    team_code: str,
    team_games_played: int,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    team_code = team_code.upper()

    team_hitters = hitters.loc[hitters.get("team").eq(team_code)].copy()
    hitter_qualification_minimum = 3.1 * team_games_played if team_games_played > 0 else None
    for hitter in team_hitters.itertuples(index=False):
        pa = _row_value(hitter, "pa")
        qualified = _qualification_flag(pa, hitter_qualification_minimum)
        rows.extend(
            _rating_rows_for_player(
                hitter,
                stat_definitions=HITTER_STAT_DEFINITIONS,
                pools=pools,
                season=season,
                as_of_date=as_of_date,
                player_group=PLAYER_GROUP_HITTER,
                baseline_group=PLAYER_GROUP_HITTER,
                qualification_metric="PA",
                qualification_value=pa,
                qualification_minimum=hitter_qualification_minimum,
                qualified=qualified,
            )
        )

    team_pitchers = pitchers.loc[pitchers.get("team").eq(team_code)].copy()
    pitcher_qualification_minimum = float(team_games_played) if team_games_played > 0 else None
    for pitcher in team_pitchers.itertuples(index=False):
        player_group = _row_value(pitcher, "player_group") or PLAYER_GROUP_RELIEVER
        baseline_group = _baseline_group_for_pitcher(player_group, pools)
        ip = _row_value(pitcher, "ip")
        qualified = _qualification_flag(ip, pitcher_qualification_minimum)
        rows.extend(
            _rating_rows_for_player(
                pitcher,
                stat_definitions=PITCHER_STAT_DEFINITIONS,
                pools=pools,
                season=season,
                as_of_date=as_of_date,
                player_group=player_group,
                baseline_group=baseline_group,
                qualification_metric="IP",
                qualification_value=ip,
                qualification_minimum=pitcher_qualification_minimum,
                qualified=qualified,
            )
        )

    return rows


def rating_tier_for_percentile(percentile: object) -> str | None:
    if percentile is None or pd.isna(percentile):
        return None
    value = float(percentile)
    if value < 15:
        return "Poor"
    if value < 40:
        return "Below Average"
    if value < 60:
        return "Average"
    if value < 75:
        return "Above Average"
    if value < 90:
        return "Very Good"
    if value < 95:
        return "Great"
    return "Elite"


def calculate_percentile(value: object, pool_values: pd.Series, *, direction: str) -> float | None:
    if value is None or pd.isna(value):
        return None
    values = pd.to_numeric(pool_values, errors="coerce").dropna()
    if values.empty:
        return None
    numeric_value = float(value)
    if direction == DIRECTION_LOWER:
        percentile = (values >= numeric_value).mean() * 100
    else:
        percentile = (values <= numeric_value).mean() * 100
    return max(0.0, min(100.0, float(percentile)))


def build_rating_display_frame(rows: pd.DataFrame, stat_definitions: tuple[StatDefinition, ...]) -> pd.DataFrame:
    rows_by_stat = {}
    if not rows.empty:
        working = rows.copy()
        working["stat_key"] = working["stat_key"].astype(str)
        rows_by_stat = {row["stat_key"]: row for _, row in working.iterrows()}

    display_rows: list[dict[str, object]] = []
    for definition in stat_definitions:
        row = rows_by_stat.get(definition.key)
        value = None if row is None else row.get("stat_value")
        percentile = None if row is None else row.get("league_percentile")
        display_rows.append(
            {
                "Stat": definition.label,
                "Value": format_stat_value(value, definition.display_format),
                "League Percentile": format_percentile(percentile),
                "Rating Tier": None if row is None else row.get("rating_tier"),
                "MLB Qualified?": "N/A" if row is None else row.get("mlb_qualified") or "N/A",
            }
        )
    return pd.DataFrame(display_rows)


def format_stat_value(value: object, display_format: str) -> str | None:
    if value is None or pd.isna(value):
        return None
    numeric_value = float(value)
    if display_format == "rate3":
        return f"{numeric_value:.3f}"
    if display_format == "pct1":
        return f"{numeric_value:.1f}%"
    if display_format == "int":
        return str(int(round(numeric_value)))
    if display_format == "decimal2":
        return f"{numeric_value:.2f}"
    return str(value)


def format_percentile(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    rounded = int(round(float(value)))
    rounded = max(0, min(100, rounded))
    if 10 <= rounded % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(rounded % 10, "th")
    return f"{rounded}{suffix}"


def _cutoff_rows_for_group(
    frame: pd.DataFrame,
    *,
    stat_definitions: tuple[StatDefinition, ...],
    pools: dict[tuple[str, str], pd.Series],
    season: int,
    as_of_date: date,
    player_group: str,
    pool_minimum_metric: str,
    pool_minimum_value: int,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for definition in stat_definitions:
        values = pd.to_numeric(frame.get(definition.key), errors="coerce").dropna()
        if values.empty:
            continue
        pools[(player_group, definition.key)] = values
        cutoffs = _percentile_cutoffs(values, direction=definition.direction)
        rows.append(
            {
                "season": season,
                "as_of_date": as_of_date,
                "player_group": player_group,
                "stat_key": definition.key,
                "stat_label": definition.label,
                "direction": definition.direction,
                "pool_minimum_metric": pool_minimum_metric,
                "pool_minimum_value": pool_minimum_value,
                "sample_size": len(values),
                "p15": cutoffs[15],
                "p40": cutoffs[40],
                "p60": cutoffs[60],
                "p75": cutoffs[75],
                "p90": cutoffs[90],
                "p95": cutoffs[95],
                "created_at": _created_at(),
            }
        )
    return rows


def _rating_rows_for_player(
    player: object,
    *,
    stat_definitions: tuple[StatDefinition, ...],
    pools: dict[tuple[str, str], pd.Series],
    season: int,
    as_of_date: date,
    player_group: str,
    baseline_group: str,
    qualification_metric: str,
    qualification_value: object,
    qualification_minimum: float | None,
    qualified: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for definition in stat_definitions:
        stat_value = _row_value(player, definition.key)
        if stat_value is None or pd.isna(stat_value):
            continue
        pool_values = pools.get((baseline_group, definition.key), pd.Series(dtype="float64"))
        percentile = calculate_percentile(stat_value, pool_values, direction=definition.direction)
        rows.append(
            {
                "season": season,
                "as_of_date": as_of_date,
                "player_name": _row_value(player, "player_name"),
                "player_key": _row_value(player, "player_key"),
                "team": _row_value(player, "team"),
                "player_group": player_group,
                "baseline_group": baseline_group,
                "stat_key": definition.key,
                "stat_label": definition.label,
                "direction": definition.direction,
                "stat_value": float(stat_value),
                "league_percentile": percentile,
                "rating_tier": rating_tier_for_percentile(percentile),
                "mlb_qualified": qualified,
                "qualification_metric": qualification_metric,
                "qualification_value": qualification_value,
                "qualification_minimum": qualification_minimum,
                "created_at": _created_at(),
            }
        )
    return rows


def _baseline_group_for_pitcher(player_group: str, pools: dict[tuple[str, str], pd.Series]) -> str:
    group = str(player_group)
    if any(key[0] == group for key in pools):
        return group
    if group == PLAYER_GROUP_CLOSER and any(key[0] == PLAYER_GROUP_RELIEVER for key in pools):
        return PLAYER_GROUP_RELIEVER
    return group


def _percentile_cutoffs(values: pd.Series, *, direction: str) -> dict[int, float]:
    numeric_values = pd.to_numeric(values, errors="coerce").dropna()
    cutoffs: dict[int, float] = {}
    for percentile in PERCENTILE_CUTOFFS:
        quantile = percentile / 100
        if direction == DIRECTION_LOWER:
            quantile = 1 - quantile
        cutoffs[percentile] = float(numeric_values.quantile(quantile))
    return cutoffs


def _replace_league_context_rows(
    conn: duckdb.DuckDBPyConnection,
    *,
    season: int,
    as_of_date: date,
    cutoff_rows: list[dict[str, object]],
    rating_rows: list[dict[str, object]],
) -> None:
    conn.execute(
        "DELETE FROM league_stat_cutoffs WHERE season = ? AND as_of_date = ?",
        [season, as_of_date],
    )
    conn.execute(
        "DELETE FROM player_league_context_ratings WHERE season = ? AND as_of_date = ?",
        [season, as_of_date],
    )

    if cutoff_rows:
        cutoff_frame = pd.DataFrame(cutoff_rows)
        conn.register("league_cutoffs_stage", cutoff_frame)
        conn.execute(
            """
            INSERT INTO league_stat_cutoffs (
                season,
                as_of_date,
                player_group,
                stat_key,
                stat_label,
                direction,
                pool_minimum_metric,
                pool_minimum_value,
                sample_size,
                p15,
                p40,
                p60,
                p75,
                p90,
                p95,
                created_at
            )
            SELECT
                season,
                as_of_date,
                player_group,
                stat_key,
                stat_label,
                direction,
                pool_minimum_metric,
                pool_minimum_value,
                sample_size,
                p15,
                p40,
                p60,
                p75,
                p90,
                p95,
                created_at
            FROM league_cutoffs_stage
            """
        )
        conn.unregister("league_cutoffs_stage")

    if rating_rows:
        rating_frame = pd.DataFrame(rating_rows)
        conn.register("player_ratings_stage", rating_frame)
        conn.execute(
            """
            INSERT INTO player_league_context_ratings (
                season,
                as_of_date,
                player_name,
                player_key,
                team,
                player_group,
                baseline_group,
                stat_key,
                stat_label,
                direction,
                stat_value,
                league_percentile,
                rating_tier,
                mlb_qualified,
                qualification_metric,
                qualification_value,
                qualification_minimum,
                created_at
            )
            SELECT
                season,
                as_of_date,
                player_name,
                player_key,
                team,
                player_group,
                baseline_group,
                stat_key,
                stat_label,
                direction,
                stat_value,
                league_percentile,
                rating_tier,
                mlb_qualified,
                qualification_metric,
                qualification_value,
                qualification_minimum,
                created_at
            FROM player_ratings_stage
            """
        )
        conn.unregister("player_ratings_stage")


def _infer_team_games_played(hitters: pd.DataFrame, pitchers: pd.DataFrame, *, team_code: str) -> int:
    team_code = team_code.upper()
    hitter_games = pd.to_numeric(hitters.loc[hitters.get("team").eq(team_code), "games"], errors="coerce")
    pitcher_games = pd.to_numeric(pitchers.loc[pitchers.get("team").eq(team_code), "games"], errors="coerce")
    values = pd.concat([hitter_games, pitcher_games], ignore_index=True).dropna()
    return int(values.max()) if not values.empty else 0


def _add_calculated_fip(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    working = frame.copy()
    innings = _numeric_from_aliases(working, ("IP",)).map(_parse_innings_pitched)
    home_runs = _numeric_from_aliases(working, ("HR",))
    walks = _numeric_from_aliases(working, ("BB",))
    hit_batters = _numeric_from_aliases(working, ("HBP",)).fillna(0)
    strikeouts = _numeric_from_aliases(working, ("SO",))
    earned_runs = _numeric_from_aliases(working, ("ER",))

    valid = innings.gt(0)
    league_era = (earned_runs[valid].sum() * 9 / innings[valid].sum()) if valid.any() and innings[valid].sum() > 0 else None
    league_fip_core = (
        (13 * home_runs[valid].sum() + 3 * (walks[valid].sum() + hit_batters[valid].sum()) - 2 * strikeouts[valid].sum())
        / innings[valid].sum()
        if valid.any() and innings[valid].sum() > 0
        else None
    )
    fip_constant = (league_era - league_fip_core) if league_era is not None and league_fip_core is not None else 3.1
    working["FIP"] = ((13 * home_runs + 3 * (walks + hit_batters) - 2 * strikeouts) / innings) + fip_constant
    working.loc[~valid, "FIP"] = pd.NA
    return working


def _rate_per_100(numerator: object, denominator: object) -> float | None:
    numerator_value = _stats_number(numerator)
    denominator_value = _stats_number(denominator)
    if numerator_value is None or denominator_value is None or denominator_value == 0:
        return None
    return numerator_value * 100 / denominator_value


def _subtract_stats(left: object, right: object) -> float | None:
    left_value = _stats_number(left)
    right_value = _stats_number(right)
    if left_value is None or right_value is None:
        return None
    return left_value - right_value


def _stats_number(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.replace("%", "").replace(",", "").strip()
        if not cleaned or "-" in cleaned:
            return None
        if cleaned.startswith("."):
            cleaned = f"0{cleaned}"
        try:
            return float(cleaned)
        except ValueError:
            return None
    try:
        if pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _pick_series(raw_df: pd.DataFrame, aliases: tuple[str, ...], default=None) -> pd.Series:
    for alias in aliases:
        if alias in raw_df.columns:
            return raw_df[alias]
    return pd.Series([default] * len(raw_df), index=raw_df.index)


def _created_at() -> datetime:
    return datetime.now()


def _round_half_up(value: float) -> int:
    return int(value + 0.5)


def _numeric_from_aliases(raw_df: pd.DataFrame, aliases: tuple[str, ...], *, is_percentage: bool = False) -> pd.Series:
    raw = _pick_series(raw_df, aliases)
    as_text = raw.astype(str)
    numeric = pd.to_numeric(as_text.str.replace("%", "", regex=False).str.replace(",", "", regex=False), errors="coerce")
    if is_percentage and not numeric.dropna().empty and numeric.dropna().abs().max() <= 1:
        numeric = numeric * 100
    return numeric


def _parse_innings_pitched(value: object) -> float:
    if value is None or pd.isna(value):
        return float("nan")
    numeric = float(value)
    whole = int(numeric)
    fraction = round(numeric - whole, 3)
    if abs(fraction - 0.1) < 0.001:
        return whole + (1 / 3)
    if abs(fraction - 0.2) < 0.001:
        return whole + (2 / 3)
    return numeric


def _qualification_flag(value: object, minimum: float | None) -> str:
    if minimum is None or value is None or pd.isna(value):
        return "N/A"
    return "Yes" if float(value) >= minimum else "No"


def _row_value(row: object, name: str):
    if hasattr(row, name):
        return getattr(row, name)
    if isinstance(row, pd.Series):
        return row.get(name)
    if isinstance(row, dict):
        return row.get(name)
    return None
