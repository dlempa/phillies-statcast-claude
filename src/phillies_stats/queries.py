from __future__ import annotations

import calendar
from datetime import date

import duckdb
import pandas as pd

from phillies_stats.config import get_config
from phillies_stats.display import format_player_name, normalize_player_key
from phillies_stats.league_context import (
    HITTER_STAT_DEFINITIONS,
    PITCHER_STAT_DEFINITIONS,
    PLAYER_GROUP_CLOSER,
    PLAYER_GROUP_RELIEVER,
    build_rating_display_frame,
    derive_pitcher_group,
    pitcher_position_to_group,
)

TEAM_RANK_STAT_DEFINITIONS = {
    "batting_average": ("hitting", "BA", "batting_average", "higher", "rate3"),
    "on_base_percentage": ("hitting", "OBP", "on_base_percentage", "higher", "rate3"),
    "ops": ("hitting", "OPS", "ops", "higher", "rate3"),
    "home_runs": ("hitting", "HR", "home_runs", "higher", "int"),
    "runs": ("hitting", "Runs", "runs", "higher", "int"),
    "era": ("pitching", "ERA", "era", "lower", "decimal2"),
    "whip": ("pitching", "WHIP", "whip", "lower", "decimal2"),
    "strikeouts": ("pitching", "Pitching K", "strikeouts", "higher", "int"),
    "walks": ("pitching", "Walks Allowed", "walks", "lower", "int"),
    "home_runs_allowed": ("pitching", "HR Allowed", "home_runs_allowed", "lower", "int"),
}
NL_EAST_TEAM_CODES = ("ATL", "MIA", "NYM", "PHI", "WSH")


def get_last_updated(conn: duckdb.DuckDBPyConnection):
    run_row = conn.execute(
        """
        SELECT MAX(completed_at)
        FROM ingestion_runs
        WHERE status = 'completed'
        """
    ).fetchone()
    if run_row and run_row[0]:
        return run_row[0]
    event_row = conn.execute("SELECT MAX(created_at) FROM statcast_events").fetchone()
    return event_row[0] if event_row else None


def get_dashboard_metrics(conn: duckdb.DuckDBPyConnection) -> dict[str, object]:
    longest_hr = conn.execute(
        """
        SELECT player_name, distance_ft
        FROM longest_home_runs
        ORDER BY rank
        LIMIT 1
        """
    ).fetchone()
    most_hrs = conn.execute(
        """
        SELECT player_name, home_run_count
        FROM player_home_run_summary
        ORDER BY home_run_count DESC, max_hr_distance_ft DESC NULLS LAST, player_name ASC
        LIMIT 1
        """
    ).fetchone()
    hardest_hr = conn.execute(
        """
        SELECT player_name, exit_velocity_mph
        FROM hardest_hit_home_runs
        LIMIT 1
        """
    ).fetchone()
    hardest_ball = conn.execute(
        """
        SELECT player_name, exit_velocity_mph
        FROM hardest_hit_balls_overall
        LIMIT 1
        """
    ).fetchone()
    return {
        "longest_hr": _format_metric_tuple(longest_hr),
        "most_hrs": _format_metric_tuple(most_hrs),
        "hardest_hr": _format_metric_tuple(hardest_hr),
        "hardest_ball": _format_metric_tuple(hardest_ball),
    }


def _format_metric_tuple(result: tuple[object, object] | None) -> tuple[object, object] | None:
    if not result:
        return result
    player_name, value = result
    return (format_player_name(player_name), value)


def get_pitching_dashboard_metrics(conn: duckdb.DuckDBPyConnection) -> dict[str, object]:
    overview = _build_pitcher_overview(conn)
    strikeout_leader = conn.execute(
        """
        SELECT player_name, strikeouts
        FROM pitcher_event_summary
        ORDER BY strikeouts DESC, player_name ASC
        LIMIT 1
        """
    ).fetchone()
    fastest_pitch = conn.execute(
        """
        SELECT player_name, release_speed
        FROM fastest_pitches
        LIMIT 1
        """
    ).fetchone()
    wins_leader = None
    innings_leader = None
    if not overview.empty:
        wins_df = overview.loc[overview["wins"].notna(), ["player_name", "wins"]].sort_values(
            ["wins", "player_name"], ascending=[False, True]
        )
        innings_df = overview.loc[overview["innings_pitched"].notna(), ["player_name", "innings_pitched"]].sort_values(
            ["innings_pitched", "player_name"], ascending=[False, True]
        )
        wins_leader = tuple(wins_df.iloc[0]) if not wins_df.empty else None
        innings_leader = tuple(innings_df.iloc[0]) if not innings_df.empty else None
    return {
        "strikeout_leader": _format_metric_tuple(strikeout_leader),
        "fastest_pitch": _format_metric_tuple(fastest_pitch),
        "wins_leader": _format_metric_tuple(wins_leader),
        "innings_leader": _format_metric_tuple(innings_leader),
    }


def get_top_longest_home_runs(
    conn: duckdb.DuckDBPyConnection,
    *,
    limit: int = 10,
    player: str | None = None,
    month: int | None = None,
    home_away: str | None = None,
) -> pd.DataFrame:
    sql = """
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY distance_ft DESC NULLS LAST, exit_velocity_mph DESC NULLS LAST, game_date ASC, player_name ASC
            ) AS rank,
            player_name,
            game_date,
            opponent,
            venue_name,
            home_away,
            distance_ft,
            exit_velocity_mph,
            launch_angle
        FROM longest_home_runs
        WHERE 1=1
    """
    params: list[object] = []
    if player:
        sql += " AND LOWER(player_name) = LOWER(?)"
        params.append(player)
    if month:
        sql += " AND EXTRACT(MONTH FROM game_date) = ?"
        params.append(month)
    if home_away:
        sql += " AND home_away = ?"
        params.append(home_away)
    sql += " ORDER BY distance_ft DESC NULLS LAST, exit_velocity_mph DESC NULLS LAST, game_date ASC, player_name ASC LIMIT ?"
    params.append(limit)
    return conn.execute(sql, params).df()


def get_hr_distance_over_time(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT game_date, player_name, distance_ft, opponent, venue_name
        FROM longest_home_runs
        ORDER BY game_date ASC, distance_ft DESC
        """
    ).df()


def get_hardest_hit_home_runs(conn: duckdb.DuckDBPyConnection, limit: int = 15) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT *
        FROM hardest_hit_home_runs
        LIMIT ?
        """,
        [limit],
    ).df()


def get_shortest_home_runs(conn: duckdb.DuckDBPyConnection, limit: int = 15) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            player_name,
            game_date,
            opponent,
            venue_name,
            distance_ft,
            exit_velocity_mph,
            launch_angle
        FROM longest_home_runs
        ORDER BY distance_ft ASC, game_date ASC, player_name ASC
        LIMIT ?
        """,
        [limit],
    ).df()


def get_player_hr_distance_stats(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            player_name,
            home_run_count,
            avg_hr_distance_ft,
            max_hr_distance_ft
        FROM player_home_run_summary
        ORDER BY home_run_count DESC, max_hr_distance_ft DESC NULLS LAST, player_name ASC
        """
    ).df()


def get_hardest_hit_balls(conn: duckdb.DuckDBPyConnection, limit: int = 10) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT *
        FROM hardest_hit_balls_overall
        LIMIT ?
        """,
        [limit],
    ).df()


def get_player_options(conn: duckdb.DuckDBPyConnection) -> list[str]:
    config = get_config()
    rows = conn.execute(
        """
        SELECT player_name, 1 AS source_priority
        FROM hitter_event_summary
        WHERE player_name IS NOT NULL
          AND TRIM(player_name) <> ''
        UNION
        SELECT player_name, 2 AS source_priority
        FROM player_league_context_ratings
        WHERE season = ?
          AND team = ?
          AND player_group = 'hitter'
          AND player_name IS NOT NULL
          AND TRIM(player_name) <> ''
        ORDER BY player_name ASC
        """,
        [config.season, config.team_code],
    ).fetchall()
    options_by_key: dict[str, tuple[int, str]] = {}
    for name, source_priority in rows:
        formatted = format_player_name(name)
        player_key = normalize_player_key(formatted)
        if not isinstance(formatted, str) or not player_key:
            continue
        existing = options_by_key.get(player_key)
        if existing is None or source_priority < existing[0]:
            options_by_key[player_key] = (int(source_priority), formatted)
    return sorted(name for _, name in options_by_key.values())


def get_player_summary(conn: duckdb.DuckDBPyConnection, player_name: str) -> dict[str, object]:
    hitter_summaries = conn.execute(
        """
        SELECT
            player_name,
            home_run_count,
            max_hr_distance_ft,
            avg_hr_distance_ft,
            hardest_hit_ball_mph
        FROM hitter_event_summary
        """
    ).df()
    hr_summary = None
    if not hitter_summaries.empty:
        summary_row = _filter_player_frame(hitter_summaries, player_name)
        if not summary_row.empty:
            row = summary_row.iloc[0]
            hr_summary = (
                _none_if_missing(row.get("home_run_count")),
                _none_if_missing(row.get("max_hr_distance_ft")),
                _none_if_missing(row.get("avg_hr_distance_ft")),
                _none_if_missing(row.get("hardest_hit_ball_mph")),
            )
    monthly = conn.execute(
        """
        SELECT player_name, month_start, home_run_count
        FROM monthly_home_run_totals
        ORDER BY month_start ASC
        """
    ).df()
    monthly = _filter_player_frame(monthly, player_name)
    if not monthly.empty:
        monthly["month_name"] = monthly["month_start"].map(lambda value: calendar.month_name[value.month])
        monthly = monthly[["month_start", "home_run_count", "month_name"]]

    home_runs = conn.execute(
        """
        SELECT
            batter_name AS player_name,
            event_id,
            game_date,
            opponent,
            venue_name,
            hit_distance_sc AS distance_ft,
            launch_speed AS exit_velocity_mph,
            launch_angle
        FROM phillies_home_runs
        ORDER BY game_date ASC, event_id ASC
        """
    ).df()
    home_runs = _filter_player_frame(home_runs, player_name)
    if not home_runs.empty:
        home_runs = home_runs.sort_values(["game_date", "event_id"]).reset_index(drop=True)
        home_runs.insert(0, "home_run_number", range(1, len(home_runs) + 1))
        home_runs = home_runs[
            [
                "home_run_number",
                "game_date",
                "opponent",
                "venue_name",
                "distance_ft",
                "exit_velocity_mph",
                "launch_angle",
            ]
        ]

    league_context = get_hitter_league_context_ratings(conn, player_name)

    return {"summary": hr_summary, "monthly": monthly, "home_runs": home_runs, "league_context": league_context}


def get_game_log(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            game_date,
            opponent,
            venue_name,
            result_text,
            hr_count,
            longest_hr_ft,
            hardest_hit_ball_mph
        FROM game_log_summaries
        ORDER BY game_date DESC
        """
    ).df()


def get_month_options(conn: duckdb.DuckDBPyConnection) -> list[int]:
    rows = conn.execute(
        """
        SELECT DISTINCT EXTRACT(MONTH FROM game_date)::INTEGER AS month_number
        FROM phillies_home_runs
        ORDER BY month_number ASC
        """
    ).fetchall()
    return [row[0] for row in rows]


def get_latest_game_date(conn: duckdb.DuckDBPyConnection) -> date | None:
    row = conn.execute("SELECT MAX(game_date) FROM statcast_events").fetchone()
    return row[0] if row else None


def get_team_context_last_updated(conn: duckdb.DuckDBPyConnection) -> date | None:
    row = conn.execute(
        """
        SELECT MAX(as_of_date)
        FROM (
            SELECT as_of_date FROM team_season_stats
            UNION ALL
            SELECT as_of_date FROM division_standings
        )
        """
    ).fetchone()
    return row[0] if row else None


def get_team_local_summary(conn: duckdb.DuckDBPyConnection) -> dict[str, object]:
    frame = conn.execute(
        """
        SELECT
            game_date,
            result_text,
            CASE WHEN phillies_home THEN home_score ELSE away_score END AS runs_for,
            CASE WHEN phillies_home THEN away_score ELSE home_score END AS runs_against
        FROM games
        WHERE result_text IS NOT NULL
        ORDER BY game_date ASC, game_pk ASC
        """
    ).df()
    if frame.empty:
        return {
            "record": "0-0",
            "wins": 0,
            "losses": 0,
            "streak": "No games",
            "runs_for": 0,
            "runs_against": 0,
            "run_differential": 0,
            "latest_game_date": None,
        }

    result_prefix = frame["result_text"].astype(str).str[0]
    wins = int(result_prefix.eq("W").sum())
    losses = int(result_prefix.eq("L").sum())
    runs_for = int(pd.to_numeric(frame["runs_for"], errors="coerce").fillna(0).sum())
    runs_against = int(pd.to_numeric(frame["runs_against"], errors="coerce").fillna(0).sum())
    latest_game_date = frame["game_date"].max()
    streak = _current_result_streak(frame)
    return {
        "record": f"{wins}-{losses}",
        "wins": wins,
        "losses": losses,
        "streak": streak,
        "runs_for": runs_for,
        "runs_against": runs_against,
        "run_differential": runs_for - runs_against,
        "latest_game_date": latest_game_date,
    }


def get_nl_east_standings(conn: duckdb.DuckDBPyConnection, *, season: int | None = None) -> pd.DataFrame:
    config = get_config(season)
    latest = _latest_as_of_date(conn, "division_standings", config.season)
    if latest is None:
        return pd.DataFrame()
    return conn.execute(
        """
        SELECT
            division_rank,
            team_abbr,
            team_name,
            wins,
            losses,
            winning_percentage,
            games_back,
            runs_scored,
            runs_allowed,
            run_differential,
            streak
        FROM division_standings
        WHERE season = ?
          AND as_of_date = ?
          AND team_abbr IN ('ATL', 'MIA', 'NYM', 'PHI', 'WSH')
        ORDER BY division_rank ASC, winning_percentage DESC, wins DESC
        """,
        [config.season, latest],
    ).df()


def get_phillies_team_rankings(conn: duckdb.DuckDBPyConnection, *, season: int | None = None) -> pd.DataFrame:
    config = get_config(season)
    latest = _latest_as_of_date(conn, "team_season_stats", config.season)
    if latest is None:
        return pd.DataFrame()
    stats = conn.execute(
        """
        SELECT *
        FROM team_season_stats
        WHERE season = ?
          AND as_of_date = ?
        """,
        [config.season, latest],
    ).df()
    if stats.empty:
        return stats

    rows: list[dict[str, object]] = []
    for _, (stat_group, label, column, direction, display_format) in TEAM_RANK_STAT_DEFINITIONS.items():
        group_frame = stats.loc[stats["stat_group"].eq(stat_group)].copy()
        if group_frame.empty or column not in group_frame.columns:
            continue
        phillies_row = group_frame.loc[group_frame["team_abbr"].eq(config.team_code)]
        if phillies_row.empty:
            continue
        value = phillies_row.iloc[0].get(column)
        rows.append(
            {
                "Stat": label,
                "Value": _format_team_stat_value(value, display_format),
                "NL Rank": _rank_team(group_frame.loc[group_frame["league"].eq("National League")], column, config.team_code, direction),
                "MLB Rank": _rank_team(group_frame, column, config.team_code, direction),
            }
        )
    return pd.DataFrame(rows)


def get_team_recent_results(conn: duckdb.DuckDBPyConnection, *, limit: int = 10) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            game_date,
            opponent,
            result_text,
            CASE WHEN phillies_home THEN home_score ELSE away_score END AS runs_for,
            CASE WHEN phillies_home THEN away_score ELSE home_score END AS runs_against
        FROM games
        WHERE result_text IS NOT NULL
        ORDER BY game_date DESC, game_pk DESC
        LIMIT ?
        """,
        [limit],
    ).df()


def get_team_run_differential_trend(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    frame = conn.execute(
        """
        SELECT
            game_date,
            opponent,
            result_text,
            CASE WHEN phillies_home THEN home_score ELSE away_score END AS runs_for,
            CASE WHEN phillies_home THEN away_score ELSE home_score END AS runs_against
        FROM games
        WHERE result_text IS NOT NULL
        ORDER BY game_date ASC, game_pk ASC
        """
    ).df()
    if frame.empty:
        return frame
    frame["game_run_differential"] = pd.to_numeric(frame["runs_for"], errors="coerce").fillna(0) - pd.to_numeric(
        frame["runs_against"], errors="coerce"
    ).fillna(0)
    frame["cumulative_run_differential"] = frame["game_run_differential"].cumsum()
    return frame


def get_team_pitching_run_prevention_trend(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    frame = conn.execute(
        """
        WITH pitching_events AS (
            SELECT
                game_pk,
                SUM(CASE WHEN is_strikeout = TRUE THEN 1 ELSE 0 END) AS strikeouts,
                SUM(CASE WHEN events IN ('walk', 'intent_walk') THEN 1 ELSE 0 END) AS walks,
                SUM(CASE WHEN events = 'home_run' THEN 1 ELSE 0 END) AS home_runs_allowed
            FROM statcast_events
            WHERE is_phillies_pitcher = TRUE
            GROUP BY game_pk
        )
        SELECT
            g.game_pk,
            g.game_date,
            g.opponent,
            g.result_text,
            CASE WHEN g.phillies_home THEN g.away_score ELSE g.home_score END AS runs_allowed,
            COALESCE(p.strikeouts, 0) AS strikeouts,
            COALESCE(p.walks, 0) AS walks,
            COALESCE(p.home_runs_allowed, 0) AS home_runs_allowed
        FROM games g
        LEFT JOIN pitching_events p
            ON g.game_pk = p.game_pk
        WHERE g.result_text IS NOT NULL
        ORDER BY g.game_date ASC, g.game_pk ASC
        """
    ).df()
    if frame.empty:
        return frame

    frame["runs_allowed"] = pd.to_numeric(frame["runs_allowed"], errors="coerce").fillna(0)
    for column in ["strikeouts", "walks", "home_runs_allowed"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0).astype(int)

    frame["game_number"] = range(1, len(frame) + 1)
    frame["rolling_5_ra_per_game"] = frame["runs_allowed"].rolling(window=5, min_periods=1).mean().round(2)
    frame["season_ra_per_game"] = (frame["runs_allowed"].cumsum() / frame["game_number"]).round(2)
    frame["runs_allowed"] = frame["runs_allowed"].astype(int)
    return frame


def get_latest_team_state_summary(conn: duckdb.DuckDBPyConnection, *, season: int | None = None) -> dict[str, object] | None:
    config = get_config(season)
    row = conn.execute(
        """
        SELECT
            season,
            as_of_date,
            headline,
            summary_text,
            tone_label,
            key_stats_json,
            sources_json,
            generated_at,
            prompt_version
        FROM team_state_summaries
        WHERE season = ?
        ORDER BY as_of_date DESC, generated_at DESC
        LIMIT 1
        """,
        [config.season],
    ).fetchone()
    if row is None:
        return None
    columns = [
        "season",
        "as_of_date",
        "headline",
        "summary_text",
        "tone_label",
        "key_stats_json",
        "sources_json",
        "generated_at",
        "prompt_version",
    ]
    return dict(zip(columns, row))


def _ensure_pitcher_summary_loaded(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute("SELECT COUNT(*) FROM pitcher_season_summary").fetchone()
    if row and row[0] > 0:
        return
    try:
        from phillies_stats.ingest import refresh_pitcher_season_summary

        config = get_config()
        refresh_pitcher_season_summary(conn, season=config.season, team_code=config.team_code)
    except Exception:
        return


def _build_pitcher_overview(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    _ensure_pitcher_summary_loaded(conn)

    event_summary = conn.execute(
        """
        SELECT
            pitcher_id,
            player_name,
            appearances,
            strikeouts,
            walks_issued,
            home_runs_allowed,
            whiffs,
            hardest_hit_allowed_mph,
            max_velocity_mph,
            avg_fastball_velocity_mph
        FROM pitcher_event_summary
        """
    ).df()
    season_summary = conn.execute(
        """
        SELECT
            pitcher_name AS player_name,
            wins,
            losses,
            games,
            games_started,
            saves,
            innings_pitched,
            strikeouts,
            walks,
            home_runs_allowed,
            era,
            whip,
            avg_fastball_velocity
        FROM pitcher_season_summary
        """
    ).df()

    event_columns = [
        "pitcher_id",
        "player_name",
        "appearances",
        "strikeouts",
        "walks_issued",
        "home_runs_allowed",
        "whiffs",
        "hardest_hit_allowed_mph",
        "max_velocity_mph",
        "avg_fastball_velocity_mph",
    ]
    season_columns = [
        "player_name",
        "wins",
        "losses",
        "games",
        "games_started",
        "saves",
        "innings_pitched",
        "strikeouts",
        "walks",
        "home_runs_allowed",
        "era",
        "whip",
        "avg_fastball_velocity",
    ]

    if not event_summary.empty:
        event_summary["player_name"] = event_summary["player_name"].map(format_player_name)
        event_summary["player_key"] = event_summary["player_name"].map(normalize_player_key)
    else:
        event_summary = pd.DataFrame(columns=event_columns + ["player_key"])

    if not season_summary.empty:
        season_summary["player_name"] = season_summary["player_name"].map(format_player_name)
        season_summary["player_key"] = season_summary["player_name"].map(normalize_player_key)
    else:
        season_summary = pd.DataFrame(columns=season_columns + ["player_key"])

    overview = event_summary.merge(
        season_summary,
        on="player_key",
        how="outer",
        suffixes=("_event", "_season"),
    )
    if overview.empty:
        return overview

    for column in [
        "player_name_event",
        "player_name_season",
        "strikeouts_event",
        "strikeouts_season",
        "walks_issued",
        "walks",
        "home_runs_allowed_event",
        "home_runs_allowed_season",
        "avg_fastball_velocity_mph",
        "avg_fastball_velocity",
        "wins",
        "losses",
        "games",
        "games_started",
        "saves",
        "innings_pitched",
        "era",
        "whip",
        "appearances",
        "whiffs",
        "hardest_hit_allowed_mph",
        "max_velocity_mph",
        "pitcher_id",
    ]:
        if column not in overview.columns:
            overview[column] = pd.NA

    overview["player_name"] = overview["player_name_season"].fillna(overview["player_name_event"])
    overview["strikeouts"] = _coalesce_series(overview["strikeouts_season"], overview["strikeouts_event"])
    overview["walks_issued"] = _coalesce_series(overview["walks"], overview["walks_issued"])
    overview["home_runs_allowed"] = _coalesce_series(
        overview["home_runs_allowed_season"],
        overview["home_runs_allowed_event"],
    )
    overview["avg_fastball_velocity_mph"] = _coalesce_series(
        overview["avg_fastball_velocity"],
        overview["avg_fastball_velocity_mph"],
    )
    overview["position"] = overview.apply(_derive_pitcher_position, axis=1)
    return overview


def _derive_pitcher_position(row: pd.Series) -> str:
    player_group = derive_pitcher_group(row.get("games_started"), row.get("games"), row.get("saves"))
    return {"starter": "Starter", "reliever": "Reliever", "closer": "Closer"}.get(player_group, "Reliever")


def _none_if_missing(value: object) -> object:
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except (AttributeError, ValueError):
            return value
    return value


def _coalesce_series(primary: pd.Series, fallback: pd.Series) -> pd.Series:
    result = primary.copy()
    missing = result.isna()
    result.loc[missing] = fallback.loc[missing]
    return result


def _filter_player_frame(frame: pd.DataFrame, player_name: str, column: str = "player_name") -> pd.DataFrame:
    if frame.empty:
        return frame
    target_key = normalize_player_key(player_name)
    working = frame.copy()
    working["_player_key"] = working[column].map(normalize_player_key)
    filtered = working.loc[working["_player_key"].eq(target_key)].drop(columns=["_player_key"])
    return filtered


def _filter_pitcher_frame(frame: pd.DataFrame, player_name: str, column: str = "player_name") -> pd.DataFrame:
    return _filter_player_frame(frame, player_name, column)


def get_hitter_league_context_ratings(
    conn: duckdb.DuckDBPyConnection,
    player_name: str,
    *,
    season: int | None = None,
) -> pd.DataFrame:
    config = get_config(season)
    raw_rows = _get_latest_league_context_rows(
        conn,
        player_name=player_name,
        season=config.season,
        player_groups=["hitter"],
    )
    return build_rating_display_frame(raw_rows, HITTER_STAT_DEFINITIONS)


def get_pitcher_league_context_ratings(
    conn: duckdb.DuckDBPyConnection,
    player_name: str,
    position: object = None,
    *,
    season: int | None = None,
) -> pd.DataFrame:
    config = get_config(season)
    player_group = pitcher_position_to_group(position)
    raw_rows = _get_latest_league_context_rows(
        conn,
        player_name=player_name,
        season=config.season,
        player_groups=[player_group],
    )
    if raw_rows.empty and player_group == PLAYER_GROUP_CLOSER:
        raw_rows = _get_latest_league_context_rows(
            conn,
            player_name=player_name,
            season=config.season,
            player_groups=[PLAYER_GROUP_RELIEVER],
        )
    return build_rating_display_frame(raw_rows, PITCHER_STAT_DEFINITIONS)


def _get_latest_league_context_rows(
    conn: duckdb.DuckDBPyConnection,
    *,
    player_name: str,
    season: int,
    player_groups: list[str],
) -> pd.DataFrame:
    player_key = normalize_player_key(player_name)
    if not player_key or not player_groups:
        return pd.DataFrame()

    placeholders = ", ".join(["?"] * len(player_groups))
    latest_params: list[object] = [season, player_key, *player_groups]
    latest_as_of = conn.execute(
        f"""
        SELECT MAX(as_of_date)
        FROM player_league_context_ratings
        WHERE season = ?
          AND player_key = ?
          AND player_group IN ({placeholders})
        """,
        latest_params,
    ).fetchone()
    if not latest_as_of or latest_as_of[0] is None:
        return pd.DataFrame()

    rows_params: list[object] = [season, latest_as_of[0], player_key, *player_groups]
    return conn.execute(
        f"""
        SELECT
            season,
            as_of_date,
            player_name,
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
            qualification_minimum
        FROM player_league_context_ratings
        WHERE season = ?
          AND as_of_date = ?
          AND player_key = ?
          AND player_group IN ({placeholders})
        """,
        rows_params,
    ).df()


def get_pitcher_options(conn: duckdb.DuckDBPyConnection) -> list[str]:
    overview = _build_pitcher_overview(conn)
    if overview.empty:
        return []
    return sorted({name for name in overview["player_name"].dropna().tolist() if name})


def get_pitcher_strikeout_leaders(conn: duckdb.DuckDBPyConnection, limit: int = 15) -> pd.DataFrame:
    overview = _build_pitcher_overview(conn)
    if overview.empty:
        return overview
    leaders = overview[
        ["player_name", "position", "strikeouts", "appearances", "walks_issued", "home_runs_allowed"]
    ].copy()
    leaders = leaders.loc[leaders["strikeouts"].notna()]
    leaders["strikeouts_per_appearance"] = (leaders["strikeouts"] / leaders["appearances"]).round(2)
    leaders = leaders.sort_values(["strikeouts", "player_name"], ascending=[False, True]).head(limit)
    return leaders[
        ["player_name", "position", "strikeouts", "appearances", "strikeouts_per_appearance", "walks_issued", "home_runs_allowed"]
    ]


def get_pitcher_strikeouts_by_month(conn: duckdb.DuckDBPyConnection, player_name: str | None = None) -> pd.DataFrame:
    sql = """
        SELECT month_start, player_name, strikeouts
        FROM pitcher_strikeouts_by_month
        WHERE strikeouts > 0
    """
    params: list[object] = []
    sql += " ORDER BY month_start ASC, strikeouts DESC, player_name ASC"
    frame = conn.execute(sql, params).df()
    if frame.empty:
        return frame
    frame["player_name"] = frame["player_name"].map(format_player_name)
    if player_name:
        frame = _filter_pitcher_frame(frame, player_name)
    return frame


def get_pitcher_strikeouts_by_opponent(conn: duckdb.DuckDBPyConnection, player_name: str) -> pd.DataFrame:
    frame = conn.execute(
        """
        SELECT player_name, opponent, strikeouts
        FROM pitcher_strikeouts_by_opponent
        ORDER BY strikeouts DESC, opponent ASC
        """,
    ).df()
    if frame.empty:
        return frame
    frame["player_name"] = frame["player_name"].map(format_player_name)
    filtered = _filter_pitcher_frame(frame, player_name)
    return filtered[["opponent", "strikeouts"]]


def get_fastest_pitches(conn: duckdb.DuckDBPyConnection, limit: int = 10) -> pd.DataFrame:
    frame = conn.execute(
        """
        SELECT
            player_name,
            game_date,
            opponent,
            pitch_name,
            release_speed
        FROM fastest_pitches
        LIMIT ?
        """,
        [limit],
    ).df()
    if frame.empty:
        return frame
    frame["player_name"] = frame["player_name"].map(format_player_name)
    return frame


def get_pitcher_velocity_summary(conn: duckdb.DuckDBPyConnection, limit: int = 15) -> pd.DataFrame:
    overview = _build_pitcher_overview(conn)
    if overview.empty:
        return overview
    summary = overview[["player_name", "position", "max_velocity_mph", "avg_fastball_velocity_mph"]].copy()
    summary = summary.loc[summary["max_velocity_mph"].notna() | summary["avg_fastball_velocity_mph"].notna()]
    summary = summary.sort_values(
        ["max_velocity_mph", "avg_fastball_velocity_mph", "player_name"],
        ascending=[False, False, True],
    ).head(limit)
    return summary


def get_pitcher_wins_leaders(conn: duckdb.DuckDBPyConnection, limit: int = 10) -> pd.DataFrame:
    overview = _build_pitcher_overview(conn)
    if overview.empty:
        return overview
    leaders = overview[["player_name", "position", "wins", "losses", "innings_pitched", "era", "whip"]].copy()
    leaders = leaders.loc[leaders["wins"].notna()]
    return leaders.sort_values(["wins", "innings_pitched", "player_name"], ascending=[False, False, True]).head(limit)


def get_pitcher_walks_leaders(conn: duckdb.DuckDBPyConnection, limit: int = 10) -> pd.DataFrame:
    overview = _build_pitcher_overview(conn)
    if overview.empty:
        return overview
    leaders = overview[["player_name", "position", "walks_issued", "strikeouts", "appearances"]].copy()
    leaders = leaders.loc[leaders["walks_issued"].notna()]
    return leaders.sort_values(["walks_issued", "strikeouts", "player_name"], ascending=[False, False, True]).head(limit)


def get_pitcher_home_run_allowed_leaders(conn: duckdb.DuckDBPyConnection, limit: int = 10) -> pd.DataFrame:
    overview = _build_pitcher_overview(conn)
    if overview.empty:
        return overview
    leaders = overview[["player_name", "position", "home_runs_allowed", "walks_issued", "strikeouts"]].copy()
    leaders = leaders.loc[leaders["home_runs_allowed"].notna()]
    return leaders.sort_values(["home_runs_allowed", "player_name"], ascending=[False, True]).head(limit)


def get_team_pitcher_velocity_trend(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    frame = conn.execute(
        """
        SELECT
            e.game_date,
            COALESCE(p.player_name, e.pitcher_name) AS player_name,
            MAX(e.release_speed) AS max_velocity_mph
        FROM statcast_events e
        LEFT JOIN players p ON e.pitcher_id = p.player_id
        WHERE e.is_phillies_pitcher = TRUE
          AND e.release_speed IS NOT NULL
          AND e.pitcher_name IS NOT NULL
        GROUP BY e.game_date, COALESCE(p.player_name, e.pitcher_name)
        ORDER BY game_date ASC, max_velocity_mph DESC
        """
    ).df()
    if frame.empty:
        return frame
    frame["player_name"] = frame["player_name"].map(format_player_name)
    return frame


def get_pitcher_profile(conn: duckdb.DuckDBPyConnection, player_name: str) -> dict[str, object]:
    overview = _build_pitcher_overview(conn)
    summary_row = _filter_pitcher_frame(overview, player_name)
    summary = None
    if not summary_row.empty:
        row = summary_row.iloc[0]
        summary = (
            row.get("wins"),
            row.get("losses"),
            row.get("innings_pitched"),
            row.get("strikeouts"),
            row.get("walks_issued"),
            row.get("home_runs_allowed"),
            row.get("era"),
            row.get("whip"),
            row.get("max_velocity_mph"),
            row.get("avg_fastball_velocity_mph"),
            row.get("whiffs"),
            row.get("hardest_hit_allowed_mph"),
            row.get("appearances"),
            row.get("games_started"),
            row.get("saves"),
            row.get("position"),
        )

    pitch_usage = conn.execute(
        """
        SELECT player_name, pitch_name, pitch_count, usage_pct
        FROM pitcher_pitch_usage
        ORDER BY pitch_count DESC, pitch_name ASC
        """
    ).df()
    if not pitch_usage.empty:
        pitch_usage["player_name"] = pitch_usage["player_name"].map(format_player_name)
        pitch_usage = _filter_pitcher_frame(pitch_usage, player_name)[["pitch_name", "pitch_count", "usage_pct"]]

    strikeouts_by_month = get_pitcher_strikeouts_by_month(conn, player_name)
    strikeouts_by_opponent = get_pitcher_strikeouts_by_opponent(conn, player_name)
    fastest_pitches = conn.execute(
        """
        SELECT player_name, game_date, opponent, pitch_name, release_speed
        FROM fastest_pitches
        ORDER BY release_speed DESC, game_date ASC, player_name ASC
        """,
    ).df()
    if not fastest_pitches.empty:
        fastest_pitches["player_name"] = fastest_pitches["player_name"].map(format_player_name)
        fastest_pitches = _filter_pitcher_frame(fastest_pitches, player_name)[
            ["game_date", "opponent", "pitch_name", "release_speed"]
        ].head(10)

    league_context = get_pitcher_league_context_ratings(conn, player_name, summary[-1] if summary else None)

    return {
        "summary": summary,
        "league_context": league_context,
        "pitch_usage": pitch_usage,
        "strikeouts_by_month": strikeouts_by_month,
        "strikeouts_by_opponent": strikeouts_by_opponent,
        "fastest_pitches": fastest_pitches,
    }


def _latest_as_of_date(conn: duckdb.DuckDBPyConnection, table_name: str, season: int) -> date | None:
    row = conn.execute(f"SELECT MAX(as_of_date) FROM {table_name} WHERE season = ?", [season]).fetchone()
    return row[0] if row else None


def _current_result_streak(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "No games"
    results = frame.sort_values("game_date", ascending=False)["result_text"].astype(str).str[0].tolist()
    first = results[0]
    if first not in {"W", "L"}:
        return "No streak"
    count = 0
    for result in results:
        if result != first:
            break
        count += 1
    return f"{first}{count}"


def _rank_team(frame: pd.DataFrame, column: str, team_code: str, direction: str) -> str | None:
    working = frame.loc[frame[column].notna(), ["team_abbr", column]].copy()
    if working.empty or not working["team_abbr"].eq(team_code).any():
        return None
    ascending = direction == "lower"
    working = working.sort_values([column, "team_abbr"], ascending=[ascending, True]).reset_index(drop=True)
    working["rank"] = range(1, len(working) + 1)
    rank = working.loc[working["team_abbr"].eq(team_code), "rank"].iloc[0]
    return f"{int(rank)} of {len(working)}"


def _format_team_stat_value(value: object, display_format: str) -> str | None:
    if value is None or pd.isna(value):
        return None
    numeric = float(value)
    if display_format == "rate3":
        return f"{numeric:.3f}"
    if display_format == "decimal2":
        return f"{numeric:.2f}"
    if display_format == "int":
        return str(int(round(numeric)))
    return str(value)
