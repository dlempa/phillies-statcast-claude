from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta
from typing import Iterable

import duckdb
import pandas as pd
import requests

from phillies_stats.config import BALLPARK_BY_TEAM
from phillies_stats.display import format_player_name


def _series(df: pd.DataFrame, column: str, default=None) -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series([default] * len(df), index=df.index)


def _first_not_null(frame: pd.DataFrame, column: str):
    if column not in frame.columns:
        return None
    non_null = frame[column].dropna()
    if non_null.empty:
        return None
    return non_null.iloc[0]


def _max_not_null(frame: pd.DataFrame, column: str):
    if column not in frame.columns:
        return None
    non_null = frame[column].dropna()
    if non_null.empty:
        return None
    return non_null.max()


def _build_event_id(row: pd.Series) -> str:
    keys = [
        row.get("game_pk"),
        row.get("at_bat_number"),
        row.get("pitch_number"),
        row.get("batter"),
        row.get("pitcher"),
        row.get("events"),
        row.get("description"),
    ]
    return "|".join("" if pd.isna(value) else str(value) for value in keys)


def _numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(_series(df, column), errors="coerce")


def _lookup_player_names(player_ids: pd.Series) -> dict[int, str]:
    ids = []
    for value in player_ids.dropna().unique().tolist():
        try:
            ids.append(int(value))
        except (TypeError, ValueError):
            continue
    if not ids:
        return {}

    name_map = _lookup_player_names_from_pybaseball(ids)
    missing_ids = [player_id for player_id in ids if player_id not in name_map]
    if missing_ids:
        name_map.update(_lookup_player_names_from_stats_api(missing_ids))
    return name_map


def _lookup_player_names_from_pybaseball(ids: list[int]) -> dict[int, str]:
    try:
        from pybaseball import playerid_reverse_lookup
    except ImportError:
        return {}

    try:
        lookup = playerid_reverse_lookup(ids, key_type="mlbam")
    except TypeError:
        lookup = playerid_reverse_lookup(ids)
    except Exception:
        return {}

    if lookup is None or lookup.empty:
        return {}

    id_column = "key_mlbam" if "key_mlbam" in lookup.columns else lookup.columns[0]
    if {"name_first", "name_last"}.issubset(lookup.columns):
        names = (lookup["name_first"].fillna("") + " " + lookup["name_last"].fillna("")).str.strip()
    else:
        names = lookup[id_column].astype(str)

    name_map: dict[int, str] = {}
    for player_id, name in zip(lookup[id_column], names):
        try:
            if pd.notna(player_id) and name:
                name_map[int(player_id)] = name
        except (TypeError, ValueError):
            continue
    return name_map


def _lookup_player_names_from_stats_api(ids: list[int]) -> dict[int, str]:
    name_map: dict[int, str] = {}
    if not ids:
        return name_map

    chunk_size = 100
    for start in range(0, len(ids), chunk_size):
        chunk = ids[start : start + chunk_size]
        try:
            response = requests.get(
                "https://statsapi.mlb.com/api/v1/people",
                params={"personIds": ",".join(str(player_id) for player_id in chunk)},
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception:
            continue

        people = payload.get("people", [])
        if not isinstance(people, list):
            continue
        for person in people:
            if not isinstance(person, dict):
                continue
            player_id = person.get("id")
            name = format_player_name(person.get("fullName"))
            try:
                if pd.notna(player_id) and name:
                    name_map[int(player_id)] = str(name)
            except (TypeError, ValueError):
                continue
    return name_map


def iter_date_windows(start_date: date, end_date: date, window_days: int = 7) -> Iterable[tuple[date, date]]:
    current = start_date
    while current <= end_date:
        window_end = min(current + timedelta(days=window_days - 1), end_date)
        yield current, window_end
        current = window_end + timedelta(days=1)


def filter_to_team_games(raw_df: pd.DataFrame, team_code: str = "PHI") -> pd.DataFrame:
    if raw_df.empty:
        return pd.DataFrame()
    if "home_team" not in raw_df.columns or "away_team" not in raw_df.columns:
        return pd.DataFrame()
    team_games = raw_df["home_team"].eq(team_code) | raw_df["away_team"].eq(team_code)
    return raw_df.loc[team_games].copy()


def fetch_statcast_window(start_date: date, end_date: date, team_code: str = "PHI") -> pd.DataFrame:
    try:
        from pybaseball import statcast
    except ImportError as exc:
        raise RuntimeError("pybaseball is required for data ingestion.") from exc

    frame = statcast(start_dt=start_date.isoformat(), end_dt=end_date.isoformat(), verbose=False)
    if frame is None or frame.empty:
        return pd.DataFrame()
    return filter_to_team_games(frame, team_code=team_code)


def fetch_pitcher_season_summary(
    conn: duckdb.DuckDBPyConnection,
    season: int,
    team_code: str = "PHI",
) -> pd.DataFrame:
    games = conn.execute(
        """
        SELECT game_pk, game_date
        FROM games
        WHERE season = ?
        ORDER BY game_date ASC, game_pk ASC
        """,
        [season],
    ).df()
    if games.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []
    for game in games.itertuples(index=False):
        url = f"https://statsapi.mlb.com/api/v1/game/{int(game.game_pk)}/boxscore"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        rows.extend(_extract_pitcher_summary_rows(response.json(), season=season, game_date=game.game_date, team_code=team_code))
    return pd.DataFrame(rows)


def normalize_pitcher_season_summary(raw_df: pd.DataFrame, season: int, team_code: str = "PHI") -> pd.DataFrame:
    if raw_df.empty:
        return pd.DataFrame()

    normalized = pd.DataFrame(
        {
            "season": season,
            "pitcher_name": _series(raw_df, "pitcher_name", default=None).fillna(_series(raw_df, "Name")).map(format_player_name),
            "team": _series(raw_df, "team", default=None).fillna(_series(raw_df, "Team")).fillna(team_code),
            "wins": _numeric_series(raw_df, "wins").fillna(_numeric_series(raw_df, "W")),
            "losses": _numeric_series(raw_df, "losses").fillna(_numeric_series(raw_df, "L")),
            "games": _numeric_series(raw_df, "games").fillna(_numeric_series(raw_df, "G")),
            "games_started": _numeric_series(raw_df, "games_started").fillna(_numeric_series(raw_df, "GS")),
            "saves": _numeric_series(raw_df, "saves").fillna(_numeric_series(raw_df, "SV")),
            "innings_pitched": _numeric_series(raw_df, "innings_pitched").fillna(_numeric_series(raw_df, "IP")),
            "strikeouts": _numeric_series(raw_df, "strikeouts").fillna(_numeric_series(raw_df, "SO")),
            "walks": _numeric_series(raw_df, "walks").fillna(_numeric_series(raw_df, "BB")),
            "home_runs_allowed": _numeric_series(raw_df, "home_runs_allowed").fillna(_numeric_series(raw_df, "HR")),
            "era": _numeric_series(raw_df, "era").fillna(_numeric_series(raw_df, "ERA")),
            "whip": _numeric_series(raw_df, "whip").fillna(_numeric_series(raw_df, "WHIP")),
            "avg_fastball_velocity": _numeric_series(raw_df, "avg_fastball_velocity").fillna(_numeric_series(raw_df, "FBv")),
            "war": _numeric_series(raw_df, "war").fillna(_numeric_series(raw_df, "WAR")),
            "game_date": pd.to_datetime(_series(raw_df, "game_date"), errors="coerce").dt.date,
        }
    )
    normalized = normalized.dropna(subset=["pitcher_name"])
    if normalized["game_date"].notna().any():
        normalized = (
            normalized.sort_values(["game_date", "pitcher_name"])
            .groupby(["season", "pitcher_name"], as_index=False)
            .last()
        )
    else:
        normalized = normalized.drop_duplicates(subset=["season", "pitcher_name"])
    normalized = normalized.drop(columns=["game_date"], errors="ignore")
    return normalized


def normalize_statcast_events(raw_df: pd.DataFrame, season: int, team_code: str = "PHI") -> pd.DataFrame:
    if raw_df.empty:
        return pd.DataFrame()

    df = raw_df.copy()
    df["game_date"] = pd.to_datetime(_series(df, "game_date"), errors="coerce").dt.date
    df["home_team"] = _series(df, "home_team")
    df["away_team"] = _series(df, "away_team")
    df["inning_topbot"] = _series(df, "inning_topbot")

    batting_team = _series(df, "batting_team")
    batting_missing = batting_team.isna()
    batting_team = batting_team.where(
        ~batting_missing,
        df["away_team"].where(df["inning_topbot"].eq("Top"), df["home_team"]),
    )
    fielding_team = _series(df, "fielding_team")
    fielding_missing = fielding_team.isna()
    fielding_team = fielding_team.where(
        ~fielding_missing,
        df["home_team"].where(df["inning_topbot"].eq("Top"), df["away_team"]),
    )

    is_phillies_batter = batting_team.eq(team_code)
    is_phillies_pitcher = fielding_team.eq(team_code)
    opponent = df["away_team"].where(df["home_team"].eq(team_code), df["home_team"])

    is_home_run = _series(df, "events").eq("home_run")
    is_strikeout = _series(df, "events").isin(["strikeout", "strikeout_double_play"])
    is_in_play = _series(df, "description").isin(["hit_into_play", "hit_into_play_no_out", "hit_into_play_score"])

    batter_ids = pd.to_numeric(_series(df, "batter"), errors="coerce")
    pitcher_ids = pd.to_numeric(_series(df, "pitcher"), errors="coerce")
    batter_name_map = _lookup_player_names(batter_ids)
    pitcher_name_map = _lookup_player_names(pitcher_ids)
    batter_names = _series(df, "batter_name", default=None).fillna(batter_ids.map(batter_name_map)).map(format_player_name)
    pitcher_names = _series(df, "pitcher_name", default=None).fillna(_series(df, "player_name", default=None))
    pitcher_names = pitcher_names.fillna(pitcher_ids.map(pitcher_name_map)).map(format_player_name)

    game_datetime = pd.to_datetime(_series(df, "game_date"), errors="coerce")
    if "game_time_utc" in df.columns:
        time_part = pd.to_datetime(df["game_time_utc"], errors="coerce")
        game_datetime = time_part.fillna(game_datetime)

    normalized = pd.DataFrame(
        {
            "event_id": df.apply(_build_event_id, axis=1),
            "season": season,
            "game_pk": _series(df, "game_pk"),
            "game_date": df["game_date"],
            "game_datetime": game_datetime,
            "inning": _series(df, "inning"),
            "inning_topbot": df["inning_topbot"],
            "at_bat_number": _series(df, "at_bat_number"),
            "pitch_number": _series(df, "pitch_number"),
            "phillies_role": pd.Series(["other"] * len(df), index=df.index),
            "is_phillies_batter": is_phillies_batter,
            "is_phillies_pitcher": is_phillies_pitcher,
            "batting_team": batting_team,
            "fielding_team": fielding_team,
            "home_team": df["home_team"],
            "away_team": df["away_team"],
            "opponent": opponent,
            "venue_name": _series(df, "venue_name", default=None).fillna(df["home_team"].map(BALLPARK_BY_TEAM)),
            "batter_id": batter_ids,
            "batter_name": batter_names,
            "pitcher_id": pitcher_ids,
            "pitcher_name": pitcher_names,
            "stand": _series(df, "stand"),
            "p_throws": _series(df, "p_throws"),
            "events": _series(df, "events"),
            "description": _series(df, "description"),
            "bb_type": _series(df, "bb_type"),
            "pitch_type": _series(df, "pitch_type"),
            "pitch_name": _series(df, "pitch_name"),
            "release_speed": pd.to_numeric(_series(df, "release_speed"), errors="coerce"),
            "effective_speed": pd.to_numeric(_series(df, "effective_speed"), errors="coerce"),
            "launch_speed": pd.to_numeric(_series(df, "launch_speed"), errors="coerce"),
            "launch_angle": pd.to_numeric(_series(df, "launch_angle"), errors="coerce"),
            "hit_distance_sc": pd.to_numeric(_series(df, "hit_distance_sc"), errors="coerce"),
            "zone": pd.to_numeric(_series(df, "zone"), errors="coerce"),
            "balls": pd.to_numeric(_series(df, "balls"), errors="coerce"),
            "strikes": pd.to_numeric(_series(df, "strikes"), errors="coerce"),
            "outs_when_up": pd.to_numeric(_series(df, "outs_when_up"), errors="coerce"),
            "estimated_ba_using_speedangle": pd.to_numeric(
                _series(df, "estimated_ba_using_speedangle"), errors="coerce"
            ),
            "estimated_woba_using_speedangle": pd.to_numeric(
                _series(df, "estimated_woba_using_speedangle"), errors="coerce"
            ),
            "woba_value": pd.to_numeric(_series(df, "woba_value"), errors="coerce"),
            "delta_home_win_exp": pd.to_numeric(_series(df, "delta_home_win_exp"), errors="coerce"),
            "post_away_score": pd.to_numeric(_series(df, "post_away_score"), errors="coerce"),
            "post_home_score": pd.to_numeric(_series(df, "post_home_score"), errors="coerce"),
            "is_home_run": is_home_run,
            "is_strikeout": is_strikeout,
            "is_in_play": is_in_play,
        }
    )

    normalized.loc[normalized["is_phillies_batter"], "phillies_role"] = "batting"
    normalized.loc[normalized["is_phillies_pitcher"], "phillies_role"] = "pitching"
    normalized = normalized.dropna(subset=["event_id", "game_pk", "game_date"]).drop_duplicates(subset=["event_id"])
    return normalized


def build_games_table(events_df: pd.DataFrame, season: int, team_code: str = "PHI") -> pd.DataFrame:
    if events_df.empty:
        return pd.DataFrame()

    rows = []
    for game_pk, frame in events_df.groupby("game_pk"):
        home_team = _first_not_null(frame, "home_team")
        away_team = _first_not_null(frame, "away_team")
        home_score = _max_not_null(frame, "post_home_score")
        away_score = _max_not_null(frame, "post_away_score")
        phillies_home = home_team == team_code
        opponent = away_team if phillies_home else home_team
        result_text = None
        if home_score is not None and away_score is not None:
            phillies_score = home_score if phillies_home else away_score
            opponent_score = away_score if phillies_home else home_score
            result_prefix = "W" if phillies_score > opponent_score else "L" if phillies_score < opponent_score else "T"
            result_text = f"{result_prefix} {int(phillies_score)}-{int(opponent_score)}"
        rows.append(
            {
                "game_pk": game_pk,
                "game_date": _first_not_null(frame, "game_date"),
                "season": season,
                "home_team": home_team,
                "away_team": away_team,
                "venue_name": _first_not_null(frame, "venue_name"),
                "phillies_home": phillies_home,
                "opponent": opponent,
                "home_score": home_score,
                "away_score": away_score,
                "result_text": result_text,
            }
        )
    return pd.DataFrame(rows)


def build_players_table(events_df: pd.DataFrame) -> pd.DataFrame:
    if events_df.empty:
        return pd.DataFrame(columns=["player_id", "player_name", "player_type", "bats", "throws", "last_seen_date"])

    batters = (
        events_df[["batter_id", "batter_name", "stand", "game_date"]]
        .dropna(subset=["batter_id", "batter_name"])
        .rename(columns={"batter_id": "player_id", "batter_name": "player_name", "stand": "bats"})
    )
    batters["throws"] = None
    batters["player_type"] = "batter"

    pitchers = (
        events_df[["pitcher_id", "pitcher_name", "p_throws", "game_date"]]
        .dropna(subset=["pitcher_id", "pitcher_name"])
        .rename(columns={"pitcher_id": "player_id", "pitcher_name": "player_name", "p_throws": "throws"})
    )
    pitchers["bats"] = None
    pitchers["player_type"] = "pitcher"

    combined = pd.concat([batters, pitchers], ignore_index=True, sort=False)
    combined["last_seen_date"] = combined["game_date"]
    grouped = (
        combined.sort_values("last_seen_date")
        .groupby("player_id", as_index=False)
        .agg(
            {
                "player_name": "last",
                "player_type": lambda values: "both" if len(set(values)) > 1 else values.iloc[-1],
                "bats": "last",
                "throws": "last",
                "last_seen_date": "max",
            }
        )
    )
    return grouped


def insert_ingestion_run(
    conn: duckdb.DuckDBPyConnection,
    *,
    season: int,
    start_date: date,
    end_date: date,
    notes: str | None = None,
) -> str:
    run_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO ingestion_runs (run_id, season, start_date, end_date, started_at, status, notes)
        VALUES (?, ?, ?, ?, ?, 'running', ?)
        """,
        [run_id, season, start_date, end_date, datetime.utcnow(), notes],
    )
    return run_id


def complete_ingestion_run(
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
    *,
    rows_seen: int,
    rows_inserted: int,
    status: str = "completed",
    notes: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE ingestion_runs
        SET completed_at = ?, status = ?, rows_seen = ?, rows_inserted = ?, notes = COALESCE(?, notes)
        WHERE run_id = ?
        """,
        [datetime.utcnow(), status, rows_seen, rows_inserted, notes, run_id],
    )


def upsert_statcast_data(
    conn: duckdb.DuckDBPyConnection,
    events_df: pd.DataFrame,
    *,
    season: int,
    team_code: str = "PHI",
) -> int:
    if events_df.empty:
        return 0

    games_df = build_games_table(events_df, season=season, team_code=team_code)
    players_df = build_players_table(events_df)

    conn.register("events_stage", events_df)
    conn.register("games_stage", games_df)
    conn.register("players_stage", players_df)

    before_count = conn.execute("SELECT COUNT(*) FROM statcast_events").fetchone()[0]

    conn.execute(
        """
        INSERT OR REPLACE INTO games (
            game_pk,
            game_date,
            season,
            home_team,
            away_team,
            venue_name,
            phillies_home,
            opponent,
            home_score,
            away_score,
            result_text
        )
        SELECT
            game_pk,
            game_date,
            season,
            home_team,
            away_team,
            venue_name,
            phillies_home,
            opponent,
            home_score,
            away_score,
            result_text
        FROM games_stage s
        """
    )

    conn.execute(
        """
        INSERT OR REPLACE INTO players (
            player_id,
            player_name,
            player_type,
            bats,
            throws,
            last_seen_date
        )
        SELECT
            player_id,
            player_name,
            player_type,
            bats,
            throws,
            last_seen_date
        FROM players_stage s
        """
    )

    conn.execute(
        """
        INSERT OR REPLACE INTO statcast_events (
            event_id,
            season,
            game_pk,
            game_date,
            game_datetime,
            inning,
            inning_topbot,
            at_bat_number,
            pitch_number,
            phillies_role,
            is_phillies_batter,
            is_phillies_pitcher,
            batting_team,
            fielding_team,
            home_team,
            away_team,
            opponent,
            venue_name,
            batter_id,
            batter_name,
            pitcher_id,
            pitcher_name,
            stand,
            p_throws,
            events,
            description,
            bb_type,
            pitch_type,
            pitch_name,
            release_speed,
            effective_speed,
            launch_speed,
            launch_angle,
            hit_distance_sc,
            zone,
            balls,
            strikes,
            outs_when_up,
            estimated_ba_using_speedangle,
            estimated_woba_using_speedangle,
            woba_value,
            delta_home_win_exp,
            post_away_score,
            post_home_score,
            is_home_run,
            is_strikeout,
            is_in_play
        )
        SELECT
            event_id,
            season,
            game_pk,
            game_date,
            game_datetime,
            inning,
            inning_topbot,
            at_bat_number,
            pitch_number,
            phillies_role,
            is_phillies_batter,
            is_phillies_pitcher,
            batting_team,
            fielding_team,
            home_team,
            away_team,
            opponent,
            venue_name,
            batter_id,
            batter_name,
            pitcher_id,
            pitcher_name,
            stand,
            p_throws,
            events,
            description,
            bb_type,
            pitch_type,
            pitch_name,
            release_speed,
            effective_speed,
            launch_speed,
            launch_angle,
            hit_distance_sc,
            zone,
            balls,
            strikes,
            outs_when_up,
            estimated_ba_using_speedangle,
            estimated_woba_using_speedangle,
            woba_value,
            delta_home_win_exp,
            post_away_score,
            post_home_score,
            is_home_run,
            is_strikeout,
            is_in_play
        FROM events_stage s
        """
    )

    conn.unregister("events_stage")
    conn.unregister("games_stage")
    conn.unregister("players_stage")

    after_count = conn.execute("SELECT COUNT(*) FROM statcast_events").fetchone()[0]
    return after_count - before_count


def refresh_missing_player_names(conn: duckdb.DuckDBPyConnection) -> int:
    missing = conn.execute(
        """
        SELECT DISTINCT batter_id AS player_id
        FROM statcast_events
        WHERE batter_id IS NOT NULL
          AND (batter_name IS NULL OR TRIM(batter_name) = '')
        UNION
        SELECT DISTINCT pitcher_id AS player_id
        FROM statcast_events
        WHERE pitcher_id IS NOT NULL
          AND (pitcher_name IS NULL OR TRIM(pitcher_name) = '')
        """
    ).df()
    if missing.empty:
        return 0

    name_map = _lookup_player_names(missing["player_id"])
    if not name_map:
        return 0

    stage = pd.DataFrame(
        [{"player_id": player_id, "player_name": player_name} for player_id, player_name in name_map.items()]
    )
    conn.register("player_name_stage", stage)
    try:
        conn.execute(
            """
            UPDATE statcast_events AS e
            SET batter_name = s.player_name
            FROM player_name_stage AS s
            WHERE e.batter_id = s.player_id
              AND (e.batter_name IS NULL OR TRIM(e.batter_name) = '')
            """
        )
        conn.execute(
            """
            UPDATE statcast_events AS e
            SET pitcher_name = s.player_name
            FROM player_name_stage AS s
            WHERE e.pitcher_id = s.player_id
              AND (e.pitcher_name IS NULL OR TRIM(e.pitcher_name) = '')
            """
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO players (
                player_id,
                player_name,
                player_type,
                bats,
                throws,
                last_seen_date
            )
            WITH staged_events AS (
                SELECT
                    e.batter_id AS player_id,
                    s.player_name,
                    'batter' AS player_type,
                    e.stand AS bats,
                    NULL AS throws,
                    e.game_date
                FROM statcast_events e
                JOIN player_name_stage s ON e.batter_id = s.player_id
                WHERE e.batter_id IS NOT NULL
                UNION ALL
                SELECT
                    e.pitcher_id AS player_id,
                    s.player_name,
                    'pitcher' AS player_type,
                    NULL AS bats,
                    e.p_throws AS throws,
                    e.game_date
                FROM statcast_events e
                JOIN player_name_stage s ON e.pitcher_id = s.player_id
                WHERE e.pitcher_id IS NOT NULL
            )
            SELECT
                player_id,
                ANY_VALUE(player_name) AS player_name,
                CASE WHEN COUNT(DISTINCT player_type) > 1 THEN 'both' ELSE ANY_VALUE(player_type) END AS player_type,
                MAX(bats) AS bats,
                MAX(throws) AS throws,
                MAX(game_date) AS last_seen_date
            FROM staged_events
            GROUP BY player_id
            """
        )
    finally:
        conn.unregister("player_name_stage")

    return len(name_map)


def upsert_pitcher_season_summary(
    conn: duckdb.DuckDBPyConnection,
    summary_df: pd.DataFrame,
) -> int:
    if summary_df.empty:
        return 0

    before_count = conn.execute("SELECT COUNT(*) FROM pitcher_season_summary").fetchone()[0]
    conn.register("pitcher_summary_stage", summary_df)
    conn.execute(
        """
        INSERT OR REPLACE INTO pitcher_season_summary (
            season,
            pitcher_name,
            team,
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
            avg_fastball_velocity,
            war,
            updated_at
        )
        SELECT
            season,
            pitcher_name,
            team,
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
            avg_fastball_velocity,
            war,
            CURRENT_TIMESTAMP
        FROM pitcher_summary_stage
        """
    )
    conn.unregister("pitcher_summary_stage")
    after_count = conn.execute("SELECT COUNT(*) FROM pitcher_season_summary").fetchone()[0]
    return max(after_count - before_count, len(summary_df))


def refresh_pitcher_season_summary(
    conn: duckdb.DuckDBPyConnection,
    *,
    season: int,
    team_code: str = "PHI",
) -> int:
    raw_df = fetch_pitcher_season_summary(conn, season=season, team_code=team_code)
    normalized = normalize_pitcher_season_summary(raw_df, season=season, team_code=team_code)
    return upsert_pitcher_season_summary(conn, normalized)


def _extract_pitcher_summary_rows(
    boxscore: dict[str, object],
    *,
    season: int,
    game_date: object,
    team_code: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    teams = boxscore.get("teams", {})
    if not isinstance(teams, dict):
        return rows

    for side in ["home", "away"]:
        team_section = teams.get(side, {})
        if not isinstance(team_section, dict):
            continue
        team_info = team_section.get("team", {})
        if not isinstance(team_info, dict) or team_info.get("abbreviation") != team_code:
            continue

        players = team_section.get("players", {})
        pitcher_ids = team_section.get("pitchers", [])
        if not isinstance(players, dict) or not isinstance(pitcher_ids, list):
            continue

        for pitcher_id in pitcher_ids:
            player = players.get(f"ID{pitcher_id}", {})
            if not isinstance(player, dict):
                continue
            season_stats = player.get("seasonStats", {})
            if not isinstance(season_stats, dict):
                continue
            pitching_stats = season_stats.get("pitching", {})
            if not isinstance(pitching_stats, dict) or not pitching_stats:
                continue
            person = player.get("person", {})
            if not isinstance(person, dict):
                continue

            rows.append(
                {
                    "season": season,
                    "game_date": game_date,
                    "pitcher_name": format_player_name(person.get("fullName")),
                    "team": team_code,
                    "wins": pitching_stats.get("wins"),
                    "losses": pitching_stats.get("losses"),
                    "games": pitching_stats.get("gamesPitched", pitching_stats.get("gamesPlayed")),
                    "games_started": pitching_stats.get("gamesStarted"),
                    "saves": pitching_stats.get("saves"),
                    "innings_pitched": pitching_stats.get("inningsPitched"),
                    "strikeouts": pitching_stats.get("strikeOuts"),
                    "walks": pitching_stats.get("baseOnBalls"),
                    "home_runs_allowed": pitching_stats.get("homeRuns"),
                    "era": pitching_stats.get("era"),
                    "whip": pitching_stats.get("whip"),
                    "avg_fastball_velocity": None,
                    "war": None,
                }
            )
    return rows


def ingest_date_range(
    conn: duckdb.DuckDBPyConnection,
    *,
    season: int,
    start_date: date,
    end_date: date,
    team_code: str = "PHI",
    window_days: int = 7,
) -> dict[str, int | str]:
    run_id = insert_ingestion_run(
        conn,
        season=season,
        start_date=start_date,
        end_date=end_date,
        notes=f"Windows of {window_days} day(s)",
    )

    rows_seen = 0
    rows_inserted = 0

    try:
        for window_start, window_end in iter_date_windows(start_date, end_date, window_days=window_days):
            raw_df = fetch_statcast_window(window_start, window_end, team_code=team_code)
            rows_seen += len(raw_df)
            normalized = normalize_statcast_events(raw_df, season=season, team_code=team_code)
            rows_inserted += upsert_statcast_data(conn, normalized, season=season, team_code=team_code)
        complete_ingestion_run(conn, run_id, rows_seen=rows_seen, rows_inserted=rows_inserted)
    except Exception as exc:
        complete_ingestion_run(conn, run_id, rows_seen=rows_seen, rows_inserted=rows_inserted, status="failed", notes=str(exc))
        raise

    return {"run_id": run_id, "rows_seen": rows_seen, "rows_inserted": rows_inserted}
