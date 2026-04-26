from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from phillies_stats.config import get_config
from phillies_stats.database import get_connection, initialize_database
from phillies_stats.ingest import ingest_date_range, refresh_missing_player_names, refresh_pitcher_season_summary
from phillies_stats.league_context import refresh_league_context
from phillies_stats.queries import get_latest_game_date
from phillies_stats.team_context import refresh_team_context


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily Phillies Statcast update.")
    parser.add_argument("--season", type=int, default=2026, help="Season year to update.")
    parser.add_argument("--lookback-days", type=int, default=3, help="Re-pull recent days to catch corrections safely.")
    parser.add_argument("--window-days", type=int, default=3, help="Number of days to request per Statcast pull.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = get_config(args.season)
    conn = get_connection(config.db_path)
    initialize_database(conn)

    latest_game_date = get_latest_game_date(conn)
    today = date.today()

    if latest_game_date is None:
        start_date = config.season_start
    else:
        start_date = max(config.season_start, latest_game_date - timedelta(days=args.lookback_days))
    end_date = min(today, config.season_end)

    result = ingest_date_range(
        conn,
        season=config.season,
        start_date=start_date,
        end_date=end_date,
        team_code=config.team_code,
        window_days=args.window_days,
    )
    player_name_rows = refresh_missing_player_names(conn)
    pitcher_summary_rows = 0
    pitcher_summary_warning = None
    try:
        pitcher_summary_rows = refresh_pitcher_season_summary(conn, season=config.season, team_code=config.team_code)
    except Exception as exc:
        pitcher_summary_warning = str(exc)

    league_context_rows = {"cutoff_rows": 0, "rating_rows": 0}
    league_context_warning = None
    try:
        league_context_rows = refresh_league_context(
            conn,
            season=config.season,
            team_code=config.team_code,
            as_of_date=end_date,
            start_date=config.season_start,
            end_date=end_date,
        )
    except Exception as exc:
        league_context_warning = str(exc)

    team_context_rows = {"team_stat_rows": 0, "standing_rows": 0}
    team_context_warning = None
    try:
        team_context_rows = refresh_team_context(conn, season=config.season, as_of_date=end_date)
    except Exception as exc:
        team_context_warning = str(exc)

    print(f"Daily update complete for {config.season}.")
    print(f"Window: {start_date} to {end_date}")
    print(f"Rows seen: {result['rows_seen']}")
    print(f"Rows inserted: {result['rows_inserted']}")
    print(f"Missing player names refreshed: {player_name_rows}")
    print(f"Pitcher summary rows refreshed: {pitcher_summary_rows}")
    print(f"League cutoff rows refreshed: {league_context_rows['cutoff_rows']}")
    print(f"Player league rating rows refreshed: {league_context_rows['rating_rows']}")
    print(f"Team stat rows refreshed: {team_context_rows['team_stat_rows']}")
    print(f"Division standing rows refreshed: {team_context_rows['standing_rows']}")
    if pitcher_summary_warning:
        print(f"Pitcher summary warning: {pitcher_summary_warning}")
    if league_context_warning:
        print(f"League context warning: {league_context_warning}")
    if team_context_warning:
        print(f"Team context warning: {team_context_warning}")
    print(f"Database: {config.db_path}")


if __name__ == "__main__":
    main()
