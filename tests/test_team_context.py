from __future__ import annotations

import unittest
from datetime import date

import pandas as pd

from support import TempDatabase

from phillies_stats.queries import get_nl_east_standings, get_phillies_team_rankings, get_team_local_summary
from phillies_stats.team_context import normalize_division_standings, normalize_team_stats, refresh_team_context


class FakeTeamContextProvider:
    def fetch_teams(self, season: int) -> pd.DataFrame:
        return _sample_teams()

    def fetch_team_stats(self, season: int, teams: pd.DataFrame) -> pd.DataFrame:
        return _sample_team_stats()

    def fetch_division_standings(self, season: int, division_id: int = 204) -> pd.DataFrame:
        return _sample_standings()


class TeamContextTests(unittest.TestCase):
    def test_local_summary_derives_record_streak_and_run_differential_from_games(self):
        with TempDatabase() as conn:
            _insert_game(conn, 1, date(2026, 4, 1), "ATL", True, 5, 2)
            _insert_game(conn, 2, date(2026, 4, 2), "ATL", True, 3, 4)
            _insert_game(conn, 3, date(2026, 4, 3), "NYM", False, 1, 6)
            _insert_game(conn, 4, date(2026, 4, 4), "NYM", False, 2, 7)

            summary = get_team_local_summary(conn)

        self.assertEqual(summary["record"], "1-3")
        self.assertEqual(summary["streak"], "L3")
        self.assertEqual(summary["runs_for"], 11)
        self.assertEqual(summary["runs_against"], 19)
        self.assertEqual(summary["run_differential"], -8)

    def test_team_stat_normalization_handles_hitting_and_pitching_rows(self):
        normalized = normalize_team_stats(_sample_team_stats(), season=2026, as_of_date=date(2026, 4, 24))

        self.assertEqual(len(normalized), 8)
        phillies_hitting = normalized.loc[
            normalized["team_abbr"].eq("PHI") & normalized["stat_group"].eq("hitting")
        ].iloc[0]
        phillies_pitching = normalized.loc[
            normalized["team_abbr"].eq("PHI") & normalized["stat_group"].eq("pitching")
        ].iloc[0]
        self.assertEqual(phillies_hitting["home_runs"], 24)
        self.assertEqual(phillies_pitching["era"], 5.10)

    def test_standings_normalization_orders_nl_east_context(self):
        normalized = normalize_division_standings(_sample_standings(), season=2026, as_of_date=date(2026, 4, 24))

        self.assertEqual(len(normalized), 3)
        self.assertEqual(normalized.loc[normalized["team_abbr"].eq("PHI"), "streak"].iloc[0], "L9")
        self.assertEqual(normalized.loc[normalized["team_abbr"].eq("ATL"), "division_rank"].iloc[0], 1)

    def test_refresh_persists_standings_and_rank_queries(self):
        with TempDatabase() as conn:
            result = refresh_team_context(
                conn,
                season=2026,
                as_of_date=date(2026, 4, 24),
                provider=FakeTeamContextProvider(),
            )
            standings = get_nl_east_standings(conn, season=2026)
            rankings = get_phillies_team_rankings(conn, season=2026)

        self.assertEqual(result, {"team_stat_rows": 8, "standing_rows": 3})
        self.assertEqual(standings.iloc[0]["team_abbr"], "ATL")
        ba_row = rankings.loc[rankings["Stat"].eq("BA")].iloc[0]
        era_row = rankings.loc[rankings["Stat"].eq("ERA")].iloc[0]
        self.assertEqual(ba_row["Value"], "0.240")
        self.assertEqual(ba_row["NL Rank"], "2 of 3")
        self.assertEqual(ba_row["MLB Rank"], "3 of 4")
        self.assertEqual(era_row["NL Rank"], "3 of 3")
        self.assertEqual(era_row["MLB Rank"], "4 of 4")


def _insert_game(conn, game_pk: int, game_date: date, opponent: str, phillies_home: bool, phillies_score: int, opponent_score: int):
    home_team = "PHI" if phillies_home else opponent
    away_team = opponent if phillies_home else "PHI"
    home_score = phillies_score if phillies_home else opponent_score
    away_score = opponent_score if phillies_home else phillies_score
    result_prefix = "W" if phillies_score > opponent_score else "L"
    conn.execute(
        """
        INSERT INTO games (
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
        VALUES (?, ?, 2026, ?, ?, 'Citizens Bank Park', ?, ?, ?, ?, ?)
        """,
        [
            game_pk,
            game_date,
            home_team,
            away_team,
            phillies_home,
            opponent,
            home_score,
            away_score,
            f"{result_prefix} {phillies_score}-{opponent_score}",
        ],
    )


def _sample_teams() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"team_id": 143, "team_abbr": "PHI", "team_name": "Phillies", "league": "National League", "division": "NL East"},
            {"team_id": 144, "team_abbr": "ATL", "team_name": "Braves", "league": "National League", "division": "NL East"},
            {"team_id": 121, "team_abbr": "NYM", "team_name": "Mets", "league": "National League", "division": "NL East"},
            {"team_id": 147, "team_abbr": "NYY", "team_name": "Yankees", "league": "American League", "division": "AL East"},
        ]
    )


def _sample_team_stats() -> pd.DataFrame:
    rows = []
    rows.extend(
        [
            _team_stat("PHI", 143, "Phillies", "National League", "hitting", avg=0.240, ops=0.710, hr=24, runs=89),
            _team_stat("ATL", 144, "Braves", "National League", "hitting", avg=0.260, ops=0.770, hr=30, runs=120),
            _team_stat("NYM", 121, "Mets", "National League", "hitting", avg=0.220, ops=0.680, hr=20, runs=82),
            _team_stat("NYY", 147, "Yankees", "American League", "hitting", avg=0.250, ops=0.760, hr=35, runs=125),
            _team_stat("PHI", 143, "Phillies", "National League", "pitching", era=5.10, whip=1.45, so=190, walks=88, hra=33),
            _team_stat("ATL", 144, "Braves", "National League", "pitching", era=3.00, whip=1.10, so=210, walks=60, hra=20),
            _team_stat("NYM", 121, "Mets", "National League", "pitching", era=4.00, whip=1.25, so=198, walks=74, hra=26),
            _team_stat("NYY", 147, "Yankees", "American League", "pitching", era=2.50, whip=1.00, so=220, walks=55, hra=15),
        ]
    )
    return pd.DataFrame(rows)


def _team_stat(
    abbr: str,
    team_id: int,
    name: str,
    league: str,
    group: str,
    *,
    avg: float | None = None,
    ops: float | None = None,
    hr: int | None = None,
    runs: int | None = None,
    era: float | None = None,
    whip: float | None = None,
    so: int | None = None,
    walks: int | None = None,
    hra: int | None = None,
) -> dict[str, object]:
    return {
        "team_id": team_id,
        "team_abbr": abbr,
        "team_name": name,
        "league": league,
        "division": "NL East" if league == "National League" else "AL East",
        "stat_group": group,
        "games": 25,
        "runs": runs,
        "home_runs": hr,
        "batting_average": avg,
        "on_base_percentage": None if avg is None else avg + 0.070,
        "ops": ops,
        "era": era,
        "whip": whip,
        "strikeouts": so,
        "walks": walks,
        "home_runs_allowed": hra,
    }


def _sample_standings() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"team_id": 144, "team_abbr": "ATL", "team_name": "Braves", "division_rank": 1, "wins": 16, "losses": 9, "winning_percentage": 0.640, "games_back": "-", "run_differential": 20, "streak": "W3"},
            {"team_id": 121, "team_abbr": "NYM", "team_name": "Mets", "division_rank": 2, "wins": 14, "losses": 11, "winning_percentage": 0.560, "games_back": "2.0", "run_differential": 8, "streak": "L1"},
            {"team_id": 143, "team_abbr": "PHI", "team_name": "Phillies", "division_rank": 5, "wins": 8, "losses": 17, "winning_percentage": 0.320, "games_back": "8.0", "run_differential": -51, "streak": "L9"},
        ]
    )


if __name__ == "__main__":
    unittest.main()
