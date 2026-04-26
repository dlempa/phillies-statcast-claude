from __future__ import annotations

import unittest
from datetime import date

import pandas as pd

from support import TempDatabase

from phillies_stats.league_context import (
    DIRECTION_LOWER,
    calculate_percentile,
    rating_tier_for_percentile,
    refresh_league_context,
)
from phillies_stats.queries import get_hitter_league_context_ratings, get_pitcher_league_context_ratings


class FakeLeagueContextProvider:
    def __init__(self, hitters: pd.DataFrame, pitchers: pd.DataFrame):
        self.hitters = hitters
        self.pitchers = pitchers

    def fetch_hitter_stats(self, season: int, *, start_date=None, end_date=None) -> pd.DataFrame:
        return self.hitters

    def fetch_pitcher_stats(self, season: int, *, start_date=None, end_date=None) -> pd.DataFrame:
        return self.pitchers


class LeagueContextTests(unittest.TestCase):
    def test_rating_tier_boundaries_use_exact_rubric(self):
        self.assertEqual(rating_tier_for_percentile(14.99), "Poor")
        self.assertEqual(rating_tier_for_percentile(15), "Below Average")
        self.assertEqual(rating_tier_for_percentile(39.99), "Below Average")
        self.assertEqual(rating_tier_for_percentile(40), "Average")
        self.assertEqual(rating_tier_for_percentile(59.99), "Average")
        self.assertEqual(rating_tier_for_percentile(60), "Above Average")
        self.assertEqual(rating_tier_for_percentile(74.99), "Above Average")
        self.assertEqual(rating_tier_for_percentile(75), "Very Good")
        self.assertEqual(rating_tier_for_percentile(89.99), "Very Good")
        self.assertEqual(rating_tier_for_percentile(90), "Great")
        self.assertEqual(rating_tier_for_percentile(94.99), "Great")
        self.assertEqual(rating_tier_for_percentile(95), "Elite")

    def test_lower_is_better_percentile_inverts_pool_rank(self):
        values = pd.Series(range(1, 101))

        best = calculate_percentile(1, values, direction=DIRECTION_LOWER)
        worst = calculate_percentile(100, values, direction=DIRECTION_LOWER)

        self.assertEqual(best, 100.0)
        self.assertEqual(worst, 1.0)
        self.assertEqual(rating_tier_for_percentile(best), "Elite")
        self.assertEqual(rating_tier_for_percentile(worst), "Poor")

    def test_refresh_persists_cutoffs_and_profile_rating_rows(self):
        hitters = _sample_hitter_frame()
        pitchers = _sample_pitcher_frame()

        with TempDatabase() as conn:
            _insert_team_games(conn, games=10)

            result = refresh_league_context(
                conn,
                season=2026,
                team_code="PHI",
                as_of_date=date(2026, 4, 12),
                provider=FakeLeagueContextProvider(hitters, pitchers),
            )
            hitter_context = get_hitter_league_context_ratings(conn, "Bryce Harper", season=2026)
            starter_context = get_pitcher_league_context_ratings(conn, "Zack Wheeler", "Starter", season=2026)
            closer_context = get_pitcher_league_context_ratings(conn, "Jhoan Duran", "Closer", season=2026)
            raw_closer_group = conn.execute(
                """
                SELECT DISTINCT player_group, baseline_group
                FROM player_league_context_ratings
                WHERE player_key = 'jhoanduran'
                """
            ).fetchone()

        self.assertEqual(result["cutoff_rows"], 26)
        self.assertEqual(result["rating_rows"], 20)

        hitter_k_row = hitter_context.loc[hitter_context["Stat"].eq("K%")].iloc[0]
        self.assertEqual(hitter_k_row["Value"], "10.0%")
        self.assertEqual(hitter_k_row["Rating Tier"], "Elite")
        self.assertEqual(hitter_k_row["MLB Qualified?"], "Yes")

        starter_era_row = starter_context.loc[starter_context["Stat"].eq("ERA")].iloc[0]
        self.assertEqual(starter_era_row["Value"], "1.50")
        self.assertEqual(starter_era_row["Rating Tier"], "Elite")
        self.assertEqual(starter_era_row["MLB Qualified?"], "Yes")

        closer_era_row = closer_context.loc[closer_context["Stat"].eq("ERA")].iloc[0]
        self.assertEqual(closer_era_row["MLB Qualified?"], "No")
        self.assertEqual(raw_closer_group, ("closer", "closer"))


def _sample_hitter_frame() -> pd.DataFrame:
    rows = []
    for index in range(1, 30):
        rows.append(
            {
                "Name": f"League Hitter {index}",
                "Team": "ATL",
                "G": 10,
                "PA": 20 + index,
                "AVG": 0.200 + index * 0.002,
                "OBP": 0.260 + index * 0.002,
                "SLG": 0.330 + index * 0.003,
                "OPS": 0.590 + index * 0.005,
                "ISO": 0.100 + index * 0.002,
                "BB%": f"{5 + index * 0.2:.1f}%",
                "K%": f"{34 - index * 0.4:.1f}%",
                "HR": index % 8,
            }
        )
    rows.append(
        {
            "Name": "Bryce Harper",
            "Team": "PHI",
            "G": 10,
            "PA": 40,
            "AVG": 0.320,
            "OBP": 0.450,
            "SLG": 0.660,
            "OPS": 1.110,
            "ISO": 0.340,
            "BB%": "18.0%",
            "K%": "10.0%",
            "HR": 8,
        }
    )
    return pd.DataFrame(rows)


def _sample_pitcher_frame() -> pd.DataFrame:
    rows = []
    for index in range(1, 10):
        rows.append(_pitcher_row(f"League Starter {index}", "ATL", ip=8 + index, games=2, starts=2, saves=0, offset=index))
    rows.append(
        _pitcher_row(
            "Zack Wheeler",
            "PHI",
            ip=12.0,
            games=2,
            starts=2,
            saves=0,
            offset=20,
            era=1.50,
            whip=0.80,
            fip=2.00,
            k_per_9=12.0,
            bb_per_9=1.0,
            hr_per_9=0.20,
        )
    )
    for index in range(1, 7):
        rows.append(_pitcher_row(f"League Reliever {index}", "ATL", ip=5 + index, games=8, starts=0, saves=0, offset=index))
    for index in range(1, 5):
        rows.append(_pitcher_row(f"League Closer {index}", "ATL", ip=5 + index, games=8, starts=0, saves=2, offset=index))
    rows.append(
        _pitcher_row(
            "Jhoan Duran",
            "PHI",
            ip=6.0,
            games=6,
            starts=0,
            saves=4,
            offset=20,
            era=1.20,
            whip=0.70,
            fip=1.90,
            k_per_9=13.5,
            bb_per_9=1.2,
            hr_per_9=0.10,
        )
    )
    return pd.DataFrame(rows)


def _pitcher_row(
    name: str,
    team: str,
    *,
    ip: float,
    games: int,
    starts: int,
    saves: int,
    offset: int,
    era: float | None = None,
    whip: float | None = None,
    fip: float | None = None,
    k_per_9: float | None = None,
    bb_per_9: float | None = None,
    hr_per_9: float | None = None,
) -> dict[str, object]:
    return {
        "Name": name,
        "Team": team,
        "IP": ip,
        "G": games,
        "GS": starts,
        "SV": saves,
        "ERA": era if era is not None else 5.00 - offset * 0.12,
        "WHIP": whip if whip is not None else 1.50 - offset * 0.02,
        "FIP": fip if fip is not None else 4.80 - offset * 0.11,
        "K/9": k_per_9 if k_per_9 is not None else 6.0 + offset * 0.25,
        "BB/9": bb_per_9 if bb_per_9 is not None else 4.0 - offset * 0.08,
        "HR/9": hr_per_9 if hr_per_9 is not None else 1.5 - offset * 0.03,
    }


def _insert_team_games(conn, *, games: int) -> None:
    for index in range(games):
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
            VALUES (?, ?, 2026, 'PHI', 'ATL', 'Citizens Bank Park', TRUE, 'ATL', 5, 3, 'W 5-3')
            """,
            [1000 + index, date(2026, 4, 1 + index)],
        )


if __name__ == "__main__":
    unittest.main()
