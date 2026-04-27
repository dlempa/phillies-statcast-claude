from __future__ import annotations

import unittest
from datetime import date

from support import TempDatabase, sample_event, sample_events_frame

from phillies_stats.ingest import upsert_statcast_data
from phillies_stats.ingest import upsert_pitcher_season_summary
from phillies_stats.queries import (
    get_dashboard_metrics,
    get_hardest_hit_balls,
    get_phillies_batted_ball_scatter,
    get_pitcher_home_run_allowed_leaders,
    get_pitcher_profile,
    get_pitcher_strikeout_leaders,
    get_pitcher_walks_leaders,
    get_pitcher_wins_leaders,
    get_team_pitching_run_prevention_trend,
    get_team_rolling_run_trend,
    get_player_options,
    get_player_summary,
    get_player_hr_distance_stats,
    get_top_longest_home_runs,
)


class QueryTests(unittest.TestCase):
    def test_longest_home_runs_leaderboard_updates_from_event_data(self):
        opening_events = sample_events_frame(
            sample_event(
                event_id="game-1|ab-1|pitch-1",
                game_pk=1,
                game_date_value=date(2026, 3, 28),
                batter_name="Kyle Schwarber",
                batter_id=1,
                hit_distance_sc=430.0,
                launch_speed=110.2,
                launch_angle=28.0,
            ),
            sample_event(
                event_id="game-2|ab-1|pitch-1",
                game_pk=2,
                game_date_value=date(2026, 3, 29),
                batter_name="Bryce Harper",
                batter_id=2,
                hit_distance_sc=415.0,
                launch_speed=108.4,
                launch_angle=30.0,
                opponent="WSH",
                away_team="WSH",
            ),
        )

        new_longer_home_run = sample_events_frame(
            sample_event(
                event_id="game-3|ab-1|pitch-1",
                game_pk=3,
                game_date_value=date(2026, 3, 30),
                batter_name="Nick Castellanos",
                batter_id=3,
                hit_distance_sc=451.0,
                launch_speed=111.0,
                launch_angle=26.0,
                opponent="LAD",
                away_team="LAD",
            )
        )

        with TempDatabase() as conn:
            upsert_statcast_data(conn, opening_events, season=2026)
            initial = get_top_longest_home_runs(conn, limit=2)

            upsert_statcast_data(conn, new_longer_home_run, season=2026)
            updated = get_top_longest_home_runs(conn, limit=3)

        self.assertEqual(initial.iloc[0]["player_name"], "Kyle Schwarber")
        self.assertEqual(updated.iloc[0]["player_name"], "Nick Castellanos")
        self.assertEqual(updated.iloc[0]["distance_ft"], 451.0)
        self.assertEqual(updated.iloc[2]["player_name"], "Bryce Harper")

    def test_team_pitching_run_prevention_trend_uses_game_scores_and_pitching_events(self):
        rows = [
            _pitching_event(
                "game-1-strikeout",
                901,
                date(2026, 4, 1),
                home_team="PHI",
                away_team="ATL",
                post_home_score=5,
                post_away_score=3,
                events="strikeout",
                is_strikeout=True,
            ),
            _pitching_event(
                "game-1-walk",
                901,
                date(2026, 4, 1),
                home_team="PHI",
                away_team="ATL",
                post_home_score=5,
                post_away_score=3,
                events="walk",
            ),
            _pitching_event(
                "game-1-homer",
                901,
                date(2026, 4, 1),
                home_team="PHI",
                away_team="ATL",
                post_home_score=5,
                post_away_score=3,
                events="home_run",
                is_home_run=True,
            ),
            _pitching_event("game-2", 902, date(2026, 4, 2), home_team="NYM", away_team="PHI", post_home_score=4, post_away_score=2),
            _pitching_event("game-3", 903, date(2026, 4, 3), home_team="PHI", away_team="MIA", post_home_score=1, post_away_score=0),
            _pitching_event("game-4", 904, date(2026, 4, 4), home_team="WSH", away_team="PHI", post_home_score=8, post_away_score=9),
            _pitching_event("game-5", 905, date(2026, 4, 5), home_team="PHI", away_team="STL", post_home_score=6, post_away_score=5),
            _pitching_event("game-6", 906, date(2026, 4, 6), home_team="CHC", away_team="PHI", post_home_score=2, post_away_score=7),
        ]

        with TempDatabase() as conn:
            upsert_statcast_data(conn, sample_events_frame(*rows), season=2026)
            trend = get_team_pitching_run_prevention_trend(conn)

        self.assertEqual(trend["runs_allowed"].tolist(), [3, 4, 0, 8, 5, 2])
        self.assertEqual(trend["game_number"].tolist(), [1, 2, 3, 4, 5, 6])
        self.assertEqual(trend.iloc[0]["strikeouts"], 1)
        self.assertEqual(trend.iloc[0]["walks"], 1)
        self.assertEqual(trend.iloc[0]["home_runs_allowed"], 1)
        self.assertEqual(trend.iloc[0]["rolling_5_ra_per_game"], 3.00)
        self.assertEqual(trend.iloc[4]["rolling_5_ra_per_game"], 4.00)
        self.assertEqual(trend.iloc[5]["rolling_5_ra_per_game"], 3.80)
        self.assertEqual(trend.iloc[5]["season_ra_per_game"], 3.67)

    def test_hardest_hit_balls_excludes_blank_names_and_limits_to_ten(self):
        rows = []
        for index in range(11):
            rows.append(
                sample_event(
                    event_id=f"game-{index}|ab-1|pitch-1",
                    game_pk=100 + index,
                    game_date_value=date(2026, 3, 26),
                    batter_name=f"player {index}",
                    batter_id=1000 + index,
                    hit_distance_sc=300.0 + index,
                    launch_speed=100.0 + index,
                    launch_angle=20.0,
                    events="single",
                    is_home_run=False,
                )
            )
        rows.append(
            sample_event(
                event_id="blank-name-row",
                game_pk=999,
                game_date_value=date(2026, 3, 26),
                batter_name="",
                batter_id=9999,
                hit_distance_sc=450.0,
                launch_speed=120.0,
                launch_angle=15.0,
                events="double",
                is_home_run=False,
            )
        )

        with TempDatabase() as conn:
            upsert_statcast_data(conn, sample_events_frame(*rows), season=2026)
            hardest_balls = get_hardest_hit_balls(conn)

        self.assertEqual(len(hardest_balls), 10)
        self.assertTrue(hardest_balls["player_name"].notna().all())
        self.assertNotIn("", hardest_balls["player_name"].tolist())

    def test_hitter_home_run_summary_rolls_up_same_player_id_with_name_variants(self):
        home_runs = sample_events_frame(
            sample_event(
                event_id="schwarber-1",
                game_pk=301,
                game_date_value=date(2026, 4, 1),
                batter_name="Kyle Schwarber",
                batter_id=656941,
                hit_distance_sc=460.0,
                launch_speed=110.7,
            ),
            sample_event(
                event_id="schwarber-2",
                game_pk=302,
                game_date_value=date(2026, 4, 2),
                batter_name="kyle schwarber",
                batter_id=656941,
                hit_distance_sc=383.0,
                launch_speed=103.2,
            ),
            sample_event(
                event_id="schwarber-3",
                game_pk=303,
                game_date_value=date(2026, 4, 3),
                batter_name="Kyle Schwarber",
                batter_id=656941,
                hit_distance_sc=397.0,
                launch_speed=107.0,
            ),
        )

        with TempDatabase() as conn:
            upsert_statcast_data(conn, home_runs, season=2026)
            metrics = get_dashboard_metrics(conn)
            player_power = get_player_hr_distance_stats(conn)

        self.assertEqual(metrics["most_hrs"], ("Kyle Schwarber", 3))
        schwarber_rows = player_power[player_power["player_name"].str.lower().eq("kyle schwarber")]
        self.assertEqual(len(schwarber_rows), 1)
        self.assertEqual(schwarber_rows.iloc[0]["home_run_count"], 3)

    def test_hitter_profile_summary_includes_zero_home_run_hitters(self):
        stott_events = sample_events_frame(
            sample_event(
                event_id="stott-single-1",
                game_pk=401,
                game_date_value=date(2026, 4, 4),
                batter_name="Bryson Stott",
                batter_id=681082,
                hit_distance_sc=312.0,
                launch_speed=107.6,
                launch_angle=10.0,
                events="single",
                is_home_run=False,
            ),
            sample_event(
                event_id="stott-groundout-1",
                game_pk=402,
                game_date_value=date(2026, 4, 5),
                batter_name="Bryson Stott",
                batter_id=681082,
                hit_distance_sc=184.0,
                launch_speed=96.1,
                launch_angle=-4.0,
                events="field_out",
                is_home_run=False,
            ),
        )

        with TempDatabase() as conn:
            upsert_statcast_data(conn, stott_events, season=2026)
            profile = get_player_summary(conn, "Bryson Stott")

        summary = profile["summary"]
        self.assertEqual(summary[0], 0)
        self.assertIsNone(summary[1])
        self.assertIsNone(summary[2])
        self.assertEqual(summary[3], 107.6)
        self.assertTrue(profile["home_runs"].empty)

    def test_hitter_options_include_league_context_only_players(self):
        with TempDatabase() as conn:
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
                    stat_value
                )
                VALUES (2026, DATE '2026-04-11', 'Justin Crawford', 'justincrawford', 'PHI',
                        'hitter', 'hitter', 'hr', 'HR', 'higher', 0)
                """
            )
            options = get_player_options(conn)

        self.assertIn("Justin Crawford", options)

    def test_hitter_options_and_summary_match_name_punctuation_variants_by_key(self):
        realmuto_event = sample_event(
            event_id="realmuto-1",
            game_pk=411,
            game_date_value=date(2026, 4, 6),
            batter_name="J. T. Realmuto",
            batter_id=592663,
            hit_distance_sc=391.0,
            launch_speed=106.5,
            launch_angle=26.0,
        )

        with TempDatabase() as conn:
            upsert_statcast_data(conn, sample_events_frame(realmuto_event), season=2026)
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
                    stat_value
                )
                VALUES (2026, DATE '2026-04-11', 'J.T. Realmuto', 'jtrealmuto', 'PHI',
                        'hitter', 'hitter', 'hr', 'HR', 'higher', 1)
                """
            )
            options = get_player_options(conn)
            profile = get_player_summary(conn, "J.T. Realmuto")

        self.assertEqual([name for name in options if "Realmuto" in name], ["J. T. Realmuto"])
        self.assertEqual(profile["summary"][0], 1)
        self.assertEqual(profile["summary"][3], 106.5)

    def test_pitcher_strikeout_leaders_uses_pitching_events(self):
        pitching_events = sample_events_frame(
            sample_event(
                event_id="pitching-game-1",
                game_pk=501,
                game_date_value=date(2026, 4, 1),
                batter_name="Opponent One",
                batter_id=501,
                pitcher_name="Wheeler, Zack",
                pitcher_id=45,
                events="strikeout",
                is_home_run=False,
                is_strikeout=True,
                phillies_role="pitching",
                is_phillies_batter=False,
                is_phillies_pitcher=True,
                batting_team="ATL",
                fielding_team="PHI",
                home_team="PHI",
                away_team="ATL",
                inning_topbot="Top",
            ),
            sample_event(
                event_id="pitching-game-2",
                game_pk=502,
                game_date_value=date(2026, 4, 2),
                batter_name="Opponent Two",
                batter_id=502,
                pitcher_name="Wheeler, Zack",
                pitcher_id=45,
                events="strikeout",
                is_home_run=False,
                is_strikeout=True,
                phillies_role="pitching",
                is_phillies_batter=False,
                is_phillies_pitcher=True,
                batting_team="NYM",
                fielding_team="PHI",
                home_team="NYM",
                away_team="PHI",
                inning_topbot="Bot",
                opponent="NYM",
            ),
        )
        pitcher_summary = sample_events_frame(
            {
                "season": 2026,
                "pitcher_name": "Zack Wheeler",
                "team": "PHI",
                "wins": 2,
                "losses": 0,
                "games": 2,
                "games_started": 2,
                "saves": 0,
                "innings_pitched": 12.0,
                "strikeouts": 14,
                "walks": 2,
                "home_runs_allowed": 1,
                "era": 1.50,
                "whip": 0.92,
                "avg_fastball_velocity": 97.5,
                "war": 0.8,
            }
        )

        with TempDatabase() as conn:
            upsert_statcast_data(conn, pitching_events, season=2026)
            upsert_pitcher_season_summary(conn, pitcher_summary)
            leaders = get_pitcher_strikeout_leaders(conn, limit=5)

        self.assertEqual(leaders.iloc[0]["player_name"], "Zack Wheeler")
        self.assertEqual(leaders.iloc[0]["strikeouts"], 14)
        self.assertEqual(leaders.iloc[0]["appearances"], 2)

    def test_pitcher_leaderboards_roll_up_same_pitcher_id_with_name_variants(self):
        pitching_events = sample_events_frame(
            sample_event(
                event_id="nola-1",
                game_pk=801,
                game_date_value=date(2026, 4, 1),
                batter_name="Opponent One",
                batter_id=801,
                pitcher_name="Aaron Nola",
                pitcher_id=605400,
                events="strikeout",
                is_home_run=False,
                is_strikeout=True,
                phillies_role="pitching",
                is_phillies_batter=False,
                is_phillies_pitcher=True,
                batting_team="ATL",
                fielding_team="PHI",
                home_team="PHI",
                away_team="ATL",
                inning_topbot="Top",
            ),
            sample_event(
                event_id="nola-2",
                game_pk=802,
                game_date_value=date(2026, 4, 2),
                batter_name="Opponent Two",
                batter_id=802,
                pitcher_name="Nola, Aaron",
                pitcher_id=605400,
                events="walk",
                is_home_run=False,
                is_strikeout=False,
                phillies_role="pitching",
                is_phillies_batter=False,
                is_phillies_pitcher=True,
                batting_team="NYM",
                fielding_team="PHI",
                home_team="NYM",
                away_team="PHI",
                inning_topbot="Bot",
                opponent="NYM",
            ),
            sample_event(
                event_id="nola-3",
                game_pk=803,
                game_date_value=date(2026, 4, 3),
                batter_name="Opponent Three",
                batter_id=803,
                pitcher_name="Aaron Nola",
                pitcher_id=605400,
                events="home_run",
                is_home_run=True,
                is_strikeout=False,
                phillies_role="pitching",
                is_phillies_batter=False,
                is_phillies_pitcher=True,
                batting_team="MIA",
                fielding_team="PHI",
                home_team="PHI",
                away_team="MIA",
                inning_topbot="Top",
                opponent="MIA",
            ),
        )
        pitcher_summary = sample_events_frame(
            {
                "season": 2026,
                "pitcher_name": "Aaron Nola",
                "team": "PHI",
                "wins": 1,
                "losses": 0,
                "games": 3,
                "games_started": 3,
                "saves": 0,
                "innings_pitched": 17.1,
                "strikeouts": 1,
                "walks": 1,
                "home_runs_allowed": 1,
                "era": 3.12,
                "whip": 1.04,
                "avg_fastball_velocity": 93.5,
                "war": 0.3,
            }
        )

        with TempDatabase() as conn:
            upsert_statcast_data(conn, pitching_events, season=2026)
            upsert_pitcher_season_summary(conn, pitcher_summary)
            strikeout_leaders = get_pitcher_strikeout_leaders(conn, limit=5)
            wins_leaders = get_pitcher_wins_leaders(conn, limit=5)
            walks_leaders = get_pitcher_walks_leaders(conn, limit=5)
            home_run_allowed = get_pitcher_home_run_allowed_leaders(conn, limit=5)

        for leaders in [strikeout_leaders, wins_leaders, walks_leaders, home_run_allowed]:
            self.assertEqual(leaders["player_name"].tolist().count("Aaron Nola"), 1)

        self.assertEqual(strikeout_leaders.iloc[0]["appearances"], 3)
        self.assertEqual(walks_leaders.iloc[0]["walks_issued"], 1)
        self.assertEqual(home_run_allowed.iloc[0]["home_runs_allowed"], 1)

    def test_pitcher_profile_merges_summary_stats_with_last_first_event_name(self):
        pitching_events = sample_events_frame(
            sample_event(
                event_id="duran-1",
                game_pk=601,
                game_date_value=date(2026, 4, 5),
                batter_name="Opponent Three",
                batter_id=601,
                pitcher_name="Duran, Jhoan",
                pitcher_id=77,
                events="strikeout",
                is_home_run=False,
                is_strikeout=True,
                phillies_role="pitching",
                is_phillies_batter=False,
                is_phillies_pitcher=True,
                batting_team="ATL",
                fielding_team="PHI",
                home_team="PHI",
                away_team="ATL",
                inning_topbot="Top",
                release_speed=101.2,
            )
        )
        pitcher_summary = sample_events_frame(
            {
                "season": 2026,
                "pitcher_name": "Jhoan Duran",
                "team": "PHI",
                "wins": 1,
                "losses": 0,
                "games": 3,
                "games_started": 0,
                "saves": 2,
                "innings_pitched": 3.1,
                "strikeouts": 5,
                "walks": 1,
                "home_runs_allowed": 0,
                "era": 0.00,
                "whip": 0.90,
                "avg_fastball_velocity": 100.4,
                "war": 0.4,
            }
        )

        with TempDatabase() as conn:
            upsert_statcast_data(conn, pitching_events, season=2026)
            upsert_pitcher_season_summary(conn, pitcher_summary)
            profile = get_pitcher_profile(conn, "Jhoan Duran")

        summary = profile["summary"]
        self.assertEqual(summary[0], 1)
        self.assertEqual(summary[1], 0)
        self.assertEqual(summary[2], 3.1)
        self.assertEqual(summary[6], 0.0)
        self.assertEqual(summary[7], 0.9)
        self.assertEqual(summary[14], 2)
        self.assertEqual(summary[15], "Closer")

    def test_pitcher_profile_uses_event_velocity_when_summary_velocity_missing(self):
        nola_event = sample_event(
            event_id="nola-velocity-1",
            game_pk=901,
            game_date_value=date(2026, 4, 8),
            batter_name="Opponent Four",
            batter_id=901,
            pitcher_name="Aaron Nola",
            pitcher_id=605400,
            events="strikeout",
            is_home_run=False,
            is_strikeout=True,
            phillies_role="pitching",
            is_phillies_batter=False,
            is_phillies_pitcher=True,
            batting_team="MIA",
            fielding_team="PHI",
            home_team="PHI",
            away_team="MIA",
            inning_topbot="Top",
            release_speed=94.5,
        )
        nola_event["pitch_name"] = None
        nola_event["pitch_type"] = "FF"
        pitcher_summary = sample_events_frame(
            {
                "season": 2026,
                "pitcher_name": "Aaron Nola",
                "team": "PHI",
                "wins": 1,
                "losses": 1,
                "games": 1,
                "games_started": 1,
                "saves": 0,
                "innings_pitched": 6.0,
                "strikeouts": 7,
                "walks": 2,
                "home_runs_allowed": 1,
                "era": 3.00,
                "whip": 1.00,
                "avg_fastball_velocity": None,
                "war": 0.1,
            }
        )

        with TempDatabase() as conn:
            upsert_statcast_data(conn, sample_events_frame(nola_event), season=2026)
            upsert_pitcher_season_summary(conn, pitcher_summary)
            profile = get_pitcher_profile(conn, "Aaron Nola")

        summary = profile["summary"]
        self.assertEqual(summary[8], 94.5)
        self.assertEqual(summary[9], 94.5)

    def test_pitcher_profile_fastest_pitches_filters_before_limiting(self):
        high_velocity_rows = [
            sample_event(
                event_id=f"high-velo-{index}",
                game_pk=9000 + index,
                game_date_value=date(2026, 4, 1),
                batter_name="Opposing Hitter",
                batter_id=8000 + index,
                pitcher_name="High Velo Pitcher",
                pitcher_id=7001,
                events="called_strike",
                is_home_run=False,
                is_in_play=False,
                release_speed=100.0 + (index / 1000),
                phillies_role="pitching",
                is_phillies_batter=False,
                is_phillies_pitcher=True,
                batting_team="ATL",
                fielding_team="PHI",
            )
            for index in range(205)
        ]
        target_rows = [
            sample_event(
                event_id=f"target-velo-{index}",
                game_pk=9500 + index,
                game_date_value=date(2026, 4, 2),
                batter_name="Opposing Hitter",
                batter_id=8500 + index,
                pitcher_name="Low Velo Pitcher",
                pitcher_id=7002,
                events="called_strike",
                is_home_run=False,
                is_in_play=False,
                release_speed=91.0 + index,
                phillies_role="pitching",
                is_phillies_batter=False,
                is_phillies_pitcher=True,
                batting_team="ATL",
                fielding_team="PHI",
            )
            for index in range(2)
        ]

        with TempDatabase() as conn:
            upsert_statcast_data(conn, sample_events_frame(*(high_velocity_rows + target_rows)), season=2026)
            profile = get_pitcher_profile(conn, "Low Velo Pitcher")

        fastest = profile["fastest_pitches"]
        self.assertEqual(len(fastest), 2)
        self.assertEqual(fastest.iloc[0]["release_speed"], 92.0)

    def test_top_longest_home_runs_includes_batter_id_for_headshots(self):
        events = sample_events_frame(
            sample_event(
                event_id="schwarber-leader",
                game_pk=1101,
                game_date_value=date(2026, 4, 1),
                batter_name="Kyle Schwarber",
                batter_id=656941,
                hit_distance_sc=470.0,
                launch_speed=112.0,
                launch_angle=27.0,
            ),
            sample_event(
                event_id="harper-runner",
                game_pk=1102,
                game_date_value=date(2026, 4, 2),
                batter_name="Bryce Harper",
                batter_id=547180,
                hit_distance_sc=420.0,
                launch_speed=108.0,
                launch_angle=29.0,
            ),
        )

        with TempDatabase() as conn:
            upsert_statcast_data(conn, events, season=2026)
            leaderboard = get_top_longest_home_runs(conn, limit=5)

        self.assertIn("batter_id", leaderboard.columns)
        self.assertEqual(int(leaderboard.iloc[0]["batter_id"]), 656941)
        self.assertEqual(int(leaderboard.iloc[1]["batter_id"]), 547180)

    def test_player_summary_returns_mlbam_id(self):
        events = sample_events_frame(
            sample_event(
                event_id="harper-mlbam",
                game_pk=2101,
                game_date_value=date(2026, 4, 5),
                batter_name="Bryce Harper",
                batter_id=547180,
                hit_distance_sc=405.0,
                launch_speed=109.4,
                launch_angle=24.0,
            )
        )

        with TempDatabase() as conn:
            upsert_statcast_data(conn, events, season=2026)
            profile = get_player_summary(conn, "Bryce Harper")

        self.assertEqual(profile["mlbam_id"], 547180)

    def test_phillies_batted_ball_scatter_returns_hr_and_other_balls(self):
        events = sample_events_frame(
            sample_event(
                event_id="bbs-hr",
                game_pk=3101,
                game_date_value=date(2026, 4, 6),
                batter_name="Trea Turner",
                batter_id=607208,
                hit_distance_sc=412.0,
                launch_speed=105.0,
                launch_angle=28.0,
                events="home_run",
                is_home_run=True,
            ),
            sample_event(
                event_id="bbs-single",
                game_pk=3102,
                game_date_value=date(2026, 4, 7),
                batter_name="Trea Turner",
                batter_id=607208,
                hit_distance_sc=185.0,
                launch_speed=98.0,
                launch_angle=10.0,
                events="single",
                is_home_run=False,
            ),
            sample_event(
                event_id="bbs-no-launch-data",
                game_pk=3103,
                game_date_value=date(2026, 4, 8),
                batter_name="Trea Turner",
                batter_id=607208,
                hit_distance_sc=None,
                launch_speed=None,
                launch_angle=None,
                events="walk",
                is_home_run=False,
            ),
        )

        with TempDatabase() as conn:
            upsert_statcast_data(conn, events, season=2026)
            scatter = get_phillies_batted_ball_scatter(conn)

        self.assertEqual(len(scatter), 2)
        hit_types = scatter["hit_type"].tolist()
        self.assertIn("Home Run", hit_types)
        self.assertIn("Other", hit_types)
        self.assertTrue(set(["exit_velocity_mph", "launch_angle", "is_home_run"]).issubset(scatter.columns))

    def test_team_rolling_run_trend_computes_window_mean(self):
        rows = [
            _pitching_event(
                f"rolling-game-{idx}",
                4100 + idx,
                date(2026, 5, idx + 1),
                home_team="PHI",
                away_team="ATL",
                post_home_score=5 + idx,
                post_away_score=2 + idx,
            )
            for idx in range(6)
        ]

        with TempDatabase() as conn:
            upsert_statcast_data(conn, sample_events_frame(*rows), season=2026)
            trend = get_team_rolling_run_trend(conn, window=3)

        self.assertEqual(len(trend), 6)
        self.assertIn("rolling_runs_for", trend.columns)
        self.assertIn("rolling_runs_against", trend.columns)
        # First two rows are below the window — should be NaN
        self.assertTrue(trend.iloc[0]["rolling_runs_for"] != trend.iloc[0]["rolling_runs_for"])
        # Window of 3 starting at index 2 averages runs_for=[5,6,7] = 6.0
        self.assertEqual(trend.iloc[2]["rolling_runs_for"], 6.0)
        self.assertEqual(trend.iloc[2]["rolling_runs_against"], 3.0)
        # Last row averages runs_for=[8,9,10] = 9.0
        self.assertEqual(trend.iloc[5]["rolling_runs_for"], 9.0)

    def test_team_rolling_run_trend_empty_when_no_games(self):
        with TempDatabase() as conn:
            trend = get_team_rolling_run_trend(conn, window=10)
        self.assertTrue(trend.empty)

    def test_pitcher_strikeout_leaders_handles_missing_season_summary(self):
        pitching_events = sample_events_frame(
            sample_event(
                event_id="pitching-no-summary-1",
                game_pk=701,
                game_date_value=date(2026, 4, 6),
                batter_name="Opponent Four",
                batter_id=701,
                pitcher_name="Sanchez, Cristopher",
                pitcher_id=61,
                events="strikeout",
                is_home_run=False,
                is_strikeout=True,
                phillies_role="pitching",
                is_phillies_batter=False,
                is_phillies_pitcher=True,
                batting_team="MIA",
                fielding_team="PHI",
                home_team="PHI",
                away_team="MIA",
                inning_topbot="Top",
            )
        )

        with TempDatabase() as conn:
            upsert_statcast_data(conn, pitching_events, season=2026)
            leaders = get_pitcher_strikeout_leaders(conn, limit=5)

        self.assertEqual(leaders.iloc[0]["player_name"], "Cristopher Sanchez")
        self.assertEqual(leaders.iloc[0]["strikeouts"], 1)
        self.assertEqual(leaders.iloc[0]["position"], "Reliever")


def _pitching_event(
    event_id: str,
    game_pk: int,
    game_date_value: date,
    *,
    home_team: str,
    away_team: str,
    post_home_score: int,
    post_away_score: int,
    events: str = "field_out",
    is_strikeout: bool = False,
    is_home_run: bool = False,
) -> dict[str, object]:
    phillies_home = home_team == "PHI"
    opponent = away_team if phillies_home else home_team
    return sample_event(
        event_id=event_id,
        game_pk=game_pk,
        game_date_value=game_date_value,
        batter_name=f"{opponent} Hitter",
        batter_id=game_pk,
        pitcher_name="Phillies Pitcher",
        pitcher_id=7000,
        events=events,
        is_home_run=is_home_run,
        is_strikeout=is_strikeout,
        is_in_play=is_home_run or events == "field_out",
        phillies_role="pitching",
        is_phillies_batter=False,
        is_phillies_pitcher=True,
        batting_team=opponent,
        fielding_team="PHI",
        home_team=home_team,
        away_team=away_team,
        opponent=opponent,
        inning_topbot="Top" if phillies_home else "Bot",
        post_home_score=post_home_score,
        post_away_score=post_away_score,
    )


if __name__ == "__main__":
    unittest.main()
