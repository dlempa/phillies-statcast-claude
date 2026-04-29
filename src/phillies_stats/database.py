from __future__ import annotations

from pathlib import Path

import duckdb


def get_connection(db_path: Path | str, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path), read_only=read_only)


def initialize_database(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS players (
            player_id BIGINT PRIMARY KEY,
            player_name TEXT NOT NULL,
            player_type TEXT,
            bats TEXT,
            throws TEXT,
            last_seen_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS games (
            game_pk BIGINT PRIMARY KEY,
            game_date DATE NOT NULL,
            season INTEGER NOT NULL,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            venue_name TEXT,
            phillies_home BOOLEAN,
            opponent TEXT,
            home_score INTEGER,
            away_score INTEGER,
            result_text TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS statcast_events (
            event_id TEXT PRIMARY KEY,
            season INTEGER NOT NULL,
            game_pk BIGINT NOT NULL,
            game_date DATE NOT NULL,
            game_datetime TIMESTAMP,
            inning INTEGER,
            inning_topbot TEXT,
            at_bat_number INTEGER,
            pitch_number INTEGER,
            phillies_role TEXT,
            is_phillies_batter BOOLEAN,
            is_phillies_pitcher BOOLEAN,
            batting_team TEXT,
            fielding_team TEXT,
            home_team TEXT,
            away_team TEXT,
            opponent TEXT,
            venue_name TEXT,
            batter_id BIGINT,
            batter_name TEXT,
            pitcher_id BIGINT,
            pitcher_name TEXT,
            stand TEXT,
            p_throws TEXT,
            events TEXT,
            description TEXT,
            bb_type TEXT,
            pitch_type TEXT,
            pitch_name TEXT,
            release_speed DOUBLE,
            effective_speed DOUBLE,
            launch_speed DOUBLE,
            launch_angle DOUBLE,
            hit_distance_sc DOUBLE,
            zone INTEGER,
            balls INTEGER,
            strikes INTEGER,
            outs_when_up INTEGER,
            estimated_ba_using_speedangle DOUBLE,
            estimated_woba_using_speedangle DOUBLE,
            woba_value DOUBLE,
            delta_home_win_exp DOUBLE,
            post_away_score INTEGER,
            post_home_score INTEGER,
            is_home_run BOOLEAN,
            is_strikeout BOOLEAN,
            is_in_play BOOLEAN,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS ingestion_runs (
            run_id TEXT PRIMARY KEY,
            season INTEGER NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            started_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            status TEXT NOT NULL,
            rows_seen INTEGER DEFAULT 0,
            rows_inserted INTEGER DEFAULT 0,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS pitcher_season_summary (
            season INTEGER NOT NULL,
            pitcher_name TEXT NOT NULL,
            team TEXT,
            wins INTEGER,
            losses INTEGER,
            games INTEGER,
            games_started INTEGER,
            saves INTEGER,
            innings_pitched DOUBLE,
            strikeouts INTEGER,
            walks INTEGER,
            home_runs_allowed INTEGER,
            era DOUBLE,
            whip DOUBLE,
            avg_fastball_velocity DOUBLE,
            war DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (season, pitcher_name)
        );

        CREATE TABLE IF NOT EXISTS league_stat_cutoffs (
            season INTEGER NOT NULL,
            as_of_date DATE NOT NULL,
            player_group TEXT NOT NULL,
            stat_key TEXT NOT NULL,
            stat_label TEXT NOT NULL,
            direction TEXT NOT NULL,
            pool_minimum_metric TEXT NOT NULL,
            pool_minimum_value DOUBLE NOT NULL,
            sample_size INTEGER NOT NULL,
            p15 DOUBLE,
            p40 DOUBLE,
            p60 DOUBLE,
            p75 DOUBLE,
            p90 DOUBLE,
            p95 DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (season, as_of_date, player_group, stat_key)
        );

        CREATE TABLE IF NOT EXISTS player_league_context_ratings (
            season INTEGER NOT NULL,
            as_of_date DATE NOT NULL,
            player_name TEXT NOT NULL,
            player_key TEXT NOT NULL,
            team TEXT,
            player_group TEXT NOT NULL,
            baseline_group TEXT NOT NULL,
            stat_key TEXT NOT NULL,
            stat_label TEXT NOT NULL,
            direction TEXT NOT NULL,
            stat_value DOUBLE,
            league_percentile DOUBLE,
            rating_tier TEXT,
            mlb_qualified TEXT,
            qualification_metric TEXT,
            qualification_value DOUBLE,
            qualification_minimum DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (season, as_of_date, player_key, player_group, stat_key)
        );

        CREATE TABLE IF NOT EXISTS team_season_stats (
            season INTEGER NOT NULL,
            as_of_date DATE NOT NULL,
            team_id INTEGER NOT NULL,
            team_abbr TEXT NOT NULL,
            team_name TEXT NOT NULL,
            league TEXT,
            division TEXT,
            stat_group TEXT NOT NULL,
            games INTEGER,
            runs INTEGER,
            runs_allowed INTEGER,
            home_runs INTEGER,
            batting_average DOUBLE,
            on_base_percentage DOUBLE,
            slugging_percentage DOUBLE,
            ops DOUBLE,
            era DOUBLE,
            whip DOUBLE,
            strikeouts INTEGER,
            walks INTEGER,
            home_runs_allowed INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (season, as_of_date, team_abbr, stat_group)
        );

        CREATE TABLE IF NOT EXISTS division_standings (
            season INTEGER NOT NULL,
            as_of_date DATE NOT NULL,
            team_id INTEGER NOT NULL,
            team_abbr TEXT NOT NULL,
            team_name TEXT NOT NULL,
            league TEXT,
            division TEXT,
            division_rank INTEGER,
            wins INTEGER,
            losses INTEGER,
            winning_percentage DOUBLE,
            games_back TEXT,
            runs_scored INTEGER,
            runs_allowed INTEGER,
            run_differential INTEGER,
            streak TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (season, as_of_date, team_abbr)
        );

        CREATE TABLE IF NOT EXISTS team_state_summaries (
            season INTEGER NOT NULL,
            as_of_date DATE NOT NULL,
            headline TEXT NOT NULL,
            summary_text TEXT NOT NULL,
            tone_label TEXT,
            key_stats_json TEXT,
            sources_json TEXT,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            prompt_version TEXT,
            PRIMARY KEY (season, as_of_date)
        );
        """
    )
    create_views(conn)


def create_views(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE OR REPLACE VIEW phillies_home_runs AS
        SELECT
            e.event_id,
            e.game_pk,
            e.game_date,
            e.opponent,
            e.venue_name,
            e.batter_id,
            COALESCE(p.player_name, e.batter_name) AS batter_name,
            e.launch_speed,
            e.launch_angle,
            e.hit_distance_sc,
            CASE WHEN e.home_team = 'PHI' THEN 'Home' ELSE 'Away' END AS home_away
        FROM statcast_events e
        LEFT JOIN players p ON e.batter_id = p.player_id
        WHERE e.is_home_run = TRUE
          AND e.is_phillies_batter = TRUE;

        CREATE OR REPLACE VIEW longest_home_runs AS
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY hit_distance_sc DESC NULLS LAST, launch_speed DESC NULLS LAST, game_date ASC, event_id ASC
            ) AS rank,
            batter_id,
            batter_name AS player_name,
            game_date,
            opponent,
            venue_name,
            home_away,
            hit_distance_sc AS distance_ft,
            launch_speed AS exit_velocity_mph,
            launch_angle
        FROM phillies_home_runs
        WHERE hit_distance_sc IS NOT NULL;

        CREATE OR REPLACE VIEW hardest_hit_home_runs AS
        SELECT
            batter_name AS player_name,
            game_date,
            opponent,
            venue_name,
            hit_distance_sc AS distance_ft,
            launch_speed AS exit_velocity_mph,
            launch_angle
        FROM phillies_home_runs
        WHERE launch_speed IS NOT NULL
        ORDER BY launch_speed DESC, hit_distance_sc DESC NULLS LAST, game_date ASC;

        CREATE OR REPLACE VIEW hardest_hit_balls_overall AS
        SELECT
            COALESCE(p.player_name, e.batter_name) AS player_name,
            e.game_date,
            e.opponent,
            e.venue_name,
            COALESCE(e.events, e.description) AS outcome,
            e.launch_speed AS exit_velocity_mph,
            e.launch_angle,
            e.hit_distance_sc AS distance_ft
        FROM statcast_events e
        LEFT JOIN players p ON e.batter_id = p.player_id
        WHERE e.is_phillies_batter = TRUE
          AND COALESCE(p.player_name, e.batter_name) IS NOT NULL
          AND TRIM(COALESCE(p.player_name, e.batter_name)) <> ''
          AND e.launch_speed IS NOT NULL
        ORDER BY e.launch_speed DESC, e.game_date ASC, e.event_id ASC;

        CREATE OR REPLACE VIEW hitter_event_summary AS
        SELECT
            e.batter_id,
            COALESCE(p.player_name, e.batter_name) AS player_name,
            COUNT(DISTINCT e.game_pk) AS games_seen,
            SUM(CASE WHEN e.is_home_run = TRUE THEN 1 ELSE 0 END) AS home_run_count,
            ROUND(AVG(CASE WHEN e.is_home_run = TRUE THEN e.hit_distance_sc END), 1) AS avg_hr_distance_ft,
            MAX(CASE WHEN e.is_home_run = TRUE THEN e.hit_distance_sc END) AS max_hr_distance_ft,
            MAX(e.launch_speed) AS hardest_hit_ball_mph
        FROM statcast_events e
        LEFT JOIN players p ON e.batter_id = p.player_id
        WHERE e.is_phillies_batter = TRUE
          AND COALESCE(p.player_name, e.batter_name) IS NOT NULL
          AND TRIM(COALESCE(p.player_name, e.batter_name)) <> ''
        GROUP BY e.batter_id, COALESCE(p.player_name, e.batter_name)
        ORDER BY home_run_count DESC, max_hr_distance_ft DESC NULLS LAST, player_name ASC;

        CREATE OR REPLACE VIEW player_home_run_summary AS
        SELECT
            batter_id,
            player_name,
            home_run_count,
            avg_hr_distance_ft,
            max_hr_distance_ft,
            hardest_hit_ball_mph
        FROM hitter_event_summary
        WHERE home_run_count > 0
        ORDER BY home_run_count DESC, max_hr_distance_ft DESC NULLS LAST, player_name ASC;

        CREATE OR REPLACE VIEW monthly_home_run_totals AS
        SELECT
            batter_name AS player_name,
            DATE_TRUNC('month', game_date) AS month_start,
            COUNT(*) AS home_run_count
        FROM phillies_home_runs
        GROUP BY batter_name, DATE_TRUNC('month', game_date)
        ORDER BY month_start ASC, home_run_count DESC, player_name ASC;

        CREATE OR REPLACE VIEW game_log_summaries AS
        SELECT
            g.game_pk,
            g.game_date,
            g.opponent,
            g.venue_name,
            g.result_text,
            COUNT(CASE WHEN e.is_home_run = TRUE AND e.is_phillies_batter = TRUE THEN 1 END) AS hr_count,
            MAX(CASE WHEN e.is_home_run = TRUE AND e.is_phillies_batter = TRUE THEN e.hit_distance_sc END) AS longest_hr_ft,
            MAX(CASE WHEN e.is_phillies_batter = TRUE THEN e.launch_speed END) AS hardest_hit_ball_mph
        FROM games g
        LEFT JOIN statcast_events e ON g.game_pk = e.game_pk
        GROUP BY g.game_pk, g.game_date, g.opponent, g.venue_name, g.result_text
        ORDER BY g.game_date DESC, g.game_pk DESC;

        CREATE OR REPLACE VIEW strikeout_leaders AS
        SELECT
            e.pitcher_id,
            COALESCE(p.player_name, e.pitcher_name) AS player_name,
            COUNT(*) AS strikeouts
        FROM statcast_events e
        LEFT JOIN players p ON e.pitcher_id = p.player_id
        WHERE e.is_phillies_pitcher = TRUE
          AND e.is_strikeout = TRUE
        GROUP BY e.pitcher_id, COALESCE(p.player_name, e.pitcher_name)
        ORDER BY strikeouts DESC, player_name ASC;

        CREATE OR REPLACE VIEW fastest_pitches AS
        SELECT
            e.pitcher_id,
            COALESCE(p.player_name, e.pitcher_name) AS player_name,
            e.game_date,
            e.opponent,
            e.venue_name,
            e.pitch_name,
            e.release_speed
        FROM statcast_events e
        LEFT JOIN players p ON e.pitcher_id = p.player_id
        WHERE e.is_phillies_pitcher = TRUE
          AND e.release_speed IS NOT NULL
        ORDER BY e.release_speed DESC, e.game_date ASC, player_name ASC;

        CREATE OR REPLACE VIEW pitcher_event_summary AS
        SELECT
            e.pitcher_id,
            COALESCE(p.player_name, e.pitcher_name) AS player_name,
            COUNT(DISTINCT e.game_pk) AS appearances,
            SUM(CASE WHEN e.is_strikeout = TRUE THEN 1 ELSE 0 END) AS strikeouts,
            SUM(CASE WHEN e.events IN ('walk', 'intent_walk') THEN 1 ELSE 0 END) AS walks_issued,
            SUM(CASE WHEN e.events = 'home_run' THEN 1 ELSE 0 END) AS home_runs_allowed,
            SUM(CASE WHEN e.description IN ('swinging_strike', 'swinging_strike_blocked') THEN 1 ELSE 0 END) AS whiffs,
            MAX(e.launch_speed) AS hardest_hit_allowed_mph,
            MAX(e.release_speed) AS max_velocity_mph,
            ROUND(
                AVG(
                    CASE
                        WHEN e.pitch_name IN ('4-Seam Fastball', 'Sinker', 'Cutter')
                          OR e.pitch_type IN ('FF', 'SI', 'FC')
                        THEN e.release_speed
                    END
                ),
                2
            )
                AS avg_fastball_velocity_mph
        FROM statcast_events e
        LEFT JOIN players p ON e.pitcher_id = p.player_id
        WHERE e.is_phillies_pitcher = TRUE
          AND e.pitcher_name IS NOT NULL
        GROUP BY e.pitcher_id, COALESCE(p.player_name, e.pitcher_name)
        ORDER BY strikeouts DESC, player_name ASC;

        CREATE OR REPLACE VIEW pitcher_strikeouts_by_month AS
        SELECT
            COALESCE(p.player_name, e.pitcher_name) AS player_name,
            DATE_TRUNC('month', e.game_date) AS month_start,
            SUM(CASE WHEN e.is_strikeout = TRUE THEN 1 ELSE 0 END) AS strikeouts
        FROM statcast_events e
        LEFT JOIN players p ON e.pitcher_id = p.player_id
        WHERE e.is_phillies_pitcher = TRUE
          AND e.pitcher_name IS NOT NULL
        GROUP BY COALESCE(p.player_name, e.pitcher_name), DATE_TRUNC('month', e.game_date)
        ORDER BY month_start ASC, strikeouts DESC, player_name ASC;

        CREATE OR REPLACE VIEW pitcher_strikeouts_by_opponent AS
        SELECT
            COALESCE(p.player_name, e.pitcher_name) AS player_name,
            e.opponent,
            SUM(CASE WHEN e.is_strikeout = TRUE THEN 1 ELSE 0 END) AS strikeouts
        FROM statcast_events e
        LEFT JOIN players p ON e.pitcher_id = p.player_id
        WHERE e.is_phillies_pitcher = TRUE
          AND e.pitcher_name IS NOT NULL
        GROUP BY COALESCE(p.player_name, e.pitcher_name), e.opponent
        ORDER BY strikeouts DESC, player_name ASC, opponent ASC;

        CREATE OR REPLACE VIEW pitcher_pitch_usage AS
        SELECT
            COALESCE(p.player_name, e.pitcher_name) AS player_name,
            e.pitch_name,
            COUNT(*) AS pitch_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY COALESCE(p.player_name, e.pitcher_name)), 2) AS usage_pct
        FROM statcast_events e
        LEFT JOIN players p ON e.pitcher_id = p.player_id
        WHERE e.is_phillies_pitcher = TRUE
          AND e.pitcher_name IS NOT NULL
          AND e.pitch_name IS NOT NULL
        GROUP BY COALESCE(p.player_name, e.pitcher_name), e.pitch_name
        ORDER BY player_name ASC, pitch_count DESC, pitch_name ASC;

        CREATE OR REPLACE VIEW pitcher_season_overview AS
        SELECT
            COALESCE(event_summary.player_name, season_summary.pitcher_name) AS player_name,
            event_summary.pitcher_id,
            season_summary.wins,
            season_summary.losses,
            season_summary.games,
            season_summary.games_started,
            season_summary.saves,
            season_summary.innings_pitched,
            COALESCE(season_summary.strikeouts, event_summary.strikeouts) AS strikeouts,
            COALESCE(season_summary.walks, event_summary.walks_issued) AS walks_issued,
            COALESCE(season_summary.home_runs_allowed, event_summary.home_runs_allowed) AS home_runs_allowed,
            season_summary.era,
            season_summary.whip,
            COALESCE(season_summary.avg_fastball_velocity, event_summary.avg_fastball_velocity_mph)
                AS avg_fastball_velocity_mph,
            event_summary.max_velocity_mph,
            event_summary.whiffs,
            event_summary.hardest_hit_allowed_mph,
            event_summary.appearances
        FROM pitcher_event_summary event_summary
        FULL OUTER JOIN pitcher_season_summary season_summary
            ON LOWER(event_summary.player_name) = LOWER(season_summary.pitcher_name);
        """
    )
