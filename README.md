# Phillies Statcast 2026 — Claude Code edition

> **Claude Code edition.** This is the parallel variant of [dlempa/phillies-statcast](https://github.com/dlempa/phillies-statcast), evolved exclusively with [Claude Code](https://claude.com/claude-code). The original repo continues under OpenAI Codex. See [COMPARISON.md](COMPARISON.md) for the head-to-head log between the two AI tools.
>
> **Live app:** https://phillies-statcast-claude.streamlit.app

A simple Streamlit app that tracks the biggest Phillies home runs and other standout Statcast moments from the 2026 season using only free tools.

The app covers hitter power stats, a live Top 10 of the longest Phillies home runs, and a pitching dashboard with run prevention trends and pitcher leaderboards. Everything runs on a DuckDB warehouse fed daily by `pybaseball`.

## Project Purpose

- Show a running Top 10 of the longest Phillies home runs of the season.
- Surface other power stats like hardest-hit home runs and hardest-hit balls overall.
- Store data in DuckDB instead of spreadsheets or hand-maintained CSV files.
- Keep the codebase small, readable, and easy to explain.

## Tech Stack

- Python
- Streamlit
- DuckDB
- pybaseball
- GitHub Actions

## Project Structure

```text
.
|-- Home.py
|-- pages/
|   |-- 1_Longest_Home_Runs.py
|   |-- 2_Power_Stats.py
|   |-- 3_Player_Summary.py
|   |-- 4_Game_Log.py
|   `-- 5_Pitching_Dashboard.py
|-- scripts/
|   |-- bootstrap_2026.py
|   `-- update_daily.py
|-- src/phillies_stats/
|   |-- config.py
|   |-- database.py
|   |-- ingest.py
|   `-- queries.py
|-- tests/
|   |-- test_ingest.py
|   |-- test_queries.py
|   `-- support.py
|-- data/
|   `-- phillies_2026.duckdb
`-- .github/workflows/update_data.yml
```

## Local Setup

Create and activate a virtual environment in PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## How To Run Streamlit

Start the app from the repo root:

```powershell
streamlit run Home.py
```

## How To Backfill Data

This pulls Phillies-related Statcast events for the configured season into DuckDB.

```powershell
python .\scripts\bootstrap_2026.py --season 2026
```

Optional date overrides:

```powershell
python .\scripts\bootstrap_2026.py --season 2026 --start-date 2026-03-01 --end-date 2026-10-01
```

## How To Run Daily Updates

This re-pulls a small recent window so the script stays idempotent and can safely catch source corrections without creating duplicate rows.

```powershell
python .\scripts\update_daily.py --season 2026
```

## How To Store A State Summary

The home page can show a generated "State of the Phillies" summary when a row exists in DuckDB. A Codex automation can generate a JSON payload after the daily stats update, then store it with:

```powershell
python .\scripts\upsert_state_summary.py --season 2026 --payload .\state_summary.json
```

The payload should include a headline, 2-3 sentence summary, compact key stats, and source links. The user-facing summary should read like baseball analysis, not mention the database, web research, or the automation.

## How To Run Tests

```powershell
python -m unittest discover -s tests -v
```

## Architecture

- `statcast_events` is the main fact table.
  It stores broad Phillies-related Statcast events, not only home runs.
- `games` stores one row per Phillies game.
- `players` stores player metadata for hitters and pitchers.
- `ingestion_runs` logs each backfill or daily update run.
- SQL views and query helpers generate the leaderboards and summary tables used by the Streamlit app.

The Top 10 longest home runs is dynamic by design. The app does not read from a hand-maintained top-10 file. It recalculates the leaderboard from the full set of stored home run events whenever the database is updated.

## Assumptions And Notes

- The database path defaults to `data/phillies_2026.duckdb` for the 2026 season, and the season can be changed with a script argument or the `PHILLIES_STATCAST_SEASON` environment variable.
- The pybaseball `team="PHI"` Statcast pull is used as the free data source for Phillies games.
- Batter names are resolved as cleanly as possible from available player IDs. If the source is missing a name, the stored value may be less polished until a later update fills it in.
- Ballpark names are mapped from the home team when the source does not provide a stadium name directly.
- Empty states are expected early in the season or before the first backfill.
