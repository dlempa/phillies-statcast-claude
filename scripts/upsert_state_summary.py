from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from phillies_stats.config import get_config
from phillies_stats.database import get_connection, initialize_database
from phillies_stats.state_summary import StateSummaryValidationError, upsert_team_state_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Store a generated State of the Phillies summary.")
    parser.add_argument("--season", type=int, default=2026, help="Season year to update.")
    parser.add_argument("--payload", required=True, help="Path to the generated summary JSON payload.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload_path = Path(args.payload)
    try:
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
    except OSError as exc:
        print(f"Unable to read summary payload: {exc}", file=sys.stderr)
        return 1
    except json.JSONDecodeError as exc:
        print(f"Summary payload is not valid JSON: {exc}", file=sys.stderr)
        return 1

    config = get_config(args.season)
    conn = get_connection(config.db_path)
    initialize_database(conn)

    try:
        row = upsert_team_state_summary(conn, payload, season=config.season)
    except StateSummaryValidationError as exc:
        print(f"Summary payload failed validation: {exc}", file=sys.stderr)
        return 1

    print(f"Stored State of the Phillies summary for {row['season']} as of {row['as_of_date']}.")
    print(f"Database: {config.db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
