"""Print the most recent game ID for any NBA team, useful for seeding NBA_GAME_ID.

Usage
-----
    python scripts/get_last_game.py NYK                        # last regular-season Knicks game
    python scripts/get_last_game.py LAL --playoffs             # last playoff Lakers game
    python scripts/get_last_game.py BOS --season 2023-24       # different season
    python scripts/get_last_game.py GSW --n 5                  # show last N games

Then run the producer against the result:
    NBA_GAME_ID=<game_id> python -m src.producer
"""

import argparse
import sys

from nba_api.stats.endpoints import teamgamelog
from nba_api.stats.static import teams


DEFAULT_SEASON = "2025-26"


def find_team(query: str) -> dict:
    """Return a team dict matching a tricode, full name, or city (case-insensitive)."""
    q = query.strip().lower()
    all_teams = teams.get_teams()
    for t in all_teams:
        if (
            t["abbreviation"].lower() == q
            or t["full_name"].lower() == q
            or t["nickname"].lower() == q
            or t["city"].lower() == q
        ):
            return t
    # Partial match fallback
    matches = [t for t in all_teams if q in t["full_name"].lower()]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        names = ", ".join(t["abbreviation"] for t in matches)
        print(f"Ambiguous team '{query}'. Matches: {names}", file=sys.stderr)
        sys.exit(1)
    print(f"Team not found: '{query}'. Use a tricode (e.g. NYK, LAL, BOS).", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("team", help="Team tricode, nickname, or city (e.g. NYK, Lakers, Boston)")
    parser.add_argument("--playoffs", action="store_true", help="Query playoff games instead of regular season")
    parser.add_argument("--season", default=DEFAULT_SEASON, help="Season string, e.g. 2023-24 (default: %(default)s)")
    parser.add_argument("--n", type=int, default=1, help="Number of recent games to show (default: 1)")
    args = parser.parse_args()

    team = find_team(args.team)
    season_type = "Playoffs" if args.playoffs else "Regular Season"

    print(f"Fetching {team['full_name']} {season_type} game log for {args.season}...")
    log = teamgamelog.TeamGameLog(
        team_id=str(team["id"]),
        season=args.season,
        season_type_all_star=season_type,
        timeout=60,
    )
    games = log.get_data_frames()[0]

    if games.empty:
        print(f"No {season_type} games found for {args.season}.")
        return

    recent = games[["Game_ID", "MATCHUP", "GAME_DATE", "WL", "PTS"]].head(args.n)
    print(recent.to_string(index=False))
    print()
    print(f"NBA_GAME_ID={games.iloc[0]['Game_ID']}")


if __name__ == "__main__":
    main()
