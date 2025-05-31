#!/usr/bin/env python3
"""
mlb_player_stats_bulk_all_with_team.py

Uses the MLB-StatsAPI wrapper (toddrob99) to:
  - Fetch all players’ hitting stats (for the current season) in paginated calls.
  - Fetch all players’ pitching stats (for the current season) in paginated calls.
  - Fetch all MLB teams’ hitting stats (for the current season) in one call via teams_stats.
  - Fetch all MLB teams’ pitching stats (for the current season) in one call via teams_stats.
  - Merge player stats into a single DataFrame and save to player_stats.csv.
  - Merge team hitting/pitching into a single team_stats.csv.

To change the output paths, edit OUTPUT_PLAYER_CSV_PATH and OUTPUT_TEAM_CSV_PATH below.
"""

import statsapi
import pandas as pd
import datetime
import os

# ─── EDIT THESE TO YOUR DESIRED OUTPUT FILEPATHS ───
# Example (Windows):
#   OUTPUT_PLAYER_CSV_PATH = r"C:\Users\charl\Downloads\player_stats.csv"
#   OUTPUT_TEAM_CSV_PATH   = r"C:\Users\charl\Downloads\team_stats.csv"
#
# Example (macOS/Linux):
#   OUTPUT_PLAYER_CSV_PATH = "/Users/charl/Downloads/player_stats.csv"
#   OUTPUT_TEAM_CSV_PATH   = "/Users/charl/Downloads/team_stats.csv"
#
OUTPUT_PLAYER_CSV_PATH = r"C:/Users/charl/Downloads/OddsFetched/player_stats.csv"
OUTPUT_TEAM_CSV_PATH   = r"C:/Users/charl/Downloads/OddsFetched/team_stats.csv"
# ─────────────────────────────────────────────────────────


def fetch_all_hitting_stats(season_year):
    """
    Pulls all players' hitting stats for season_year, handling pagination.
    Returns a DataFrame with columns: playerId, playerName, hitting_<metric>...
    """
    all_rows = []
    offset = 0
    limit = 100

    while True:
        params = {
            "stats": "season",
            "season": season_year,
            "group": "hitting",
            "playerPool": "ALL",
            "hydrate": "person([id,name])",
            "limit": limit,
            "offset": offset
        }
        raw = statsapi.get("stats", params)
        stats_blocks = raw.get("stats", [])

        # If no stats returned or all splits empty, we’re done.
        if not stats_blocks or all(len(block.get("splits", [])) == 0 for block in stats_blocks):
            break

        batch_rows = []
        for stat_block in stats_blocks:
            for split in stat_block.get("splits", []):
                stat_values = split.get("stat", {})
                player_id = split["player"]["id"]
                player_name = split["player"]["fullName"]
                row = {
                    "playerId": player_id,
                    "playerName": player_name
                }
                # Prefix each hitting metric with "hitting_"
                for fld, val in stat_values.items():
                    row[f"hitting_{fld}"] = val
                batch_rows.append(row)

        if not batch_rows:
            break

        all_rows.extend(batch_rows)
        offset += limit

    return pd.DataFrame(all_rows)


def fetch_all_pitching_stats(season_year):
    """
    Pulls all players' pitching stats for season_year, handling pagination.
    Returns a DataFrame with columns: playerId, playerName, pitching_<metric>...
    """
    all_rows = []
    offset = 0
    limit = 100

    while True:
        params = {
            "stats": "season",
            "season": season_year,
            "group": "pitching",
            "playerPool": "ALL",
            "hydrate": "person([id,name])",
            "limit": limit,
            "offset": offset
        }
        raw = statsapi.get("stats", params)
        stats_blocks = raw.get("stats", [])

        # If no stats returned or all splits empty, we’re done.
        if not stats_blocks or all(len(block.get("splits", [])) == 0 for block in stats_blocks):
            break

        batch_rows = []
        for stat_block in stats_blocks:
            for split in stat_block.get("splits", []):
                stat_values = split.get("stat", {})
                player_id = split["player"]["id"]
                player_name = split["player"]["fullName"]
                row = {
                    "playerId": player_id,
                    "playerName": player_name
                }
                # Prefix each pitching metric with "pitching_"
                for fld, val in stat_values.items():
                    row[f"pitching_{fld}"] = val
                batch_rows.append(row)

        if not batch_rows:
            break

        all_rows.extend(batch_rows)
        offset += limit

    return pd.DataFrame(all_rows)


def fetch_all_team_hitting_stats(season_year):
    """
    Uses the teams_stats endpoint to pull every MLB team’s hitting stats for season_year.
    Adds "stats": "season" to satisfy the required parameters.
    Returns a DataFrame with columns: teamId, teamName, team_hitting_<metric>...
    """
    params = {
        "season": season_year,
        "group": "hitting",
        "stats": "season",
        "sportIds": 1
    }
    raw = statsapi.get("teams_stats", params)
    stats_blocks = raw.get("stats", [])

    # Typically stats_blocks[0]["splits"] is a list of per‐team splits
    if not stats_blocks or not stats_blocks[0].get("splits"):
        return pd.DataFrame()

    splits = stats_blocks[0]["splits"]
    rows = []
    for split in splits:
        stat_values = split.get("stat", {})
        team_info = split.get("team", {})
        team_id = team_info.get("id")
        team_name = team_info.get("name")
        row = {
            "teamId": team_id,
            "teamName": team_name
        }
        # Prefix each metric with "team_hitting_"
        for fld, val in stat_values.items():
            row[f"team_hitting_{fld}"] = val
        rows.append(row)

    return pd.DataFrame(rows)


def fetch_all_team_pitching_stats(season_year):
    """
    Uses the teams_stats endpoint to pull every MLB team’s pitching stats for season_year.
    Adds "stats": "season" to satisfy the required parameters.
    Returns a DataFrame with columns: teamId, teamName, team_pitching_<metric>...
    """
    params = {
        "season": season_year,
        "group": "pitching",
        "stats": "season",
        "sportIds": 1
    }
    raw = statsapi.get("teams_stats", params)
    stats_blocks = raw.get("stats", [])

    if not stats_blocks or not stats_blocks[0].get("splits"):
        return pd.DataFrame()

    splits = stats_blocks[0]["splits"]
    rows = []
    for split in splits:
        stat_values = split.get("stat", {})
        team_info = split.get("team", {})
        team_id = team_info.get("id")
        team_name = team_info.get("name")
        row = {
            "teamId": team_id,
            "teamName": team_name
        }
        # Prefix each metric with "team_pitching_"
        for fld, val in stat_values.items():
            row[f"team_pitching_{fld}"] = val
        rows.append(row)

    return pd.DataFrame(rows)


def main():
    # 1) Ensure output directories exist
    player_output_dir = os.path.dirname(OUTPUT_PLAYER_CSV_PATH)
    team_output_dir = os.path.dirname(OUTPUT_TEAM_CSV_PATH)
    if player_output_dir and not os.path.isdir(player_output_dir):
        os.makedirs(player_output_dir, exist_ok=True)
    if team_output_dir and not os.path.isdir(team_output_dir):
        os.makedirs(team_output_dir, exist_ok=True)

    current_year = datetime.date.today().year

    # 2) Fetch player hitting stats
    print(f"Fetching all hitting stats for {current_year} (paginated)...")
    hitting_df = fetch_all_hitting_stats(current_year)
    print(f"  → Retrieved hitting stats for {len(hitting_df)} rows.")

    # 3) Fetch player pitching stats
    print(f"Fetching all pitching stats for {current_year} (paginated)...")
    pitching_df = fetch_all_pitching_stats(current_year)
    print(f"  → Retrieved pitching stats for {len(pitching_df)} rows.")

    # 4) Merge player DataFrames on playerId (outer join)
    merged_players = pd.merge(
        hitting_df,
        pitching_df,
        on="playerId",
        how="outer",
        suffixes=("_hit", "_pit"),
        copy=False
    )

    # 5) Reconstruct a single "playerName" column if pandas added suffixes
    if "playerName_hit" in merged_players.columns or "playerName_pit" in merged_players.columns:
        merged_players["playerName"] = (
            merged_players.get("playerName_hit", pd.Series(dtype="object"))
                          .fillna(merged_players.get("playerName_pit", pd.Series(dtype="object")))
        )
        merged_players.drop(
            columns=[c for c in ["playerName_hit", "playerName_pit"]
                     if c in merged_players.columns],
            inplace=True
        )
    elif "playerName" not in merged_players.columns:
        # Edge‐case: neither side had playerName
        hit_map = hitting_df.set_index("playerId")["playerName"].to_dict() if "playerName" in hitting_df.columns else {}
        pit_map = pitching_df.set_index("playerId")["playerName"].to_dict() if "playerName" in pitching_df.columns else {}
        merged_players["playerName"] = merged_players["playerId"].map(hit_map).fillna(
            merged_players["playerId"].map(pit_map)
        )

    # 6) Reorder player columns: playerId, playerName, hitting_*, pitching_*
    cols = ["playerId"]
    if "playerName" in merged_players.columns:
        cols.append("playerName")
    hitting_cols = sorted([c for c in merged_players.columns if c.startswith("hitting_")])
    pitching_cols = sorted([c for c in merged_players.columns if c.startswith("pitching_")])
    cols += hitting_cols + pitching_cols
    merged_players = merged_players[[c for c in cols if c in merged_players.columns]]

    # 7) Write player_stats.csv
    merged_players.to_csv(OUTPUT_PLAYER_CSV_PATH, index=False)
    print(f"\nWrote {len(merged_players)} player rows to:\n  {OUTPUT_PLAYER_CSV_PATH}")

    # 8) Fetch all teams’ hitting stats
    print(f"\nFetching all team hitting stats for {current_year}...")
    team_hitting_df = fetch_all_team_hitting_stats(current_year)
    print(f"  → Retrieved team hitting stats for {len(team_hitting_df)} teams.")

    # 9) Fetch all teams’ pitching stats
    print(f"Fetching all team pitching stats for {current_year}...")
    team_pitching_df = fetch_all_team_pitching_stats(current_year)
    print(f"  → Retrieved team pitching stats for {len(team_pitching_df)} teams.")

    # 10) Merge team‐level hitting/pitching on teamId, teamName (outer join)
    merged_teams = pd.merge(
        team_hitting_df,
        team_pitching_df,
        on=["teamId", "teamName"],
        how="outer",
        copy=False
    )

    # 11) Reorder team columns: teamId, teamName, team_hitting_*, team_pitching_*
    team_cols = ["teamId", "teamName"]
    hitting_cols = sorted([c for c in merged_teams.columns if c.startswith("team_hitting_")])
    pitching_cols = sorted([c for c in merged_teams.columns if c.startswith("team_pitching_")])
    team_cols += hitting_cols + pitching_cols
    merged_teams = merged_teams[[c for c in team_cols if c in merged_teams.columns]]

    # 12) Write team_stats.csv
    merged_teams.to_csv(OUTPUT_TEAM_CSV_PATH, index=False)
    print(f"\nWrote {len(merged_teams)} team rows to:\n  {OUTPUT_TEAM_CSV_PATH}")


if __name__ == "__main__":
    main()
