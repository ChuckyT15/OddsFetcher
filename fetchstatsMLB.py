#!/usr/bin/env python3
"""
mlb_player_stats_bulk_all.py

Uses the MLB-StatsAPI wrapper (toddrob99) to:
  - Fetch all players’ hitting stats (for the current season) in paginated calls.
  - Fetch all players’ pitching stats (for the current season) in paginated calls.
  - Merge them into a single DataFrame and save to player_stats.csv at a user-specified location.

This handles the 100‐record default limit by looping through "limit" and "offset" until
all records are retrieved.

To change the output path, just edit the OUTPUT_CSV_PATH variable below.
"""

import statsapi
import pandas as pd
import datetime
import os

# ─── EDIT THIS TO YOUR DESIRED OUTPUT FILEPATH ───
# Example (Windows):
# OUTPUT_CSV_PATH = r"C:\Users\charl\Downloads\player_stats.csv"
# Example (macOS/Linux):
# OUTPUT_CSV_PATH = "/Users/charl/Downloads/player_stats.csv"
OUTPUT_CSV_PATH = r"C:/Users/charl/Downloads/OddsFetched/player_stats.csv"  # ← Change as needed
# ──────────────────────────────────────────────────


def fetch_all_hitting_stats(season_year):
    """
    Uses the 'stats' endpoint to pull all players' hitting stats for season_year,
    handling pagination (default limit = 100). Returns a DataFrame with columns:
      playerId, playerName, hitting_<metric>...
    """
    all_rows = []
    offset = 0
    limit = 100  # API default is 100; page in increments of 100

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

        # If no data returned at all or all splits are empty, we’re done
        if not stats_blocks or all(len(block.get("splits", [])) == 0 for block in stats_blocks):
            break

        batch_rows = []
        for stat_block in stats_blocks:
            for split in stat_block.get("splits", []):
                stat_values = split["stat"]
                player_id = split["player"]["id"]
                player_name = split["player"]["fullName"]  # Extract the player’s full name
                row = {
                    "playerId": player_id,
                    "playerName": player_name
                }
                # Prefix every hitting metric with "hitting_"
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
    Uses the 'stats' endpoint to pull all players' pitching stats for season_year,
    handling pagination similarly. Returns a DataFrame with columns:
      playerId, playerName, pitching_<metric>...
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
            "hydrate": "person([id,name])",  # ← Make sure we ask for name here, too
            "limit": limit,
            "offset": offset
        }
        raw = statsapi.get("stats", params)
        stats_blocks = raw.get("stats", [])

        # Stop if there is no data or all splits are empty
        if not stats_blocks or all(len(block.get("splits", [])) == 0 for block in stats_blocks):
            break

        batch_rows = []
        for stat_block in stats_blocks:
            for split in stat_block.get("splits", []):
                stat_values = split["stat"]
                player_id = split["player"]["id"]
                player_name = split["player"]["fullName"]  # Extract the player’s full name
                row = {
                    "playerId": player_id,
                    "playerName": player_name
                }
                # Prefix every pitching metric with "pitching_"
                for fld, val in stat_values.items():
                    row[f"pitching_{fld}"] = val
                batch_rows.append(row)

        if not batch_rows:
            break

        all_rows.extend(batch_rows)
        offset += limit

    return pd.DataFrame(all_rows)


def main():
    # 1) Ensure the directory exists for OUTPUT_CSV_PATH
    output_dir = os.path.dirname(OUTPUT_CSV_PATH)
    if output_dir and not os.path.isdir(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    current_year = datetime.date.today().year

    # 2) Fetch all hitting stats in paginated calls
    print(f"Fetching all hitting stats for {current_year} (paginated)...")
    hitting_df = fetch_all_hitting_stats(current_year)
    print(f"  → Retrieved hitting stats for {len(hitting_df)} players.")

    # 3) Fetch all pitching stats in paginated calls
    print(f"Fetching all pitching stats for {current_year} (paginated)...")
    pitching_df = fetch_all_pitching_stats(current_year)
    print(f"  → Retrieved pitching stats for {len(pitching_df)} players.")

    # 4) Merge on playerId (outer join to keep pure hitters AND pure pitchers)
    merged = pd.merge(
        hitting_df,
        pitching_df,
        on="playerId",
        how="outer",
        suffixes=("_hit", "_pit"),
        copy=False
    )

    # 5) Reconstruct a single `playerName` if pandas gave us suffixes:
    #    - If both sides had "playerName", pandas will name them "playerName_hit" and "playerName_pit".
    #    - If only one side had it, we pull from whichever side is non-null.
    if "playerName_hit" in merged.columns or "playerName_pit" in merged.columns:
        # Create one unified column, preferring hitting side if both exist.
        merged["playerName"] = (
            merged.get("playerName_hit", pd.Series(dtype="object"))
                  .fillna(merged.get("playerName_pit", pd.Series(dtype="object")))
        )
        # Drop the old suffixed columns
        merged.drop(columns=[c for c in ["playerName_hit", "playerName_pit"] if c in merged.columns], inplace=True)
    elif "playerName" not in merged.columns:
        # (Edge‐case) If for some reason neither side ended up with `playerName`, then build a map manually:
        hit_map = hitting_df.set_index("playerId")["playerName"].to_dict() if "playerName" in hitting_df.columns else {}
        pit_map = pitching_df.set_index("playerId")["playerName"].to_dict() if "playerName" in pitching_df.columns else {}
        merged["playerName"] = merged["playerId"].map(hit_map).fillna(merged["playerId"].map(pit_map))

    # 6) Reorder columns: playerId, playerName, then all hitting_*, then all pitching_*
    #    If `playerName` is still missing (shouldn’t happen now), we simply skip it in the reorder.
    cols = ["playerId"]
    if "playerName" in merged.columns:
        cols.append("playerName")

    # Collect all columns that start with hitting_ (sorted alphabetically)
    hitting_cols = sorted([c for c in merged.columns if c.startswith("hitting_")])
    # Collect all columns that start with pitching_ (sorted alphabetically)
    pitching_cols = sorted([c for c in merged.columns if c.startswith("pitching_")])

    cols += hitting_cols
    cols += pitching_cols

    # Finally, reindex merged to those columns (if any are missing, pandas will complain—so we only include what exists)
    merged = merged[[c for c in cols if c in merged.columns]]

    # 7) Write to CSV at the user‐specified path
    merged.to_csv(OUTPUT_CSV_PATH, index=False)
    print(f"\nWrote {len(merged)} rows (one per player) to:\n  {OUTPUT_CSV_PATH}")


if __name__ == "__main__":
    main()
