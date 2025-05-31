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
OUTPUT_CSV_PATH = r"C:/Users/charl/Downloads/OddsFetched/player_stats.csv" # ADD YOUR PATH HERE
# ──────────────────────────────────────────────────

def fetch_all_hitting_stats(season_year):
    """
    Uses the 'stats' endpoint to pull all players' hitting stats for season_year,
    handling pagination (default limit = 100). Returns a DataFrame with columns:
      playerId, playerName, hitting_<metric>...
    """
    all_rows = []
    offset = 0
    limit = 100  # API default is 100; we’ll page in increments of 100
    
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
        
        # If no data returned, break
        if not stats_blocks or all(len(block.get("splits", [])) == 0 for block in stats_blocks):
            break
        
        # Extract every split from each block
        batch_rows = []
        for stat_block in stats_blocks:
            for split in stat_block.get("splits", []):
                stat_values = split["stat"]
                player_id = split["player"]["id"]
                player_name = split["player"]["fullName"]
                row = {"playerId": player_id, "playerName": player_name}
                # prefix every hitting metric with "hitting_"
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
      playerId, pitching_<metric>...
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
            "limit": limit,
            "offset": offset
        }
        raw = statsapi.get("stats", params)
        stats_blocks = raw.get("stats", [])
        
        if not stats_blocks or all(len(block.get("splits", [])) == 0 for block in stats_blocks):
            break
        
        batch_rows = []
        for stat_block in stats_blocks:
            for split in stat_block.get("splits", []):
                stat_values = split["stat"]
                player_id = split["player"]["id"]
                row = {"playerId": player_id}
                for fld, val in stat_values.items():
                    row[f"pitching_{fld}"] = val
                batch_rows.append(row)
        
        if not batch_rows:
            break
        
        all_rows.extend(batch_rows)
        offset += limit
    
    return pd.DataFrame(all_rows)


def main():
    # Ensure the directory for OUTPUT_CSV_PATH exists
    output_dir = os.path.dirname(OUTPUT_CSV_PATH)
    if output_dir and not os.path.isdir(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    current_year = datetime.date.today().year
    
    # 1) Fetch all hitting stats in paginated calls
    print(f"Fetching all hitting stats for {current_year} (paginated)...")
    hitting_df = fetch_all_hitting_stats(current_year)
    print(f"  → Retrieved hitting stats for {len(hitting_df)} players.")
    
    # 2) Fetch all pitching stats in paginated calls
    print(f"Fetching all pitching stats for {current_year} (paginated)...")
    pitching_df = fetch_all_pitching_stats(current_year)
    print(f"  → Retrieved pitching stats for {len(pitching_df)} players.")
    
    # 3) Merge on playerId (outer join to keep pure hitters and pure pitchers)
    merged = pd.merge(
        hitting_df,
        pitching_df,
        on="playerId",
        how="outer",
        copy=False
    )
    
    # 4) Reorder columns: playerId, playerName, hitting_..., pitching_...
    cols = ["playerId", "playerName"] + \
           sorted([c for c in merged.columns if c.startswith("hitting_")]) + \
           sorted([c for c in merged.columns if c.startswith("pitching_")])
    merged = merged[cols]
    
    # 5) Write to CSV at the user-specified path
    merged.to_csv(OUTPUT_CSV_PATH, index=False)
    print(f"\nWrote {len(merged)} rows (one per player) to:\n  {OUTPUT_CSV_PATH}")


if __name__ == "__main__":
    main()
