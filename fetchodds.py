import requests
import json
from datetime import datetime, date
from requests.exceptions import HTTPError

API_KEY   = "04d4c837f7bcb3224dd30c6adc55becc"
SPORT_KEY = "baseball_mlb"
REGIONS   = "us"
ODDS_FMT  = "american"
DATE_FMT  = "iso"
PATH = r"C:/Users/charl/Downloads/OddsFetched/odds.json" # ADD PATHNAME HERE CHRISTOPHER

# The only supported markets for MLB on the main odds endpoint:
FEATURED_MARKETS = ["h2h", "spreads", "totals"]


def fetch_featured_odds(markets):
    """
    Fetch upcoming odds from the featured endpoint.
    Automatically retries with only FEATURED_MARKETS on a 422 error.
    """
    url = f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/odds"
    params = {
        "apiKey":     API_KEY,
        "regions":    REGIONS,
        "markets":    ",".join(markets),
        "oddsFormat": ODDS_FMT,
        "dateFormat": DATE_FMT
    }
    try:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
    except HTTPError as e:
        if resp.status_code == 422:
            # Invalid markets for this sport—fallback to featured only
            print("Warning: some markets unsupported for MLB; retrying with featured markets only.")
            params["markets"] = ",".join(FEATURED_MARKETS)
            resp = requests.get(url, params=params)
            resp.raise_for_status()
        else:
            raise
    return resp.json()


def fetch_event_odds(event_id, extra_markets):
    """
    Fetch *all* markets for a single event, including props, innings, etc.
    extra_markets should be a list of specific market keys you need, e.g.
      ["batter_home_runs","pitcher_strikeouts","totals_1st_5_innings"]
    """
    url = f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/events/{event_id}/odds"
    params = {
        "apiKey":     API_KEY,
        "regions":    REGIONS,
        "markets":    ",".join(extra_markets),
        "oddsFormat": ODDS_FMT,
        "dateFormat": DATE_FMT
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


if __name__ == "__main__":
    # 1) Attempt to pull *all* markets in one go (will 422)
    desired_markets = FEATURED_MARKETS + ["player_props"]
    games = fetch_featured_odds(desired_markets)

    # Write the raw JSON to a specific file path
    with open(PATH, "w") as f:
        json.dump(games, f, indent=2)
    print("Saved featured odds to C:\\Users\\charl\\Documents\\odds.json")

    # 2) Filter to today’s games
    today = date.today()
    today_games = [
      g for g in games
      if datetime.fromisoformat(g["commence_time"].replace("Z","+00:00")).date() == today
    ]

    # 3) Print featured odds
    for game in today_games:
        print(f"{game['commence_time']} — {game['away_team']} @ {game['home_team']}")
        for book in game["bookmakers"]:
            for m in book["markets"]:
                outcomes = ', '.join(f"{o['name']} {o['price']}" for o in m['outcomes'])
                print(f"  [{m['key']}] {outcomes}")

    # 4) (Optional) For each game, fetch extra markets one by one:
    EXTRA_MARKETS = [
      # customize these keys based on what baseball props you need:
      "batter_home_runs",
      "batter_hits",
      "pitcher_strikeouts",
      "totals_1st_5_innings",
    ]
    for game in today_games:
        eid = game["id"]
        full = fetch_event_odds(eid, EXTRA_MARKETS)
        # Append additional markets to the same file
        with open(PATH, "a") as f:
            f.write('\n')
            json.dump(full, f, indent=2)
        print(f"--- Additional markets for event {eid} ---")
        for book in full["bookmakers"]:
            for m in book["markets"]:
                outcomes = ', '.join(f"{o['name']} {o['price']}" for o in m['outcomes'])
                print(f"  [{m['key']}] {outcomes}")
