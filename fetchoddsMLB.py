import requests
import csv
from datetime import datetime, date
from requests.exceptions import HTTPError

API_KEY   = "04d4c837f7bcb3224dd30c6adc55becc"
SPORT_KEY = "baseball_mlb"
REGIONS   = "us"
ODDS_FMT  = "american"
DATE_FMT  = "iso"
CSV_PATH  = r"C:/Users/charl/Downloads/OddsFetched/odds.csv"  # Path to output CSV

# The only supported markets for MLB on the main odds endpoint:
FEATURED_MARKETS = ["h2h", "spreads", "totals"]


def fetch_featured_odds(markets):
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
            print("Warning: fallback to featured markets only.")
            params["markets"] = ",".join(FEATURED_MARKETS)
            resp = requests.get(url, params=params)
            resp.raise_for_status()
        else:
            raise
    return resp.json()


def fetch_event_odds(event_id, extra_markets):
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
    # Fetch featured odds
    desired_markets = FEATURED_MARKETS + ["player_props"]
    games = fetch_featured_odds(desired_markets)

    # Filter today's games
    today = date.today()
    today_games = [
        g for g in games
        if datetime.fromisoformat(g["commence_time"].replace("Z", "+00:00")).date() == today
    ]

    # Open CSV for writing
    with open(CSV_PATH, mode='w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'commence_time', 'away_team', 'home_team',
            'bookmaker', 'market_key', 'outcome_name', 'price'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Write featured odds rows
        for game in today_games:
            time = game['commence_time']
            away = game['away_team']
            home = game['home_team']
            for book in game['bookmakers']:
                bookmaker = book.get('title', book.get('key'))
                for m in book['markets']:
                    market = m['key']
                    for o in m['outcomes']:
                        writer.writerow({
                            'commence_time': time,
                            'away_team': away,
                            'home_team': home,
                            'bookmaker': bookmaker,
                            'market_key': market,
                            'outcome_name': o['name'],
                            'price': o['price']
                        })

        # Fetch and write additional markets
        EXTRA_MARKETS = [
            "batter_home_runs",
            "batter_hits",
            "pitcher_strikeouts",
            "totals_1st_5_innings",
        ]
        for game in today_games:
            full = fetch_event_odds(game['id'], EXTRA_MARKETS)
            for book in full['bookmakers']:
                bookmaker = book.get('title', book.get('key'))
                for m in book['markets']:
                    market = m['key']
                    for o in m['outcomes']:
                        writer.writerow({
                            'commence_time': full.get('commence_time', game['commence_time']),
                            'away_team': full.get('away_team', game['away_team']),
                            'home_team': full.get('home_team', game['home_team']),
                            'bookmaker': bookmaker,
                            'market_key': market,
                            'outcome_name': o['name'],
                            'price': o['price']
                        })

    print(f"Exported odds to CSV: {CSV_PATH}")
