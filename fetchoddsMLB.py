import requests
import csv
from datetime import datetime, date, time, timezone
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


def write_odds_row(writer, game_info, bookmaker, market, outcome):
    """Helper function to write a row with enhanced player information"""
    # Extract player information for player props
    outcome_name = outcome['name']
    player_name = outcome.get('player_name', '')
    description = outcome.get('description', '')
    
    # For player props, combine the information for better clarity
    if player_name:
        if description and description != outcome_name:
            full_outcome_name = f"{player_name} - {description}"
        else:
            full_outcome_name = f"{player_name} - {outcome_name}"
    else:
        full_outcome_name = outcome_name
    
    writer.writerow({
        'commence_time': game_info['commence_time'],
        'away_team': game_info['away_team'],
        'home_team': game_info['home_team'],
        'bookmaker': bookmaker,
        'market_key': market,
        'outcome_name': full_outcome_name,
        'player_name': player_name,
        'description': description,
        'price': outcome['price'],
        'point': outcome.get('point', '')  # For spreads and totals
    })


if __name__ == "__main__":
    # Fetch featured odds
    desired_markets = FEATURED_MARKETS + ["player_props"]
    games = fetch_featured_odds(desired_markets)

    # Filter games for today until 11:59 PM local time
    now = datetime.now()
    today_start = datetime.combine(now.date(), time.min)  # Today at 00:00:00
    today_end = datetime.combine(now.date(), time(23, 59, 59))  # Today at 23:59:59
    
    print(f"Looking for games between {today_start} and {today_end} local time")
    
    today_games = []
    for g in games:
        # Parse the UTC time from API
        game_time_utc = datetime.fromisoformat(g["commence_time"].replace("Z", "+00:00"))
        # Convert to local time for comparison
        game_time_local = game_time_utc.replace(tzinfo=timezone.utc).astimezone()
        # Remove timezone info for comparison with naive datetime
        game_time_local_naive = game_time_local.replace(tzinfo=None)
        
        if today_start <= game_time_local_naive <= today_end:
            today_games.append(g)
            print(f"Including game: {g['away_team']} @ {g['home_team']} at {game_time_local_naive}")
    
    print(f"Found {len(today_games)} games for today")

    # Open CSV for writing with enhanced fieldnames
    with open(CSV_PATH, mode='w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'commence_time', 'away_team', 'home_team',
            'bookmaker', 'market_key', 'outcome_name', 
            'player_name', 'description', 'price', 'point'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Write featured odds rows
        for game in today_games:
            game_info = {
                'commence_time': game['commence_time'],
                'away_team': game['away_team'],
                'home_team': game['home_team']
            }
            
            for book in game['bookmakers']:
                bookmaker = book.get('title', book.get('key'))
                for m in book['markets']:
                    market = m['key']
                    for o in m['outcomes']:
                        write_odds_row(writer, game_info, bookmaker, market, o)

        # Fetch and write additional markets
        EXTRA_MARKETS = [
            "batter_home_runs",
            "batter_hits", 
            "batter_rbis",
            "batter_runs_scored",
            "batter_stolen_bases",
            "batter_singles",
            "batter_doubles",
            "batter_triples",
            "batter_walks",
            "batter_strikeouts",
            "batter_hits_runs_rbis",
            "pitcher_strikeouts",
            "pitcher_hits_allowed",
            "pitcher_walks",
            "pitcher_earned_runs",
            "pitcher_outs",
            "totals_1st_5_innings",
        ]
        
        for game in today_games:
            try:
                full = fetch_event_odds(game['id'], EXTRA_MARKETS)
                game_info = {
                    'commence_time': full.get('commence_time', game['commence_time']),
                    'away_team': full.get('away_team', game['away_team']),
                    'home_team': full.get('home_team', game['home_team'])
                }
                
                for book in full['bookmakers']:
                    bookmaker = book.get('title', book.get('key'))
                    for m in book['markets']:
                        market = m['key']
                        for o in m['outcomes']:
                            write_odds_row(writer, game_info, bookmaker, market, o)
                            
            except HTTPError as e:
                print(f"Error fetching additional markets for game {game['id']}: {e}")
                continue

    print(f"Exported odds to CSV: {CSV_PATH}")
    print("Player names are now included in separate 'player_name' column and combined in 'outcome_name' for player props.")
