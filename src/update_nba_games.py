#!/usr/bin/env python

# - get max game_date where status is 'Final'
# - query the API with that date and the preceeding dates
# - format the data into insert values
# - write an upsert query to add the rows

import requests
from datetime import datetime, timedelta
from nba_pg_ingestion_utils import query_postgres, get_row_to_insert
from get_nba_games import format_games_data
# import pytz

def get_start_and_end_dates() -> tuple[str, str]:
    """
    blah
    """
    query = """ SELECT MAX(game_date) FROM nba_basketball.game WHERE status = 'Final';
    """
    max_game_date = query_postgres(query)[0]
    start_date = (max_game_date - timedelta(days=2)).strftime("%Y-%m-%d")
    end_date = datetime.now().date().strftime("%Y-%m-%d")

    return start_date, end_date

def get_games_to_update(url: str, start_date: str, end_date: str) -> str:
    current_page = 1
    total_pages = 1
    to_upsert = ""
    while current_page <= total_pages and total_pages != 0:
        params = {
            "per_page": 10,
            "page": current_page,
            "start_date": start_date,
            "end_date": end_date,
        }
        # Query the API
        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 200:
            data = response.json()["data"]
            meta = response.json()["meta"]

            for row in data:
                formatted = format_games_data(row)
                row_to_insert = "("+get_row_to_insert(formatted)+")"
                to_upsert += f"\n{row_to_insert},"

            current_page = meta["current_page"] + 1
            total_pages = meta["total_pages"]
        else:
            print(f"API Response Error: {response.status_code}")
            print(response.reason)
            break
    return to_upsert


start_date, end_date = get_start_and_end_dates()
print(f"Updating the games table with games between {start_date} and {end_date}.")

to_upsert = get_games_to_update(
    url="https://www.balldontlie.io/api/v1/games",
    start_date=start_date,
    end_date=end_date
    )
to_upsert = to_upsert[:-1] # get rid of last comma

upsert_query = """
INSERT INTO nba_basketball.game_test
(
    game_id,
    game_date,
    home_team_id,
    home_team_score,
    visitor_team_id,
    visitor_team_score,
    season,
    post_season,
    status
    )
VALUES"""
upsert_query += to_upsert
upsert_query += """
ON CONFLICT (game_id) DO UPDATE SET
    game_id = EXCLUDED.game_id,
    game_date = EXCLUDED.game_date,
    home_team_id = EXCLUDED.home_team_id,
    home_team_score = EXCLUDED.home_team_score,
    visitor_team_id = EXCLUDED.visitor_team_id,
    visitor_team_score = EXCLUDED.visitor_team_score,
    season = EXCLUDED.season,
    post_season = EXCLUDED.post_season,
    status = EXCLUDED.status
;
"""
print(upsert_query)
