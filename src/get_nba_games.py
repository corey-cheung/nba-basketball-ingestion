#!/usr/bin/env python

import csv
import requests
from nba_pg_ingestion_utils import *

url = "https://www.balldontlie.io/api/v1/games?dates[]=1993-04-30"
url = "https://www.balldontlie.io/api/v1/games?dates[]=2023-12-20&per_page=2&page=5"
# url = "https://www.balldontlie.io/api/v1/games?seasons[]=2021" # all data
# url= "https://www.balldontlie.io/api/v1/teams?page=1"
params = {'per_page':'2', 'page':'5', 'dates[]':['2023-12-20','2023-12-21']}


response = requests.get(url, params=params, timeout=10)

if response.status_code == 200:
    data = response.json()["data"]
    meta = response.json()["meta"]

print(meta)
print(data)


def format_games_data(game: dict[str, str | int | dict[str, str | int]]) -> dict:
    """
    Format each row of game data retrieved from the API.

    Parameters:
        game: A dictionary representing data about one NBA game.
    """
    formatted = {}
    formatted['game_id'] = game['id']
    formatted['game_date'] = game['date'][:10]
    formatted['home_team_id'] = game['home_team']['id']
    formatted['home_team_score'] = game['home_team_score']
    formatted['visitor_team_id'] = game['visitor_team']['id']
    formatted['visitor_team_score'] = game['visitor_team_score']
    formatted['season'] = game['season']
    formatted['post_season'] = game['postseason']
    formatted['status'] = game['status']

    return formatted


def get_teams_data(url, per_page, page):
    params = params = {'per_page':per_page, 'page':page, 'dates[]':['2023-12-20','2023-12-21']}

    response = requests.get(url, params=params, timeout=10)

    if response.status_code == 200:
        data = response.json()["data"]
        meta = response.json()["meta"]

        # Main logic
        data = [format_games_data(i) for i in data]
        csv_path = os.path.join(os.getcwd(), "src/data_backfill/nba_games.csv")
        write_to_csv(path=csv_path, data=data, truncate=False)

        # Recursively call the function until we get to the last page
        if meta['total_pages'] == meta['current_page']: # base case
            return None
        else:
            get_teams_data(url=url, page=page + 1, per_page=per_page)
    else:
        print(f"Error: {response.status_code}")
        print(response.reason)
        return None

get_teams_data(url="https://www.balldontlie.io/api/v1/games",page=1, per_page=2)
