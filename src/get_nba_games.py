#!/usr/bin/env python

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


# for i in data:
#     print(i)
#     print("\n")

"""
CREATE TABLE nba_basketball.game (
    game_id INTEGER PRIMARY KEY,
    game_date DATE,
    home_team_id INTEGER, -- FKs to team
    home_team_score INTEGER,
    visitor_team_id INTEGER, -- FKs to team
    visitor_team_score INTEGER,
    season INTEGER,
    post_season BOOLEAN,
    status TEXT
)
"""

def get_teams_data(url, per_page, page):
    params = params = {'per_page':per_page, 'page':page, 'dates[]':['2023-12-20','2023-12-21']}

    response = requests.get(url, params=params, timeout=10)

    if response.status_code == 200:
        data = response.json()["data"]
        meta = response.json()["meta"]

    # print(meta['current_page'])
    for i in data:
        print(get_row_to_insert(i))
        print('\n')
    if meta['total_pages'] == meta['current_page']:
        return None
    else:
        get_teams_data(url=url, page=page + 1, per_page=2)

# get_teams_data(url="https://www.balldontlie.io/api/v1/games",page=1, per_page=2)

# print(data[0])

test_game = data[0]
def format_games_data(game: dict) -> dict:
    """
    Format each row of game data retrieved from the API.
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
# print(test_game)

print(format_games_data(data[0]))

import csv


csv_path = os.path.join(os.getcwd(), "src/data_backfill/nba_games.csv")
def write_to_csv(path: str, data: list[dict[str, str | int]], truncate: bool):
    """
    Format data received from the ball don't lie API and write it to a CSV.

    Parameters:
        path: The path of the csv file.
        data: A list of dictionaries, each dictionary is a row to insert into the csv.
        truncate: Should the csv file be truncated before inserting.
    """
    # Check if CSV exists
    csv_exists = os.path.isfile(path)
    if truncate and csv_exists:
        os.remove(path)
    with open(path, 'a', encoding="UTF-8") as games_csv:
        # csv_writer = csv.writer(games_csv)
        for row in data:
            formatted = format_games_data(row)
            formatted = get_row_to_insert(formatted)
            print(formatted)
            games_csv.write(f"{formatted}\n")

write_to_csv(path=csv_path, data=data, truncate=False)
