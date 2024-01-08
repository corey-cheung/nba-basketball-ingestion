#!/usr/bin/env python
"""
get game stats
"""

import os
import requests
from tenacity import retry, stop_after_attempt, wait_fixed
from nba_pg_ingestion_utils import handle_nulls, write_to_csv, generate_db_objects

print("getting dat game stat quick fast fr")

@handle_nulls
def format_box_score(data):
    formatted = {}
    formatted["box_score_id"] = data["id"]

    if data["game"] == None:
        formatted["game_id"] = None
    else:
        formatted["game_id"] = data["game"]["id"]

    if data["player"] == None:
        formatted["player_id"] = None
    else:
        formatted["player_id"] = data["player"]["id"]

    if data["team"] == None:
        formatted["team_id"] = None
    else:
        formatted["team_id"] = data["team"]["id"]

    formatted["pts"] = data["pts"]
    formatted["reb"] = data["reb"]
    formatted["ast"] = data["ast"]
    formatted["blk"] = data["blk"]
    formatted["stl"] = data["stl"]
    formatted["turnover"] = data["turnover"]
    formatted["oreb"] = data["oreb"]
    formatted["dreb"] = data["dreb"]
    formatted["fg3_pct"] = data["fg3_pct"]
    formatted["fg3a"] = data["fg3a"]
    formatted["fg3m"] = data["fg3m"]
    formatted["fg_pct"] = data["fg_pct"]
    formatted["fga"] = data["fga"]
    formatted["fgm"] = data["fgm"]
    formatted["ft_pct"] = data["ft_pct"]
    formatted["fta"] = data["fta"]
    formatted["ftm"] = data["ftm"]
    formatted["min"] = data["min"]
    formatted["pf"] = data["pf"]

    return formatted


@retry(stop=stop_after_attempt(10), wait=wait_fixed(60)) # 60 seconds
def get_game_stats(per_page, page, game_ids=None, seasons=None, truncate=False):
    url = "https://www.balldontlie.io/api/v1/stats"
    params = {
        "per_page": per_page,
        "page": page,
        "game_ids[]": game_ids,
        "seasons[]": seasons,
    }
    response = requests.get(url=url, params=params, timeout=120)
    if response.status_code == 200:
        data = response.json()["data"]
        meta = response.json()["meta"]
        print(f"season {seasons}: got data from page {meta['current_page']} of {meta['total_pages']}")

        data = [format_box_score(i) for i in data]
        csv_path = os.path.join(os.getcwd(), "src/data_backfill/nba_box_scores.csv")
        write_to_csv(path=csv_path, data=data, truncate=truncate)

        # call the function recursively
        if meta["current_page"]  == meta["total_pages"] or meta["total_pages"] == 0:
            return

        get_game_stats(
            per_page=per_page,
            page=page+1, # loop to the next page
            game_ids=game_ids,
            seasons=seasons,
            truncate=False # Never truncate when looping to the next page
            )
    elif response.status_code in (503,429):
        # Retry on 503 errors after waiting for 60 seconds
        print(f"{response.status_code}: retrying")
        raise Exception(f"{response.status_code} error - Retry after waiting for 60 seconds")
    else:
        print(f"API Response Error: {response.status_code}")
        print(response.reason)
        return None

    # current_page = page # over write the global
    # total_pages = page + 1

    # while current_page <= total_pages and total_pages != 0:

    #     url = "https://www.balldontlie.io/api/v1/stats"
    #     params = {
    #         "per_page": per_page,
    #         "page": current_page,
    #         "game_ids[]": game_ids,
    #         "seasons[]": seasons,
    #     }
    #     response = requests.get(url=url, params=params, timeout=120)

    #     if response.status_code == 200:
    #         data = response.json()["data"]
    #         meta = response.json()["meta"]
    #         print(f"season {seasons}: got data from page {meta['current_page']} of {meta['total_pages']}")

    #         print('attempting to format')
    #         data = [format_box_score(i) for i in data]
    #         print('formatted data')
    #         csv_path = os.path.join(os.getcwd(), "src/data_backfill/nba_box_scores.csv")
    #         write_to_csv(path=csv_path, data=data, truncate=truncate)
    #         print('wrote to csv')

    #         # update the pages
    #         current_page = meta["current_page"] + 1
    #         total_pages = meta["total_pages"]

    #     elif response.status_code in (503,429):
    #         # Retry on 503 errors after waiting for 60 seconds
    #         print(f"{response.status_code}: retrying")
    #         raise Exception(f"{response.status_code} error - Retry after waiting for 60 seconds")
    #     else:
    #         print(f"API Response Error: {response.status_code}")
    #         print(response.reason)
    #         break

def main():

    batch1 = list(range(2020,2010, -1))
    # batch2 = list(range(2010,2000, -1))
    # batch3 = list(range(2000,1990, -1))
    # batch4 = list(range(1990,1980, -1))
    # batch5 = list(range(1980,1970, -1))
    # batch6 = list(range(1970,1960, -1))
    # batch7 = list(range(1960,1939, -1))

    get_game_stats(
        per_page=100,
        page=200,
        seasons=[2022],
        # game_ids=["27404"],
        truncate=False,
        )

    # # Create table
    # with open(
    #     os.path.join(os.getcwd(), "src/sql/create_box_score_table.sql"),
    #     encoding="UTF-8",
    # ) as query:
    #     query = query.read()
    # # Copy CSV into table
    # csv_path = os.path.join(os.getcwd(), "src/data_backfill/nba_box_scores.csv")
    # query += "\nCOPY nba_basketball.box_score FROM "
    # query += f"'{csv_path}' DELIMITER ',' QUOTE '''' NULL 'NULL' csv;"
    # generate_db_objects(query)


if __name__ == "__main__":
    main()
