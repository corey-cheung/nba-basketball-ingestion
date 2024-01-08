#!/usr/bin/env python
"""
Backfill the NBA games table with a bulk upload of games. Query the ball don't lie NBA
games endpoint. Format the data and write to a CSV, then
copy the result into a postgres table.
API docs: https://www.balldontlie.io/home.html#games
"""
import os

import requests

from nba_pg_ingestion_utils import generate_db_objects, write_to_csv


def format_games_data(
    game: dict[str, str | int | dict[str, str | int]]
) -> dict[str, str | int]:
    """
    Format each row of game data retrieved from the API.

    Parameters:
        game: A dictionary representing data about one NBA game.
    """
    formatted = {}
    formatted["game_id"] = game["id"]
    formatted["game_date"] = game["date"][:10]
    formatted["home_team_id"] = game["home_team"]["id"]
    formatted["home_team_score"] = game["home_team_score"]
    formatted["visitor_team_id"] = game["visitor_team"]["id"]
    formatted["visitor_team_score"] = game["visitor_team_score"]
    formatted["season"] = game["season"]
    formatted["post_season"] = game["postseason"]
    formatted["status"] = game["status"]

    return formatted


# pylint: disable=R0913, R1710
def get_games_data(
    url: str,
    per_page: int,
    page: int,
    dates: list[str] = None,
    seasons: list[str] = None,
    start_date: str = None,
    end_date: str = None,
    truncate: bool = False,
) -> None:
    """
    Query the data from the API recursively through each page. Format the data and write
    it to a csv file.

    Parameters:
        url: The url of the games endpoint.
        per_page: The number of items to return in each page of the API response.
        page: The page number of the paginated API response.
    """
    params = {
        "per_page": per_page,
        "page": page,
        "dates[]": dates,
        "seasons[]": seasons,
        "start_date": start_date,
        "end_date": end_date,
    }
    # Query the API
    response = requests.get(url, params=params, timeout=10)

    if response.status_code == 200:
        data = response.json()["data"]
        meta = response.json()["meta"]

        # format the data and write to a csv file
        print(f"getting data from page {meta['current_page']} of {meta['total_pages']}")
        data = [format_games_data(i) for i in data]
        csv_path = os.path.join(os.getcwd(), "src/data_backfill/nba_games.csv")
        write_to_csv(path=csv_path, data=data, truncate=truncate)

        # Recursively call the function until we get to the last page
        if (
            meta["total_pages"] == meta["current_page"] or meta["total_pages"] == 0 or meta["current_page"] % 200 == 0
        ):  # base case
            return None

        get_games_data(
            url=url,
            page=page + 1,  # loop to next page
            per_page=per_page,
            dates=dates,
            seasons=seasons,
            start_date=start_date,
            end_date=end_date,
            truncate=False,  # Never truncate when looping to the next page
        )

    else:
        print(f"API Response Error: {response.status_code}")
        print(response.reason)
        return None


def main() -> None:
    """
    Query the games end point, format and write to a CSV, then copy into a postgres
    table. Batches of 200 pages are used to avoid API rate limiting and python max
    recursion depth errors.
    """
    # Query API, format, and write to CSV
    get_games_data(
        url="https://www.balldontlie.io/api/v1/games",
        page=1,
        per_page=100,  # max 100
        truncate=False,
    )
    print('done first 200')
    get_games_data(
        url="https://www.balldontlie.io/api/v1/games",
        page=201,
        per_page=100,  # max 100
        truncate=False,
    )
    print('done first 400')
    get_games_data(
        url="https://www.balldontlie.io/api/v1/games",
        page=401,
        per_page=100,  # max 100
        truncate=False,
    )
    print('done first 600')
    get_games_data(
        url="https://www.balldontlie.io/api/v1/games",
        page=601,
        per_page=100,  # max 100
        truncate=False,
    )
    # Create table
    with open(
        os.path.join(os.getcwd(), "src/sql/create_game_table.sql"),
        encoding="UTF-8",
    ) as query:
        query = query.read()
    # Copy CSV into table
    csv_path = os.path.join(os.getcwd(), "src/data_backfill/nba_games.csv")
    query += "\nCOPY nba_basketball.game FROM "
    query += f"'{csv_path}' DELIMITER ',' QUOTE '''' csv;"
    generate_db_objects(query)



if __name__ == "__main__":
    main()
