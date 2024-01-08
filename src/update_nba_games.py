#!/usr/bin/env python
"""
Query the nba_basketball.game table to find a the max game date with a 'Final' status.
Apply look back and query the game data from the API between this start date
and the current date. With the new data, upsert the nba_basketball.games postgres table.
"""

from datetime import datetime, timedelta

import requests

from get_nba_games import format_games_data
from nba_pg_ingestion_utils import (
    generate_db_objects,
    get_row_to_insert,
    query_postgres,
)


def get_start_and_end_dates(look_back) -> tuple[str, str]:
    """
    Return the start and end dates for the API query parameters.

    Parameters:
        look_back: A look back window in days of data to query, for a factor of safety
            or when a bigger backfill is needed.
    """
    print(f"look_back days: {look_back}")
    query = """ SELECT MAX(game_date) FROM nba_basketball.game WHERE status = 'Final';
    """
    max_game_date = query_postgres(query)[0]
    start_date = (max_game_date - timedelta(days=look_back)).strftime("%Y-%m-%d")
    end_date = datetime.now().date().strftime("%Y-%m-%d")

    return start_date, end_date


def get_games_to_update(url: str, start_date: str=None, end_date: str=None) -> str:
    """
    Query the games endpoint and format the data into a string for an insert query.

    Parameters:
        url: The url of the games endpoint.
        start_date: The starting date for the games to be queried.
        end_date: The final date for the games to queried.
    """
    current_page = 1
    total_pages = 1
    to_upsert = []
    while current_page <= total_pages and total_pages != 0:
        params = {
            "per_page": 100,
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
                row_to_insert = "(" + get_row_to_insert(formatted) + ")"
                # to_upsert += f"\n{row_to_insert},"
                to_upsert.append(row_to_insert)

            current_page = meta["current_page"] + 1
            total_pages = meta["total_pages"]
        else:
            print(f"API Response Error: {response.status_code}")
            print(response.reason)
            break
    return set(to_upsert)


def main(look_back: int) -> None:
    """
    Find the max game date to update and apply a look back. Query the API and upsert the
    new data.

    Parameters:
        look_back: A look back window in days of data to query, for a factor of safety
            or when a bigger backfill is needed.
    """
    start_date, end_date = get_start_and_end_dates(look_back=look_back)
    print(f"Updating the games table with games between {start_date} and {end_date}.")

    to_upsert = get_games_to_update(
        url="https://www.balldontlie.io/api/v1/games",
        start_date=start_date,
        end_date=end_date,
    )

    to_upsert_string = ""
    for row in to_upsert:
        to_upsert_string += f"\n{row},"

    upsert_query = f"""
    INSERT INTO nba_basketball.game
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
    VALUES
    {to_upsert_string[:-1]}
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
    # print(upsert_query)
    generate_db_objects(upsert_query)


if __name__ == "__main__":
    main(look_back=2)
