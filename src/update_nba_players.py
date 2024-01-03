#!/usr/bin/env python
"""
Get nba players:

- query players from the API
- create insert DML
- create pg objects
"""

import os

import requests

from nba_pg_ingestion_utils import (
    generate_db_objects,
    get_row_to_insert,
    handle_apostrophes,
    handle_nulls,
)


@handle_nulls
@handle_apostrophes
def format_player_data(
    player: dict[str, str | int | dict[str, str | int]]
) -> dict[str, str | int]:
    """
    Format each row of player data retrieved from the API.

    Parameters:
        player: A dictionary representing data about an NBA player.
    """
    formatted = {}
    formatted["player_id"] = player["id"]
    formatted["position"] = player["position"]
    formatted["team_id"] = player["team"]["id"]
    formatted["first_name"] = player["first_name"]
    formatted["last_name"] = player["last_name"]
    formatted["height_feet"] = player["height_feet"]
    formatted["height_inches"] = player["height_inches"]
    formatted["weight_pounds"] = player["weight_pounds"]

    return formatted


def get_players_to_update(
    url: str, page: int, insert_statement: str = ""
) -> str | None:
    """
    Query the players end point recursively through each page. Format the data and
    append each row to a string.

    Parameters:
        url: The url of the player endpoint.
        page: The page number of the paginated API response.
        insert_statement: The string to append each formatted row. This will be used in
            a SQL query later on.
    """
    params = {"per_page": 100, "page": page}
    response = requests.get(url, params=params, timeout=10)

    if response.status_code == 200:
        data = response.json()["data"]
        meta = response.json()["meta"]

        print(f"getting data from page {meta['current_page']} of {meta['total_pages']}")
        for player in data:
            formatted = format_player_data(player)
            to_insert = get_row_to_insert(formatted)
            insert_statement += f"\n({to_insert}),"

        # Recursively call the function until we get to the last page
        if (
            meta["current_page"] == meta["total_pages"] or meta["total_pages"] == 0
        ):  # base case
            return insert_statement

        # The base case has an explict return that handles the termination of the
        # recursion, but discards it up the function chain. We need a return here to
        # pass the result up.
        return get_players_to_update(
            url="https://www.balldontlie.io/api/v1/players",
            insert_statement=insert_statement,
            page=page + 1,
        )

    print(f"API Response Error: {response.status_code}")
    print(response.reason)
    return None


def main() -> None:
    """
    Query the players endpoint, format the data and append to a DDL query. Execute the
    query against postgres and upsert the player data.
    """
    rows = get_players_to_update(
        url="https://www.balldontlie.io/api/v1/players", insert_statement="", page=1
    )
    with open(
        os.path.join(os.getcwd(), "src/sql/create_player_table.sql"), encoding="UTF-8"
    ) as query:
        query = query.read()
    upsert_query = f"""
    INSERT INTO nba_basketball.player
    (
        player_id,
        position,
        team_id,
        first_name,
        last_name,
        height_feet,
        height_inches,
        weight_pounds
        )
    VALUES
    {rows[:-1]}
    ON CONFLICT (player_id) DO UPDATE SET
        player_id = EXCLUDED.player_id,
        position = EXCLUDED.position,
        team_id = EXCLUDED.team_id,
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        height_feet = EXCLUDED.height_feet,
        height_inches = EXCLUDED.height_inches,
        weight_pounds = EXCLUDED.weight_pounds
    ;
    """
    query += upsert_query
    generate_db_objects(query)


if __name__ == "__main__":
    main()
