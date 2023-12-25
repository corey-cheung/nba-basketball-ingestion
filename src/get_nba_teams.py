#!/usr/bin/env python
"""
Query the "ball don't lie" nba teams end point
and ingest the data into a postgres table.
API docs: https://www.balldontlie.io/home.html#teams
"""
import os

import requests

from nba_pg_ingestion_utils import generate_db_objects, get_row_to_insert


def query_teams_endpoint(url: str) -> list[dict[str, str | int]] | None:
    """
    Query the teams API and return the data.
    When the response status code isn't 200 return the error message.

    Parameters:
        url: The url of the teams endpoint.
    """
    response = requests.get(url, timeout=10)
    if response.status_code == 200:
        data = response.json()["data"]
        return data
    print(f"Error: {response.status_code}")
    print(response.reason)
    return None


def main(url: str) -> None:
    """
    Query the teamms endpoint, parse the result into DDL and DML,
    then populate the teams table in postgres.

    Parameters:
        url: The url of the teams endpoint.
    """

    # get create schema, create table and insert into table queries
    with open(
        os.path.join(os.getcwd(), "src/sql/create_schema_and_team_table.sql"),
        encoding="UTF-8",
    ) as query:
        query = query.read()

    teams = query_teams_endpoint(url)
    insert = """INSERT INTO nba_basketball.team
(team_id, team_name_abbreviation, city, conference, division, team_full_name, team_name)
VALUES"""

    # append rows to the insert query, last row needs a ";"
    for team in teams[:-1]:
        insert += f"\n({get_row_to_insert(team)}),"
    insert += f"\n({get_row_to_insert(teams[-1])});"

    query += f"\n{insert}"

    generate_db_objects(query)

    print("created the teams schema and table")


if __name__ == "__main__":
    main(url="https://www.balldontlie.io/api/v1/teams?per_page=100")
