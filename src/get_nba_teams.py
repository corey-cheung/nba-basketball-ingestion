#!/usr/bin/env python
"""Create the nba_basketball schema and teams table."""

import os
import requests
import psycopg2

print("ball don't lie")


def get_row_to_insert(team: dict) -> str:
    """
    Return the row to be used in a SQL insert statement.

    Parameters:
        blah blah
    """
    values = [
        str(i) if str(i).isnumeric() else "'" + str(i) + "'" for i in team.values()
    ]  # non-integers will need a literal "'" in the insert DML
    row = "(" + ", ".join(values) + ")"
    return row


def query_teams_endpoint(url: str) -> list[dict[str, str | int]]:
    """
    Query the teams API and return the data.
    When the response status code isn't 200 return the error message.

    Parameters:
        blah blah
    """
    response = requests.get(url, timeout=10)
    if response.status_code == 200:
        data = response.json()["data"]
        return data
    print(f"Error: {response.status_code}")
    print(response.reason)
    return None


def create_schema_and_table(create_schema, create_table, insert):
    """
    Create database objects

    Parameters:
        blah blah
    """

    try:
        conn = psycopg2.connect(
            dbname="dev",
            user=os.environ.get("PG_USER"),
            password=os.environ.get("PG_PASSWORD"),
            host="127.0.0.1",  # localhost
            port="5432",
        )
        cursor = conn.cursor()
        print("connected to postgres!")

        cursor.execute(create_schema)
        cursor.execute(create_table)
        cursor.execute(insert)
        conn.commit()

        print("created schema and table")
    finally:
        if conn is None:
            conn.close()


def main(url):
    """
    Main function.

    Parameters:
        blah blah
    """

    # get create schema, create table and insert into table queries
    with open(os.path.join(os.getcwd(), "src/sql/create_schema.sql")) as query:
        create_schema = query.read()

    with open(os.path.join(os.getcwd(), "src/sql/create_table_team.sql")) as query:
        create_table = query.read()

    teams = query_teams_endpoint(url)
    insert = """INSERT INTO nba_basketball.team
(team_id, team_name_abbreviation, city, conference, division, team_full_name, team_name)
VALUES"""

    # append rows to the insert query, last row needs a ";"
    for team in teams[:-1]:
        insert += f"\n{get_row_to_insert(team)},"
    insert += f"\n{get_row_to_insert(teams[-1])};"

    create_schema_and_table(create_schema, create_table, insert)


if __name__ == "__main__":
    main(url="https://www.balldontlie.io/api/v1/teams?page=1")

# teams = query_teams_endpoint(url="https://www.balldontlie.io/api/v1/teams?page=1")
# for team in teams:
#     print(get_row_to_insert(team))
