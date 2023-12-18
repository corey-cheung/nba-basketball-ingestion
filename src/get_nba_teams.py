#!/usr/bin/env python

import requests

print("ball don't lie")


def get_row_to_insert(team: dict) -> str:
    """
    Return the row to be used in a SQL insert statement.
    """

    row = "(" + ", ".join(str(i) for i in team.values()) + ")"
    return row


def query_teams_endpoint(url: str) -> list[dict[str, str | int]]:
    """
    Query the teams API and return the data.
    When the response status code isn't 200 return the error message.
    """
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()["data"]
        return data
    else:
        print(f"Error: {response.status_code}")
        print(response.reason)


url = "https://www.balldontlie.io/api/v1/teams?page=1"
teams = query_teams_endpoint(url)
for team in teams:
    print(get_row_to_insert(team))
