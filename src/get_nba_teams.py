#!/usr/bin/env python

import requests

print("ima do some requests")

url = "https://www.balldontlie.io/api/v1/teams"

response = requests.get(url)
print(response.status_code)
data = response.json()["data"]
print(data)
