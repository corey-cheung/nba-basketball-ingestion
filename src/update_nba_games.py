#!/usr/bin/env python

# - get max game_date where status is 'Final'
# - query the API with that date and the preceeding dates
# - format the data into insert values
# - write an upsert query to add the rows

import os
import psycopg2

def query_postgres(query, fetchall=False):
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

        cursor.execute(query)
        if fetchall:
            result = cursor.fetchall()
        else:
            result = cursor.fetchone()

        return result

    finally:
        if conn is None:
            conn.close()

query = """ SELECT * FROM nba_basketball.game limit 10;
"""

print(query_postgres(query, fetchall=True))
