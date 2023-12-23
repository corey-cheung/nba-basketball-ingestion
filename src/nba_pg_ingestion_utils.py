import os
import psycopg2

def get_row_to_insert(data: dict[str, str | int]) -> str:
    """
    Break down the json results from API and return a string
    to be used in the SQL insert statement.

    Parameters:
        data: A dictionary that represents one row of the table we want to insert into.
    """
    values = [
        str(i) if str(i).isnumeric() else "'" + str(i) + "'" for i in data.values()
    ]  # non-integers will need a literal "'" in the insert DML
    row = "(" + ", ".join(values) + ")"
    return row

def generate_db_objects(query: str) -> None:
    """
    Create postgres database objects from the provided DDL and DML.

    Parameters:
        query: The query to be executed against postgres.
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

        cursor.execute(query)
        conn.commit()

    finally:
        if conn is None:
            conn.close()
