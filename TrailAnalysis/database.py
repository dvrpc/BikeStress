"""
This module exists to be imported anywhere a PostgreSQL connection is needed. 

TODO: Anywhere the first code block exists, replace it with the second block

    REPLACE THIS:
        con = psql.connect(dbname = "BikeStress_p3", host = "localhost", port = 5432, user = "postgres", password = "sergt")
        cur = con.cursor()

    WITH THIS:
        from database import connection
        cur = connection.cursor()

Once this change is complete, you can swap in a new analysis DB by altering
one line of code, where DB_NAME is defined.

"""

import psycopg2

DB_NAME = "BikeStress_p3"

db_connection_info = {
    "database": DB_NAME,
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "sergt"
}

connection = psycopg2.connect(**db_connection_info)