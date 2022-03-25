from http import server
from lib2to3.pgen2 import driver
from sqlalchemy import create_engine
import pandas as pd
import os
import psycopg2
import pyodbc

from dotenv import load_dotenv
load_dotenv()

db_pass = os.getenv("db_pass")
db_user = os.getenv("db_user")
db_name = os.getenv("db_name")
db_host = os.getenv("db_host")


# variables


def data_extract():
    try:
        source_connection = psycopg2.connect(user=db_user,
                                             password=db_pass,
                                             host=db_host,
                                             dbname=db_name)
        src_cursor = source_connection.cursor()

        src_cursor.execute("""SELECT table_schema ||'.'|| table_name
                          FROM information_schema.tables
                          WHERE table_type='BASE TABLE'
                          and table_schema not in ('pg_catalog', 'information_schema')""")

        src_table = src_cursor.fetchall()

        for t in src_table:

            table = t[0]

            df = pd.read_sql(f"select * from {table}", source_connection)

       # load(df,source_connection)
    except Exception as e:
        print('connection failed' + str(e))

    finally:
        source_connection.close()


data_extract()
