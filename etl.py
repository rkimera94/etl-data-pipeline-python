from fileinput import close
from genericpath import exists
from http import server
from lib2to3.pgen2 import driver
from operator import index, le
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
                          and table_schema not in ('pg_catalog', 'information_schema')
                          and  table_name  not like 'stg%';""")

        src_table = src_cursor.fetchall()

        for t in src_table:

            table = t[0]

            df = pd.read_sql(f"select * from {table}", source_connection)
            # get the table name

            table_name = table.split('.')[1]

            load_data(df, table_name)
    except Exception as e:
        print('connection failed' + str(e))

    finally:
        source_connection.close()


def load_data(df, table_name):
    try:
        number_rows = 5
        engine = create_engine(
            f'postgresql://{db_user}:{db_pass}@{db_host}:5432/{db_name}')
        add_row = number_rows + len(df)

        print(
            f'importing {number_rows} to {add_row}..... for table {table_name}')

        df.to_sql(f'stg_{table_name}', engine,
                  if_exists='replace', index=False)
        number_rows += len(df)

        print('Data loaded successfully')

    except Exception as e:
        print('Database connection to insert data Failed', str(e))
    finally:
        print('Dataspace')


try:
    data_extract()
except Exception as e:
    print('Data migratio Failed', str(e))
