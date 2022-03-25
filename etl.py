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
        src_connection = psycopg2.connect(user=db_user,
                                          password=db_pass,
                                          host=db_host,
                                          dbname=db_name)

        print(src_connection)
    except:
        print('connection failed')
        pass


data_extract()
