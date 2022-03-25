from http import server
import imp
from lib2to3.pgen2 import driver
from sqlalchemy import create_engine
import pandas as pd
import os
import psycopg2
import pydoc


# variables

pwd = os.environ['pg_pwd']
user = os.environ['pg_user']
database = os.environ['']
db_server = os.environ['host_server']
sql_server_drive = "{ODBC Driver 17 for SQL Server}"


print('xxx', pwd)


def data_extract():
    try:
        src_connection = pydoc.connection('DRIVER =' + sql_server_drive + ';SERVER=' +
                                          db_server + '\SQLEXPRESS' + ';DATABASE=' + database + ';UID=' + user + ';PWD' + pwd)
        print(0)
    except:
        print(0)
        pass
