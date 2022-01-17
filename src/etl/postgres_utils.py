"""
Useful functions for postgres database
"""
import pandas as pd
import os
import yaml
import numpy as np
from pathlib import Path
from io import StringIO

import psycopg2
from psycopg2.extensions import register_adapter, AsIs

psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


def nan_to_null(f,
                _NULL=psycopg2.extensions.AsIs('NULL'),
                _Float=psycopg2.extensions.Float):
    if not np.isnan(f):
        return _Float(f)
    return _NULL


psycopg2.extensions.register_adapter(float, nan_to_null)

# SQL_DIR = Path(r"/home/paola/Documents/CHEF/sql/")
SQL_DIR = Path(__file__).parent.parent.parent / 'sql'


def get_credentials():
    """ get connection details
    Returns:
        cfg: diccionario con host, usuario, constraseña, puerto y nombre de
        la base de datos
    """
    usr_dir = os.path.join(str(Path.home()), ".chef")
    with open(os.path.join(usr_dir, "config.yml")) as f:
        config_file = yaml.safe_load(f)
    cfg = config_file['postgres']
    return cfg


def get_connection():
    """ create new engine
    Returns:
        connection: engine
    """
    cfg = get_credentials()
    try:
        connection = psycopg2.connect(user=cfg["user"],
                                      password=cfg["password"],
                                      host=cfg["host"],
                                      port=cfg["port"],
                                      database=cfg["db_name"])
        return connection
    except (Exception, psycopg2.Error) as error:
        print("Error while creating connection", error)


def get_select(query):
    """ Obtiene dataframe de resultado de un select
    Returns:
        df: dataframe con resultado del select
    """
    try:
        connection = get_connection()
        df = pd.read_sql_query(query, con=connection)
        connection.close()
        return df
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)


def get_select_raw(query):
    """ Obtiene dataframe de resultado de un select
    Returns:
        df: dataframe con resultado del select
    """
    try:
        usr_dir = os.path.join(str(Path.home()), ".chef")
        with open(os.path.join(usr_dir, "config.yml")) as f:
            config_file = yaml.safe_load(f)
        cfg = config_file['postgres_raw']

        connection = psycopg2.connect(user=cfg["user"],
                                      password=cfg["password"],
                                      host=cfg["host"],
                                      port=cfg["port"],
                                      database=cfg["db_name"])
        df = pd.read_sql_query(query, con=connection)
        connection.close()
        return df
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)


def execute_query(query):
    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        connection.close()
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)


def show_select(query):
    """ Imprime resultados de SELECT """
    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute(query)

        print("Selecting rows from table using cursor.fetchall")
        records = cursor.fetchall()

        print("Print each row and it's columns values")
        for row in records:
            print(row)

        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)


def get_records(query):
    """ Imprime resultados de SELECT """
    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute(query)

        records = cursor.fetchall()

        cursor.close()
        connection.close()
        return records

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)


def save_select(query, csv_name):
    """ Imprime resultados de SELECT """
    try:
        connection = get_connection()
        cursor = connection.cursor()

        cursor.execute(query)

        print("Selecting rows from table using cursor.fetchall")
        records = cursor.fetchall()

        df = pd.DataFrame(records)
        df.to_csv(csv_name)

        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)


def execute_sql(file_dir):
    """ Sirve para ejecutar archivos .sql """
    try:
        connection = get_connection()
        cursor = connection.cursor()

        print(file_dir)
        cursor.execute(open(file_dir, "r", encoding="utf-8").read())
        connection.commit()
        cursor.close()
        connection.close()
    except (Exception, psycopg2.Error) as error:
        print("Error while executing sql file", error)


def insert_df(df, table_name):
    try:
        connection = get_connection()
        cursor = connection.cursor()

        num_cols = len(df.columns)
        query = """insert into {table_name}
        values (%s {new_cols}) """.format(table_name=table_name,
                                          new_cols=', %s' * (num_cols - 1))

        for i in range(len(df)):
            values = tuple(df.iloc[i].values)
            cursor.execute(query, values)

        connection.commit()
        cursor.close()
        connection.close()
    except Exception as error:
        print(error)


def insert_query(query, record_to_insert):
    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute(query, record_to_insert)
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as error:
        print(error)


def copy_df(df, table_name):
    try:
        connection = get_connection()
        cursor = connection.cursor()

        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep='~')
        buffer.seek(0)

        cursor.copy_from(buffer, table_name, null='None', sep='~')
        connection.commit()
        cursor.close()
    except Exception as error:
        print(error)


def get_generic_query(table_name):
    filename = SQL_DIR / "04-features" / "generic" / "{table_name}.sql".format(table_name=table_name)

    with open(filename, 'r', encoding="utf-8") as file:
        sql_query = file.read()

    return sql_query
