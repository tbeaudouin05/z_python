# Functions to access and retrieve data from Redcat

import os
import psycopg2
import datetime

def get_query(query_name):
    """Retrieves the query belonging to query_name and returns it as a string."""
    data_file = open("data_queries/{}.sql".format(query_name), "r")
    query = data_file.read()
    data_file.close()
    return query

def run_query(host, user, pwd, db, port, query):
    """Executes a query on a database and returns the data and columns variable,
    containing the table data and column names respectively."""
    conn = psycopg2.connect(host=host, user = user, password = pwd,
        database = db, port = port)
    conn.set_session(autocommit = True)
    cursor = conn.cursor()
    cursor.execute(query)
    columns = cursor.description
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data, columns

def create_file(col_names, table):
    """Creates a csv file for table and writes the column names into it."""
    filename = "data_files/{}.csv".format(table)

    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))

    data_file = open(filename, "w")
    data_file.write("|".join(str(col[0]) for col in col_names) + "\n")
    data_file.close()

def append_to_file(data, table):
    """Appends data to the table csv file."""
    filename = "data_files/{}.csv".format(table)

    data_file = open(filename, "a")
    for row in data:
        row_list = []
        for col in row:
            if type(col) is datetime.date:
                row_list.append(col.strftime("%Y-%m-%d"))
            else:
                row_list.append(str(col))
        data_file.write("|".join(str(col) for col in row_list) + "\n")
