#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sqlite3
import json
from pprint import pprint

from pymongo import MongoClient
from sqlite3 import Error

from pymongo.collection import Collection
from pymongo.database import Database

# Directory containing MongoDB query files
query_dir = './queries'


def connect_mongodb(connection, database, collection) -> (Database, Collection):
    try:
        mongo_client = MongoClient(connection)
        db = mongo_client[database]
        return db, db[collection]
    except Error as e:
        print(e)


# Function to execute MongoDB query and return results
def execute_mongodb_aggregate(c: Collection, pipeline):
    return c.aggregate(pipeline)


# Function to create SQLite3 database and table based on JSON data
def create_sqlite_database(data, name):
    try:
        conn = sqlite3.connect(name)
        cursor = conn.cursor()

        table_name = os.path.splitext(os.path.basename(name))[0].replace('.', '_')

        # Drop table if exists
        drop_table_query = f"DROP TABLE IF EXISTS '{table_name}';"
        cursor.execute(drop_table_query)
        conn.commit()
        # Create table based on first result JSON data keys
        create_table_query = f"CREATE TABLE IF NOT EXISTS '{table_name}' ("
        for key, value in data[0].items():
            if isinstance(value, int):
                create_table_query += f"{key} INTEGER,"
            elif isinstance(value, float):
                create_table_query += f"{key} REAL,"
            else:
                create_table_query += f"{key} TEXT,"
        create_table_query = create_table_query[:-1] + ");"
        print(create_table_query)
        cursor.execute(create_table_query)
        conn.commit()
        # Insert data into table
        for row in data:
            keys_str = ",".join(row.keys())
            question_marks = ("?," * len(row.keys()))[:-1]
            insert_query = f"INSERT INTO {table_name} ({keys_str}) VALUES ({question_marks});"
            print(insert_query)
            print(row.values())
            cursor.execute(insert_query, tuple(row.values()))
        conn.commit()
        conn.close()
    except Error as e:
        print(e)


# Iterate through MongoDB query files in the directory
for filename in os.listdir(query_dir):
    query_file = os.path.join(query_dir, filename)

    # Read the MongoDB query from the file
    with open(query_file, 'r') as file:
        query = json.load(file)

    # Get connection type and database name from the query
    connection_type = query['connection_type']

    result = []

    if connection_type == 'mongodb':
        session, collection = connect_mongodb(query['connection'], collection=query['collection'], database=query['database'])
        # Execute the MongoDB query
        if 'aggregate' in query:
            cursor = execute_mongodb_aggregate(collection, query['aggregate'])
            for document in cursor:
                result.append(document)
        else:
            print("Query type for MongoDB not supported yet.")
    else:
        print("Connection type not supported yet.")

    pprint(result)
    # Create a SQLite3 database and save the result
    if result:
        db_name = f"./db/{os.path.splitext(filename)[0]}.sqlite3"
        create_sqlite_database(result, db_name)

print("Execution completed. SQLite3 databases created.")
