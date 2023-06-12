#!/usr/bin/env python3

import sqlite3
import os
import pathlib
import warnings

from .array_converter import sentinel

def schema_init(connection: sqlite3.Connection):
    """
    Initialize the database schema on the given connection.
    Automatically commits these changes.
    """

    cursor = connection.cursor()
    cursor.execute("PRAGMA busy_timeout = 30000")

    cursor.execute("CREATE TABLE IF NOT EXISTS data_values(is_inline INT, av array, load_promise TEXT, relapath TEXT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS configurations(ensemble INT, ensemble_relapath TEXT, load_promise TEXT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS ensembles(name TEXT, abspath TEXT UNIQUE, description TEXT)")

    cursor.execute("CREATE TABLE IF NOT EXISTS collections(name TEXT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS collections_contains(collection INT, configuration INT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS measurements(configuration INT, value INT, name TEXT)")

    cursor.execute("CREATE TABLE IF NOT EXISTS means(collection INT, name TEXT, value INT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS jackknifes(collection INT, configuration INT, name TEXT, value INT)")

    cursor.execute("CREATE TABLE IF NOT EXISTS db_meta(abspath TEXT)")

    connection.commit()


    # Find the abspath that will be used for external storage
    cursor = connection.cursor()
    c = cursor.execute("PRAGMA database_list")
    abspath = None
    dbfilename = None
    for dbid, name, fname in c.fetchall():
        if(name != "main"):
            continue
        abspath = fname

    if(abspath is None):
        raise ValueError("database >>main<< not found in connection. See the code above the error.")

    if(abspath == ""):
        abspath = os.getcwd()
        dbfilename = "_memory_db_"
        warnings.warn("warning: this is probably a :memory: database. Using cwd as database abspath.")

    if(dbfilename is None):
        dbfilename = os.path.basename(abspath)
    abspath = os.path.dirname(abspath)

    true_abspath = os.path.join(abspath, (dbfilename + ".filestorage"))

    if(os.path.isdir(true_abspath)):
       warnings.warn("path already exists. Is schema_init a mistake?")
    else:
        try:
            os.makedirs(true_abspath)
        except:
            raise IOError(f"failed to create database file storage path: {true_abspath}")

    cursor.execute("INSERT INTO db_meta VALUES(?)", (true_abspath,))

    connection.commit()



