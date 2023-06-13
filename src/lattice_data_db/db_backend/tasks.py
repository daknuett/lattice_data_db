#!/usr/bin/env python3
"""
Find and define computation tasks on the database.
"""

import sqlite3 
from .db_objecthandles import Collection, Configuration

def find_configurations_for_collection(connection: sqlite3.Connection, collection: str):
    """
    Find the configurations that are in the given collection. 
    The collection is given by name, a list of Configuration objects are returned.
    """
    coll = Collection.findby_name(connection, collection)
    if(isinstance(coll, list)):
        # name clash. 
        raise ValueError(f"name clash found: several collections with name {collection}")

    return [Configuration.load(connection, cid) for cid in coll._configurations]

def find_missing_configurations_for_collection(connection: sqlite3.Connection, collection: str, measurement_name: str):
    """
    Finds all the configurations on which ``measurement_name`` has not been measured.
    
    """
    coll = Collection.findby_name(connection, collection)
    if(isinstance(coll, list)):
        # name clash. 
        raise ValueError(f"name clash found: several collections with name {collection}")

    all_configurations =  coll._configurations

    cursor = connection.cursor()
    c = cursor.execute("SELECT measurements.configuration FROM measurements INNER JOIN collections_contains ON measurements.configuration = collections_contains.configuration WHERE collections_contains.collection = ? AND measurements.name=?", (coll._id, measurement_name))
    already_measured = {f[0] for f in c}

    missing_configurations = [Configuration.load(connection, c) for c in all_configurations if c not in already_measured]
    return missing_configurations
