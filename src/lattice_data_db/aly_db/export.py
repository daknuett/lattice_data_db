#!/usr/bin/env python3

import sqlite3
import numpy

from ..db_backend.db_objecthandles import Collection, Measurement, DBValue, Configuration

def export_measurement_collection(connection: sqlite3.Connection, measurement_name: str, collection_name: str, locals=None):
    """
    Export a collection of measurements from the database.
    If the measurements are returned either as a list of values, if they are stored on-file 
    or as a numpy array, if they are stored in-line.


    """

    collection = Collection.findby_name(connection, collection_name)
    if(isinstance(collection, list)):
        raise ValueError(f"Collection name clash detected: {collection_name}")
    cursor = connection.cursor()
    c = cursor.execute(
            "SELECT measurements.rowid "\
            "FROM measurements INNER JOIN collections_contains "\
            "ON measurements.configuration = collections_contains.configuration "\
            "WHERE collections_contains.collection = ? AND measurements.name=?"
            , (collection._id, measurement_name))

    measurements = [Measurement.load(connection, v[0], locals=locals) for v in c]

    configurations = [m._configuration for m in measurements]
    values = [m._value._value for m in measurements]

    # see if the values are numpy arrays:
    if(measurements[0]._value._is_external):
        return configurations, values
    return configurations, numpy.array(values)
