#!/usr/bin/env python3

"""
Store and load numpy arrays in SQLite3.
"""

import sqlite3
import numpy
import io
import base64

def adapt_array(arr):
    out = io.BytesIO()
    numpy.save(out, arr, allow_pickle=False)
    out.seek(0, 0)
    return sqlite3.Binary(base64.b64encode(out.read()))

def convert_array(text):
    fin = io.BytesIO(base64.b64decode(text))
    return numpy.load(fin)

sqlite3.register_adapter(numpy.ndarray, adapt_array)
sqlite3.register_converter("array", convert_array)

sentinel = None
