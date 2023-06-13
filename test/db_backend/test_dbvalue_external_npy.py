from lattice_data_db.db_backend.db_objecthandles import DBValue 
from lattice_data_db.db_backend.schema import schema_init
import sqlite3

import numpy as np

def test_dbvalue_store_load(tmp_path):
    # This test is a bit phony. Usually, one will want to store the values 
    # via a proper mechanism (such as gpt.save, or hdf5). We just use numpy for a quick 
    # test on whether the paths work.
    conn = sqlite3.connect(tmp_path / "test.db", detect_types=sqlite3.PARSE_DECLTYPES)
    schema_init(conn)
    class stupid_object:
        def __init__(self, v):
            self.v = v 
    value = DBValue(stupid_object(np.arange(-1, 2, 0.1)), store_file=lambda p,o: np.save(p, o.v), promise_loadfile="lambda fname: stupid_object(numpy.load(fname + '.npy'))")
    rid = value.store(conn)

    value2 = DBValue.load(conn, rid, locals=locals())

    assert np.allclose(value._value.v, value2._value.v)
