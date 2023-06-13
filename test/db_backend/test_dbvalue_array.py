from lattice_data_db.db_backend.db_objecthandles import DBValue 
from lattice_data_db.db_backend.schema import schema_init
import sqlite3

import numpy as np

def test_dbvalue_store_load(tmp_path):
    conn = sqlite3.connect(tmp_path / "test.db", detect_types=sqlite3.PARSE_DECLTYPES)
    schema_init(conn)
    value = DBValue(np.arange(-1, 2, 0.01))
    rid = value.store(conn)

    value2 = DBValue.load(conn, rid)

    assert np.allclose(value._value, value2._value)
