import os
import pytest 
import numpy as np

from lattice_data_db.htp_db.datastore import HTPStore

def test_create_db(tmp_path):
    new_store = HTPStore.new("test_store", abspath=tmp_path)

    assert new_store._info["info"]["class"] == "HTPStore"
    assert new_store._info["info"]["loadpromise"] == "lambda s: numpy.load(s + '.npy')"
    assert new_store._info["info"]["jrnlprefix"] == HTPStore.jrnl_prefix
    assert os.path.exists(tmp_path / "test_store" / HTPStore.dbinfo_name)

