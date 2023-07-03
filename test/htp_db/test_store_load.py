import numpy as np
import os 
import pytest 

from lattice_data_db.htp_db.datastore import HTPStore

def test_store_load(tmp_path):
    store = HTPStore.new("test_store", abspath=tmp_path)
    data = np.array([1, 12, 1])

    store.store("test_data", data)
    store.sync_journal()

    assert store.synced_journal is True
    assert np.allclose(store.get_data("test_data"), data)

def test_store_load_different(tmp_path):
    store = HTPStore.new("test_store", abspath=tmp_path)
    data = np.array([1, 12, 1])
    data2 = np.array([1, 54, 1])
    store.store("test_data", data)
    store.store("test_data2", data2)
    store.sync_journal()

    assert store.synced_journal is True
    assert np.allclose(store.get_data("test_data"), data)
    assert np.allclose(store.get_data("test_data2"), data2)
