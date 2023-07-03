import os 
import pytest 
import numpy as np
import glob
import json
import time

from lattice_data_db.htp_db.datastore import HTPStore


def test_store_jrnl_present(tmp_path):
    new_store = HTPStore.new("test_store", abspath=tmp_path)
    data = np.array([1, 12, 1])
    new_store.store("test_data", data)

    jrnlnames = glob.glob(os.path.join(new_store._full_path, new_store._info["info"]["jrnlprefix"]) + "*")

    assert len(jrnlnames) == 1

    with open(jrnlnames[0]) as fin:
        jrnl = json.load(fin)
        
    assert jrnl["name"] == "test_data"
    assert jrnl["file"] == "test_data"

def test_store_jrnl_data_correct(tmp_path):
    new_store = HTPStore.new("test_store", abspath=tmp_path)
    data = np.array([1, 12, 1])
    new_store.store("test_data", data)
    jrnlnames = glob.glob(os.path.join(new_store._full_path, new_store._info["info"]["jrnlprefix"]) + "*")
    with open(jrnlnames[0]) as fin:
        jrnl = json.load(fin)

    data_file_name = os.path.join(new_store._full_path, jrnl["file"])
    data_read = np.load(data_file_name + ".npy")

    assert np.allclose(data, data_read)

def test_store_jrnl_sync(tmp_path):
    new_store = HTPStore.new("test_store", abspath=tmp_path)
    data = np.array([1, 12, 1])
    new_store.store("test_data", data)

    assert new_store.synced_journal is False

    assert new_store.sync_journal() == 1

    assert "test_data" in new_store.tags

def test_store_jrnl_doubler(tmp_path):
    new_store = HTPStore.new("test_store", abspath=tmp_path)
    data = np.array([1, 12, 1])
    new_store.store("test_data", data)
    new_store.store("test_data", data)

    assert new_store.synced_journal is False

    with pytest.raises(Exception) as exc_info:
        new_store.sync_journal()

    assert exc_info.value.args[0].startswith("journal entry doubler: test_data")


@pytest.mark.slow
def test_store_jrnl_collision(tmp_path):
    new_store = HTPStore.new("test_store", abspath=tmp_path)
    data = np.array([1, 12, 1])
    new_store.store("test_data", data)
    time.sleep(2)
    new_store.sync_journal()
    assert new_store.synced_journal is True

    new_store.store("test_data", data)

    assert new_store.synced_journal is False

    with pytest.raises(Exception) as exc_info:
        new_store.sync_journal()

    assert exc_info.value.args[0].startswith("journal entry collision: test_data")


