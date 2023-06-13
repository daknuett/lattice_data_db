from lattice_data_db.db_backend.db_objecthandles import Collection, Configuration, Measurement, DBValue
from lattice_data_db.db_backend.tasks import find_configurations_for_collection, find_missing_configurations_for_collection


def test_find_configurations_for_collection_empty(small_populated_db):
    coll = Collection("test", [])
    coll.store(small_populated_db)

    confs = find_configurations_for_collection(small_populated_db, "test")

    assert confs == []

def conf_eq(c1, c2):
    result = True 
    result = result and c1._relapath == c2._relapath
    result = result and c1._load_promise == c2._load_promise
    result = result and c1._ensemble == c2._ensemble
    result = result and c1._id == c2._id

    return result

def test_find_configurations_for_collection(small_populated_db):
    coll = Collection("test", [1, 4, 5])
    coll.store(small_populated_db)
    confs_expect = [Configuration.load(small_populated_db, i) for i in [1, 4, 5]]

    confs = find_configurations_for_collection(small_populated_db, "test")

    assert len(confs) == len(confs_expect)
    assert all([conf_eq(c1, c2) for c1, c2 in zip(confs_expect, confs)]) 

def test_find_missing_configurations_for_collection_none_done(small_populated_db):
    coll = Collection("test", [1, 4, 5])
    coll.store(small_populated_db)
    confs_expect = [Configuration.load(small_populated_db, i) for i in [1, 4, 5]]

    confs = find_missing_configurations_for_collection(small_populated_db, "test", "test_measurement")

    assert len(confs) == len(confs_expect)
    assert all([conf_eq(c1, c2) for c1, c2 in zip(confs_expect, confs)]) 

def test_find_missing_configurations_for_collection_some_done(small_populated_db):
    coll = Collection("test", [1, 4, 5])
    coll.store(small_populated_db)
    confs_expect = [Configuration.load(small_populated_db, i) for i in [1, 5]]
    measure = Measurement(4, DBValue(12), "test_measurement")
    measure.store(small_populated_db)

    confs = find_missing_configurations_for_collection(small_populated_db, "test", "test_measurement")

    assert len(confs) == len(confs_expect)
    assert all([conf_eq(c1, c2) for c1, c2 in zip(confs_expect, confs)]) 


def test_find_missing_configurations_for_collection_all_done(small_populated_db):
    coll = Collection("test", [1, 4, 5])
    coll.store(small_populated_db)
    confs_expect = []
    for cid in [1, 4, 5]:
        measure = Measurement(cid, DBValue(12), "test_measurement")
        measure.store(small_populated_db)

    confs = find_missing_configurations_for_collection(small_populated_db, "test", "test_measurement")

    assert len(confs) == len(confs_expect)
    assert all([conf_eq(c1, c2) for c1, c2 in zip(confs_expect, confs)]) 
