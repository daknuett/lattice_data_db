from lattice_data_db.db_backend.db_objecthandles import Collection, Configuration

def test_collection_store_load(small_populated_db):
    config_ids = [1, 2, 3, 4]
    configs = [Configuration.load(small_populated_db, cid) for cid in config_ids]
    collection = Collection("small_test_collection", configs)
    rid = collection.store(small_populated_db)

    collection2 = Collection.load(small_populated_db, rid)

    assert collection._name == collection2._name
    assert collection._configurations == collection2._configurations



def test_collection_store_search(small_populated_db):
    config_ids = [1, 2, 3, 4]
    configs = [Configuration.load(small_populated_db, cid) for cid in config_ids]
    collection = Collection("small_test_collection", configs)
    rid = collection.store(small_populated_db)

    collection2 = Collection.findby_name(small_populated_db, "small_test_collection")

    assert isinstance(collection2, Collection)
    assert collection._name == collection2._name
    assert collection._configurations == collection2._configurations
