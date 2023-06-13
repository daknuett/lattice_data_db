from lattice_data_db.db_backend.db_objecthandles import DBValue, Measurement, Configuration
import numpy as np


def test_measurement_store_load(small_populated_db):
    value  = DBValue(0.5923)
    configuration = Configuration.load(small_populated_db, 1)
    measure = Measurement(configuration, value, "PlaquetteAverage")
    rid = measure.store(small_populated_db)

    measure2 = Measurement.load(small_populated_db, rid)

    assert measure2._name == measure._name
    assert measure2._id == measure._id
    assert measure2._configuration == measure._configuration
    assert measure2._value._value == measure._value._value



