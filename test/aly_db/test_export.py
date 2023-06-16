from lattice_data_db.aly_db.export import export_measurement_collection

import numpy as np


def test_export_measurement_collection_origint(populated_db):
    expect_configs = [1,2,3,4,5]
    expect_values = np.array(list(range(1200, 1250, 10)))

    configurations, values = export_measurement_collection(populated_db, "test_measurement_1", "test_collection")


    assert [c._id for c in configurations] == expect_configs
    assert np.allclose(expect_values, values)


