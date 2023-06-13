from lattice_data_db.db_backend.db_objecthandles import Ensemble, Configuration
from lattice_data_db.db_backend.schema import schema_init
import sqlite3

import pytest


@pytest.fixture(scope="function")
def small_populated_db(tmp_path):
    conn = sqlite3.connect(tmp_path / "test.db", detect_types=sqlite3.PARSE_DECLTYPES)
    schema_init(conn)

    ensemble = Ensemble("ens001", "/data/ensembles/ens001", '''{ "lattice_size": [ 8, 8, 8, 16 ], "Nd": 4, "beta": 6.0, "action_name": "g.qcd.gauge.action.wilson(beta)", "rng_algo": "vectorized_ranlux24_24_64", "rng_seed": "0xdeadbeef", "startconfig": "g.qcd.gauge.unit(grid)", "grid_gn": "g.grid(lattice_size, g.double)", "ensname": "ens_001", "markov_gn": "g.algorithms.markov.su2_heat_bath(rng)", "n_want": 5000 } ''')
    erid = ensemble.store(conn)

    for i in range(1200, 1300, 10):
        configuration = Configuration(ensemble, f"{i}.config", "np.load") 
        configuration.store(conn)

    return conn

