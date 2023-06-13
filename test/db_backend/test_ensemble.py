from lattice_data_db.db_backend.db_objecthandles import Ensemble
from lattice_data_db.db_backend.schema import schema_init
import sqlite3

import numpy as np

def test_ensemble_store_load(tmp_path):
    conn = sqlite3.connect(tmp_path / "test.db", detect_types=sqlite3.PARSE_DECLTYPES)
    schema_init(conn)
    ensemble = Ensemble("ens001", "/data/ensembles/ens001", '''{ "lattice_size": [ 8, 8, 8, 16 ], "Nd": 4, "beta": 6.0, "action_name": "g.qcd.gauge.action.wilson(beta)", "rng_algo": "vectorized_ranlux24_24_64", "rng_seed": "0xdeadbeef", "startconfig": "g.qcd.gauge.unit(grid)", "grid_gn": "g.grid(lattice_size, g.double)", "ensname": "ens_001", "markov_gn": "g.algorithms.markov.su2_heat_bath(rng)", "n_want": 5000 } ''')
    rid = ensemble.store(conn)

    ensemble2 = Ensemble.load(conn, rid)

    assert ensemble._name == ensemble2._name
    assert ensemble._abspath == ensemble2._abspath
    assert ensemble._description == ensemble2._description
    assert ensemble._id == ensemble2._id

def test_ensemble_store_search(tmp_path):
    conn = sqlite3.connect(tmp_path / "test.db", detect_types=sqlite3.PARSE_DECLTYPES)
    schema_init(conn)
    ensemble = Ensemble("ens001", "/data/ensembles/ens001", '''{ "lattice_size": [ 8, 8, 8, 16 ], "Nd": 4, "beta": 6.0, "action_name": "g.qcd.gauge.action.wilson(beta)", "rng_algo": "vectorized_ranlux24_24_64", "rng_seed": "0xdeadbeef", "startconfig": "g.qcd.gauge.unit(grid)", "grid_gn": "g.grid(lattice_size, g.double)", "ensname": "ens_001", "markov_gn": "g.algorithms.markov.su2_heat_bath(rng)", "n_want": 5000 } ''')
    rid = ensemble.store(conn)

    ensemble2 = Ensemble.findby_name(conn, "ens001")

    assert isinstance(ensemble2, Ensemble)
    assert ensemble._name == ensemble2._name
    assert ensemble._abspath == ensemble2._abspath
    assert ensemble._description == ensemble2._description
    assert ensemble._id == ensemble2._id
