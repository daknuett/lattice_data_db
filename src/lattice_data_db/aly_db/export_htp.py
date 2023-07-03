#!/usr/bin/env python3

import numpy

from ..htp_db.datastore import HTPStore

def export_measurement_group(store: HTPStore, group: str, locals=None):
    store.sync_journal()

    tags = store.groups[group]
    data = np.array([store.get_data(tag, locals=locals) for tag in tags])
    all_meta = store.meta
    meta = [all_meta[tag] for tag in tags]
    return tags, data, meta

