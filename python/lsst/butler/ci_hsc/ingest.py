
import os
import sqlite3

import numpy as np

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ..orm.schema import Base


CASTERS = {
    "i": int,
    "f": float,
    "U": str,
    "S": bytes,
}


def getDatabaseName(root):
    return os.path.join(root, "ci_hsc.sqlite3")


def createTables(root):
    engine = create_engine("sqlite:///{}".format(getDatabaseName(root)))
    maker = sessionmaker()
    maker.configure(bind=engine)
    Base.metadata.create_all(engine)
    maker.close_all()


def loadTable(db, root, name, placeholder="?", extra=None):
    array = np.load(os.path.join(root, "{}.npy".format(name)))
    if extra is None:
        extra = {}
    keys = tuple(array.dtype.names)
    fields = ", ".join(keys + tuple(extra.keys()))
    placeholders = ", ".join([placeholder] * len(keys) + list(extra.values()))
    sql = "INSERT INTO {name} ({fields}) VALUES ({placeholders})".format(
        name=name, fields=fields, placeholders=placeholders
    )
    casters = tuple(CASTERS[array.dtype[k].kind] for k in keys)

    # sqlite interprets numpy types as bytes unless you cast them to Python
    # builtin types
    def sanitize(record):
        return tuple(caster(val) for caster, val in zip(casters, record))

    db.executemany(sql, (sanitize(r) for r in array))


def run(root, create=True):
    if create:
        createTables(root)
    db = sqlite3.connect(getDatabaseName(root))
    db.execute("INSERT INTO Run (run_id, registry_id, tag) VALUES (0, 1, 'ingest')")
    loadTable(db, root, "DatasetType")
    loadTable(db, root, "DatasetTypeUnits")
    loadTable(db, root, "Dataset", extra={"unit_pack": "''"})
    db.commit()

if __name__ == "__main__":
    import sys
    run(sys.argv[1])
