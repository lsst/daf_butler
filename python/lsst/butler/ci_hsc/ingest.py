
import os
import sqlite3

import numpy as np

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from . import hsc
from .. import units
from ..schema import metadata


CASTERS = {
    "i": int,
    "f": float,
    "U": lambda x: str(x) if x else None,
    "S": bytes,
}


DATA_ROOT = os.path.join(
    os.path.split(__file__)[0],
    "../../../../tests/data/ci_hsc"
)


def createTables(filename):
    engine = create_engine("sqlite:///{}".format(filename))
    maker = sessionmaker()
    maker.configure(bind=engine)
    metadata.create_all(engine)
    maker.close_all()


def loadTable(db, name, placeholder="?", extra=None):
    array = np.load(os.path.join(DATA_ROOT, "{}.npy".format(name)))
    if extra is None:
        extra = {}
    keys = tuple(array.dtype.names)
    fields = ", ".join('"%s"' % k for k in (keys + tuple(extra.keys())))
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


def run(filename=None, create=True, skipCamera=False, verbose=False):
    """Construct a SQLite database with Registry data for the ci_hsc test data.

    The created database will have Dataset entries for raw, biad, dark, and flat,
    associated with the appropriate DataUnits.

    Parameters
    ----------
    filename: str
        Name of the file that contiains / will contain the SQLite database.
        Defaults to 'tests/data/ci_hsc/ci_hsc.sqlite3'.
        The special ':memory:' database is not currently supported.'
    create: bool
        If True, delete the file if it exists and define the schema.  If False,
        the database must exist and the full schema must already be present.
    skipCamera: bool
        If True, do not load the Camera, PhysicalFilter, or PhysicalSensor
        tables, and instead assume they are already present in the database.
    verbose: bool
        If True, echo all database queries to stdout.
    """
    if not filename:
        filename = os.path.join(DATA_ROOT, "ci_hsc.sqlite3")
    if create:
        if os.path.exists(filename):
            os.remove(filename)
        createTables(filename)
    db = sqlite3.connect(filename)
    if verbose:
        db.set_trace_callback(print)
    db.execute("INSERT INTO Run (run_id, registry_id, tag) VALUES (0, 1, 'ingest')")
    loadTable(db, "DatasetType")
    loadTable(db, "DatasetTypeUnits")
    loadTable(db, "Dataset", extra={"unit_pack": "''"})
    loadTable(db, "AbstractFilter")
    if not skipCamera:
        camera = units.Camera.instances["HSC"]
        db.execute("INSERT INTO Camera (camera_name, module) VALUES (?, ?)",
                   (camera.name, camera.__module__))
        loadTable(db, "PhysicalFilter")
        loadTable(db, "PhysicalSensor")
    elif create:
        raise RuntimeError("Cannot skip Camera definitions when creating a new database.")
    loadTable(db, "Visit")
    loadTable(db, "Snap")
    loadTable(db, "ObservedSensor")
    loadTable(db, "VisitDatasetJoin")
    loadTable(db, "PhysicalSensorDatasetJoin")
    loadTable(db, "VisitRange")
    loadTable(db, "VisitRangeDatasetJoin")
    loadTable(db, "PhysicalSensorDatasetJoin")
    db.commit()

if __name__ == "__main__":
    run()
