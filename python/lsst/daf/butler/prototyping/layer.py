from __future__ import annotations

__all__ = ["RegistryLayer"]

from typing import (
    Type,
    TYPE_CHECKING,
)

from ..core.dimensions import DimensionUniverse

if TYPE_CHECKING:
    from .database import Database
    from .collections import CollectionManager
    from .opaque import OpaqueTableManager
    from .dimensions import DimensionTableManager
    from .datasets import DatasetTableManager
    from .quanta import QuantumTableManager


class RegistryLayer:
    """A struct that aggregates the `Database` and manager objects that
    implement a layer in a `Registry`.

    Parameters
    ----------
    db : `Database`
        An object representing a particular namespace/schema in a particular
        database engine.
    universe : `DimensionUniverse`
        Universe of all dimensions and dimension elements known to the
        `Registry`.  Must be the same for all layers in a `Registry`.
    collections : `type`
        Concrete subclass of `CollectionManager` that will be used to manage
        the collections (including runs) in this layer.
    opaque : `type`
        Concrete subclass of `OpaqueTableManager` that will be used to manage
        the opaque tables (typically used for `Datastore` internals) in this
        layer.
    dimensions : `type`
        Concrete subclass of `DimensionTableManager` that will be used to
        manage the dimension records in this layer.
    datasets : `type`
        Concrete subclass of `DatasetTableManager` that will be used to manage
        the dataset records in this layer.
    quanta : `type`
        Concrete subclass of `QuantumTableManager` that will be used to manage
        the quantum records in this layer.
    create : `bool`
        If `True`, create all tables that must be present in any layer,
        regardless of what entities exist in it.
    """

    def __init__(self, db: Database, *, universe: DimensionUniverse,
                 collections: Type[CollectionManager],
                 opaque: Type[OpaqueTableManager],
                 dimensions: Type[DimensionTableManager],
                 datasets: Type[DatasetTableManager],
                 quanta: Type[QuantumTableManager],
                 create: bool = True):
        self.db = db
        with db.declareStaticTables(create=create) as context:
            self.collections = collections.initialize(self.db, context)
            self.opaque = opaque.initialize(self.db, context)
            self.dimensions = dimensions.initialize(self.db, context, universe=universe)
            self.datasets = datasets.initialize(self.db, context, collections=self.collections,
                                                quanta=quanta, universe=universe)
            self.quanta = quanta.initialize(self.db, context, collections=self.collections,
                                            datasets=self.datasets, universe=universe)

    collections: CollectionManager
    """Manager object for collections and runs.
    """

    opaque: OpaqueTableManager
    """Manager object for the opaque tables that store `Datastore` internal
    records.
    """

    dimensions: DimensionTableManager
    """Manager object for `Dimension` and `DimensionElement` records.
    """

    datasets: DatasetTableManager
    """Manager object for the tables that represent datasets themselves.
    """

    quanta: QuantumTableManager
    """Manager object for the tables that represent quanta and their
    relationships to datasets.
    """

    db: Database
    """An object representing the particular namespace/schema in a particular
    database engine where this layer exists.
    """
