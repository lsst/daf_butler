from __future__ import annotations

__all__ = ["QuantumTableRecords", "QuantumTableManager"]

from abc import ABC, abstractmethod
from typing import (
    Any,
    Optional,
    Tuple,
    TYPE_CHECKING
)

from ...core.dimensions import DimensionGraph, DimensionUniverse
from ...core.schema import FieldSpec, TableSpec

from ..quantum import Quantum

if TYPE_CHECKING:
    from .database import Database, StaticTablesContext, DatasetTableManager, CollectionManager


class QuantumTableRecords(ABC):
    """An interface that manages the quantum records identified by a particular
    set of dimensions in a `RegistryLayer`.

    Parameters
    ----------
    dimensions : `DimensionGraph`
        Dimensions that are used to identify all quanta managed by this object.
    """
    def __init__(self, dimensions: DimensionGraph):
        self.dimensions = dimensions

    @abstractmethod
    def start(self, quantum: Quantum):
        """Insert a new quantum record and write all information that can
        be expected to be present before it is executed.

        Parameters
        ----------
        quantum : `Quantum`
            Object to be written to the database.  Will be updated on return
            with the new ``id`` and ``origin`` values if these were `None`
            on input.

        Notes
        -----
        This writes the ``run_id`` and ``task`` fields, along with all
        predicted inputs.  Other fields are deferred to `finish`, and outputs
        are written when those datasets are themselves inserted.
        """
        raise NotImplementedError()

    @abstractmethod
    def finish(self, quantum: Quantum):
        """Update a quantum's representation in the database with information
        that can only be known after it is executed.

        Parameters
        ----------
        quantum : `Quantum`
            Object to be written to the database.  Must have ``id`` and
            ``origin`` attributes that are not `None` and match a quantum
            already added to this layer by `start`.

        Notes
        -----
        This writes the ``host`` and ``timespan`` fields, and qualifies any
        datasets in `Quantum.predictedInputs` that were are not also in
        `Quantum.actualInputs` as predicted only.  It is assumed that the
        predicted inputs have not changed since `start` was called.
        """
        raise NotImplementedError()

    # TODO: we need methods for fetching and querying (but don't yet have the
    # high-level Registry API defined enough to know what we need).

    dimensions: DimensionGraph
    """Dimensions that are used to identify all quanta managed by this object.
    """


class QuantumTableManager(ABC):
    """An interface that manages the tables that describe quanta in a
    `RegistryLayer`.

    `QuantumTableManager` primarily serves as a container and factory for
    `QuantumTableRecords` instances, which each provide access to the records
    for a different set of dimensions.
    """

    @classmethod
    @abstractmethod
    def initialize(cls, db: Database, context: StaticTablesContext, *, collections: CollectionManager,
                   datasets: DatasetTableManager, universe: DimensionUniverse) -> QuantumTableManager:
        """Construct an instance of the manager.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        schema : `StaticTablesContext`
            Context object obtained from `Database.declareStaticTables`; used
            to declare any tables that should always be present in a layer
            implemented with this manager.
        collections: `CollectionManager`
            Manager object for the collections in the same layer.
        datasets: `DatasetTableManager`
            Manager object for the datasets in the same layer.
        universe : `DimensionUniverse`
            Universe graph containing dimensions known to this `Registry`.

        Returns
        -------
        manager : `QuantumTableManager`
            An instance of a concrete `QuantumTableManager` subclass.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def addQuantumForeignKey(cls, tableSpec: TableSpec, *, name: str = "quantum",
                             onDelete: Optional[str] = None, **kwds: Any) -> Tuple[FieldSpec, FieldSpec]:
        """Add a foreign key (field and constraint) referencing the quantum
        table.

        Parameters
        ----------
        tableSpec : `TableSpec`
            Specification for the table that should reference the quantum
            table.  Will be modified in place.
        name: `str`, optional
            A name to use for the prefix of the new field; the full names are
            ``{name}_id`` and ``{name}_origin``.
        onDelete: `str`, optional
            One of "CASCADE" or "SET NULL", indicating what should happen to
            the referencing row if the collection row is deleted.  `None`
            indicates that this should be an integrity error.
        kwds
            Additional keyword arguments are forwarded to the `FieldSpec`
            constructor (only the ``name`` and ``dtype`` arguments are
            otherwise provided).

        Returns
        -------
        idSpec : `FieldSpec`
            Specification for the ID field.
        originSpec : `FieldSpec`
            Specification for the origin field.
        """
        raise NotImplementedError()

    @abstractmethod
    def refresh(self, *, universe: DimensionUniverse):
        """Ensure all other operations on this manager are aware of any
        quantum dimensions that may have been registered by other clients since
        it was initialized or last refreshed.
        """
        raise NotImplementedError()

    @abstractmethod
    def get(self, dimensions: DimensionGraph) -> Optional[QuantumTableRecords]:
        """Return an object that provides access to quanta with the given
        dimensions.

        Parameters
        ----------
        dimensions : `DimensionGraph`
            Dimensions used to identify the quanta of interest.

        Returns
        -------
        records : `QuantumTableRecords` or `None`
            The object representing the records for quanta with the given
            dimensions, or `None` if there is no such object in this layer.

        Note
        ----
        Quantum dimensions registered by another client of the same layer since
        the last call to `initialize` or `refresh` may not be found.
        """
        raise NotImplementedError()

    @abstractmethod
    def register(self, dimensions: DimensionGraph) -> QuantumTableRecords:
        """Ensure that this layer can hold records for the given dimensions,
        creating new tables as necessary.

        Parameters
        ----------
        dimensions : `DimensionGraph`
            Dimensions used to identify the quanta of interest.

        Returns
        -------
        records : `QuantumTableRecords`
            The object representing the records for quanta with the given
            dimensions.

        Raises
        ------
        TransactionInterruption
            Raised if this operation is invoked within a `Database.transaction`
            context.
        """
        raise NotImplementedError()
