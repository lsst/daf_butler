from __future__ import annotations
from typing import (
    Any,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    TYPE_CHECKING,
    Union,
)
from datetime import datetime

if TYPE_CHECKING:
    from .core.dimensions import (
        DataId,
        Dimension,
        DimensionElement,
        DimensionGraph,
        DimensionRecord,
    )
    from .core.queries import DatasetTypeExpression, CollectionsExpression
    from .core.datasets import DatasetType
    from .core.utils import transactional
    from .core.schema import TableSpec
    from .core.run import Run
    from .core.quantum import Quantum
    from .iterables import DataIdIterable, DatasetIterable, SingleDatasetTypeIterable


def interrupting(func):
    # TODO
    return func


class Registry:

    @interrupting
    def registerOpaqueTable(self, name: str, spec: TableSpec, *, write: bool = True):
        """Ensure that a table for use by a `Datastore` or other data
        repository client exists, creating it if necessary.

        Opaque table records can be added via `insertOpaqueData`, retrieved via
        `fetchOpaqueData`, and removed via `deleteOpaqueData`.

        Parameters
        ----------
        name : `str`
            Logical name of the opaque table.  This may differ from the
            actual name used in the database by a prefix and/or suffix.
        spec : `TableSpec`
            Specification for the table to be added.
        write : `bool`
            If `True`, allow inserts into this table.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @transactional
    def insertOpaqueData(self, name: str, *data: dict):
        """Insert records into an opaque table.

        Parameters
        ----------
        name : `str`
            Logical name of the opaque table.  Must match the name used in a
            previous call to `registerOpaqueTable`.
        data
            Each additional positional argument is a dictionary that represents
            a single row to be added.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def fetchOpaqueData(self, name: str, **where: Any) -> Iterator[dict]:
        """Retrieve records from an opaque table.

        Parameters
        ----------
        name : `str`
            Logical name of the opaque table.  Must match the name used in a
            previous call to `registerOpaqueTable`.
        where
            Additional keyword arguments are interpreted as equality
            constraints that restrict the returned rows (combined with AND);
            keyword arguments are column names and values are the values they
            must have.

        Yields
        ------
        row : `dict`
            A dictionary representing a single result row.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @transactional
    def deleteOpaqueData(self, name: str, **where: Any):
        """Remove records from an opaque table.

        Parameters
        ----------
        name : `str`
            Logical name of the opaque table.  Must match the name used in a
            previous call to `registerOpaqueTable`.
        where
            Additional keyword arguments are interpreted as equality
            constraints that restrict the deleted rows (combined with AND);
            keyword arguments are column names and values are the values they
            must have.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @interrupting
    def registerRun(self, name: str) -> Run:
        raise NotImplementedError("Must be implemented by subclass")

    @transactional
    def updateRun(self, run: Run):
        raise NotImplementedError("Must be implemented by subclass")

    def registerCollection(self, name: str, *, calibration: bool = False):
        raise NotImplementedError("Must be implemented by subclass")

    @interrupting
    def registerQuanta(self, dimensions: DimensionGraph, *, write: bool = True):
        raise NotImplementedError("Must be implemented by subclass")

    @transactional
    def insertQuantum(self, quantum: Quantum) -> Quantum:
        raise NotImplementedError("Must be implemented by subclass")

    @transactional
    def updateQuantum(self, quantum: Quantum):
        raise NotImplementedError("Must be implemented by subclass")

    @interrupting
    def registerDimensionElement(self, element: DimensionElement, *, write: bool = True):
        """Ensure that the `Registry` supports insertions of the given
        dimension element.

        It is not an error to register the same `DimensionElement` twice.

        Parameters
        ----------
        element : `DimensionElement`
            The `DimensionElement`
        write : `bool`
            If `True`, allow inserts of this element.  Passing `False`
            generally does nothing but check that the given element is valid
            for this registry, and is provided as a convenience to make this
            method more similar to the other ``register`` methods.

        Raises
        ------
        LookupError
            This `DimensionElement` is not part of this registry's
            `DimensionUniverse`.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @transactional
    def insertDimensionData(self, element: Union[DimensionElement, str],
                            *data: Union[dict, DimensionRecord],
                            conform: bool = True):
        """Insert one or more dimension records into the database.

        Parameters
        ----------
        element : `DimensionElement` or `str`
            The `DimensionElement` or name thereof that identifies the table
            records will be inserted into.
        data : `dict` or `DimensionRecord` (variadic)
            One or more records to insert.
        conform : `bool`, optional
            If `False` (`True` is default) perform no checking or conversions,
            and assume that ``element`` is a `DimensionElement` instance and
            ``data`` is a one or more `DimensionRecord` instances of the
            appropriate subclass.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def fetchDimensionData(self, element: Union[DimensionElement, str], *dataIds: DataId,
                           iterable: Optional[DataIdIterable]
                           ) -> Iterator[DimensionRecord]:
        raise NotImplementedError("Must be implemented by subclass")

    @interrupting
    def registerDatasetType(self, datasetType: DatasetType, *, write: bool = True):
        """Ensure a `DatasetType` is recognized by the `Registry`.

        It is not an error to register the same `DatasetType` twice.

        Parameters
        ----------
        datasetType : `DatasetType`
            The `DatasetType` to be added.
        write : `bool`
            If `True`, ensure the registry supports insertions of this
            dataset type.

        Raises
        ------
        ValueError
            Raised if the dimensions or storage class are invalid.
        ConflictingDefinitionError
            Raised if this DatasetType is already registered with a different
            definition.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def fetchDatasetType(self, name: str) -> DatasetType:
        raise NotImplementedError("Must be implemented by subclass")

    @transactional
    def insertDatasets(self, datasetType: DatasetType, run: Run, dataIds: DataIdIterable, *,
                       recursive: bool = True, producer: Optional[Quantum] = None
                       ) -> DatasetIterable:
        """Insert one or more dataset entries into the database.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def findDatasets(self, datasetType: DatasetType, collections: List[str], dataIds: DataIdIterable = None,
                     *, recursive: bool = True) -> SingleDatasetTypeIterable:
        """Search one or more collections (in order) for datasets.

        Unlike queryDatasets, this method requires complete data IDs and does
        not accept DatasetType or Collection expressions.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @transactional
    def deleteDatasets(self, datasets: DatasetIterable, *, recursive: bool = True):
        raise NotImplementedError("Must be implemented by subclass")

    @transactional
    def associate(self, collection: str, datasets: DatasetIterable, *,
                  begin: Optional[datetime] = None, end: Optional[datetime] = None,
                  recursive: bool = True):
        raise NotImplementedError("Must be implemented by subclass")

    @transactional
    def disassociate(self, collection: str, datasets: DatasetIterable, *, recursive: bool = True):
        raise NotImplementedError("Must be implemented by subclass")

    @interrupting
    def registerDatasetLocation(self, datastoreName: str):
        raise NotImplementedError("Must be implemented by subclass")

    @transactional
    def insertDatasetLocations(self, datastoreName: str, datasets: DatasetIterable):
        raise NotImplementedError("Must be implemented by subclass")

    def fetchDatasetLocations(self, datasets: DatasetIterable):
        raise NotImplementedError("Must be implemented by subclass")

    @transactional
    def deleteDatasetLocations(self, datastoreName: str, datasets: DatasetIterable):
        raise NotImplementedError("Must be implemented by subclass")

    def queryDimensions(self, dimensions: Iterable[Union[Dimension, str]], *,
                        dataId: Optional[DataId] = None,
                        datasets: Optional[Mapping[DatasetTypeExpression, CollectionsExpression]] = None,
                        where: Optional[str] = None,
                        **kwds) -> DataIdIterable:
        """Query for and iterate over data IDs matching user-provided criteria.

        Parameters
        ----------
        dimensions : `~collections.abc.Iterable` of `Dimension` or `str`
            The dimensions of the data IDs to yield, as either `Dimension`
            instances or `str`.  Will be automatically expanded to a complete
            `DimensionGraph`.
        dataId : `dict` or `DataCoordinate`, optional
            A data ID whose key-value pairs are used as equality constraints
            in the query.
        datasets : `~collections.abc.Mapping`, optional
            Datasets whose existence in the registry constrain the set of data
            IDs returned.  This is a mapping from a dataset type expression
            (a `str` name, a true `DatasetType` instance, a `Like` pattern
            for the name, or ``...`` for all DatasetTypes) to a collections
            expression (a sequence of `str` or `Like` patterns, or `...` for
            all collections).
        where : `str`, optional
            A string expression similar to a SQL WHERE clause.  May involve
            any column of a dimension table or (as a shortcut for the primary
            key column of a dimension table) dimension name.
        kwds
            Additional keyword arguments are forwarded to
            `DataCoordinate.standardize` when processing the ``dataId``
            argument (and may be used to provide a constraining data ID even
            when the ``dataId`` argument is `None`).

        Yields
        ------
        dataId : `DataCoordinate`
            Data IDs matching the given query parameters.  Order is
            unspecified.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def queryDatasets(self, datasetType: DatasetTypeExpression, *,
                      collections: CollectionsExpression,
                      dimensions: Optional[Iterable[Union[Dimension, str]]] = None,
                      dataId: Optional[DataId] = None,
                      where: Optional[str] = None,
                      **kwds) -> DatasetIterable:
        """Query for and iterate over dataset references matching user-provided
        criteria.

        Parameters
        ----------
        datasetType : `DatasetType`, `str`, `Like`, or ``...``
            An expression indicating type(s) of datasets to query for.
            ``...`` may be used to query for all known DatasetTypes.
            Multiple explicitly-provided dataset types cannot be queried in a
            single call to `queryDatasets` even though wildcard expressions
            can, because the results would be identical to chaining the
            iterators produced by multiple calls to `queryDatasets`.
        collections: `~collections.abc.Sequence` of `str` or `Like`, or ``...``
            An expression indicating the collections to be searched for
            datasets.  ``...`` may be passed to search all collections.
        dimensions : `~collections.abc.Iterable` of `Dimension` or `str`
            Dimensions to include in the query (in addition to those used
            to identify the queried dataset type(s)), either to constrain
            the resulting datasets to those for which a matching dimension
            exists, or to relate the dataset type's dimensions to dimensions
            referenced by the ``dataId`` or ``where`` arguments.
        dataId : `dict` or `DataCoordinate`, optional
            A data ID whose key-value pairs are used as equality constraints
            in the query.
        where : `str`, optional
            A string expression similar to a SQL WHERE clause.  May involve
            any column of a dimension table or (as a shortcut for the primary
            key column of a dimension table) dimension name.
        kwds
            Additional keyword arguments are forwarded to
            `DataCoordinate.standardize` when processing the ``dataId``
            argument (and may be used to provide a constraining data ID even
            when the ``dataId`` argument is `None`).

        Yields
        ------
        handle : `ResolvedDatasetHandle`
            Dataset references matching the given query criteria.  These
            are grouped by `DatasetType` if the query evaluates to multiple
            dataset types, but order is otherwise unspecified.

        Raises
        ------
        TypeError
            Raised when the arguments are incompatible, such as when a
            collection wildcard is pass when ``deduplicate`` is `True`.

        Notes
        -----
        When multiple dataset types are queried via a wildcard expression, the
        results of this operation are equivalent to querying for each dataset
        type separately in turn, and no information about the relationships
        between datasets of different types is included.  In contexts where
        that kind of information is important, the recommended pattern is to
        use `queryDimensions` to first obtain data IDs (possibly with the
        desired dataset types and collections passed as constraints to the
        query), and then use multiple (generally much simpler) calls to
        `queryDatasets` with the returned data IDs passed as constraints.
        """
        raise NotImplementedError("Must be implemented by subclass")
