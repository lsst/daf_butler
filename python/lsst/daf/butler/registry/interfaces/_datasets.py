# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

from ... import ddl

__all__ = ("DatasetRecordStorageManager",)

from abc import abstractmethod
from collections.abc import Iterable, Mapping, Sequence, Set
from typing import TYPE_CHECKING, Any

from lsst.daf.relation import Relation

from ..._dataset_ref import DatasetId, DatasetIdGenEnum, DatasetRef
from ..._dataset_type import DatasetType
from ..._exceptions import DatasetTypeError, DatasetTypeNotSupportedError
from ..._timespan import Timespan
from ...dimensions import DataCoordinate
from ._versioning import VersionedExtension, VersionTuple

if TYPE_CHECKING:
    from ...direct_query_driver import QueryJoiner  # new query system, server+direct only
    from .._caching_context import CachingContext
    from .._collection_summary import CollectionSummary
    from ..queries import SqlQueryContext  # old registry query system
    from ._collections import CollectionManager, CollectionRecord, RunRecord
    from ._database import Database, StaticTablesContext
    from ._dimensions import DimensionRecordStorageManager


class DatasetRecordStorageManager(VersionedExtension):
    """An interface that manages the tables that describe datasets.

    `DatasetRecordStorageManager` primarily serves as a container and factory
    for `DatasetRecordStorage` instances, which each provide access to the
    records for a different `DatasetType`.

    Parameters
    ----------
    registry_schema_version : `VersionTuple` or `None`, optional
        Version of registry schema.
    """

    def __init__(self, *, registry_schema_version: VersionTuple | None = None) -> None:
        super().__init__(registry_schema_version=registry_schema_version)

    @abstractmethod
    def clone(
        self,
        *,
        db: Database,
        collections: CollectionManager,
        dimensions: DimensionRecordStorageManager,
        caching_context: CachingContext,
    ) -> DatasetRecordStorageManager:
        """Make an independent copy of this manager instance bound to new
        instances of `Database` and other managers.

        Parameters
        ----------
        db : `Database`
            New `Database` object to use when instantiating the manager.
        collections : `CollectionManager`
            New `CollectionManager` object to use when instantiating the
            manager.
        dimensions : `DimensionRecordStorageManager`
            New `DimensionRecordStorageManager` object to use when
            instantiating the manager.
        caching_context : `CachingContext`
            New `CachingContext` object to use when instantiating the manager.

        Returns
        -------
        instance : `DatasetRecordStorageManager`
            New manager instance with the same configuration as this instance,
            but bound to a new Database object.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def initialize(
        cls,
        db: Database,
        context: StaticTablesContext,
        *,
        collections: CollectionManager,
        dimensions: DimensionRecordStorageManager,
        caching_context: CachingContext,
        registry_schema_version: VersionTuple | None = None,
    ) -> DatasetRecordStorageManager:
        """Construct an instance of the manager.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        context : `StaticTablesContext`
            Context object obtained from `Database.declareStaticTables`; used
            to declare any tables that should always be present.
        collections : `CollectionManager`
            Manager object for the collections in this `Registry`.
        dimensions : `DimensionRecordStorageManager`
            Manager object for the dimensions in this `Registry`.
        caching_context : `CachingContext`
            Object controlling caching of information returned by managers.
        registry_schema_version : `VersionTuple` or `None`
            Schema version of this extension as defined in registry.

        Returns
        -------
        manager : `DatasetRecordStorageManager`
            An instance of a concrete `DatasetRecordStorageManager` subclass.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def addDatasetForeignKey(
        cls,
        tableSpec: ddl.TableSpec,
        *,
        name: str = "dataset",
        constraint: bool = True,
        onDelete: str | None = None,
        **kwargs: Any,
    ) -> ddl.FieldSpec:
        """Add a foreign key (field and constraint) referencing the dataset
        table.

        Parameters
        ----------
        tableSpec : `ddl.TableSpec`
            Specification for the table that should reference the dataset
            table.  Will be modified in place.
        name : `str`, optional
            A name to use for the prefix of the new field; the full name is
            ``{name}_id``.
        constraint : `bool`, optional
            If `False` (`True` is default), add a field that can be joined to
            the dataset primary key, but do not add a foreign key constraint.
        onDelete : `str`, optional
            One of "CASCADE" or "SET NULL", indicating what should happen to
            the referencing row if the collection row is deleted.  `None`
            indicates that this should be an integrity error.
        **kwargs
            Additional keyword arguments are forwarded to the `ddl.FieldSpec`
            constructor (only the ``name`` and ``dtype`` arguments are
            otherwise provided).

        Returns
        -------
        idSpec : `ddl.FieldSpec`
            Specification for the ID field.
        """
        raise NotImplementedError()

    @abstractmethod
    def refresh(self) -> None:
        """Ensure all other operations on this manager are aware of any
        dataset types that may have been registered by other clients since
        it was initialized or last refreshed.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_dataset_type(self, name: str) -> DatasetType:
        """Look up a dataset type by name.

        Parameters
        ----------
        name : `str`
            Name of a parent dataset type.

        Returns
        -------
        dataset_type : `DatasetType`
            The object representing the records for the given dataset type.

        Raises
        ------
        MissingDatasetTypeError
            Raised if there is no dataset type with the given name.
        """
        raise NotImplementedError()

    def conform_exact_dataset_type(self, dataset_type: DatasetType | str) -> DatasetType:
        """Conform a value that may be a dataset type or dataset type name to
        just the dataset type name, while checking that the dataset type is not
        a component and (if a `DatasetType` instance is given) has the exact
        same definition in the registry.

        Parameters
        ----------
        dataset_type : `str` or `DatasetType`
            Dataset type object or name.

        Returns
        -------
        dataset_type : `DatasetType`
            The corresponding registered dataset type.

        Raises
        ------
        DatasetTypeError
            Raised if ``dataset_type`` is a component, or if its definition
            does not exactly match the registered dataset type.
        MissingDatasetTypeError
            Raised if this dataset type is not registered at all.
        """
        if isinstance(dataset_type, DatasetType):
            dataset_type_name = dataset_type.name
            given_dataset_type = dataset_type
        else:
            dataset_type_name = dataset_type
            given_dataset_type = None
        parent_name, component = DatasetType.splitDatasetTypeName(dataset_type_name)
        if component is not None:
            raise DatasetTypeNotSupportedError(
                f"Component dataset {dataset_type_name!r} is not supported in this context."
            )
        registered_dataset_type = self.get_dataset_type(dataset_type_name)
        if given_dataset_type is not None and registered_dataset_type != given_dataset_type:
            raise DatasetTypeError(
                f"Given dataset type {given_dataset_type} is not identical to the "
                f"registered one {registered_dataset_type}."
            )
        return registered_dataset_type

    @abstractmethod
    def register_dataset_type(self, dataset_type: DatasetType) -> bool:
        """Ensure that this `Registry` can hold records for the given
        `DatasetType`, creating new tables as necessary.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Dataset type for which a table should created (as necessary) and
            an associated `DatasetRecordStorage` returned.

        Returns
        -------
        inserted : `bool`
            `True` if the dataset type did not exist in the registry before.

        Notes
        -----
        This operation may not be invoked within a `Database.transaction`
        context.
        """
        raise NotImplementedError()

    @abstractmethod
    def remove_dataset_type(self, name: str) -> None:
        """Remove the dataset type.

        Parameters
        ----------
        name : `str`
            Name of the dataset type.
        """
        raise NotImplementedError()

    @abstractmethod
    def resolve_wildcard(
        self,
        expression: Any,
        missing: list[str] | None = None,
        explicit_only: bool = False,
    ) -> list[DatasetType]:
        """Resolve a dataset type wildcard expression.

        Parameters
        ----------
        expression : `~typing.Any`
            Expression to resolve.  Will be passed to
            `DatasetTypeWildcard.from_expression`.
        missing : `list` of `str`, optional
            String dataset type names that were explicitly given (i.e. not
            regular expression patterns) but not found will be appended to this
            list, if it is provided.
        explicit_only : `bool`, optional
            If `True`, require explicit `DatasetType` instances or `str` names,
            with `re.Pattern` instances deprecated and ``...`` prohibited.

        Returns
        -------
        dataset_types : `list` [ `DatasetType` ]
            A list of resolved dataset types.
        """
        raise NotImplementedError()

    @abstractmethod
    def getDatasetRef(self, id: DatasetId) -> DatasetRef | None:
        """Return a `DatasetRef` for the given dataset primary key
        value.

        Parameters
        ----------
        id : `DatasetId`
            Primary key value for the dataset.

        Returns
        -------
        ref : `DatasetRef` or `None`
            Object representing the dataset, or `None` if no dataset with the
            given primary key values exists in this layer.
        """
        raise NotImplementedError()

    @abstractmethod
    def getCollectionSummary(self, collection: CollectionRecord) -> CollectionSummary:
        """Return a summary for the given collection.

        Parameters
        ----------
        collection : `CollectionRecord`
            Record describing the collection for which a summary is to be
            retrieved.

        Returns
        -------
        summary : `CollectionSummary`
            Summary of the dataset types and governor dimension values in
            this collection.
        """
        raise NotImplementedError()

    @abstractmethod
    def fetch_summaries(
        self,
        collections: Iterable[CollectionRecord],
        dataset_types: Iterable[DatasetType] | Iterable[str] | None = None,
    ) -> Mapping[Any, CollectionSummary]:
        """Fetch collection summaries given their names and dataset types.

        Parameters
        ----------
        collections : `~collections.abc.Iterable` [`CollectionRecord`]
            Collection records to query.
        dataset_types : `~collections.abc.Iterable` [`DatasetType`] or `None`
            Dataset types to include into returned summaries. If `None` then
            all dataset types will be included.

        Returns
        -------
        summaries : `~collections.abc.Mapping` [`Any`, `CollectionSummary`]
            Collection summaries indexed by collection record key. This mapping
            will also contain all nested non-chained collections of the chained
            collections.
        """
        raise NotImplementedError()

    @abstractmethod
    def ingest_date_dtype(self) -> type:
        """Return type of the ``ingest_date`` column."""
        raise NotImplementedError()

    @abstractmethod
    def insert(
        self,
        dataset_type_name: str,
        run: RunRecord,
        data_ids: Iterable[DataCoordinate],
        id_generation_mode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE,
    ) -> list[DatasetRef]:
        """Insert one or more dataset entries into the database.

        Parameters
        ----------
        dataset_type_name : `str`
            Name of the dataset type.
        run : `RunRecord`
            The record object describing the `~CollectionType.RUN` collection
            these datasets will be associated with.
        data_ids : `~collections.abc.Iterable` [ `DataCoordinate` ]
            Expanded data IDs (`DataCoordinate` instances) for the
            datasets to be added.   The dimensions of all data IDs must be the
            same as ``dataset_type.dimensions``.
        id_generation_mode : `DatasetIdGenEnum`
            With `UNIQUE` each new dataset is inserted with its new unique ID.
            With non-`UNIQUE` mode ID is computed from some combination of
            dataset type, dataId, and run collection name; if the same ID is
            already in the database then new record is not inserted.

        Returns
        -------
        datasets : `list` [ `DatasetRef` ]
            References to the inserted datasets.
        """
        raise NotImplementedError()

    @abstractmethod
    def import_(
        self,
        dataset_type: DatasetType,
        run: RunRecord,
        data_ids: Mapping[DatasetId, DataCoordinate],
    ) -> list[DatasetRef]:
        """Insert one or more dataset entries into the database.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Type of dataset to import.  Also used as the dataset type for
            the returned refs.
        run : `RunRecord`
            The record object describing the `~CollectionType.RUN` collection
            these datasets will be associated with.
        data_ids : `~collections.abc.Mapping`
            Mapping from dataset ID to data ID.

        Returns
        -------
        datasets : `list` [ `DatasetRef` ]
            References to the inserted or existing datasets.
        """
        raise NotImplementedError()

    @abstractmethod
    def delete(self, datasets: Iterable[DatasetId | DatasetRef]) -> None:
        """Fully delete the given datasets from the registry.

        Parameters
        ----------
        datasets : `~collections.abc.Iterable` [ `DatasetId` or `DatasetRef` ]
            Datasets to be deleted.  If `DatasetRef` instances are passed,
            only the `DatasetRef.id` attribute is used.
        """
        raise NotImplementedError()

    @abstractmethod
    def associate(
        self, dataset_type: DatasetType, collection: CollectionRecord, datasets: Iterable[DatasetRef]
    ) -> None:
        """Associate one or more datasets with a collection.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Type of all datasets.
        collection : `CollectionRecord`
            The record object describing the collection.  ``collection.type``
            must be `~CollectionType.TAGGED`.
        datasets : `~collections.abc.Iterable` [ `DatasetRef` ]
            Datasets to be associated.  All datasets must have the same
            `DatasetType` as ``dataset_type``, but this is not checked.

        Notes
        -----
        Associating a dataset into collection that already contains a
        different dataset with the same `DatasetType` and data ID will remove
        the existing dataset from that collection.

        Associating the same dataset into a collection multiple times is a
        no-op, but is still not permitted on read-only databases.
        """
        raise NotImplementedError()

    @abstractmethod
    def disassociate(
        self, dataset_type: DatasetType, collection: CollectionRecord, datasets: Iterable[DatasetRef]
    ) -> None:
        """Remove one or more datasets from a collection.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Type of all datasets.
        collection : `CollectionRecord`
            The record object describing the collection.  ``collection.type``
            must be `~CollectionType.TAGGED`.
        datasets : `~collections.abc.Iterable` [ `DatasetRef` ]
            Datasets to be disassociated.  All datasets must have the same
            `DatasetType` as ``dataset_type``, but this is not checked.
        """
        raise NotImplementedError()

    @abstractmethod
    def certify(
        self,
        dataset_type: DatasetType,
        collection: CollectionRecord,
        datasets: Iterable[DatasetRef],
        timespan: Timespan,
        context: SqlQueryContext,
    ) -> None:
        """Associate one or more datasets with a calibration collection and a
        validity range within it.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Type of all datasets.
        collection : `CollectionRecord`
            The record object describing the collection.  ``collection.type``
            must be `~CollectionType.CALIBRATION`.
        datasets : `~collections.abc.Iterable` [ `DatasetRef` ]
            Datasets to be associated.  All datasets must have the same
            `DatasetType` as ``dataset_type``, but this is not checked.
        timespan : `Timespan`
            The validity range for these datasets within the collection.
        context : `SqlQueryContext`
            The object that manages database connections, temporary tables and
            relation engines for this query.

        Raises
        ------
        ConflictingDefinitionError
            Raised if the collection already contains a different dataset with
            the same `DatasetType` and data ID and an overlapping validity
            range.
        DatasetTypeError
            Raised if ``dataset_type.isCalibration() is False``.
        CollectionTypeError
            Raised if
            ``collection.type is not CollectionType.CALIBRATION``.
        """
        raise NotImplementedError()

    @abstractmethod
    def decertify(
        self,
        dataset_type: DatasetType,
        collection: CollectionRecord,
        timespan: Timespan,
        *,
        data_ids: Iterable[DataCoordinate] | None = None,
        context: SqlQueryContext,
    ) -> None:
        """Remove or adjust datasets to clear a validity range within a
        calibration collection.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Type of all datasets.
        collection : `CollectionRecord`
            The record object describing the collection.  ``collection.type``
            must be `~CollectionType.CALIBRATION`.
        timespan : `Timespan`
            The validity range to remove datasets from within the collection.
            Datasets that overlap this range but are not contained by it will
            have their validity ranges adjusted to not overlap it, which may
            split a single dataset validity range into two.
        data_ids : `~collections.abc.Iterable` [ `DataCoordinate` ], optional
            Data IDs that should be decertified within the given validity range
            If `None`, all data IDs for ``dataset_type`` in ``collection`` will
            be decertified.
        context : `SqlQueryContext`
            The object that manages database connections, temporary tables and
            relation engines for this query.

        Raises
        ------
        DatasetTypeError
            Raised if ``dataset_type.isCalibration() is False``.
        CollectionTypeError
            Raised if
            ``collection.type is not CollectionType.CALIBRATION``.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_relation(
        self,
        dataset_type: DatasetType,
        *collections: CollectionRecord,
        columns: Set[str],
        context: SqlQueryContext,
    ) -> Relation:
        """Return a `sql.Relation` that represents a query for this
        `DatasetType` in one or more collections.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Type of dataset to query for.
        *collections : `CollectionRecord`
            The record object(s) describing the collection(s) to query.  May
            not be of type `CollectionType.CHAINED`.  If multiple collections
            are passed, the query will search all of them in an unspecified
            order, and all collections must have the same type.  Must include
            at least one collection.
        columns : `~collections.abc.Set` [ `str` ]
            Columns to include in the relation.  See `Query.find_datasets` for
            most options, but this method supports one more:

            - ``rank``: a calculated integer column holding the index of the
                collection the dataset was found in, within the ``collections``
                sequence given.
        context : `SqlQueryContext`
            The object that manages database connections, temporary tables and
            relation engines for this query.

        Returns
        -------
        relation : `~lsst.daf.relation.Relation`
            Representation of the query.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_query_joiner(
        self, dataset_type: DatasetType, collections: Sequence[CollectionRecord], fields: Set[str]
    ) -> QueryJoiner:
        """Make a `..direct_query_driver.QueryJoiner` that represents a search
        for datasets of this type.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Type of dataset to query for.
        collections : `~collections.abc.Sequence` [ `CollectionRecord` ]
            Collections to search, in order, after filtering out collections
            with no datasets of this type via collection summaries.
        fields : `~collections.abc.Set` [ `str` ]
            Names of fields to make available in the joiner.  Options include:

            - ``dataset_id`` (UUID)
            - ``run`` (collection name, `str`)
            - ``collection`` (collection name, `str`)
            - ``collection_key`` (collection primary key, manager-dependent)
            - ``timespan`` (validity range, or unbounded for non-calibrations)
            - ``ingest_date`` (time dataset was ingested into repository)

            Dimension keys for the dataset type's required dimensions are
            always included.

        Returns
        -------
        joiner : `..direct_query_driver.QueryJoiner`
            A query-construction object representing a table or subquery.
        """
        raise NotImplementedError()

    @abstractmethod
    def refresh_collection_summaries(self, dataset_type: DatasetType) -> None:
        """Make sure that collection summaries for this dataset type are
        consistent with the contents of the dataset tables.

        Parameters
        ----------
        dataset_type : `DatasetType`
            Dataset type whose summary entries should be refreshed.
        """
        raise NotImplementedError()
