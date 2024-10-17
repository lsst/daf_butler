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

__all__ = ("DimensionRecordStorageManager",)

from abc import abstractmethod
from collections.abc import Iterable, Set
from typing import TYPE_CHECKING, Any

from lsst.daf.relation import Join, Relation

from ...dimensions import (
    DataCoordinate,
    DimensionElement,
    DimensionGroup,
    DimensionRecord,
    DimensionRecordSet,
    DimensionUniverse,
)
from ...dimensions.record_cache import DimensionRecordCache
from ._versioning import VersionedExtension, VersionTuple

if TYPE_CHECKING:
    from ...direct_query_driver import QueryBuilder, QueryJoiner  # Future query system (direct,server).
    from ...queries.tree import Predicate  # Future query system (direct,client,server).
    from .. import queries  # Old Registry.query* system.
    from ._database import Database, StaticTablesContext


class DimensionRecordStorageManager(VersionedExtension):
    """An interface for managing the dimension records in a `Registry`.

    `DimensionRecordStorageManager` primarily serves as a container and factory
    for `DimensionRecordStorage` instances, which each provide access to the
    records for a different `DimensionElement`.

    Parameters
    ----------
    universe : `DimensionUniverse`
        Universe of all dimensions and dimension elements known to the
        `Registry`.
    registry_schema_version : `VersionTuple` or `None`, optional
        Version of registry schema.

    Notes
    -----
    In a multi-layer `Registry`, many dimension elements will only have
    records in one layer (often the base layer).  The union of the records
    across all layers forms the logical table for the full `Registry`.
    """

    def __init__(self, *, universe: DimensionUniverse, registry_schema_version: VersionTuple | None = None):
        super().__init__(registry_schema_version=registry_schema_version)
        self.universe = universe

    @abstractmethod
    def clone(self, db: Database) -> DimensionRecordStorageManager:
        """Make an independent copy of this manager instance bound to a new
        `Database` instance.

        Parameters
        ----------
        db : `Database`
            New `Database` object to use when instantiating the manager.

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
        universe: DimensionUniverse,
        registry_schema_version: VersionTuple | None = None,
    ) -> DimensionRecordStorageManager:
        """Construct an instance of the manager.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        context : `StaticTablesContext`
            Context object obtained from `Database.declareStaticTables`; used
            to declare any tables that should always be present in a layer
            implemented with this manager.
        universe : `DimensionUniverse`
            Universe graph containing dimensions known to this `Registry`.
        registry_schema_version : `VersionTuple` or `None`
            Schema version of this extension as defined in registry.

        Returns
        -------
        manager : `DimensionRecordStorageManager`
            An instance of a concrete `DimensionRecordStorageManager` subclass.
        """
        raise NotImplementedError()

    def fetch_cache_dict(self) -> dict[str, DimensionRecordSet]:
        """Return a `dict` that can back a `DimensionRecordSet`.

        This method is intended as the ``fetch`` callback argument to
        `DimensionRecordCache`, in contexts where direct SQL queries are
        possible.
        """
        raise NotImplementedError()

    @abstractmethod
    def insert(
        self,
        element: DimensionElement,
        *records: DimensionRecord,
        replace: bool = False,
        skip_existing: bool = False,
    ) -> None:
        """Insert one or more records into storage.

        Parameters
        ----------
        element : `DimensionElement`
            Dimension element that provides the definition for records.
        *records : `DimensionRecord`
            One or more instances of the `DimensionRecord` subclass for the
            element this storage is associated with.
        replace : `bool`, optional
            If `True` (`False` is default), replace existing records in the
            database if there is a conflict.
        skip_existing : `bool`, optional
            If `True` (`False` is default), skip insertion if a record with
            the same primary key values already exists.

        Raises
        ------
        TypeError
            Raised if the element does not support record insertion.
        sqlalchemy.exc.IntegrityError
            Raised if one or more records violate database integrity
            constraints.
        """
        raise NotImplementedError()

    @abstractmethod
    def sync(self, record: DimensionRecord, update: bool = False) -> bool | dict[str, Any]:
        """Synchronize a record with the database, inserting it only if it does
        not exist and comparing values if it does.

        Parameters
        ----------
        record : `DimensionRecord`
            An instance of the `DimensionRecord` subclass for the
            element this storage is associated with.
        update : `bool`, optional
            If `True` (`False` is default), update the existing record in the
            database if there is a conflict.

        Returns
        -------
        inserted_or_updated : `bool` or `dict`
            `True` if a new row was inserted, `False` if no changes were
            needed, or a `dict` mapping updated column names to their old
            values if an update was performed (only possible if
            ``update=True``).

        Raises
        ------
        DatabaseConflictError
            Raised if the record exists in the database (according to primary
            key lookup) but is inconsistent with the given one.
        TypeError
            Raised if the element does not support record synchronization.
        sqlalchemy.exc.IntegrityError
            Raised if one or more records violate database integrity
            constraints.
        """
        raise NotImplementedError()

    @abstractmethod
    def fetch_one(
        self,
        element_name: str,
        data_id: DataCoordinate,
        cache: DimensionRecordCache,
    ) -> DimensionRecord | None:
        """Retrieve a single record from storage.

        Parameters
        ----------
        element_name : `str`
            Name of the dimension element for the record to fetch.
        data_id : `DataCoordinate`
            Data ID of the record to fetch.  Implied dimensions do not need to
            be present.
        cache : `DimensionRecordCache`
            Cache to look in first.

        Returns
        -------
        record : `DimensionRecord` or `None`
            Fetched record, or *possibly* `None` if there was no match for the
            given data ID.
        """
        raise NotImplementedError()

    @abstractmethod
    def save_dimension_group(self, group: DimensionGroup) -> int:
        """Save a `DimensionGroup` definition to the database, allowing it to
        be retrieved later via the returned key.

        If this dimension group has already been saved, this method just
        returns the key already associated with it.

        Parameters
        ----------
        group : `DimensionGroup`
            Set of dimensions to save.

        Returns
        -------
        key : `int`
            Integer used as the unique key for this `DimensionGroup` in the
            database.

        Raises
        ------
        TransactionInterruption
            Raised if this operation is invoked within a `Database.transaction`
            context.
        """
        raise NotImplementedError()

    @abstractmethod
    def load_dimension_group(self, key: int) -> DimensionGroup:
        """Retrieve a `DimensionGroup` that was previously saved in the
        database.

        Parameters
        ----------
        key : `int`
            Integer used as the unique key for this `DimensionGroup` in the
            database.

        Returns
        -------
        dimensions : `DimensionGroup`
            Retrieved dimensions.

        Raises
        ------
        KeyError
            Raised if the given key cannot be found in the database.
        """
        raise NotImplementedError()

    @abstractmethod
    def join(
        self,
        element_name: str,
        target: Relation,
        join: Join,
        context: queries.SqlQueryContext,
    ) -> Relation:
        """Join this dimension element's records to a relation.

        Parameters
        ----------
        element_name : `str`
            Name of the dimension element whose relation should be joined in.
        target : `~lsst.daf.relation.Relation`
            Existing relation to join to.  Implementations may require that
            this relation already include dimension key columns for this
            dimension element and assume that dataset or spatial join relations
            that might provide these will be included in the relation tree
            first.
        join : `~lsst.daf.relation.Join`
            Join operation to use when the implementation is an actual join.
            When a true join is being simulated by other relation operations,
            this objects `~lsst.daf.relation.Join.min_columns` and
            `~lsst.daf.relation.Join.max_columns` should still be respected.
        context : `.queries.SqlQueryContext`
            Object that manages relation engines and database-side state (e.g.
            temporary tables) for the query.

        Returns
        -------
        joined : `~lsst.daf.relation.Relation`
            New relation that includes this relation's dimension key and record
            columns, as well as all columns in ``target``,  with rows
            constrained to those for which this element's dimension key values
            exist in the registry and rows already exist in ``target``.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_spatial_join_relation(
        self,
        element1: str,
        element2: str,
        context: queries.SqlQueryContext,
        existing_relationships: Set[frozenset[str]] = frozenset(),
    ) -> tuple[Relation, bool]:
        """Create a relation that represents the spatial join between two
        dimension elements.

        Parameters
        ----------
        element1 : `str`
            Name of one of the elements participating in the join.
        element2 : `str`
            Name of the other element participating in the join.
        context : `.queries.SqlQueryContext`
            Object that manages relation engines and database-side state
            (e.g. temporary tables) for the query.
        existing_relationships : `~collections.abc.Set` [ `frozenset` [ `str` \
                ] ], optional
            Relationships between dimensions that are already present in the
            relation the result will be joined to.  Spatial join relations
            that duplicate these relationships will not be included in the
            result, which may cause an identity relation to be returned if
            a spatial relationship has already been established.

        Returns
        -------
        relation : `lsst.daf.relation.Relation`
            New relation that represents a spatial join between the two given
            elements.  Guaranteed to have key columns for all required
            dimensions of both elements.
        needs_refinement : `bool`
            Whether the returned relation represents a conservative join that
            needs refinement via native-iteration predicate.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_query_joiner(self, element: DimensionElement, fields: Set[str]) -> QueryJoiner:
        """Make a `..direct_query_driver.QueryJoiner` that represents a
        dimension element table.

        Parameters
        ----------
        element : `DimensionElement`
            Dimension element the table corresponds to.
        fields : `~collections.abc.Set` [ `str` ]
            Names of fields to make available in the joiner.  These can be any
            metadata or alternate key field in the element's schema, including
            the special ``region`` and ``timespan`` fields. Dimension keys in
            the element's schema are always included.

        Returns
        -------
        joiner : `..direct_query_driver.QueryJoiner`
            A query-construction object representing a table or subquery.  This
            is guaranteed to have rows that are unique over dimension keys and
            all possible key values for this dimension, so joining in a
            dimension element table:

             - never introduces duplicates into the query's result rows;
             - never restricts the query's rows *except* to ensure
               required-implied relationships are followed.
        """
        raise NotImplementedError()

    @abstractmethod
    def process_query_overlaps(
        self,
        dimensions: DimensionGroup,
        predicate: Predicate,
        join_operands: Iterable[DimensionGroup],
        calibration_dataset_types: Set[str],
    ) -> tuple[Predicate, QueryBuilder]:
        """Process a query's WHERE predicate and dimensions to handle spatial
        and temporal overlaps.

        Parameters
        ----------
        dimensions : `..dimensions.DimensionGroup`
            Full dimensions of all tables to be joined into the query (even if
            they are not included in the query results).
        predicate : `..queries.tree.Predicate`
            Boolean column expression that may contain user-provided  spatial
            and/or temporal overlaps intermixed with other constraints.
        join_operands : `~collections.abc.Iterable` [ \
                `..dimensions.DimensionGroup` ]
            Dimensions of tables or subqueries that are already going to be
            joined into the query that may establish their own spatial or
            temporal relationships (e.g. a dataset search with both ``visit``
            and ``patch`` dimensions).
        calibration_dataset_types : `~collections.abc.Set` [ `str` ]
            The names of dataset types that have been joined into the query via
            a search that includes at least one calibration collection.

        Returns
        -------
        predicate : `..queries.tree.Predicate`
            A version of the given predicate that preserves the overall
            behavior of the filter while possibly rewriting overlap expressions
            that have been partially moved into ``builder`` as some combination
            of new nested predicates, joins, and postprocessing.
        builder : `..direct_query_driver.QueryBuilder`
            A query-construction helper object that includes any initial joins
            and postprocessing needed to handle overlap expression extracted
            from the original predicate.

        Notes
        -----
        Implementations must delegate to `.queries.overlaps.OverlapsVisitor`
        (possibly by subclassing it) to ensure "automatic" spatial and temporal
        joins are added consistently by all query-construction implementations.
        """
        raise NotImplementedError()

    universe: DimensionUniverse
    """Universe of all dimensions and dimension elements known to the
    `Registry` (`DimensionUniverse`).
    """
