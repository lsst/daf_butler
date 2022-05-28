# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

__all__ = ("QueryBuilder",)

from typing import Any, Iterable

import sqlalchemy.sql

from ...core import DatasetType, DimensionElement, SimpleQuery, SkyPixDimension, sql
from .._exceptions import DataIdValueError, MissingSpatialOverlapError
from .._query_backend import QueryBackend
from ..interfaces import DatabaseDimensionRecordStorage, GovernorDimensionRecordStorage
from ..wildcards import CollectionSearch, CollectionWildcard
from ._query import DirectQuery, DirectQueryUniqueness, EmptyQuery, Query
from ._structs import QueryColumns, QuerySummary


class QueryBuilder:
    """A builder for potentially complex queries that join tables based
    on dimension relationships.

    Parameters
    ----------
    summary : `QuerySummary`
        Struct organizing the dimensions involved in the query.
    backend : `QueryBackend`
        Backend object that represents the `Registry` implementation.
    doomed_by : `Iterable` [ `str` ], optional
        A list of messages (appropriate for e.g. logging or exceptions) that
        explain why the query is known to return no results even before it is
        executed.  Queries with a non-empty list will never be executed.
    """

    def __init__(
        self,
        summary: QuerySummary,
        backend: QueryBackend,
        doomed_by: Iterable[str] = (),
    ):
        self.summary = summary
        self._backend = backend
        doomed_by = list(doomed_by)
        self.relation = (
            backend.unit_relation
            if not doomed_by
            else backend.make_doomed_relation(*doomed_by, columns=frozenset())
        )

        self._validateGovernors()

    def _validateGovernors(self) -> None:
        """Check that governor dimensions specified by query actually exist.

        This helps to avoid mistakes in governor values. It also implements
        consistent failure behavior for cases when governor dimensions are
        specified in either DataId ow WHERE clause.

        Raises
        ------
        DataIdValueError
            Raised when governor dimension values are not found.
        """
        for dimension, bounds in self.summary.where.constraints.dimensions.items():
            storage = self._backend.managers.dimensions[self.summary.requested.universe[dimension]]
            if isinstance(storage, GovernorDimensionRecordStorage):
                try:
                    bounds.with_concrete_bounds(storage.values)
                except LookupError as err:
                    raise DataIdValueError(
                        f"Unknown values specified for governor dimension {dimension}: {set(err.args[0])}."
                    ) from None

    def joinDimensionElement(self, element: DimensionElement) -> None:
        """Add the table for a `DimensionElement` to the query.

        This automatically joins the element table to all other tables in the
        query with which it is related, via both dimension keys and spatial
        and temporal relationships.

        External calls to this method should rarely be necessary; `finish` will
        automatically call it if the `DimensionElement` has been identified as
        one that must be included.

        Parameters
        ----------
        element : `DimensionElement`
            Element for which a table should be added.  The element must be
            associated with a database table (see `DimensionElement.hasTable`).
        """
        storage = self._backend.managers.dimensions[element]
        columns: set[str] = set()
        if self.summary.where.expression_predicate is not None:
            columns.update(
                self.summary.where.expression_predicate.columns_required.dimension_records.get(
                    element.name, ()
                )
            )
        if self.summary.order_by is not None:
            columns.update(self.summary.order_by.columns_required.dimension_records.get(element.name, ()))
        if element in self.summary.spatial:
            columns.add("region")
        if element in self.summary.temporal:
            columns.add("timespan")
        self.relation = storage.join(self.relation, columns)

    def joinDataset(
        self, datasetType: DatasetType, collections: Any, *, isResult: bool = True, findFirst: bool = False
    ) -> bool:
        """Add a dataset search or constraint to the query.

        Unlike other `QueryBuilder` join methods, this *must* be called
        directly to search for datasets of a particular type or constrain the
        query results based on the exists of datasets.  However, all dimensions
        used to identify the dataset type must have already been included in
        `QuerySummary.requested` when initializing the `QueryBuilder`.

        Parameters
        ----------
        datasetType : `DatasetType`
            The type of datasets to search for.
        collections : `Any`
            An expression that fully or partially identifies the collections
            to search for datasets, such as a `str`, `re.Pattern`, or iterable
            thereof.  `...` can be used to return all collections. See
            :ref:`daf_butler_collection_expressions` for more information.
        isResult : `bool`, optional
            If `True` (default), include the dataset ID column in the
            result columns of the query, allowing complete `DatasetRef`
            instances to be produced from the query results for this dataset
            type.  If `False`, the existence of datasets of this type is used
            only to constrain the data IDs returned by the query.
            `joinDataset` may be called with ``isResult=True`` at most one time
            on a particular `QueryBuilder` instance.
        findFirst : `bool`, optional
            If `True` (`False` is default), only include the first match for
            each data ID, searching the given collections in order.  Requires
            that all entries in ``collections`` be regular strings, so there is
            a clear search order.  Ignored if ``isResult`` is `False`.

        Returns
        -------
        anyRecords : `bool`
            If `True`, joining the dataset table was successful and the query
            should proceed.  If `False`, we were able to determine (from the
            combination of ``datasetType`` and ``collections``) that there
            would be no results joined in from this dataset, and hence (due to
            the inner join that would normally be present), the full query will
            return no results.
        """
        assert datasetType in self.summary.datasets
        if isResult and findFirst:
            collections = CollectionSearch.fromExpression(collections)
        else:
            collections = CollectionWildcard.fromExpression(collections)
        rejections: list[str] = []
        collection_records = self._backend.resolve_dataset_collections(
            datasetType,
            collections,
            constraints=self.summary.where.constraints,
            rejections=rejections,
            allow_calibration_collections=(
                not findFirst and not (self.summary.temporal or self.summary.mustHaveKeysJoined.temporal)
            ),
        )
        columns_requested = {"dataset_id", "run", "ingest_date"} if isResult else frozenset()
        if not collection_records:
            relation = self._backend.make_doomed_dataset_relation(datasetType, columns_requested, rejections)
        elif isResult and findFirst:
            relation = self._backend.make_dataset_search_relation(
                datasetType,
                collection_records,
                columns_requested,
                constraints=self.summary.where.constraints,
            )
        else:
            relation = self._backend.make_dataset_query_relation(
                datasetType,
                collection_records,
                columns_requested,
                constraints=self.summary.where.constraints,
            )
        self.relation = self.relation.join(relation)
        return not relation.doomed_by

    def _joinMissingDimensionElements(self) -> None:
        """Join all dimension element tables that were identified as necessary
        by `QuerySummary` and have not yet been joined.

        For internal use by `QueryBuilder` only; will be called (and should
        only by called) by `finish`.
        """
        # Join all DimensionElement tables that we need for spatial/temporal
        # joins/filters or a nontrivial WHERE expression.
        # We iterate over these in *reverse* topological order to minimize the
        # number of tables joined.  For example, the "visit" table provides
        # the primary key value for the "instrument" table it depends on, so we
        # don't need to join "instrument" as well unless we had a nontrivial
        # expression on it (and hence included it already above).
        for element in self.summary.universe.sorted(self.summary.mustHaveTableJoined, reverse=True):
            self.joinDimensionElement(element)
        # Join in any requested Dimension tables that don't already have their
        # primary keys identified by the query.
        for dimension in self.summary.universe.sorted(self.summary.mustHaveKeysJoined, reverse=True):
            if dimension.name not in self.relation.columns.dimensions:
                self.joinDimensionElement(dimension)

    def _addSpatialJoins(self) -> list[sql.Postprocessor]:
        if len(self.summary.spatial) > 2:
            # This flat set isn't even the right data structure for capturing
            # more than one spatial join; we'd need a set of pairs (and get
            # those pairs from the user).
            raise NotImplementedError("Multiple spatial joins are not supported.")
        if len(self.summary.spatial) < 2:
            # Nothing to do; this isn't a join, and spatial constraints are
            # handled in _addWhereClause.
            return []
        (element1, element2) = self.summary.spatial
        storage1 = self._backend.managers.dimensions[element1]
        storage2 = self._backend.managers.dimensions[element2]
        overlaps: sql.Relation | None = None
        postprocessors: list[sql.Postprocessor] = []
        match (storage1, storage2):
            case [
                DatabaseDimensionRecordStorage() as db_storage1,
                DatabaseDimensionRecordStorage() as db_storage2,
            ]:
                # Manager classes guarantee that we only need to try this in
                # one direction; both storage objects know about the other or
                # neither do.
                overlaps = db_storage1.get_spatial_join_relation(
                    db_storage2.element, self.relation.constraints
                )
                if overlaps is None:
                    common_skypix_overlap1 = db_storage1.get_spatial_join_relation(
                        self._backend.managers.dimensions.universe.commonSkyPix, self.relation.constraints
                    )
                    common_skypix_overlap2 = db_storage2.get_spatial_join_relation(
                        self._backend.managers.dimensions.universe.commonSkyPix, self.relation.constraints
                    )
                    assert (
                        common_skypix_overlap1 is not None and common_skypix_overlap2 is not None
                    ), "Overlaps with the common skypix dimension should always be available,"
                    overlaps = common_skypix_overlap1.join(common_skypix_overlap2)
                    postprocessors.append(
                        sql.Postprocessor.from_spatial_join(
                            sql.DimensionRecordColumnTag(element1.name, "region"),
                            sql.DimensionRecordColumnTag(element2.name, "region"),
                        )
                    )
            case [DatabaseDimensionRecordStorage() as db_storage, other]:
                overlaps = db_storage.get_spatial_join_relation(other.element, self.relation.constraints)
            case [other, DatabaseDimensionRecordStorage() as db_storage]:
                overlaps = db_storage.get_spatial_join_relation(other.element, self.relation.constraints)
        if overlaps is None:
            raise MissingSpatialOverlapError(
                f"No materialized overlaps for spatial join {self.summary.spatial}."
            )
        self.relation = self.relation.join(overlaps)
        return postprocessors

    def _addWhereClause(self) -> list[sql.Postprocessor]:
        """Add a WHERE clause to the query under construction, connecting all
        joined dimensions to the expression and data ID dimensions from
        `QuerySummary`.

        For internal use by `QueryBuilder` only; will be called (and should
        only by called) by `finish`.
        """
        # Append WHERE clause terms from predicates.
        predicates: list[sql.Predicate] = []
        postprocessors: list[sql.Postprocessor] = []
        if self.summary.where.expression_predicate is not None:
            predicates.append(self.summary.where.expression_predicate)
        if self.summary.where.data_id:
            known_dimensions = self.summary.where.data_id.graph.intersection(self.summary.mustHaveKeysJoined)
            known_data_id = self.summary.where.data_id.subset(known_dimensions)
            predicates.append(sql.Predicate.from_data_coordinate(known_data_id))
        if self.summary.where.spatial_constraint is not None:
            for dimension_name in self.relation.columns.dimensions:
                dimension = self._backend.managers.column_types.universe[dimension_name]
                if dimension_name not in self.summary.where.data_id.graph.names and isinstance(
                    dimension, SkyPixDimension
                ):
                    predicates.append(
                        sql.Predicate.from_spatial_constraint(
                            self.summary.where.spatial_constraint, dimension
                        )
                    )
            for region_column in self.relation.columns.get_spatial_regions():
                postprocessors.append(
                    sql.Postprocessor.from_spatial_constraint(
                        self.summary.where.spatial_constraint, region_column
                    )
                )
        if self.summary.where.temporal_constraint is not None:
            for element_name, column_names in self.relation.columns.dimension_records.items():
                if "timespan" in column_names and element_name not in self.summary.where.data_id.graph.names:
                    predicates.append(
                        sql.Predicate.from_temporal_constraint(
                            self.summary.where.temporal_constraint,
                            sql.DimensionRecordColumnTag(element_name, "timespan"),
                        )
                    )
        self.relation = self.relation.selected(*predicates)
        return postprocessors

    def finish(self, joinMissing: bool = True) -> Query:
        """Finish query constructing, returning a new `Query` instance.

        Parameters
        ----------
        joinMissing : `bool`, optional
            If `True` (default), automatically join any missing dimension
            element tables (according to the categorization of the
            `QuerySummary` the builder was constructed with).  `False` should
            only be passed if the caller can independently guarantee that all
            dimension relationships are already captured in non-dimension
            tables that have been manually included in the query.

        Returns
        -------
        query : `Query`
            A `Query` object that can be executed and used to interpret result
            rows.
        """
        postprocessors: list[sql.Postprocessor] = []
        if joinMissing:
            postprocessors.extend(self._addSpatialJoins())
            self._joinMissingDimensionElements()
        postprocessors.extend(self._addWhereClause())
        self.relation = self.relation.postprocessed(*postprocessors)
        if not self.relation.columns:
            return EmptyQuery(
                self.summary.requested.universe,
                backend=self._backend,
                doomed_by=self.relation.doomed_by,
            )
        sql_from, columns_available, where = self.relation.to_sql_parts()
        simple_query = SimpleQuery()
        simple_query.join(sql_from)
        simple_query.where.extend(where)
        if columns_available is None:
            columns_available = sql.ColumnTag.extract_logical_column_mapping(
                self.relation.columns, sql_from.columns, self._backend.managers.column_types
            )
        old_columns = QueryColumns.from_logical_columns(
            columns_available, self.summary.datasets, self._backend.managers.column_types
        )
        order_by: list[sqlalchemy.sql.ColumnElement] = []
        if self.summary.order_by is not None:
            order_by.extend(self.summary.order_by.to_sql_columns(columns_available))
        return DirectQuery(
            graph=self.summary.requested,
            uniqueness=DirectQueryUniqueness.NOT_UNIQUE,
            spatial_constraint=self.summary.where.spatial_constraint,
            simpleQuery=simple_query,
            columns=old_columns,
            order_by_columns=order_by,
            limit=self.summary.limit,
            backend=self._backend,
            doomed_by=self.relation.doomed_by,
        )
