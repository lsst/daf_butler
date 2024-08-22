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

__all__ = ("QueryBuilder",)

import itertools
from typing import Any

from lsst.daf.relation import ColumnExpression, ColumnTag, Diagnostics, Relation

from ..._column_categorization import ColumnCategorization
from ..._column_tags import DatasetColumnTag, DimensionKeyColumnTag, DimensionRecordColumnTag
from ..._dataset_type import DatasetType
from ..wildcards import CollectionWildcard
from ._query import Query
from ._query_backend import QueryBackend
from ._query_context import QueryContext
from ._structs import QuerySummary


class QueryBuilder:
    """A builder for potentially complex queries that join tables based
    on dimension relationships.

    Parameters
    ----------
    summary : `QuerySummary`
        Struct organizing the dimensions involved in the query.
    backend : `QueryBackend`
        Backend object that represents the `Registry` implementation.
    context : `QueryContext`, optional
        Object that manages relation engines and database-side state (e.g.
        temporary tables) for the query.  Must have been created by
        ``backend.context()``, which is used if ``context`` is not provided.
    relation : `~lsst.daf.relation.Relation`, optional
        Initial relation for the query.
    """

    def __init__(
        self,
        summary: QuerySummary,
        backend: QueryBackend,
        context: QueryContext | None = None,
        relation: Relation | None = None,
    ):
        self.summary = summary
        self._backend = backend
        self._context = backend.context() if context is None else context
        self.relation = self._context.make_initial_relation(relation)
        self._governor_constraints = self._backend.resolve_governor_constraints(
            self.summary.dimensions, self.summary.where.governor_constraints
        )

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
        collections = CollectionWildcard.from_expression(collections)
        if isResult and findFirst:
            collections.require_ordered()
        rejections: list[str] = []
        collection_records = self._backend.resolve_dataset_collections(
            datasetType,
            collections,
            governor_constraints=self._governor_constraints,
            rejections=rejections,
            allow_calibration_collections=(not findFirst and not self.summary.dimensions.temporal),
        )
        columns_requested = {"dataset_id", "run", "ingest_date"} if isResult else frozenset()
        if not collection_records:
            relation = self._backend.make_doomed_dataset_relation(
                datasetType, columns_requested, rejections, self._context
            )
        elif isResult and findFirst:
            relation = self._backend.make_dataset_search_relation(
                datasetType,
                collection_records,
                columns_requested,
                self._context,
            )
        else:
            relation = self._backend.make_dataset_query_relation(
                datasetType,
                collection_records,
                columns_requested,
                self._context,
            )
        self.relation = self.relation.join(relation)
        return not Diagnostics.run(relation).is_doomed

    def _addWhereClause(self, categorized_columns: ColumnCategorization) -> None:
        """Add a WHERE clause to the query under construction, connecting all
        joined dimensions to the expression and data ID dimensions from
        `QuerySummary`.

        For internal use by `QueryBuilder` only; will be called (and should
        only by called) by `finish`.

        Parameters
        ----------
        categorized_columns : `ColumnCategorization`
            Struct that organizes the columns in ``self.relation`` by
            `ColumnTag` type.
        """
        # Append WHERE clause terms from predicates.
        if self.summary.where.expression_predicate is not None:
            self.relation = self.relation.with_rows_satisfying(
                self.summary.where.expression_predicate,
                preferred_engine=self._context.preferred_engine,
                require_preferred_engine=True,
            )
        if self.summary.where.data_id:
            known_dimensions = self.summary.where.data_id.dimensions.intersection(self.summary.dimensions)
            known_data_id = self.summary.where.data_id.subset(known_dimensions)
            self.relation = self.relation.with_rows_satisfying(
                self._context.make_data_coordinate_predicate(known_data_id),
                preferred_engine=self._context.preferred_engine,
                require_preferred_engine=True,
            )
        if self.summary.region is not None:
            for skypix_dimension in categorized_columns.filter_skypix(self._backend.universe):
                self.relation = self.relation.with_rows_satisfying(
                    self._context.make_spatial_region_skypix_predicate(
                        skypix_dimension,
                        self.summary.region,
                    ),
                    preferred_engine=self._context.preferred_engine,
                    require_preferred_engine=True,
                )
            for element in categorized_columns.filter_spatial_region_dimension_elements():
                self.relation = self.relation.with_rows_satisfying(
                    self._context.make_spatial_region_overlap_predicate(
                        ColumnExpression.reference(DimensionRecordColumnTag(element, "region")),
                        ColumnExpression.literal(self.summary.region),
                    ),
                    preferred_engine=self._context.iteration_engine,
                    transfer=True,
                )

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
        if joinMissing:
            spatial_joins = []
            for family1, family2 in itertools.combinations(self.summary.dimensions.spatial, 2):
                spatial_joins.append(
                    (
                        family1.choose(self.summary.dimensions).name,
                        family2.choose(self.summary.dimensions).name,
                    )
                )
            self.relation = self._backend.make_dimension_relation(
                self.summary.dimensions,
                columns=self.summary.columns_required,
                context=self._context,
                spatial_joins=spatial_joins,
                initial_relation=self.relation,
                governor_constraints=self._governor_constraints,
            )
        categorized_columns = ColumnCategorization.from_iterable(self.relation.columns)
        self._addWhereClause(categorized_columns)
        query = Query(
            self.summary.dimensions,
            self._backend,
            context=self._context,
            relation=self.relation,
            governor_constraints=self._governor_constraints,
            is_deferred=True,
            has_record_columns=False,
        )
        if self.summary.order_by is not None:
            query = query.sorted(self.summary.order_by.terms)
        if self.summary.limit is not None:
            query = query.sliced(
                start=self.summary.limit[0],
                stop=(
                    self.summary.limit[0] + self.summary.limit[1]
                    if self.summary.limit[1] is not None
                    else None
                ),
            )
        projected_columns: set[ColumnTag] = set()
        projected_columns.update(DimensionKeyColumnTag.generate(self.summary.requested.names))
        for dataset_type in self.summary.datasets:
            for dataset_column_name in ("dataset_id", "run"):
                tag = DatasetColumnTag(dataset_type.name, dataset_column_name)
                if tag in self.relation.columns:
                    projected_columns.add(tag)
        return query.projected(
            dimensions=self.summary.requested,
            columns=projected_columns,
            drop_postprocessing=False,
            unique=False,
        )
