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

__all__ = ("QueryContext",)

from abc import abstractmethod
from collections.abc import Iterable, Set
from contextlib import AbstractContextManager
from typing import Any

import lsst.sphgeom
from lsst.daf.relation import (
    ColumnExpression,
    ColumnTag,
    Engine,
    EngineError,
    Predicate,
    Processor,
    Relation,
    UnaryOperation,
    iteration,
)

from ..._column_tags import DimensionKeyColumnTag
from ..._timespan import Timespan
from ...dimensions import DataCoordinate, SkyPixDimension


class QueryContext(Processor, AbstractContextManager["QueryContext"]):
    """A context manager interface for query operations that require some
    connection-like state.

    Notes
    -----
    `QueryContext` implementations are usually paired with a `QueryBackend`
    implementation, with the division of responsibilities as follows:

    - `QueryContext` implements the `lsst.daf.relation.Processor` interface,
      and is hence responsible for executing multi-engine relation trees.

    - `QueryContext` manages all state whose lifetime is a single query or set
      of related queries (e.g. temporary tables) via its context manager
      interface.  Methods that do not involve this state should not require the
      context manager to have been entered.

    - `QueryContext` objects should be easily to construct by registry helper
      code that doesn't have access to the full `Registry` data structure
      itself, while `QueryBackend` instances can generally only be constructed
      by code that does see essentially the full registry (for example,
      `SqlQueryBackend` holds a `RegistryManagerInstances` struct, while
      `SqlQueryContext` can be constructed with just a `Database` and
      `ColumnTypeInfo`).

    - `QueryBackend.context` is a factory for the associated `QueryContext`
      type.

    - `QueryBackend` methods that return relations accept the `QueryContext`
      returned by its `~QueryBackend.context` method in case those methods
      require state that should be cleaned up after the query is complete.
    """

    def __init__(self) -> None:
        self.iteration_engine = iteration.Engine()
        self.iteration_engine.functions[regions_overlap.__name__] = regions_overlap

    iteration_engine: iteration.Engine
    """The relation engine that all relations must ultimately be transferred
    to in order to be executed by this context.
    """

    @property
    def preferred_engine(self) -> Engine:
        """Return the relation engine that this context prefers to execute
        operations in (`lsst.daf.relation.Engine`).
        """
        return self.iteration_engine

    @property
    @abstractmethod
    def is_open(self) -> bool:
        """Whether the context manager has been entered (`bool`)."""
        raise NotImplementedError()

    def make_initial_relation(self, relation: Relation | None = None) -> Relation:
        """Construct an initial relation suitable for this context.

        Parameters
        ----------
        relation : `Relation`, optional
            A user-provided initial relation.  Must be included by
            implementations when provided, but may be modified (e.g. by adding
            a transfer to a new engine) when need to satisfy the context's
            invariants.
        """
        if relation is None:
            return self.preferred_engine.make_join_identity_relation()
        return relation

    def fetch_iterable(self, relation: Relation) -> iteration.RowIterable:
        """Execute the given relation and return its rows as an iterable of
        mappings.

        Parameters
        ----------
        relation : `Relation`
            Relation representing the query to execute.

        Returns
        -------
        rows : `~lsst.daf.relation.iteration.RowIterable`
            An iterable over rows, with each row a mapping from `ColumnTag`
            to column value.

        Notes
        -----
        A transfer to `iteration_engine` will be added to the root (end) of the
        relation tree if the root is not already in the iteration engine.

        Any transfers from other engines or persistent materializations will be
        handled by delegating to `process` before execution in the iteration
        engine.

        To ensure the result is a multi-pass Python collection in memory,
        ensure the given tree ends with a materialization operation in the
        iteration engine.
        """
        # This transfer does nothing if the relation is already in the
        # iteration engine.
        relation = relation.transferred_to(self.iteration_engine)
        relation = self.process(relation)
        return self.iteration_engine.execute(relation)

    @abstractmethod
    def count(self, relation: Relation, *, exact: bool = True, discard: bool = False) -> int:
        """Count the number of rows in the given relation.

        Parameters
        ----------
        relation : `Relation`
            Relation whose rows are to be counted.
        exact : `bool`, optional
            If `True` (default), return the exact number of rows.  If `False`,
            returning an upper bound is permitted if it can be done much more
            efficiently, e.g. by running a SQL ``SELECT COUNT(*)`` query but
            ignoring client-side filtering that would otherwise take place.
        discard : `bool`, optional
            If `True`, compute the exact count even if it would require running
            the full query and then throwing away the result rows after
            counting them.  If `False`, this is an error, as the user would
            usually be better off executing the query first to fetch its rows
            into a new query (or passing ``exact=False``).  Ignored if
            ``exact=False``.

        Returns
        -------
        n_rows : `int`
            Number of rows in the relation, or an upper bound.  This includes
            duplicates, if there are any.

        Raises
        ------
        RuntimeError
            Raised if an exact count was requested and could not be obtained
            without fetching and discarding rows.
        """
        raise NotImplementedError()

    @abstractmethod
    def any(self, relation: Relation, *, execute: bool = True, exact: bool = True) -> bool:
        """Check whether this relation has any result rows at all.

        Parameters
        ----------
        relation : `Relation`
            Relation to be checked.
        execute : `bool`, optional
            If `True`, execute at least a ``LIMIT 1`` query if it cannot be
            determined prior to execution that the query would return no rows.
        exact : `bool`, optional
            If `True`, run the full query and perform post-query filtering if
            needed, until at least one result row is found.  If `False`, the
            returned result does not account for post-query filtering, and
            hence may be `True` even when all result rows would be filtered
            out.

        Returns
        -------
        any_rows : `bool`
            Whether the relation has any rows, or if it may have any rows if
            ``exact=False``.

        Raises
        ------
        RuntimeError
            Raised if an exact check was requested and could not be obtained
            without executing the query.
        """
        raise NotImplementedError()

    def transfer(self, source: Relation, destination: Engine, materialize_as: str | None) -> Any:
        # Docstring inherited from lsst.daf.relation.Processor.
        raise NotImplementedError("No transfers expected by base QueryContext implementation.")

    def materialize(self, base: Relation, name: str) -> Any:
        # Docstring inherited from lsst.daf.relation.Processor.
        if base.engine == self.iteration_engine:
            return self.iteration_engine.execute(base).materialized()
        raise EngineError(f"Unexpected engine {base.engine} for base QueryContext implementation.")

    @abstractmethod
    def restore_columns(
        self,
        relation: Relation,
        columns_required: Set[ColumnTag],
    ) -> tuple[Relation, set[ColumnTag]]:
        """Return a modified relation tree that attempts to restore columns
        that were dropped by a projection operation.

        Parameters
        ----------
        relation : `Relation`
            Original relation tree.
        columns_required : `~collections.abc.Set` [ `ColumnTag` ]
            Columns to attempt to restore.  May include columns already
            present in the relation.

        Returns
        -------
        modified : `Relation`
            Possibly-modified tree with any projections that had dropped
            requested columns replaced by projections that do not drop these
            columns.  Care is taken to ensure that join common columns and
            deduplication behavior is preserved, even if that means some
            columns are not restored.
        columns_found : `set` [ `ColumnTag` ]
            Columns from those requested that are present in ``modified``.
        """
        raise NotImplementedError()

    @abstractmethod
    def strip_postprocessing(self, relation: Relation) -> tuple[Relation, list[UnaryOperation]]:
        """Return a modified relation tree without any iteration-engine
        operations and any transfer to the iteration engine at the end.

        Parameters
        ----------
        relation : `Relation`
            Original relation tree.

        Returns
        -------
        modified : `Relation`
            Stripped relation tree, with engine != `iteration_engine`.
        stripped : `UnaryOperation`
            Operations that were stripped, in the same order they should be
            reapplied (with ``transfer=True,
            preferred_engine=iteration_engine``) to recover the original tree.
        """
        raise NotImplementedError()

    @abstractmethod
    def drop_invalidated_postprocessing(self, relation: Relation, new_columns: Set[ColumnTag]) -> Relation:
        """Return a modified relation tree without iteration-engine operations
        that require columns that are not in the given set.

        Parameters
        ----------
        relation : `Relation`
            Original relation tree.
        new_columns : `~collections.abc.Set` [ `ColumnTag` ]
            The only columns that postprocessing operations may require if they
            are to be retained in the returned relation tree.

        Returns
        -------
        modified : `Relation`
            Modified relation tree with postprocessing operations incompatible
            with ``new_columns`` removed.
        """
        raise NotImplementedError()

    def make_data_coordinate_predicate(
        self,
        data_coordinate: DataCoordinate,
        full: bool | None = None,
    ) -> Predicate:
        """Return a `~lsst.daf.relation.column_expressions.Predicate` that
        represents a data ID constraint.

        Parameters
        ----------
        data_coordinate : `DataCoordinate`
            Data ID whose keys and values should be transformed to predicate
            equality constraints.
        full : `bool`, optional
            Whether to include constraints on implied dimensions (default is to
            include implied dimensions if ``data_coordinate`` has them).

        Returns
        -------
        predicate : `lsst.daf.relation.column_expressions.Predicate`
            New predicate
        """
        if full is None:
            full = data_coordinate.hasFull()
        dimension_names = (
            data_coordinate.required if not full else data_coordinate.dimensions.data_coordinate_keys
        )
        terms: list[Predicate] = []
        for dimension_name in dimension_names:
            dimension = data_coordinate.universe.dimensions[dimension_name]
            dtype = dimension.primaryKey.getPythonType()
            terms.append(
                ColumnExpression.reference(DimensionKeyColumnTag(dimension_name), dtype=dtype).eq(
                    ColumnExpression.literal(data_coordinate[dimension_name], dtype=dtype)
                )
            )
        return Predicate.logical_and(*terms)

    def make_spatial_region_skypix_predicate(
        self,
        dimension: SkyPixDimension,
        region: lsst.sphgeom.Region,
    ) -> Predicate:
        """Return a `~lsst.daf.relation.column_expressions.Predicate` that
        tests whether two region columns overlap.

        This operation only works with `iteration engines
        <lsst.daf.relation.iteration.Engine>`; it is usually used to refine the
        result of a join on `SkyPixDimension` columns in SQL.

        Parameters
        ----------
        dimension : `SkyPixDimension`
            Dimension whose key column is being constrained.
        region : `lsst.sphgeom.Region`
            Spatial region constraint to test against.

        Returns
        -------
        predicate : `lsst.daf.relation.column_expressions.Predicate`
            New predicate with the `DimensionKeyColumn` associated with
            ``dimension`` as its only required column.
        """
        ref = ColumnExpression.reference(DimensionKeyColumnTag(dimension.name), dtype=int)
        terms: list[Predicate] = []
        for begin, end in dimension.pixelization.envelope(region):
            if begin + 1 == end:
                terms.append(ref.eq(ColumnExpression.literal(begin, dtype=int)))
            else:
                terms.append(
                    ref.ge(ColumnExpression.literal(begin, dtype=int)).logical_and(
                        ref.lt(ColumnExpression.literal(end, dtype=int))
                    )
                )
        return Predicate.logical_or(*terms)

    def make_spatial_region_overlap_predicate(
        self,
        lhs: ColumnExpression,
        rhs: ColumnExpression,
    ) -> Predicate:
        """Return a `~lsst.daf.relation.column_expressions.Predicate` that
        tests whether two regions overlap.

        This operation only works with
        `iteration engines <lsst.daf.relation.iteration.Engine>`; it is usually
        used to refine the result of a join or constraint on `SkyPixDimension`
        columns in SQL.

        Parameters
        ----------
        lhs : `lsst.daf.relation.column_expressions.ColumnExpression`
            Expression for one spatial region.
        rhs : `lsst.daf.relation.column_expressions.ColumnExpression`
            Expression for the other spatial region.

        Returns
        -------
        predicate : `lsst.daf.relation.column_expressions.Predicate`
            New predicate with ``lhs`` and ``rhs`` as its required columns.
        """
        return lhs.predicate_method(regions_overlap.__name__, rhs, supporting_engine_types={iteration.Engine})

    def make_timespan_overlap_predicate(
        self,
        tag: ColumnTag,
        timespan: Timespan,
    ) -> Predicate:
        """Return a `~lsst.daf.relation.column_expressions.Predicate` that
        tests whether a timespan column overlaps a timespan literal.

        Parameters
        ----------
        tag : `ColumnTag`
            Identifier for a timespan column.
        timespan : `Timespan`
            Timespan literal selected rows must overlap.

        Returns
        -------
        predicate : `lsst.daf.relation.column_expressions.Predicate`
            New predicate.
        """
        return ColumnExpression.reference(tag, dtype=Timespan).predicate_method(
            "overlaps", ColumnExpression.literal(timespan)
        )

    def make_data_id_relation(
        self, data_ids: Set[DataCoordinate], dimension_names: Iterable[str]
    ) -> Relation:
        """Transform a set of data IDs into a relation.

        Parameters
        ----------
        data_ids : `~collections.abc.Set` [ `DataCoordinate` ]
            Data IDs to upload.  All must have at least the dimensions given,
            but may have more.
        dimension_names : `~collections.abc.Iterable` [ `str` ]
            Names of dimensions that will be the columns of the relation.

        Returns
        -------
        relation : `Relation`
            Relation in the iteration engine.
        """
        tags = DimensionKeyColumnTag.generate(dimension_names)
        payload = iteration.RowSequence(
            [{tag: data_id[tag.dimension] for tag in tags} for data_id in data_ids]
        ).to_mapping(tags)
        return self.iteration_engine.make_leaf(frozenset(tags), payload, name_prefix="upload")


def regions_overlap(a: lsst.sphgeom.Region, b: lsst.sphgeom.Region) -> bool:
    """Test whether a pair of regions overlap.

    Parameters
    ----------
    a : `lsst.sphgeom.Region`
        One region.
    b : `lsst.sphgeom.Region`
        The other region.

    Returns
    -------
    overlap : `bool`
        Whether the regions overlap.
    """
    return not (a.relate(b) & lsst.sphgeom.DISJOINT)
