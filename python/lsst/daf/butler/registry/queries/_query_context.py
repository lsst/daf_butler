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

__all__ = ("QueryContext",)

from contextlib import AbstractContextManager
from typing import Any

import lsst.sphgeom
from lsst.daf.relation import (
    ColumnExpression,
    ColumnTag,
    Engine,
    EngineError,
    LeafRelation,
    Predicate,
    Processor,
    Relation,
    iteration,
)

from ...core import DataCoordinate, DimensionKeyColumnTag, SkyPixDimension, Timespan


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
            return LeafRelation.make_join_identity(self.preferred_engine)
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
        handled by delegating to `process_relation` before execution in the
        iteration engine.

        To ensure the result is a multi-pass Python collection in memory,
        ensure the given tree ends with a materialization operation in the
        iteration engine.
        """
        # This transfer does nothing if the relation is already in the
        # iteration engine.
        relation = relation.transferred_to(self.iteration_engine)
        relation = self.process(relation)
        return self.iteration_engine.execute(relation)

    def transfer(self, source: Relation, destination: Engine, materialize_as: str | None) -> Any:
        # Docstring inherited from lsst.daf.relation.Processor.
        raise NotImplementedError("No transfers expected by base QueryContext implementation.")

    def materialize(self, base: Relation, name: str) -> Any:
        # Docstring inherited from lsst.daf.relation.Processor.
        if base.engine == self.iteration_engine:
            return self.iteration_engine.execute(base).materialized()
        raise EngineError(f"Unexpected engine {base.engine} for base QueryContext implementation.")

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
        dimensions = data_coordinate.graph.required if not full else data_coordinate.graph.dimensions
        terms: list[Predicate] = []
        for dimension in dimensions:
            dtype = dimension.primaryKey.getPythonType()
            terms.append(
                ColumnExpression.reference(DimensionKeyColumnTag(dimension.name), dtype=dtype).eq(
                    ColumnExpression.literal(data_coordinate[dimension.name], dtype=dtype)
                )
            )
        return Predicate.logical_and(*terms)

    def make_spatial_region_skypix_predicate(
        self,
        dimension: SkyPixDimension,
        region: lsst.sphgeom.Region,
    ) -> Predicate:
        """Return a `~lsst.daf.relation.column_expressions.Predicate` that
        tests whether two region columns overlap

        This operation only works with `iteration engines
        <lsst.daf.relation.iteration.Engine>`; it is usually used to refine the
        result of a join on `SkyPixDimension` columns in SQL.

        Parameters
        ---------
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
        tests whether two regions overlap

        This operation only works with
        `iteration engines <lsst.daf.relation.iteration.Engine>`; it is usually
        used to refine the result of a join or constraint on `SkyPixDimension`
        columns in SQL.

        Parameters
        ---------
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
