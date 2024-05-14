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

__all__ = ("OverlapsVisitor",)

import itertools
from collections.abc import Hashable, Iterable, Sequence, Set
from typing import Generic, Literal, TypeVar, cast

from lsst.sphgeom import Region

from .._exceptions import InvalidQueryError
from .._topology import TopologicalFamily
from ..dimensions import DimensionElement, DimensionGroup
from . import tree
from .visitors import PredicateVisitFlags, SimplePredicateVisitor

_T = TypeVar("_T", bound=Hashable)


class _NaiveDisjointSet(Generic[_T]):
    """A very naive (but simple) implementation of a "disjoint set" data
    structure for strings, with mostly O(N) performance.

    This class should not be used in any context where the number of elements
    in the data structure is large.  It intentionally implements a subset of
    the interface of `scipy.cluster.DisJointSet` so that non-naive
    implementation could be swapped in if desired.

    Parameters
    ----------
    superset : `~collections.abc.Iterable` [ `str` ]
        Elements to initialize the disjoint set, with each in its own
        single-element subset.
    """

    def __init__(self, superset: Iterable[_T]):
        self._subsets = [{k} for k in superset]
        self._subsets.sort(key=len, reverse=True)

    def merge(self, a: _T, b: _T) -> bool:  # numpydoc ignore=PR04
        """Merge the subsets containing the given elements.

        Parameters
        ----------
        a :
            Element whose subset should be merged.
        b :
            Element whose subset should be merged.

        Returns
        -------
        merged : `bool`
            `True` if a merge occurred, `False` if the elements were already in
            the same subset.
        """
        for i, subset in enumerate(self._subsets):
            if a in subset:
                break
        else:
            raise KeyError(f"Merge argument {a!r} not in disjoin set {self._subsets}.")
        for j, subset in enumerate(self._subsets):
            if b in subset:
                break
        else:
            raise KeyError(f"Merge argument {b!r} not in disjoin set {self._subsets}.")
        if i == j:
            return False
        i, j = sorted((i, j))
        self._subsets[i].update(self._subsets[j])
        del self._subsets[j]
        self._subsets.sort(key=len, reverse=True)
        return True

    def subsets(self) -> Sequence[Set[_T]]:
        """Return the current subsets, ordered from largest to smallest."""
        return self._subsets

    @property
    def n_subsets(self) -> int:
        """The number of subsets."""
        return len(self._subsets)


class OverlapsVisitor(SimplePredicateVisitor):
    """A helper class for dealing with spatial and temporal overlaps in a
    query.

    Parameters
    ----------
    dimensions : `DimensionGroup`
        Dimensions of the query.

    Notes
    -----
    This class includes logic for extracting explicit spatial and temporal
    joins from a WHERE-clause predicate and computing automatic joins given the
    dimensions of the query.  It is designed to be subclassed by query driver
    implementations that want to rewrite the predicate at the same time.
    """

    def __init__(self, dimensions: DimensionGroup):
        self.dimensions = dimensions
        self._spatial_connections = _NaiveDisjointSet(self.dimensions.spatial)
        self._temporal_connections = _NaiveDisjointSet(self.dimensions.temporal)

    def run(self, predicate: tree.Predicate, join_operands: Iterable[DimensionGroup]) -> tree.Predicate:
        """Process the given predicate to extract spatial and temporal
        overlaps.

        Parameters
        ----------
        predicate : `tree.Predicate`
            Predicate to process.
        join_operands : `~collections.abc.Iterable` [ `DimensionGroup` ]
            The dimensions of logical tables being joined into this query;
            these can included embedded spatial and temporal joins that can
            make it unnecessary to add new ones.

        Returns
        -------
        predicate : `tree.Predicate`
            A possibly-modified predicate that should replace the original.
        """
        result = predicate.visit(self)
        if result is None:
            result = predicate
        for join_operand_dimensions in join_operands:
            self.add_join_operand_connections(join_operand_dimensions)
        for a, b in self.compute_automatic_spatial_joins():
            join_predicate = self.visit_spatial_join(a, b, PredicateVisitFlags.HAS_AND_SIBLINGS)
            if join_predicate is None:
                join_predicate = tree.Predicate.compare(
                    tree.DimensionFieldReference.model_construct(element=a, field="region"),
                    "overlaps",
                    tree.DimensionFieldReference.model_construct(element=b, field="region"),
                )
            result = result.logical_and(join_predicate)
        for a, b in self.compute_automatic_temporal_joins():
            join_predicate = self.visit_temporal_dimension_join(a, b, PredicateVisitFlags.HAS_AND_SIBLINGS)
            if join_predicate is None:
                join_predicate = tree.Predicate.compare(
                    tree.DimensionFieldReference.model_construct(element=a, field="timespan"),
                    "overlaps",
                    tree.DimensionFieldReference.model_construct(element=b, field="timespan"),
                )
            result = result.logical_and(join_predicate)
        return result

    def visit_comparison(
        self,
        a: tree.ColumnExpression,
        operator: tree.ComparisonOperator,
        b: tree.ColumnExpression,
        flags: PredicateVisitFlags,
    ) -> tree.Predicate | None:
        # Docstring inherited.
        if operator == "overlaps":
            if tree.is_one_timespan_and_one_datetime(a, b):
                # Can be transformed directly without special handling here.
                return None
            elif a.column_type == "region":
                return self.visit_spatial_overlap(a, b, flags)
            elif b.column_type == "timespan":
                return self.visit_temporal_overlap(a, b, flags)
            else:
                raise AssertionError(f"Unexpected column type {a.column_type} for overlap.")
        return None

    def add_join_operand_connections(self, operand_dimensions: DimensionGroup) -> None:
        """Add overlap connections implied by a table or subquery.

        Parameters
        ----------
        operand_dimensions : `DimensionGroup`
            Dimensions of of the table or subquery.

        Notes
        -----
        We assume each join operand to a `tree.Select` has its own
        complete set of spatial and temporal joins that went into generating
        its rows.  That will naturally be true for relations originating from
        the butler database, like dataset searches and materializations, and if
        it isn't true for a data ID upload, that would represent an intentional
        association between non-overlapping things that we'd want to respect by
        *not* adding a more restrictive automatic join.
        """
        for a_family, b_family in itertools.pairwise(operand_dimensions.spatial):
            self._spatial_connections.merge(a_family, b_family)
        for a_family, b_family in itertools.pairwise(operand_dimensions.temporal):
            self._temporal_connections.merge(a_family, b_family)

    def compute_automatic_spatial_joins(self) -> list[tuple[DimensionElement, DimensionElement]]:
        """Return pairs of dimension elements that should be spatially joined.

        Returns
        -------
        joins : `list` [ `tuple` [ `DimensionElement`, `DimensionElement` ] ]
            Automatic joins.

        Notes
        -----
        This takes into account explicit joins extracted by `run` and implicit
        joins added by `add_join_operand_connections`, and only returns
        additional joins if there is an unambiguous way to spatially connect
        any dimensions that are not already spatially connected. Automatic
        joins are always the most fine-grained join between sets of dimensions
        (i.e. ``visit_detector_region`` and ``patch`` instead of ``visit`` and
        ``tract``), but explicitly adding a coarser join between sets of
        elements will prevent the fine-grained join from being added.
        """
        return self._compute_automatic_joins("spatial", self._spatial_connections)

    def compute_automatic_temporal_joins(self) -> list[tuple[DimensionElement, DimensionElement]]:
        """Return pairs of dimension elements that should be spatially joined.

        Returns
        -------
        joins : `list` [ `tuple` [ `DimensionElement`, `DimensionElement` ] ]
            Automatic joins.

        Notes
        -----
        See `compute_automatic_spatial_joins` for information on how automatic
        joins are determined.  Joins to dataset validity ranges are never
        automatic.
        """
        return self._compute_automatic_joins("temporal", self._temporal_connections)

    def _compute_automatic_joins(
        self, kind: Literal["spatial", "temporal"], connections: _NaiveDisjointSet[TopologicalFamily]
    ) -> list[tuple[DimensionElement, DimensionElement]]:
        if connections.n_subsets <= 1:
            # All of the joins we need are already present.
            return []
        if connections.n_subsets > 2:
            raise InvalidQueryError(
                f"Too many disconnected sets of {kind} families for an automatic "
                f"join: {connections.subsets()}.  Add explicit {kind} joins to avoid this error."
            )
        a_subset, b_subset = connections.subsets()
        if len(a_subset) > 1 or len(b_subset) > 1:
            raise InvalidQueryError(
                f"A {kind} join is needed between {a_subset} and {b_subset}, but which join to "
                "add is ambiguous.  Add an explicit spatial join to avoid this error."
            )
        # We have a pair of families that are not explicitly or implicitly
        # connected to any other families; add an automatic join between their
        # most fine-grained members.
        (a_family,) = a_subset
        (b_family,) = b_subset
        return [
            (
                cast(DimensionElement, a_family.choose(self.dimensions.elements, self.dimensions.universe)),
                cast(DimensionElement, b_family.choose(self.dimensions.elements, self.dimensions.universe)),
            )
        ]

    def visit_spatial_overlap(
        self, a: tree.ColumnExpression, b: tree.ColumnExpression, flags: PredicateVisitFlags
    ) -> tree.Predicate | None:
        """Dispatch a spatial overlap comparison predicate to handlers.

        This method should rarely (if ever) need to be overridden.

        Parameters
        ----------
        a : `tree.ColumnExpression`
            First operand.
        b : `tree.ColumnExpression`
            Second operand.
        flags : `tree.PredicateLeafFlags`
            Information about where this overlap comparison appears in the
            larger predicate tree.

        Returns
        -------
        replaced : `tree.Predicate` or `None`
            The predicate to be inserted instead in the processed tree, or
            `None` if no substitution is needed.
        """
        match a, b:
            case tree.DimensionFieldReference(element=a_element), tree.DimensionFieldReference(
                element=b_element
            ):
                return self.visit_spatial_join(a_element, b_element, flags)
            case tree.DimensionFieldReference(element=element), region_expression:
                pass
            case region_expression, tree.DimensionFieldReference(element=element):
                pass
            case _:
                raise AssertionError(f"Unexpected arguments for spatial overlap: {a}, {b}.")
        if region := region_expression.get_literal_value():
            return self.visit_spatial_constraint(element, region, flags)
        raise AssertionError(f"Unexpected argument for spatial overlap: {region_expression}.")

    def visit_temporal_overlap(
        self, a: tree.ColumnExpression, b: tree.ColumnExpression, flags: PredicateVisitFlags
    ) -> tree.Predicate | None:
        """Dispatch a temporal overlap comparison predicate to handlers.

        This method should rarely (if ever) need to be overridden.

        Parameters
        ----------
        a : `tree.ColumnExpression`-
            First operand.
        b : `tree.ColumnExpression`
            Second operand.
        flags : `tree.PredicateLeafFlags`
            Information about where this overlap comparison appears in the
            larger predicate tree.

        Returns
        -------
        replaced : `tree.Predicate` or `None`
            The predicate to be inserted instead in the processed tree, or
            `None` if no substitution is needed.
        """
        match a, b:
            case tree.DimensionFieldReference(element=a_element), tree.DimensionFieldReference(
                element=b_element
            ):
                return self.visit_temporal_dimension_join(a_element, b_element, flags)
            case _:
                # We don't bother differentiating any other kind of temporal
                # comparison, because in all foreseeable database schemas we
                # wouldn't have to do anything special with them, since they
                # don't participate in automatic join calculations and they
                # should be straightforwardly convertible to SQL.
                return None

    def visit_spatial_join(
        self, a: DimensionElement, b: DimensionElement, flags: PredicateVisitFlags
    ) -> tree.Predicate | None:
        """Handle a spatial overlap comparison between two dimension elements.

        The default implementation updates the set of known spatial connections
        (for use by `compute_automatic_spatial_joins`) and returns `None`.

        Parameters
        ----------
        a : `DimensionElement`
            One element in the join.
        b : `DimensionElement`
            The other element in the join.
        flags : `tree.PredicateLeafFlags`
            Information about where this overlap comparison appears in the
            larger predicate tree.

        Returns
        -------
        replaced : `tree.Predicate` or `None`
            The predicate to be inserted instead in the processed tree, or
            `None` if no substitution is needed.
        """
        if a.spatial == b.spatial:
            raise InvalidQueryError(f"Spatial join between {a} and {b} is not necessary.")
        self._spatial_connections.merge(
            cast(TopologicalFamily, a.spatial), cast(TopologicalFamily, b.spatial)
        )
        return None

    def visit_spatial_constraint(
        self,
        element: DimensionElement,
        region: Region,
        flags: PredicateVisitFlags,
    ) -> tree.Predicate | None:
        """Handle a spatial overlap comparison between a dimension element and
        a literal region.

        The default implementation just returns `None`.

        Parameters
        ----------
        element : `DimensionElement`
            The dimension element in the comparison.
        region : `lsst.sphgeom.Region`
            The literal region in the comparison.
        flags : `tree.PredicateLeafFlags`
            Information about where this overlap comparison appears in the
            larger predicate tree.

        Returns
        -------
        replaced : `tree.Predicate` or `None`
            The predicate to be inserted instead in the processed tree, or
            `None` if no substitution is needed.
        """
        return None

    def visit_temporal_dimension_join(
        self, a: DimensionElement, b: DimensionElement, flags: PredicateVisitFlags
    ) -> tree.Predicate | None:
        """Handle a temporal overlap comparison between two dimension elements.

        The default implementation updates the set of known temporal
        connections (for use by `compute_automatic_temporal_joins`) and returns
        `None`.

        Parameters
        ----------
        a : `DimensionElement`
            One element in the join.
        b : `DimensionElement`
            The other element in the join.
        flags : `tree.PredicateLeafFlags`
            Information about where this overlap comparison appears in the
            larger predicate tree.

        Returns
        -------
        replaced : `tree.Predicate` or `None`
            The predicate to be inserted instead in the processed tree, or
            `None` if no substitution is needed.
        """
        if a.temporal == b.temporal:
            raise InvalidQueryError(f"Temporal join between {a} and {b} is not necessary.")
        self._temporal_connections.merge(
            cast(TopologicalFamily, a.temporal), cast(TopologicalFamily, b.temporal)
        )
        return None
