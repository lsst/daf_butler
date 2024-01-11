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

from .._topology import TopologicalFamily
from ..dimensions import DimensionElement, DimensionGroup
from . import relation_tree as rt

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

    def add(self, k: _T) -> bool:  # numpydoc ignore=PR04
        """Add a new element as its own single-element subset unless it is
        already present.

        Parameters
        ----------
        k
            Value to add.

        Returns
        -------
        added : `bool`:
            `True` if the value was actually added, `False` if it was already
            present.
        """
        for subset in self._subsets:
            if k in subset:
                return False
        self._subsets.append({k})
        return True

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


class OverlapsVisitor:
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

    def run(self, predicate: rt.Predicate, join_operands: Iterable[DimensionGroup]) -> rt.Predicate:
        result = self.process_predicate(predicate, frozenset())
        for join_operand_dimensions in join_operands:
            self.add_join_operand_connections(join_operand_dimensions)
        for a, b in self.compute_automatic_spatial_joins():
            result = result.logical_and(
                self.visit_spatial_join(
                    rt.Comparison(
                        a=rt.DimensionFieldReference(element=a, field="region"),
                        b=rt.DimensionFieldReference(element=b, field="region"),
                        operator="overlaps",
                    ),
                    a=a,
                    b=b,
                    parents=frozenset(),
                )
            )
        for a, b in self.compute_automatic_temporal_joins():
            result = result.logical_and(
                self.visit_temporal_dimension_join(
                    rt.Comparison(
                        a=rt.DimensionFieldReference(element=a, field="timespan"),
                        b=rt.DimensionFieldReference(element=b, field="timespan"),
                        operator="overlaps",
                    ),
                    a=a,
                    b=b,
                    parents=frozenset(),
                )
            )
        return result

    def process_predicate(
        self, predicate: rt.Predicate, parents: Set[Literal["and", "or", "not"]]
    ) -> rt.Predicate:
        """Process a WHERE-clause predicate.

        This method traverses a `relation_tree.Predicate` tree and calls the
        various ``visit_*`` methods when it encounters a spatial or temporal
        overlap expression.

        Parameters
        ----------
        predicate : `relation_tree.Predicate`
            Tree to process.
        parents : `~collections.abc.Set` of "and", "or", and "not" literals
            What kinds of logical operators are used to connect this predicate
            to others, to be modified and forwarded to ``visit_*`` methods.

        Returns
        -------
        predicate : `relation_tree.Predicate`
            Processed predicate, with overlap comparisons replaced according to
            the return values of the ``visit_*`` methods.
        """
        match predicate:
            case rt.LogicalAnd() | rt.LogicalOr():
                processed: list[rt.Predicate] = []
                any_changed = False
                parents |= {predicate.predicate_type}
                for operand in predicate.operands:
                    if (new_operand := self.process_predicate(operand, parents)) is not operand:
                        any_changed = True
                    processed.append(new_operand)
                if any_changed:
                    return type(predicate).fold(*processed)
                else:
                    return predicate
            case rt.LogicalNot():
                if (
                    new_operand := self.process_predicate(predicate.operand, parents | {"not"})
                ) is not predicate.operand:
                    return new_operand.logical_not()
                else:
                    return predicate
            case rt.Comparison(operator="overlaps"):
                if predicate.a.column_type == "region":
                    return self.visit_spatial_overlap(predicate, parents)
                elif predicate.b.column_type == "timespan":
                    return self.visit_temporal_overlap(predicate, parents)
                else:
                    raise AssertionError(f"Unexpected column type {predicate.a.column_type} for overlap.")
            case _:
                return predicate

    def add_join_operand_connections(self, operand_dimensions: DimensionGroup) -> None:
        """Add overlap connections implied by a table or subquery.

        Parameters
        ----------
        operand_dimensions : `DimensionGroup`
            Dimensions of of the table or subquery.

        Notes
        -----
        We assume each join operand to a `relation_tree.Select` has its own
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
        This takes into account explicit joins extracted by `process` and
        implicit joins added by `add_join_operand_connections`, and only
        returns additional joins if there is an unambiguous way to spatially
        connect any dimensions that are not already spatially connected.
        Automatic joins are always the most fine-grained join between sets of
        dimensions (i.e. ``visit_detector_region`` and ``patch`` instead of
        ``visit`` and ``tract``), but explicitly adding a coarser join between
        sets of elements will prevent the fine-grained join from being added.
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
        if connections.n_subsets == 1:
            # All of the joins we need are already present.
            return []
        if connections.n_subsets > 2:
            raise rt.InvalidRelationError(
                f"Too many disconnected sets of {kind} families for an automatic "
                f"join: {connections.subsets()}.  Add explicit {kind} joins to avoid this error."
            )
        a_subset, b_subset = connections.subsets()
        if len(a_subset) > 1 or len(b_subset) > 1:
            raise rt.InvalidRelationError(
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
        self,
        comparison: rt.Comparison,
        parents: Set[Literal["and", "or", "not"]],
    ) -> rt.Predicate:
        """Dispatch a spatial overlap comparison predicate to handlers.

        This method should rarely (if ever) need to be overridden.

        Parameters
        ----------
        comparison : `relation_tree.Comparison`
            Predicate object to handle.
        parents : `~collections.abc.Set` of "and", "or", and "not" literals
            What kinds of logical operators are used to connect this predicate
            to others, to be forwarded to other ``visit_*`` methods.

        Returns
        -------
        replaced : `relation_tree.Predicate`
            The predicate to be inserted instead of ``comparison`` in the
            processed tree.
        """
        match comparison.a, comparison.b:
            case rt.DimensionFieldReference(element=a), rt.DimensionFieldReference(element=b):
                return self.visit_spatial_join(comparison, a, b, parents)
            case rt.DimensionFieldReference(element=element), rt.RegionColumnLiteral(value=region):
                return self.visit_spatial_constraint(comparison, element, region, parents)
            case rt.RegionColumnLiteral(value=region), rt.DimensionFieldReference(element=element):
                return self.visit_spatial_constraint(comparison, element, region, parents)
        raise AssertionError(f"Unexpected arguments for spatial overlap: {comparison.a}, {comparison.b}.")

    def visit_temporal_overlap(
        self,
        comparison: rt.Comparison,
        parents: Set[Literal["and", "or", "not"]],
    ) -> rt.Predicate:
        """Dispatch a temporal overlap comparison predicate to handlers.

        This method should rarely (if ever) need to be overridden.

        Parameters
        ----------
        comparison : `relation_tree.Comparison`
            Predicate object to handle.
        parents : `~collections.abc.Set` of "and", "or", and "not" literals
            What kinds of logical operators are used to connect this predicate
            to others, to be forwarded to other ``visit_*`` methods.

        Returns
        -------
        replaced : `relation_tree.Predicate`
            The predicate to be inserted instead of ``comparison`` in the
            processed tree.
        """
        match comparison.a, comparison.b:
            case rt.DimensionFieldReference(element=a), rt.DimensionFieldReference(element=b):
                return self.visit_temporal_dimension_join(comparison, a, b, parents)
            case _:
                # We don't bother differentiating any other kind of temporal
                # comparison, because in all foreseeable database schemas we
                # wouldn't have to do anything special with them, since they
                # don't participate in automatic join calculations and they
                # should be straightforwardly convertible to SQL.
                return comparison

    def visit_spatial_join(
        self,
        comparison: rt.Comparison,
        a: DimensionElement,
        b: DimensionElement,
        parents: Set[Literal["and", "or", "not"]],
    ) -> rt.Predicate:
        """Handle a spatial overlap comparison between two dimension elements.

        The default implementation updates the set of known spatial connections
        (for use by `compute_automatic_spatial_joins`) and returns
        ``comparison``.

        Parameters
        ----------
        comparison : `relation_tree.Comparison`
            Predicate object to handle.
        a : `DimensionElement`
            One element in the join.
        b : `DimensionElement`
            The other element in the join.
        parents : `~collections.abc.Set` of "and", "or", and "not" literals
            What kinds of logical operators are used to connect this predicate
            to others, to be forwarded to other ``visit_*`` methods.

        Returns
        -------
        replaced : `relation_tree.Predicate`
            The predicate to be inserted instead of ``comparison`` in the
            processed tree.
        """
        if a.spatial == b.spatial:
            raise rt.InvalidRelationError(f"Spatial join between {a} and {b} is not necessary.")
        self._spatial_connections.merge(
            cast(TopologicalFamily, a.spatial), cast(TopologicalFamily, b.spatial)
        )
        return comparison

    def visit_spatial_constraint(
        self,
        comparison: rt.Comparison,
        element: DimensionElement,
        region: Region,
        parents: Set[Literal["and", "or", "not"]],
    ) -> rt.Predicate:
        """Handle a spatial overlap comparison between a dimension element and
        a literal region.

        The default implementation just returns ``comparison``.

        Parameters
        ----------
        comparison : `relation_tree.Comparison`
            Predicate object to handle.
        element : `DimensionElement`
            The dimension element in the comparison.
        region : `lsst.sphgeom.Region`
            The literal region in the comparison.
        parents : `~collections.abc.Set` of "and", "or", and "not" literals
            What kinds of logical operators are used to connect this predicate
            to others, to be forwarded to other ``visit_*`` methods.

        Returns
        -------
        replaced : `relation_tree.Predicate`
            The predicate to be inserted instead of ``comparison`` in the
            processed tree.
        """
        return comparison

    def visit_temporal_dimension_join(
        self,
        comparison: rt.Comparison,
        a: DimensionElement,
        b: DimensionElement,
        parents: Set[Literal["and", "or", "not"]],
    ) -> rt.Comparison:
        """Handle a temporal overlap comparison between two dimension elements.

        The default implementation updates the set of known temporal
        connections (for use by `compute_automatic_temporal_joins`) and returns
        ``comparison``.

        Parameters
        ----------
        comparison : `relation_tree.Comparison`
            Predicate object to handle.
        a : `DimensionElement`
            One element in the join.
        b : `DimensionElement`
            The other element in the join.
        parents : `~collections.abc.Set` of "and", "or", and "not" literals
            What kinds of logical operators are used to connect this predicate
            to others, to be forwarded to other ``visit_*`` methods.

        Returns
        -------
        replaced : `relation_tree.Predicate`
            The predicate to be inserted instead of ``comparison`` in the
            processed tree.
        """
        if a.temporal == b.temporal:
            raise rt.InvalidRelationError(f"Temporal join between {a} and {b} is not necessary.")
        self._temporal_connections.merge(
            cast(TopologicalFamily, a.temporal), cast(TopologicalFamily, b.temporal)
        )
        return comparison
