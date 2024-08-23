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
from collections.abc import Hashable, Iterable, Mapping, Sequence, Set
from typing import Generic, Literal, TypeVar, cast

from lsst.sphgeom import Region

from .._exceptions import InvalidQueryError
from .._topology import TopologicalFamily, TopologicalRelationshipEndpoint, TopologicalSpace
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


class CalibrationTemporalEndpoint(TopologicalRelationshipEndpoint):
    """An implementation of the "topological relationship endpoint" interface
    for a calibration dataset search.

    Parameters
    ----------
    dataset_type_name : `str`
        Name of the dataset type.

    Notes
    -----
    This lets validity range lookups participate in the logic that checks to
    see if an explicit spatial/temporal join in the WHERE expression is present
    and hence an automatic join is unnecessary.  That logic is simple for
    datasets, since each "family" is a single dataset type that only has one
    endpoint (whereas different dimensions like tract and patch can belong to
    the same family).
    """

    def __init__(self, dataset_type_name: str):
        self.dataset_type_name = dataset_type_name

    @property
    def name(self) -> str:
        return self.dataset_type_name

    @property
    def topology(self) -> Mapping[TopologicalSpace, TopologicalFamily]:
        return {TopologicalSpace.TEMPORAL: CalibrationTemporalFamily(self.dataset_type_name)}


class CalibrationTemporalFamily(TopologicalFamily):
    """An implementation of the "topological relationship endpoint" interface
    for a calibration dataset search.

    See `CalibrationTemporalEndpoint` for rationale.

    Parameters
    ----------
    dataset_type_name : `str`
        Name of the dataset type.
    """

    def __init__(self, dataset_type_name: str):
        super().__init__(dataset_type_name, TopologicalSpace.TEMPORAL)

    def choose(self, dimensions: DimensionGroup) -> CalibrationTemporalEndpoint:
        return CalibrationTemporalEndpoint(self.name)

    def make_column_reference(self, endpoint: TopologicalRelationshipEndpoint) -> tree.DatasetFieldReference:
        return tree.DatasetFieldReference(dataset_type=endpoint.name, field="timespan")


class OverlapsVisitor(SimplePredicateVisitor):
    """A helper class for dealing with spatial and temporal overlaps in a
    query.

    Parameters
    ----------
    dimensions : `DimensionGroup`
        Dimensions of the query.
    calibration_dataset_types : `~collections.abc.Set` [ `str` ]
        The names of dataset types that have been joined into the query via
        a search that includes at least one calibration collection.

    Notes
    -----
    This class includes logic for extracting explicit spatial and temporal
    joins from a WHERE-clause predicate and computing automatic joins given the
    dimensions of the query.  It is designed to be subclassed by query driver
    implementations that want to rewrite the predicate at the same time.
    """

    def __init__(self, dimensions: DimensionGroup, calibration_dataset_types: Set[str]):
        self.dimensions = dimensions
        self._spatial_connections = _NaiveDisjointSet(self.dimensions.spatial)
        temporal_families: list[TopologicalFamily] = [
            CalibrationTemporalFamily(name) for name in calibration_dataset_types
        ]
        temporal_families.extend(self.dimensions.temporal)
        self._temporal_connections = _NaiveDisjointSet(temporal_families)

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
        result = result.logical_and(self._add_automatic_joins("spatial", self._spatial_connections))
        result = result.logical_and(self._add_automatic_joins("temporal", self._temporal_connections))
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

    def _add_automatic_joins(
        self,
        kind: Literal["spatial", "temporal"],
        connections: _NaiveDisjointSet[TopologicalFamily],
    ) -> tree.Predicate:
        if connections.n_subsets <= 1:
            # All of the joins we need are already present.
            return tree.Predicate.from_bool(True)
        if connections.n_subsets > 2:
            raise InvalidQueryError(
                f"Too many disconnected sets of {kind} families for an automatic "
                f"join: {connections.subsets()}.  Add explicit {kind} joins to avoid this error."
            )
        a_subset, b_subset = connections.subsets()
        if len(a_subset) > 1 or len(b_subset) > 1:
            raise InvalidQueryError(
                f"A {kind} join is needed between {a_subset} and {b_subset}, but which join to "
                "add is ambiguous.  Add an explicit spatial or temporal join to avoid this error."
            )
        # We have a pair of families that are not explicitly or implicitly
        # connected to any other families; add an automatic join between their
        # most fine-grained members.
        (a_family,) = a_subset
        (b_family,) = b_subset
        a = a_family.make_column_reference(a_family.choose(self.dimensions))
        b = b_family.make_column_reference(b_family.choose(self.dimensions))
        join_predicate = self.visit_comparison(a, "overlaps", b, PredicateVisitFlags.HAS_AND_SIBLINGS)
        if join_predicate is None:
            join_predicate = tree.Predicate.compare(a, "overlaps", b)
        return join_predicate

    def visit_spatial_overlap(
        self,
        a: tree.ColumnExpression,
        b: tree.ColumnExpression,
        flags: PredicateVisitFlags,
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
        self,
        a: tree.ColumnExpression,
        b: tree.ColumnExpression,
        flags: PredicateVisitFlags,
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
            case (
                tree.DimensionFieldReference(element=a_element),
                tree.DimensionFieldReference(element=b_element),
            ):
                return self.visit_temporal_dimension_join(a_element, b_element, flags)
            case (
                tree.DatasetFieldReference(dataset_type=a_dataset),
                tree.DimensionFieldReference(element=b_element),
            ):
                return self.visit_validity_range_dimension_join(a_dataset, b_element, flags)
            case (
                tree.DimensionFieldReference(element=a_element),
                tree.DatasetFieldReference(dataset_type=b_dataset),
            ):
                return self.visit_validity_range_dimension_join(b_dataset, a_element, flags)
            case (
                tree.DatasetFieldReference(dataset_type=a_dataset),
                tree.DatasetFieldReference(dataset_type=b_dataset),
            ):
                return self.visit_validity_range_join(a_dataset, b_dataset, flags)
            case _:
                # Other cases do not participate in automatic join logic and
                # do not require the predicate to be rewritten.
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

    def visit_validity_range_dimension_join(
        self, a: str, b: DimensionElement, flags: PredicateVisitFlags
    ) -> tree.Predicate | None:
        """Handle a temporal overlap comparison between two dimension elements.

        The default implementation updates the set of known temporal
        connections (for use by `compute_automatic_temporal_joins`) and returns
        `None`.

        Parameters
        ----------
        a : `str`
            Name of a calibration dataset type.
        b : `DimensionElement`
            The dimension element to join the dataset validity range to.
        flags : `tree.PredicateLeafFlags`
            Information about where this overlap comparison appears in the
            larger predicate tree.

        Returns
        -------
        replaced : `tree.Predicate` or `None`
            The predicate to be inserted instead in the processed tree, or
            `None` if no substitution is needed.
        """
        self._temporal_connections.merge(CalibrationTemporalFamily(a), cast(TopologicalFamily, b.temporal))
        return None

    def visit_validity_range_join(self, a: str, b: str, flags: PredicateVisitFlags) -> tree.Predicate | None:
        """Handle a temporal overlap comparison between two dimension elements.

        The default implementation updates the set of known temporal
        connections (for use by `compute_automatic_temporal_joins`) and returns
        `None`.

        Parameters
        ----------
        a : `str`
            Name of a calibration dataset type.
        b : `DimensionElement`
            Another claibration dataset type to join to.
        flags : `tree.PredicateLeafFlags`
            Information about where this overlap comparison appears in the
            larger predicate tree.

        Returns
        -------
        replaced : `tree.Predicate` or `None`
            The predicate to be inserted instead in the processed tree, or
            `None` if no substitution is needed.
        """
        self._temporal_connections.merge(CalibrationTemporalFamily(a), CalibrationTemporalFamily(b))
        return None
