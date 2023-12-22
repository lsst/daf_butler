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

__all__ = ("JoinTuple", "JoinArg", "standardize_join_arg")

import itertools
from collections.abc import Hashable, Iterable, Sequence, Set
from typing import Annotated, Generic, Literal, TypeAlias, TypeVar, Union

import pydantic

from ..._named import NamedValueAbstractSet
from ..._topology import TopologicalFamily
from ...dimensions import DimensionGroup

_T = TypeVar("_T", bound=Hashable)


def _reorder_join_tuple(original: tuple[str, str]) -> tuple[str, str]:
    if original[0] < original[1]:
        return original
    if original[0] > original[1]:
        return original[::-1]
    raise ValueError("Join tuples may not represent self-joins.")


JoinTuple: TypeAlias = Annotated[tuple[str, str], pydantic.AfterValidator(_reorder_join_tuple)]


JoinArg: TypeAlias = Union[tuple[str, str], Iterable[tuple[str, str]]]


def standardize_join_arg(arg: JoinArg, kind: Literal["spatial", "temporal"]) -> frozenset[JoinTuple]:
    """Standardize a `JoinArg` into a `frozenset` of `JoinTuple`.

    Parameters
    ----------
    arg : `tuple` [ `str`, `str` ] or `~collections.abc.Iterable` [ \
            `tuple` [ `str`, `str` ] ]
        Argument to standardize.
    kind : `str`
        Either "spatial" or "temporal".

    Returns
    -------
    joins : `frozenset` [ `JoinTuple` ]
        Set of sorted join pairs.
    """
    # MyPy is not smart enough to figure out how the match arms narrow the
    # type.
    result = set()
    match arg:
        case (str(), str()) as pair:
            result.add(_reorder_join_tuple(pair))  # type: ignore[arg-type]
        case iterable:
            for pair in iterable:  # type: ignore[assignment]
                match pair:
                    case (str(), str()):
                        result.add(_reorder_join_tuple(pair))  # type: ignore[arg-type]
                    case _:
                        raise TypeError(f"Invalid argument {arg} for {kind} join.")
    return frozenset(result)


def complete_joins(
    dimensions: DimensionGroup,
    join_operand_families: Iterable[NamedValueAbstractSet[TopologicalFamily]],
    explicit_joins: frozenset[JoinTuple],
    kind: Literal["spatial", "temporal"],
) -> frozenset[JoinTuple]:
    """Compute automatic spatial or temporal joins for a `Select` relation.

    Parameters
    ----------
    dimensions : `DimensionGroup`
        All dimensions being joined together.
    join_operand_families : `~collections.abc.Iterable` [ \
            `NamedValueAbstractSet` [ `TopologicalFamily` ] ]
        Sets of topological families, each from a different relation being
        joined in (e.g. a dataset search or materialized query).
    explicit_joins : `frozenset` [ `tuple` [ `str`, `str` ] ]
        Join criteria added manually to the relation tree.
    kind : `str`
        Whether these are spatial or temporal joins.

    Returns
    -------
    joins : `frozenset` [ `tuple` [ `str`, `str` ] ]
        Set of both explicit and automatic joins.
    """
    # Gather all "families" that could participate in joins - these are things
    # like "observation regions" (visit and visit_detector) or "skymap regions"
    # (tract and patch) or a sky pixelization scheme.  Members of a family
    # represent different granularities and don't need to be joined to each
    # other spatially or temporally.
    # When computing temporal joins, this does _not_ include calibration
    # validity ranges, as joins to those are always explicit.
    all_families: NamedValueAbstractSet[TopologicalFamily] = getattr(dimensions, kind)
    if len(all_families) <= 1:
        # No possibility of an automatic join; the only kind we might have is
        # between a temporal dimension and a dataset validity range.
        return explicit_joins
    # Put those families into a disjoint-set data structure, to help us track
    # which of them are already connected.
    connections = _NaiveDisjointSet(all_families)
    for a_element, b_element in explicit_joins:
        # Ignore any explicit joins that do not involve dimensions (i.e.
        # dataset validity ranges), since those never take the place of
        # dimension-dimension joins.
        if a_element in dimensions.elements and b_element in dimensions.elements:
            connections.merge(
                getattr(dimensions.universe[a_element], kind),
                getattr(dimensions.universe[b_element], kind),
            )
    for operand_families in join_operand_families:
        # We assume each join operand has its own complete set of spatial and
        # temporal joins that went into generating its rows.  That will
        # naturally be true for relations originating from the butler database,
        # like dataset searches and materializations, and if it isn't true for
        # a data ID upload, that would represent an intentional association
        # between non-overlapping things that we'd want to respect by _not_
        # adding more restrictive automatic join.
        for a_family, b_family in itertools.pairwise(operand_families):
            connections.merge(a_family, b_family)
    if connections.n_subsets == 1:
        # All of the joins we need are already present.
        return explicit_joins
    if connections.n_subsets > 2:
        raise ValueError(
            f"Too many disconnected sets of {kind} families for an automatic join: {connections.subsets()}. "
            "Add explicit {kind} joins to avoid this error."
        )
    a_subset, b_subset = connections.subsets()
    if len(a_subset) > 1 or len(b_subset) > 1:
        raise ValueError(
            f"A {kind} join is needed between {a_subset} and {b_subset}, but which join to "
            "add is ambiguous.  Add an explicit spatial join to avoid this error."
        )
    # We have a pair of families that are not explicitly or implicitly
    # connected to any other families; add an automatic join between their
    # most fine-grained members.
    (a_family,) = a_subset
    (b_family,) = b_subset
    result = set(explicit_joins)
    result.add(
        _reorder_join_tuple(
            (
                a_family.choose(dimensions.elements, dimensions.universe).name,
                b_family.choose(dimensions.elements, dimensions.universe).name,
            )
        )
    )
    return frozenset(result)


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
