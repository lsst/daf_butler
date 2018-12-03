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

__all__ = ("DimensionGraph", "DimensionConfig")

import itertools
from ..config import ConfigSubset
from .sets import DimensionSet
from .elements import Dimension, DimensionJoin


class DimensionConfig(ConfigSubset):
    component = "dimensions"
    requiredKeys = ("version", "units", "joins")
    defaultConfigFile = "dimensions.yaml"


class DimensionGraph:
    r"""A automatically-expanded collection of both `Dimension`\s and
    `DimensionJoin`\s.
    """

    @staticmethod
    def fromConfig(config=None):
        config = DimensionConfig(config)

        universe = DimensionGraph(universe=None)

        # Now we'll construct dictionaries from the bits of config that are
        # necessary to topologically sort the units and joins.
        # We'll empty these "to do" dicts as we populate the graph.

        dimensionsToDo = {}  # {unit name: frozenset of names of unit dependencies}
        joinsToDo = {}  # {join name: frozenset of related unit names}

        for dimensionName, subconfig in config["units"].items():
            dimensionsToDo[dimensionName] = frozenset(
                subconfig.get(".dependencies.required", ())
            ).union(
                subconfig.get(".dependencies.optional", ())
            )

        for joinName, subconfig in config["joins"].items():
            joinsToDo[joinName] = frozenset(subconfig["lhs"] + subconfig["rhs"])

        while dimensionsToDo:

            # Create and add to universe any DimensionUnits whose
            # dependencies are already in the universe.
            namesOfUnblockedUnits = [name for name, deps in dimensionsToDo.items()
                                     if deps.issubset(universe._dimensions.names)]
            namesOfUnblockedUnits.sort()  # break topological ties with lexicographical sort
            if not namesOfUnblockedUnits:
                raise ValueError("Cycle detected: [{}]".format(", ".join(dimensionsToDo.keys())))
            for dimensionName in namesOfUnblockedUnits:
                dimension = Dimension._construct(universe, name=dimensionName,
                                                 config=config["units"][dimensionName])
                universe._dimensions._elements[dimensionName] = dimension
                del dimensionsToDo[dimensionName]

            # Create and add to universe any DimensionJoins whose
            # related DimensionUnits are already in the universe.
            namesOfUnblockedJoins = [name for name, related in joinsToDo.items()
                                     if related.issubset(universe._dimensions.names)]
            namesOfUnblockedJoins.sort()  # break topological ties with lexicographical sort
            for joinName in namesOfUnblockedJoins:
                join = DimensionJoin._construct(universe, name=joinName, config=config["joins"][joinName])
                universe._joins._elements[joinName] = join
                del joinsToDo[joinName]

        if joinsToDo:
            raise ValueError("Orphaned join(s) found: [{}]".format(", ".join(joinsToDo.keys())))

        # The 'summarizes' field of DimensionJoin needs to be populated in the
        # opposite order (DimensionJoins that relate fewer Dimensions
        # summarize those that relate more), so we put off setting those
        # attributes until everything else is initialized.
        for join in universe._joins:
            names = config["joins", join.name].get("summarizes", ())
            join._summarizes = DimensionSet._construct(universe._joins, names)

        return universe

    def __init__(self, universe, dimensions=(), joins=()):
        if universe is None:
            self._universe = self
            self._dimensions = DimensionSet._construct(parent=None, elements=())
            self._joins = DimensionSet._construct(parent=None, elements=())
        else:
            self._universe = universe

            # Make a set of the names of requested dimensions and all of their dependencies.
            dimensionNames = set()
            for dimension in dimensions:
                if not isinstance(dimension, Dimension):
                    dimension = self.universe._dimensions[dimension]
                dimensionNames.add(dimension.name)
                dimensionNames |= dimension.dependencies().names

            # Make a set of the names of all requested joins.
            joinNames = set()
            for join in joins:
                if not isinstance(join, DimensionJoin):
                    join = self.universe._joins[join]
                joinNames.add(join.name)
                # Add to the dimension set any dimensions that are required by the requested joins.
                dimensionNames |= join.dependencies().names

            self._dimensions = DimensionSet._construct(universe._dimensions, dimensionNames,
                                                       expand=True, optional=True)

            # Add to the join set any joins that are implied by the set of dimensions.
            for join in self.universe._joins:
                if self._dimensions.issuperset(join.dependencies()):
                    joinNames.add(join.name)

            self._joins = DimensionSet._construct(universe._joins, joinNames, expand=True, optional=True)

    def __repr__(self):
        return f"DimensionGraph({self._dimensions}, joins={self._joins})"

    @property
    def universe(self):
        """The graph of all dimensions compatible with self (`DimensionGraph`).

        The universe of a `DimensionGraph` that is itself a universe is
        ``self``.
        """
        return self._universe

    @property
    def asSet(self):
        """A set view of the `Dimensions` in ``self`` (`DimensionSet`).
        """
        return self._dimensions

    def joins(self, summaries=True):
        """Return the `DimensionJoin` objects held by this graph.

        Parameters
        ----------
        summaries : `bool`
            If `False`, filter out joins that summarize any other joins being
            returned (in most cases only the most precise join between a group
            of dimensions is needed).

        Returns
        -------
        joins : `DimensionSet`
            The joins held by this graph, possibly filtered.
        """
        if summaries:
            return self._joins
        return DimensionSet._construct(
            self.universe._joins,
            [join.name for join in self._joins if join.summarizes.isdisjoint(self._joins)]
        )

    def __len__(self):
        return len(self._dimensions)

    def __iter__(self):
        return iter(self._dimensions)

    def __contains__(self, key):
        return key in self._dimensions

    def __getitem__(self, name):
        return self._dimensions[name]

    def get(self, name, default=None):
        """Return the element with the given ``name``, or ``default`` if it
        does not exist.
        """
        return self._dimensions.get(name, default)

    @property
    def names(self):
        """The names of all elements (`set`-like, immutable).

        The order of the names is consistent with the iteration order of the
        `DimensionSet` itself.
        """
        return self._dimensions.names

    @property
    def primaryKey(self):
        """The names of all fields that uniquely identify these dimensions in
        a data ID dict (`frozenset` of `str).
        """
        return self._dimensions.primaryKey

    def __hash__(self):
        return hash(self.names)

    def __eq__(self, other):
        return self._dimensions == other

    def __le__(self, other):
        return self._dimensions <= other

    def __lt__(self, other):
        return self._dimensions < other

    def __ge__(self, other):
        return self._dimensions >= other

    def __gt__(self, other):
        return self._dimensions > other

    def issubset(self, other):
        """Return `True` if all dimensions in ``self`` are also in ``other``.

        The empty graph is a subset of all graphs (including the empty graph).
        """
        return self <= other

    def issuperset(self, other):
        """Return `True` if all dimensions in ``other`` are also in ``self``,
        and `False` otherwise.

        All graphs (including the empty graph) are supersets of the empty
        graph.
        """
        return self >= other

    def isdisjoint(self, other):
        """Return `True` if there are no dimension in both ``self`` and
        ``other``, and `False` otherwise.

        All graphs (including the empty graph) are disjoint with the empty
        graph.
        """
        return self._dimensions.isdisjoint(other)

    def union(self, *others):
        """Return a new graph containing all elements that are in ``self`` or
        any of the other given g.

        Parameters
        ----------
        *others : iterable over `Dimension` or `str` name.
            Other sets whose elements should be included in the result.

        Returns
        -------
        result : `DimensionGraph`
            Graph containing any elements in any of the inputs.
        """
        return DimensionGraph(self.universe, self._dimensions.union(*others))

    def intersection(self, *others):
        """Return a new graph containing all elements that are in both  ``self``
        and all of the other given sets.

        Parameters
        ----------
       *others : iterable over `Dimension` or `str` name.
            Other sets whose elements may be included in the result.

        Returns
        -------
        result : `DimensionGraph`
            Graph containing all elements in all of the inputs.
        """
        return DimensionGraph(self.universe, self._dimensions.intersection(*others))

    def __or__(self, other):
        if isinstance(other, DimensionGraph):
            return self.union(other)
        return NotImplemented

    def __and__(self, other):
        if isinstance(other, DimensionGraph):
            return self.intersection(other)
        return NotImplemented

    def getRegionHolder(self):
        """Return the Dimension that provides the
        spatial region for this combination of dimensions.

        Returns
        -------
        holder : `Dimension`, or None.
            Dimension object that provides a unique region for this set, or
            None if no dimensions in this set have a region.
            ``holder.hasRegion`` is guaranteed to be ``True`` if ``holder``
            is not `None`.

        Raises
        ------
        ValueError
            Raised if multiple unrelated dimensions have regions, which
            indicates that there is no unique region for this set.
        """
        candidate = None
        for dimension in itertools.chain(self, self.joins(summaries=False)):
            if dimension.hasRegion:
                if candidate is not None:
                    if candidate in dimension.dependencies():
                        # this dimension is more specific than the old
                        # candidate; replace it (e.g. Tract is replaced with
                        # Patch).
                        candidate = dimension
                    else:
                        # opposite relationship should be impossible given
                        # topological iteration order...
                        assert dimension not in candidate.dependencies()
                        # ...so we must have unrelated dimensions with regions
                        raise ValueError(
                            f"Unrelated dimensions {candidate.name} and {dimension.name} both have regions"
                        )
                else:
                    candidate = dimension
        return candidate

    def findIf(self, predicate, default=None):
        """Return the element in ``self`` that matches the given predicate.

        Parameters
        ----------
        predicate : callable
            Callable that takes a single `DimensionElement` argument and
            returns a `bool`, indicating whether the given value should be
            returned.
        default : `DimensionElement`, optional
            Object to return if no matching elements are found.

        Returns
        -------
        matching : `DimensionElement`
            Element matching the given predicate.

        Raises
        ------
        ValueError
            Raised if multiple elements match the given predicate.
        """
        return self._dimensions.findIf(predicate, default=default)
