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
from .sets import DimensionSet, toNameSet
from .elements import Dimension, DimensionJoin


class DimensionConfig(ConfigSubset):
    component = "dimensions"
    requiredKeys = ("version", "units", "joins")
    defaultConfigFile = "dimensions.yaml"


class DimensionGraph:
    r"""A automatically-expanded collection of both `Dimension`\s and
    `DimensionJoin`\s.

    A `DimensionGraph` behaves much like a `DimensionSet` containing only
    `Dimension` objects, with a few key differences:

     - A `DimensionGraph` always contains all of the required dependencies of
       any of its elements.  That means some standard set operations (namely
       `symmetric_difference`/`^` and `difference`/`-`) can't behave the way
       one would expect, and hence aren't available (that's why
       `DimensionGraph` doesn't inherit from `collections.abc.Set` while
       `DimensionSet` does).

     - Any `DimensionJoin`\s whose dependencies are present in the graph are
       accessible via the `joins()` method (which is why we consider this
       graph - it contains the relationships as well as the elements of
       a system of dimensions).

    Unlike all other dimension classes, `DimensionGraph` objects can be
    constructed directly, but only after a special "universe" `DimensionGraph`
    is constructed via a call to `fromConfig`.  A "universe" graph contains
    all dimension elements, and all other `DimensionGraph`\s or
    `DimensionSet`\s constructed from that universe are subsets of it.

    Parameters
    ----------
    universe : `DimensionGraph`
        The ultimate graph the new graph should be a subset of.
    dimensions : iterable of `Dimension` or `str`
        `Dimension` objects that should be included in the graph, or their
        string names.  Note that the new graph will in general return more
        than these dimensions, due to expansion to include dependenies.
    joins : iterable of `DimensionJoin` or `str`.
        `DimensionJoin` objects that should be included in the graph, or their
        string names.
    optional : `bool`
        If `True`, expand the graph to (recursively) include optional
        dependencies as well as required dependencies.
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

        backrefs = {}

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
                universe._elements._elements[dimensionName] = dimension
                for link in dimension.primaryKey:
                    backrefs.setdefault(link, set()).add(dimensionName)
                for link in dimension.dependencies(optional=True, required=False).links:
                    backrefs.setdefault(link, set()).add(dimensionName)
                del dimensionsToDo[dimensionName]

            # Create and add to universe any DimensionJoins whose
            # related DimensionUnits are already in the universe.
            namesOfUnblockedJoins = [name for name, related in joinsToDo.items()
                                     if related.issubset(universe._dimensions.names)]
            namesOfUnblockedJoins.sort()  # break topological ties with lexicographical sort
            for joinName in namesOfUnblockedJoins:
                join = DimensionJoin._construct(universe, name=joinName, config=config["joins"][joinName])
                universe._joins._elements[joinName] = join
                universe._elements._elements[joinName] = join
                for link in join.primaryKey:
                    backrefs.setdefault(link, set()).add(joinName)
                del joinsToDo[joinName]

        if joinsToDo:
            raise ValueError("Orphaned join(s) found: [{}]".format(", ".join(joinsToDo.keys())))

        # The 'summarizes' field of DimensionJoin needs to be populated in the
        # opposite order (DimensionJoins that relate fewer Dimensions
        # summarize those that relate more), so we put off setting those
        # attributes until everything else is initialized.
        for join in universe._joins:
            names = config["joins", join.name].get("summarizes", ())
            join._summarizes = DimensionSet(universe, names)

        # Finish setting up dictionary linking link names to dimension
        # elements by transforming local 'backrefs' variable's sets string
        # names into true DimensionSets in unverse._backrefs
        for linkName, nameSet in backrefs.items():
            universe._backrefs[linkName] = DimensionSet(universe, nameSet)
        return universe

    def __init__(self, universe, dimensions=(), joins=(), optional=False):
        if universe is None:
            self._universe = self
            self._elements = DimensionSet(self, elements=())
            self._dimensions = DimensionSet(self, elements=())
            self._joins = DimensionSet(self, elements=())
            self._backrefs = {}
        else:
            if universe.universe is not universe:
                raise ValueError("'universe' argument is not a universe graph.")
            self._universe = universe

            # Make a set of the names of requested dimensions and all of their dependencies.
            dimensionNames = set(toNameSet(dimensions).names)

            # Make a set of the names of all requested joins.
            joinNames = set()
            for join in joins:
                if not isinstance(join, DimensionJoin):
                    join = self.universe._joins[join]
                joinNames.add(join.name)
                # Add to the dimension set any dimensions that are required by the requested joins.
                dimensionNames |= join.dependencies().names

            # Make the Dimension set, expanding to include dependencies
            self._dimensions = DimensionSet(universe, dimensionNames, expand=True, optional=optional)

            # Add to the join set any joins that are implied by the set of dimensions.
            for join in self.universe._joins:
                if self._dimensions.issuperset(join.dependencies()):
                    joinNames.add(join.name)

            self._joins = DimensionSet(universe, joinNames, expand=False)

            self._elements = DimensionSet(universe, self._dimensions.names | self._joins.names)

            self._backrefs = universe._backrefs

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

    @property
    def optionals(self):
        """A set containing all (recursive) optional dependencies of the
        dimensions in this graph that are not included in the graph itself
        (`DimensionSet`).
        """
        everything = self.asSet.expanded(optional=True)
        return everything - self.asSet

    @property
    def elements(self):
        """A set containing both `Dimension` and `DimensionJoin` objects
        (`DimensionSet`).
        """
        return self._elements

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
        return DimensionSet(
            self.universe,
            [join.name for join in self._joins if join.summarizes.isdisjoint(self._joins)]
        )

    def __len__(self):
        return len(self._dimensions)

    def __iter__(self):
        return iter(self._dimensions)

    def __contains__(self, key):
        return key in self._dimensions

    def __getitem__(self, key):
        return self._dimensions[key]

    def get(self, key, default=None):
        """Return the element with the given name, or ``default`` if it
        does not exist.

        ``key`` may also be a `Dimension`, in which case an equivalent
        object will be returned.
        """
        return self._dimensions.get(key, default)

    @property
    def names(self):
        """The names of all elements (`set`-like, immutable).

        The order of the names is consistent with the iteration order of the
        `DimensionSet` itself.
        """
        return self._dimensions.names

    @property
    def links(self):
        """The names of all fields that uniquely identify these dimensions in
        a data ID dict (`frozenset` of `str`).
        """
        return self._dimensions.links

    def __hash__(self):
        return hash(self._dimensions)

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
        """Return `True` if there are no dimensions in both ``self`` and
        ``other``, and `False` otherwise.

        All graphs (including the empty graph) are disjoint with the empty
        graph.
        """
        return self._dimensions.isdisjoint(other)

    def union(self, *others, optional=False):
        """Return a new graph containing all elements that are in ``self`` or
        any of the other given graphs.

        Parameters
        ----------
        *others : iterable over `Dimension` or `str`.
            Other sets whose elements should be included in the result.

        Returns
        -------
        result : `DimensionGraph`
            Graph containing any elements in any of the inputs.
        """
        return DimensionGraph(self.universe, self._dimensions.union(*others), optional=optional)

    def intersection(self, *others, optional=False):
        """Return a new graph containing all elements that are in both  ``self``
        and all of the other given graphs.

        Parameters
        ----------
       *others : iterable over `Dimension` or `str`.
            Other sets whose elements may be included in the result.

        Returns
        -------
        result : `DimensionGraph`
            Graph containing all elements in all of the inputs.
        """
        return DimensionGraph(self.universe, self._dimensions.intersection(*others), optional=optional)

    def __or__(self, other):
        if isinstance(other, DimensionGraph):
            return self.union(other)
        return NotImplemented

    def __and__(self, other):
        if isinstance(other, DimensionGraph):
            return self.intersection(other)
        return NotImplemented

    def extract(self, elements, optional=False):
        r"""Return a new graph containing the given elements.

        Parameters
        ----------
        elements : iterable of `DimensionElement` or `str`
            `Dimension`\s, `DimensionJoin`\s, or names thereof.  These must be
            in ``self`` or ``self.joins()``.
        optional : `bool`
            If `True`, expand the result to include optional dependencies as
            well as required dependencies.

        Return
        ------
        subgraph : `DimensionGraph`
            A new graph containing at least the elements and their
            dependencies.
        """
        if isinstance(elements, DimensionGraph) and not optional and self.issuperset(elements):
            return elements
        elements = toNameSet(elements)
        if not self.elements.issuperset(elements):
            raise ValueError(
                "Elements {} not in this graph.".format(elements.difference(self.names))
            )
        return DimensionGraph(self.universe,
                              dimensions=self._dimensions.intersection(elements),
                              joins=self._joins.intersection(elements),
                              optional=optional)

    def getRegionHolder(self):
        """Return the Dimension that provides the spatial region for this
        combination of dimensions.

        Returns
        -------
        holder : `DimensionElement`, or None.
            `Dimension` or `DimensionJoin` that provides a unique region for
            this graph, or ``None`` if there is no region or multiple regions.
            ``holder.hasRegion`` is guaranteed to be ``True`` if ``holder`` is
            not `None`.
        """
        # TODO: consider caching this result, as it'll never change.
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
                        return None
                else:
                    candidate = dimension
        return candidate

    def withLink(self, link):
        """Return the set of `Dimension` and `DimensionJoin` objects that have
        the given link name in their primary or foreign keys.

        Unlike most other `DimensionGraph` operations, this method does not
        limit its results to the elements included in ``self``.

        Parameters
        ----------
        link : `str`
            Primary key field name.

        Returns
        -------
        dimensions : `DimensionSet`
            Set potentially containing both `Dimension` and `DimensionJoin`
            elements.
        """
        return self._backrefs[link]

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
