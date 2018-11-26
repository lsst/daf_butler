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
from ..utils import PrivateConstructorMeta
from .sets import UniverseMismatchError, DimensionUnitSet, DimensionJoinSet
from .elements import DimensionUnit, DimensionJoin


class DimensionConfig(ConfigSubset):
    component = "dimensions"
    requiredKeys = ("version", "units", "joins")
    defaultConfigFile = "dimensions.yaml"


class DimensionGraph(metaclass=PrivateConstructorMeta):
    r"""A self-consistent collection containing both `DimensionUnit` and
    `DimensionJoin` objects.

    `DimensionGraph` is conceptually just the combination of a
    `DimensionUnitSet` (accessible via the `units` property) and a
    `DimensionJoinSet` (accessible via the `joins` method) that are
    guaranteed to have the following properties:

     - `units` always contains all dependencies (both required and optional)
     of all of its elements.

     - `joins()` returns all joins whose dependencies are present in `units`
       (though by default it may filter out some of these; see the `joins`
       method documentation).

    As it is not directly iterable over either of these, `DimensionGraph` does
    not implement the `collections.abc.Set` interface, but it does provide
    its set comparison, intersection, and union operations.
    Set difference and symmetric difference are not supported, as these are in
    conflict with the requirement a graph always includes all of the
    dependenices of all of its dimensions.

    `DimensionGraph`\s can only be constructed by loading definitions from
    config via the `fromConfig` method.  This returns a special "universe"
    `DimensionGraph` that defines the maximal `DimensionUnit` and
    `DimensionJoin` sets.  Dimension objects from different universes are not
    interoperable.
    """

    @staticmethod
    def fromConfig(config=None):
        config = DimensionConfig(config)

        # The special universe graph needs to refer to itself at several levels,
        # so it has a bit of an ugly construction pattern just to get to an
        # empty graph.
        universe = DimensionGraph._construct(universe=None, units=None, joins=None)
        universe._universe = universe
        universe._units = DimensionUnitSet(universe)
        universe._joins = DimensionJoinSet(universe)

        # Now we'll construct dictionaries from the bits of config that are
        # necessary to topologically sort the units and joins.
        # We'll empty these "to do" dicts as we populate the graph.

        unitsToDo = {}  # {unit name: frozenset of names of unit dependencies}
        joinsToDo = {}  # {join name: frozenset of related unit names}

        for unitName, subconfig in config["units"].items():
            unitsToDo[unitName] = frozenset(
                subconfig.get(".dependencies.required", ())
            ).union(
                subconfig.get(".dependencies.optional", ())
            )

        for joinName, subconfig in config["joins"].items():
            joinsToDo[joinName] = frozenset(subconfig["lhs"] + subconfig["rhs"])

        while unitsToDo:

            # Create and add to universe any DimensionUnits whose
            # dependencies are already in the universe.
            namesOfUnblockedUnits = [name for name, deps in unitsToDo.items()
                                     if deps.issubset(universe._units.names)]
            namesOfUnblockedUnits.sort()  # break topological ties with lexicographical sort
            if not namesOfUnblockedUnits:
                raise ValueError("Cycle detected: [{}]".format(", ".join(unitsToDo.keys())))
            for unitName in namesOfUnblockedUnits:
                unit = DimensionUnit._construct(universe, name=unitName, config=config["units"][unitName])
                universe._units._elements[unitName] = unit
                del unitsToDo[unitName]

            # Create and add to universe any DimensionJoins whose
            # related DimensionUnits are already in the universe.
            namesOfUnblockedJoins = [name for name, related in joinsToDo.items()
                                     if related.issubset(universe._units.names)]
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
            join._summarizes = DimensionJoinSet(universe, names)

        return universe

    def __init__(self, universe, units, joins):
        self._universe = universe
        self._units = units
        self._joins = joins

    def __repr__(self):
        return f"DimensionGraph(units={str(self._units)}, joins={str(self._joins)})"

    @property
    def universe(self):
        """The graph of all dimensions compatible with self (`DimensionGraph`).

        The universe of a `DimensionGraph` that is itself a universe is
        ``self``.
        """
        return self._universe

    @property
    def units(self):
        """The `DimensionUnit` objects held by this graph (`DimensionUnitSet`).

        This is always expanded to include optional dependencies as well as
        required dependencies.
        """
        return self._units

    def joins(self, summaries=False):
        """Return the `DimensionJoin` objects held by this graph.

        Parameters
        ----------
        summaries : `bool`
            If `False` (default), filter out joins that summarize any other
            joins being returned (in most cases only the most precise join
            between a group of units is needed).

        Returns
        -------
        joins : `DimensionJoinSet`
            The joins held by this graph, possibly filtered.
        """
        if summaries:
            return self._joins
        return DimensionJoinSet(self.universe, [join.name for join in self._joins
                                                if join.summarizes.isdisjoint(self._joins)])

    @property
    def primaryKey(self):
        """The names of all fields that uniquely identify these dimensions in
        a data ID dict (`frozenset` of `str).
        """
        return frozenset().union(*(d.primaryKey for d in self.dimensions))

    def __bool__(self):
        # Can't rely on __len__ (as with most containers) because this isn't
        # actually a container.  But it can still be empty or non-empty.
        return bool(self.units)

    def __eq__(self, other):
        # Just comparing units should be sufficient; joins are always
        # determined from which units are present.
        return self.units == other.units

    def __le__(self, other):
        return self.units <= other.units

    def __lt__(self, other):
        return self.units < other.units

    def __ge__(self, other):
        return self.units >= other.units

    def __gt__(self, other):
        return self.units > other.units

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
        return self.units.isdisjoint(other.units)

    def union(self, *others):
        """Return a new graph containing all dimensions that are in ``self`` or
        any of the other given graphs.

        Parameters
        ----------
        *others : `DimensionGraph`
            Other graphs whose dimensions should be included in the result.

        Returns
        -------
        result : `DimensionGraph`
            A graph containing all dimensions in any of ``self`` or ``others``.
        """
        units = self.units.union(*(other.units for other in others), optional=True)
        return self.universe.subgraph(units=units)

    def intersection(self, *others):
        """Return a new graph containing all elements that are in both  ``self``
        and all of the other given graph.

        Parameters
        ----------
        *others : `DimensionGraph`
            Other graphs whose dimensions may be included in the result.

        Returns
        -------
        result : `DimensionGraph`
            A graph containing all dimension in all of ``self`` and ``others``.
        """
        units = self.units.intersection(*(other.units for other in others), optional=True)
        return self.subgraph(units=units)

    def __or__(self, other):
        return self.union(other)

    def __and__(self, other):
        return self.intersection(other)

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
        for dimension in itertools.chain(self.units, self.joins()):
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

    def subgraph(self, units=None, joins=(), where=None):
        r"""Return a subgraph of self containing the given dimensions and their
        dependencies and joins.

        Parameters
        ----------
        units : sequence of `DimensionUnit`, or `str`, optional
            Units that must be included in the returned graph, either as
            `DimensionUnit` instances or their string names.
        joins : sequence of `DimensionJoin`, optional
            Joins that must be included in the returned graph, either as
            `DimensionJoin` instances or their string names.
        where : callable
            A predicate that accepts a single `DimensionUnit` argument and
            returns a `bool`, indicating whether that unit should be included
            in the graph.  If ``where`` is provided and ``units`` is not, the
            predicate is applied to all `DimensionUnit`\s in ``self.units``.
            If ``where`` and ``units`` are both provided, the predicate is
            applied to just the given ``units``.  Units brought in as
            dependencies of ``joins`` or units accepted by the predicate are
            always included.
        """
        if units is None:
            if where is not None:
                units = self.units
            else:
                units = ()
        if where is None:
            where = lambda unit: True  # noqa E371
        # Make a set of the names of requested units and all of their dependencies.
        unitNames = set()
        for unit in units:
            if not isinstance(unit, DimensionUnit):
                unit = self._units[unit]
            else:
                UniverseMismatchError.check(self, unit)
            if not where(unit):
                continue
            unitNames.add(unit.name)
            unitNames |= unit.dependencies().names
        # Make a set of the names of all requested joins.
        joinNames = set()
        for join in joins:
            if not isinstance(join, DimensionJoin):
                join = self._joins[join]
            else:
                UniverseMismatchError.check(self, join)
            joinNames.add(join.name)
            # Add to the unit set any units that are required by the requested joins.
            unitNames |= join.dependencies().names
        # Add to the join set any joins that are implied by the set of units.
        for join in self._joins:
            if unitNames.issuperset(join.dependencies().names):
                joinNames.add(join.name)
        return DimensionGraph._construct(
            self.universe,
            units=DimensionUnitSet(self.universe, unitNames, optional=True),
            joins=DimensionJoinSet(self.universe, joinNames)
        )
