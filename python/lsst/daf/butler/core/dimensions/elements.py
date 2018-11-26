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

__all__ = ("Dimension", "DimensionUnit", "DimensionJoin")

from ..utils import PrivateConstructorMeta
from .sets import DimensionUnitSet


class Dimension(metaclass=PrivateConstructorMeta):
    """Base class for elements in the dimension schema.

    `Dimension` objects are not directly constructable; they can only be
    obtained from a `DimensionGraph`.
    """

    def __init__(self, universe, name, hasRegion, link, required, optional, doc):
        self._universe = universe
        self._name = name
        self._hasRegion = hasRegion
        self._link = frozenset(link)
        self._doc = doc
        self._requiredDependencies = DimensionUnitSet(universe, required, optional=False)
        self._allDependencies = DimensionUnitSet(universe, set(required) | set(optional), optional=True)
        # Compute _primaryKeys dict, used to back primaryKeys property.
        primaryKey = set(self.link)
        for dimension in self.dependencies(optional=False):
            primaryKey |= dimension.link
        self._primaryKey = frozenset(primaryKey)

    def __eq__(self, other):
        try:
            return self.universe is other.universe and self.name == other.name
        except AttributeError:
            return NotImplemented

    def __hash__(self):
        return hash(self.name)

    @property
    def universe(self):
        """The graph of all dimensions compatible with self (`DimensionGraph`).
        """
        return self._universe

    @property
    def name(self):
        """Name of this dimension (`str`, read-only).

        Also assumed to be the name of the associated table (if present).
        """
        return self._name

    @property
    def hasRegion(self):
        """Whether this dimension is associated with a region on the sky
        (`bool`).
        """
        return self._hasRegion

    @property
    def doc(self):
        """Documentation for this dimension (`str`).
        """
        return self._doc

    @property
    def primaryKey(self):
        """The names of fields that uniquely identify this dimension in a
        data ID dict (`frozenset` of `str`).
        """
        return self._primaryKey

    @property
    def link(self):
        """Primary key fields that are used only by this dimension, not any
        dependencies (`frozenset` of `str`).
        """
        return self._link

    def dependencies(self, optional=True):
        """Return the set of dimension units this dimension depends on.

        Parameters
        ----------
        optional : `bool`
            If `True` (default), include optional as well as required
            dependencies. The primary keys of required dependencies are always
            included in the primary key of the dimension that depends on them,
            while the primary keys of optional dependencies are not.

        Returns
        -------
        dependencies : `DimensionSet` of `DimensionUnit`
        """
        return self._allDependencies if optional else self._requiredDependencies


class DimensionUnit(Dimension):
    r"""A discrete unit of data used to organize `Dataset`\s and associate
    them with metadata.

    `DimensionUnit` instances represent concepts such as "Instrument",
    "Detector", "Visit" and "SkyMap", which are usually associated with
    database tables.  A `DatasetType` is associated with a fixed combination
    of `DimensionUnit`\s.

    `DimensionUnit` objects are not directly constructable; they can only be
    obtained from a `DimensionGraph`.
    """

    def __init__(self, universe, name, config):
        super().__init__(universe, name, hasRegion=config.get("hasRegion", False), link=config["link"],
                         required=config.get(".dependencies.required", ()),
                         optional=config.get(".dependencies.optional", ()),
                         doc=config["doc"])
        self._graph = None

    def __repr__(self):
        return "DimensionUnit({})".format(self.name)

    @property
    def graph(self):
        """The minimal graph that contains this `Dimension` (`DimensionGraph`).
        """
        if self._graph is None:
            self._graph = self.universe.subgraph([self])
        return self._graph


class DimensionJoin(Dimension):
    r"""A join that relates two or more `DimensionUnit`\s.

    `DimensionJoin`\s usually map to many-to-many join tables or views that
    relate `DimensionUnit` tables.

    `DimensionJoin` objects are not directly constructable; they can only be
    obtained from a `DimensionGraph`.
    """

    def __init__(self, universe, name, config):
        lhs = list(config["lhs"])
        rhs = list(config["rhs"])
        super().__init__(universe, name, hasRegion=config.get("hasRegion", False), link=(),
                         required=lhs + rhs, optional=(), doc=config["doc"])
        self._lhs = DimensionUnitSet(universe, lhs, optional=False)
        self._rhs = DimensionUnitSet(universe, rhs, optional=False)
        # self._summarizes initialized later in DimensionMeta.fromConfig.

    @property
    def lhs(self):
        r"""The `DimensionUnits`\s on the left hand side of the join
        (`DimensionUnitSet`).

        Left vs. right is completely arbitrary; the terminology simply provides
        an easy way to distiguish between the two sides.
        """
        return self._lhs

    @property
    def rhs(self):
        r"""The `DimensionUnits`\s on the right hand side of the join
        (`DimensionUnitSet`).

        Left vs. right is completely arbitrary; the terminology simply provides
        an easy way to distiguish between the two sides.
        """
        return self._rhs

    @property
    def summarizes(self):
        r"""A set of other `DimensionJoin`\s that provide more fine-grained
        relationships than this one (`DimensionJoinSet`).

        When a join "summarizes" another, it means the table for that join
        could (at least conceptually) be defined as a aggregate view on the
        summarized join table. For example, "TractSkyPixJoin" summarizes
        "PatchSkyPixJoin", because the set of SkyPix rows associated with a
        Tract row is just the set of all SkyPix rows associated with all
        Patches associated with that Tract.
        """
        return self._summarizes

    def __repr__(self):
        return "DimensionJoin({})".format(self.name)
