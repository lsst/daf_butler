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

__all__ = ("DimensionElement", "Dimension", "DimensionJoin")

from ..utils import PrivateConstructorMeta


class DimensionElement(metaclass=PrivateConstructorMeta):
    """Base class for elements in the dimension schema.

    `DimensionElement` has exactly two subclasses: `Dimension` and
    `DimensionJoin`.

    `DimensionElement` objects are not directly constructable; they can only
    be obtained (possibly indirectly) from a special "universe"
    `DimensionGraph` loaded from configuration.
    """

    #
    # Constructor is private, so its docs are just comments.
    #
    # Parameters
    # ----------
    # universe : `DimensionGraph`
    #     Ultimate-parent `DimensionGraph` that constructed this element.
    # name : `str`
    #     Name of the element.  Also the name of any SQL table or view
    #     associated with it.
    # hasRegion : `bool`
    #     Whether entries for this dimension are associated with a region
    #     on the sky.
    # links : iterable of `str`
    #     The names of primary key fields used by this element but not any of
    #     its dependencies.
    # required : iterable of `str`
    #     The names of the `Dimension`\ s whose primary keys are a subset of
    #     this element's primary key.
    # implied : iterable of `str`
    #     The names of the `Dimension`\ s whose primary keys are specified by
    #     foreign keys in this element.
    # doc : `str`
    #     Documentation string for this element.
    #
    def __init__(self, universe, name, hasRegion, links, required, implied, doc):
        from .sets import DimensionSet
        self._universe = universe
        self._name = name
        self._hasRegion = hasRegion
        self._localLinks = frozenset(links)
        self._doc = doc
        self._requiredDependencies = DimensionSet(universe, required, expand=True, implied=False)
        self._impliedDependencies = DimensionSet(universe, implied, expand=False)
        # Compute _primaryKeys dict, used to back primaryKeys property.
        expandedLinks = set(self._localLinks)
        for dimension in self.dependencies(implied=False):
            expandedLinks |= dimension.links(expand=True)
        self._expandedLinks = frozenset(expandedLinks)

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

        Also assumed to be the name of any associated table or view.
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

    def links(self, expand=True):
        """Return the names of fields that uniquely identify this dimension in
        a data ID dict.

        Parameters
        ----------
        expand: `bool`
            If `True` (default) include links associated with required
            dependencies.

        Returns
        -------
        links : `frozenset` of `str`
            Set of field names.
        """
        if expand:
            return self._expandedLinks
        else:
            return self._localLinks

    def dependencies(self, required=True, implied=False):
        """Return the set of dimensions this dimension depends on.

        Parameters
        ----------
        required : `bool`
            If `True` (default), include required dependencies.  Required
            dependences are always expanded recursively.
        implied : `bool`
            If `True`, return implied dependencies.

        Returns
        -------
        dependencies : `DimensionSet`
        """
        if required:
            if implied:
                return self._requiredDependencies | self._impliedDependencies
            else:
                return self._requiredDependencies
        elif implied:
            return self._impliedDependencies
        raise ValueError("At least one of 'required' and 'implied' must be True.")

    def graph(self, implied=False):
        """Return the minimal `DimensionGraph` that contains ``self``.

        Parameters
        ----------
        implied : `bool`
            If `True`, include implied as well as required dependencies.

        Returns
        -------
        graph : `DimensionGraph`
        """
        return self.universe.extract([self], implied=implied)


class Dimension(DimensionElement):
    r"""A discrete dimension of data used to organize Datasets and associate
    them with metadata.

    `Dimension` instances represent concepts such as "Instrument",
    "Detector", "Visit" and "SkyMap", which are usually associated with
    database tables.  A `DatasetType` is associated with a fixed combination
    of `Dimension`\s.

    `Dimension` objects are not directly constructable; they can only be
    obtained from a `DimensionGraph`.
    """

    #
    # Constructor is private, so its docs are just comments.
    #
    # Parameters
    # ----------
    # universe : `DimensionGraph`
    #     Ultimate-parent DimensionGraph that constructed this element.
    # name : `str`
    #     Name of the element.  Also the name of any SQL table or view
    #     associated with it.
    # config : `Config`
    #     Sub-config corresponding to this `Dimension`.
    #
    def __init__(self, universe, name, config):
        super().__init__(universe, name, hasRegion=config.get("hasRegion", False), links=config["link"],
                         required=config.get(".dependencies.required", ()),
                         implied=config.get(".dependencies.implied", ()),
                         doc=config["doc"])

    def __repr__(self):
        return "Dimension({})".format(self.name)


class DimensionJoin(DimensionElement):
    r"""A join that relates two or more `Dimension`\s.

    `DimensionJoin`\s usually map to many-to-many join tables or views that
    relate `Dimension` tables.

    `DimensionJoin` objects are not directly constructable; they can only be
    obtained from a `DimensionGraph`.
    """

    #
    # Constructor is private, so its docs are just comments.
    #
    # Parameters
    # ----------
    # universe : `DimensionGraph`
    #     Ultimate-parent DimensionGraph that constructed this element.
    # name : `str`
    #     Name of the element.  Also the name of any SQL table or view
    #     associated with it.
    # config : `Config`
    #     Sub-config corresponding to this `DimensionJoin`.
    #
    def __init__(self, universe, name, config):
        from .sets import DimensionSet

        lhs = list(config["lhs"])
        rhs = list(config["rhs"])
        super().__init__(universe, name, hasRegion=config.get("hasRegion", False), links=(),
                         required=lhs + rhs, implied=(), doc=config["doc"])
        self._lhs = DimensionSet(universe, lhs, implied=False)
        self._rhs = DimensionSet(universe, rhs, implied=False)
        # self._summarizes initialized later in DimensionGraph.fromConfig.

    @property
    def lhs(self):
        r"""The `Dimension`\s on the left hand side of the join
        (`DimensionSet`).

        Left vs. right is completely arbitrary; the terminology simply provides
        an easy way to distinguish between the two sides.
        """
        return self._lhs

    @property
    def rhs(self):
        r"""The `Dimension`\s on the right hand side of the join
        (`DimensionSet`).

        Left vs. right is completely arbitrary; the terminology simply provides
        an easy way to distinguish between the two sides.
        """
        return self._rhs

    @property
    def summarizes(self):
        r"""A set of other `DimensionJoin`\s that provide more fine-grained
        relationships than this one (`DimensionJoinSet`).

        When a join "summarizes" another, it means the table for that join
        could (at least conceptually) be defined as an aggregate view on the
        summarized join table. For example, "TractSkyPixJoin" summarizes
        "PatchSkyPixJoin", because the set of SkyPix rows associated with a
        Tract row is just the set of SkyPix rows associated with all Patches
        associated with that Tract.  Or, in SQL:

        .. code-block:: sql

            CREATE VIEW TractSkyPixJoin AS
            SELECT DISTINCT skymap, tract, skypix FROM PatchSkyPixJoin;
        """
        return self._summarizes

    def __repr__(self):
        return "DimensionJoin({})".format(self.name)
