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

from itertools import chain

from .config import ConfigSubset
from .utils import TopologicalSet, iterable

__all__ = ("DataUnit", "DataUnitRegistry", "DataUnitConfig")


class DataUnit:
    """A discrete abstract unit of data that can be associated with
    metadata and used to label datasets.

    `DataUnit` instances represent concrete units such as e.g. `Camera`,
    `Sensor`, `Visit` and `SkyMap`.

    Parameters
    ----------
    name : `str`
        Name of this `DataUnit`.
        Also assumed to be the name of the primary table (if present).
    requiredDependencies : `frozenset`
        Related `DataUnit` instances on which existence this `DataUnit`
        instance depends.
    optionalDependencies : `frozenset`
        Related `DataUnit` instances that may also be provided (and when they
        are, they must be kept in sync).
    link : `tuple`
        Names of columns that form the `DataUnit` specific part of the
        primary-key in this `DataUnit` table and are also the names of the
        link column in the Datasets table.
    spatial : `bool`, optional
        Is this a spatial `DataUnit`? If so then it either has a ``region``
        column, or some other way to get a region (e.g. ``SkyPix``).
    """

    def __init__(self, name, requiredDependencies, optionalDependencies,
                 link=(), spatial=False):
        self._name = name
        self._requiredDependencies = frozenset(requiredDependencies)
        self._optionalDependencies = frozenset(optionalDependencies)
        self._link = link
        self._primaryKey = None
        self._spatial = spatial

    def __repr__(self):
        return "DataUnit({})".format(self.name)

    def __eq__(self, other):
        try:
            return self.name == other.name
        except AttributeError:
            return NotImplemented

    def __hash__(self):
        return hash(self.name)

    @property
    def name(self):
        """Name of this `DataUnit` (`str`, read-only).

        Also assumed to be the name of the primary table (if present)."""
        return self._name

    @property
    def requiredDependencies(self):
        """Related `DataUnit` instances on which existence this `DataUnit`
        instance depends (`frozenset`, read-only).
        """
        return self._requiredDependencies

    @property
    def optionalDependencies(self):
        """Related `DataUnit` instances that may also be provided (and when
        they are, they must be kept in sync) (`frozenset`, read-only).
        """
        return self._optionalDependencies

    @property
    def dependencies(self):
        """The union of `requiredDependencies` and `optionalDependencies`
        (`frozenset`, read-only).
        """
        return self.requiredDependencies.union(self.optionalDependencies)

    @property
    def link(self):
        """Names of columns that form the `DataUnit` specific part of the
        primary-key in this `DataUnit` table and are also the names of the
        link column in the Datasets table (`tuple`).
        """
        return self._link

    @property
    def primaryKey(self):
        """Full primary-key column name tuple.  Consists of the ``link`` of
        this `DataUnit` and that of all its ``requiredDependencies`` (`set`).
        """
        if self._primaryKey is None:
            self._primaryKey = set(self.link)
            for dependency in self.requiredDependencies:
                self._primaryKey.update(dependency.primaryKey)
        return self._primaryKey

    @property
    def spatial(self):
        """Is this a spatial `DataUnitJoin`?
        """
        return self._spatial

    def validateId(self, dataId):
        """Check if given dataId is valid.

        Parameters
        ----------
        dataId : `dict`
            A `dict` of `DataUnit` link name, value pairs that label the
            `DatasetRef` within a Collection.

        Raises
        ------
        ValueError
            If a value for a required dependency is missing.
        """
        missing = self.primaryKey - set(dataId.keys())
        if missing:
            raise ValueError("Missing required keys: {} from {} for DataUnit {}".format(
                missing, dataId, self.name))


class DataUnitJoin:
    """Represents a join between one or more `DataUnit`s.

    Parameters
    ----------
    name : `str`
        Name of this `DataUnit` (`str`, read-only).
        Also assumed to be the name of the primary table (if present).
    lhs : `tuple`
        Left-hand-side of the join.
    rhs : `tuple`
        Right-hand-side of the join.
    summarizes : `DataUnitJoin`
        Summarizes this other `DataUnitJoin`.
    relates : `tuple` of `DataUnit`
        The DataUnits in this relationship.
    spatial : `bool`, optional
        Is this a spatial `DataUnit`? If so then it either has a ``region``
        column, or some other way to get a region (e.g. ``SkyPix``).
    """

    def __init__(self, name, lhs=None, rhs=None, summarizes=None,
                 relates=None, spatial=False):
        self._name = name
        self._lhs = lhs
        self._rhs = rhs
        self._summarizes = summarizes
        self._relates = relates
        self._spatial = spatial

    @property
    def name(self):
        """Name of this `DataUnit` (`str`, read-only).

        Also assumed to be the name of the primary table (if present)."""
        return self._name

    @property
    def lhs(self):
        return self._lhs

    @property
    def rhs(self):
        return self._rhs

    @property
    def relates(self):
        return self._relates

    @property
    def spatial(self):
        return self._spatial

    @property
    def summarizes(self):
        return self._summarizes

    @property
    def primaryKey(self):
        """Full primary-key column name tuple.
        """
        keys = frozenset()
        for dataUnit in self.relates:
            keys |= dataUnit.primaryKey
        return keys


class DataUnitConfig(ConfigSubset):
    component = "dataUnits"
    requiredKeys = ("version", "dimensions")
    defaultConfigFile = "dataUnits.yaml"


class DataUnitRegistry:
    """Instances of this class keep track of `DataUnit` relations.

    Entries in this `dict`-like object represent `DataUnit` instances,
    keyed on `DataUnit` names.
    """

    def __init__(self):
        self._dataUnitNames = None
        self._dataUnits = {}
        self._dataUnitRegions = {}
        self.links = {}
        self.constraints = []
        self.joins = {}
        self._dataUnitsByLinkColumnName = {}
        self._spatialDataUnits = frozenset()

    @classmethod
    def fromConfig(cls, config):
        """Alternative constructor.

        Build a `DataUnitRegistry` instance from a `Config` object.

        Parameters
        ----------
        config : `DataUnitConfig`
            DataUnit configuration containing "dimensions", "joins" entries.
        """
        dataUnitRegistry = cls()
        dataUnitRegistry._initDataUnitNames(config["dimensions"])
        dataUnitRegistry._initDataUnits(config["dimensions"])
        dataUnitRegistry._initDataUnitJoins(config["joins"])
        return dataUnitRegistry

    def __len__(self):
        return len(self._dataUnits)

    def __getitem__(self, dataUnitName):
        return self._dataUnits[dataUnitName]

    def __setitem__(self, dataUnitName, dataUnit):
        assert isinstance(dataUnit, DataUnit)
        self._dataUnits[dataUnitName] = dataUnit

    def __iter__(self):
        return iter(self._dataUnitNames)

    def keys(self):
        return iter(self._dataUnitNames)

    def values(self):
        return (self[dataUnitName] for dataUnitName in self._dataUnitNames)

    def items(self):
        for dataUnitName in self._dataUnitNames:
            yield (dataUnitName, self[dataUnitName])

    def getRegionHolder(self, *dataUnits):
        """Return the DataUnit or DataUnitJoin that holds region for the
        given combination of DataUnits.

        Arguments may be either DataUnit instances or their names (but not
        link names), and required dependencies may or may not be included.
        Optional dependencies must not be included.

        Returned object can be either `DataUnitJoin` or `DataUnit`. Use
        ``table`` and/or ``regionColumn`` properties of returned object to
        retrieve region data from database table.

        Returns
        -------
        `DataUnitJoin` or `DataUnit` instance.

        Raises
        ------
        KeyError
            Raised if there is no Region associated with the given combination
            of DataUnits.
        """
        expanded = set()
        # Add all dependencies, so the caller can say just "Patch" as well as
        # "SkyMap", "Tract", "Patch" and get the same result.
        for u in dataUnits:
            unit = u if isinstance(u, DataUnit) else self[u]
            expanded.add(unit)
            expanded |= unit.requiredDependencies
        dataUnitNames = frozenset(unit.name for unit in expanded)
        return self._dataUnitRegions[dataUnitNames & self._spatialDataUnits]

    def getJoin(self, lhs, rhs):
        """Return the DataUnitJoin that relates the given DataUnit names.

        While DataUnitJoins are associated with a specific ordering or lhs and
        rhs, this method tries both.

        Parameters
        ----------
        lhs : `str` or sequence
            DataUnit name or sequence of names for one side of the join.
        rhs : `str` or sequence
            DataUnit name or sequence of names for the other side of the join.

        Returns
        -------
        join : `DataUnitJoin`
            The DataUnitJoin that relates the given DataUnits, or None.
        """
        lhs = frozenset(iterable(lhs))
        rhs = frozenset(iterable(rhs))
        return self.joins.get((lhs, rhs), None) or self.joins.get((rhs, lhs), None)

    def _initDataUnitNames(self, config):
        """Initialize DataUnit names.

        Because `DataUnit` entries may apear in any order in the `Config`,
        but dependencies between them define a topological order in which
        objects should be created, store them in a `TopologicalSet`.

        Parameters
        ----------
        config : `SchemaConfig`
            The `dataUnits` component of a `SchemaConfig`.
        """
        self._dataUnitNames = TopologicalSet(config)
        for dataUnitName, dataUnitDescription in config.items():
            if "dependencies" in dataUnitDescription:
                dependencies = dataUnitDescription["dependencies"]
                for category in ("required", "optional"):
                    if category in dependencies:
                        for dependency in dependencies[category]:
                            self._dataUnitNames.connect(dependency, dataUnitName)

    def _initDataUnits(self, config):
        """Initialize `DataUnit` entries.

        Parameters
        ----------
        config : `Config`
            The `dataUnits` component of a `DataUnitConfig`.
        """
        # Visit DataUnits in dependency order
        for dataUnitName in self._dataUnitNames:
            dataUnitDescription = config[dataUnitName]
            requiredDependencies = ()
            optionalDependencies = ()
            spatial = dataUnitDescription.get("spatial", False)
            link = dataUnitDescription.get("link")
            if "dependencies" in dataUnitDescription:
                dependencies = dataUnitDescription["dependencies"]
                if "required" in dependencies:
                    requiredDependencies = (self[name] for name in dependencies["required"])
                if "optional" in dependencies:
                    optionalDependencies = (self[name] for name in dependencies["optional"])
            dataUnit = DataUnit(name=dataUnitName,
                                requiredDependencies=requiredDependencies,
                                optionalDependencies=optionalDependencies,
                                link=link,
                                spatial=spatial)
            self[dataUnitName] = dataUnit
            for linkColumnName in link:
                self._dataUnitsByLinkColumnName[linkColumnName] = dataUnit
            if spatial:
                self._spatialDataUnits |= frozenset((dataUnitName, ))
                # The DataUnit (or DataUnitJoin) instance that can be used
                # to retreive the region is keyed based on the union
                # of the DataUnit and its required dependencies that are also spatial.
                # E.g. "Patch" is keyed on ("Tract", "Patch").
                # This requires that DataUnit's are visited in topologically sorted order
                # (which they are).
                key = frozenset((dataUnitName, ) +
                                tuple(d.name for d in dataUnit.requiredDependencies
                                      if d.name in self._spatialDataUnits))
                self._dataUnitRegions[key] = dataUnit

    def _initDataUnitJoins(self, config):
        """Initialize `DataUnit` join entries.

        Parameters
        ----------
        config : `DataUnitConfig`
            Configuration describing `DataUnit` join relations.
        """
        for dataUnitJoinName, dataUnitJoinDescription in config.items():
            lhs = frozenset((dataUnitJoinDescription.get("lhs", None)))
            rhs = frozenset((dataUnitJoinDescription.get("rhs", None)))
            dataUnitNames = lhs | rhs
            relates = frozenset(self[name] for name in dataUnitNames)
            summarizes = dataUnitJoinDescription.get("summarizes", None)
            spatial = dataUnitJoinDescription.get("spatial", False)
            dataUnitJoin = DataUnitJoin(name=dataUnitJoinName,
                                        lhs=lhs,
                                        rhs=rhs,
                                        summarizes=summarizes,
                                        spatial=spatial,
                                        relates=relates)
            self.joins[(lhs, rhs)] = dataUnitJoin
            if spatial:
                self._dataUnitRegions[dataUnitNames] = dataUnitJoin
                self._spatialDataUnits |= frozenset(dataUnitNames)

    def getPrimaryKeyNames(self, dataUnitNames):
        """Get all primary-key column names for the given ``dataUnitNames``.

        Parameters
        ----------
        dataUnitNames : `sequence`
            A sequence of `DataUnit` names.

        Returns
        -------
        primaryKeyNames : `set`
            All primary-key column names for the given ``dataUnitNames``.
        """
        return set(chain.from_iterable(self[name].primaryKey for name in dataUnitNames))

    def getByLinkName(self, name):
        """Get a `DataUnit` for which ``name`` is part of the link.

        Parameters
        ----------
        name : `str`
            Link name.

        Returns
        -------
        dataUnit : `DataUnit`
            The corresponding `DataUnit` instance.

        Raises
        ------
        KeyError
            When the provided ``name`` does not correspond to a link
            for any of the `DataUnit` entries in the registry.
        """
        return self._dataUnitsByLinkColumnName[name]
