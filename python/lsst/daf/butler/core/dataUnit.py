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

from .utils import TopologicalSet

__all__ = ("DataUnit", "DataUnitRegistry")


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
        Names of columns that form the `DataUnit` specific part of the primary-key
        in this `DataUnit` table and are also the names of the link column in
        the Datasets table.
    table : `sqlalchemy.core.Table`, optional
        When not ``None`` the primary table entry corresponding to this
        `DataUnit`.
    """
    def __init__(self, name, requiredDependencies, optionalDependencies, link=(), table=None):
        self._name = name
        self._requiredDependencies = frozenset(requiredDependencies)
        self._optionalDependencies = frozenset(optionalDependencies)
        self._table = table
        self._link = link
        self._primaryKey = None

    def __repr__(self):
        return "DataUnit({})".format(self.name)

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
        """Related `DataUnit` instances that may also be provided (and when they
        are, they must be kept in sync) (`frozenset`, read-only).
        """
        return self._optionalDependencies

    @property
    def dependencies(self):
        """The union of `requiredDependencies` and `optionalDependencies`
        (`frozenset`, read-only).
        """
        return self.requiredDependencies.union(self.optionalDependencies)

    @property
    def table(self):
        """When not ``None`` the primary table entry corresponding to this
        `DataUnit` (`sqlalchemy.core.Table`, optional).
        """
        return getattr(self, '_table', None)

    @property
    def link(self):
        """Names of columns that form the `DataUnit` specific part of the primary-key
        in this `DataUnit` table and are also the names of the link column in
        the Datasets table (`tuple`).
        """
        return self._link

    @property
    def primaryKey(self):
        """Full primary-key column name tuple.  Consists of the ``link`` of this
        `DataUnit` and that of all its ``requiredDependencies`` (`set`).
        """
        if self._primaryKey is None:
            self._primaryKey = set(self.link)
            for dependency in self.requiredDependencies:
                self._primaryKey.update(dependency.primaryKey)
        return self._primaryKey

    @property
    def linkColumns(self):
        """Dictionary keyed on ``link`` names with `sqlalchemy.Column` entries
        into this `DataUnit` primary table as values (`dict`).
        """
        return {name: self.table.columns[name] for name in self.link}

    @property
    def primaryKeyColumns(self):
        """Dictionary keyed on ``primaryKey`` names with `sqlalchemy.Column` entries
        into this `DataUnit` primary table as values (`dict`).
        """
        return {name: self.table.columns[name] for name in self.primaryKey}

    def validateId(self, dataId):
        """Check if given dataId is valid.

        Parameters
        ----------
        dataId : `dict`
            A `dict` of `DataUnit` column name, value pairs.

        Raises
        ------
        ValueError
            If a value for a required dependency is missing.
        """
        missing = self.primaryKey - set(dataId.keys())
        if missing:
            raise ValueError("Missing required keys: {} from {} for DataUnit {}".format(
                missing, dataId, self.name))


class DataUnitRegistry:
    """Instances of this class keep track of `DataUnit` relations.

    Entries in this `dict`-like object represent `DataUnit` instances,
    keyed on `DataUnit` names.
    """
    def __init__(self):
        self._dataUnitNames = None
        self._dataUnits = {}
        self.links = {}

    @classmethod
    def fromConfig(cls, config, builder=None):
        """Alternative constructor.

        Build a `DataUnitRegistry` instance from a `Config` object and an
        (optional) `SchemaBuilder`.

        Parameters
        ----------
        config : `SchemaConfig`
            `Registry` schema configuration describing `DataUnit` relations.
        builder : `SchemaBuilder`, optional
            When given, create `sqlalchemy.core.Table` entries for every `DataUnit` table.
        """
        dataUnitRegistry = cls()
        dataUnitRegistry._initDataUnitNames(config)
        dataUnitRegistry._initDataUnits(config, builder)
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

    def _initDataUnitNames(self, config):
        """Initialize DataUnit names.

        Because `DataUnit` entries may apear in any order in the `Config`,
        but dependencies between them define a topological order in which objects
        should be created, store them in a `TopologicalSet`.

        Parameters
        ----------
        config : `SchemaConfig`
            Schema configuration describing `DataUnit` relations.
        """
        self._dataUnitNames = TopologicalSet(config)
        for dataUnitName, dataUnitDescription in config.items():
            if 'dependencies' in dataUnitDescription:
                dependencies = dataUnitDescription['dependencies']
                for category in ('required', 'optional'):
                    if category in dependencies:
                        for dependency in dependencies[category]:
                            self._dataUnitNames.connect(dependency, dataUnitName)

    def _initDataUnits(self, config, builder):
        """Initialize `DataUnit` entries.

        Parameters
        ----------
        config : `SchemaConfig`
            Schema configuration describing `DataUnit` relations.
        builder : `SchemaBuilder`, optional
            When given, create `sqlalchemy.core.Table` entries for every `DataUnit` table.
        """
        # Visit DataUnits in dependency order
        for dataUnitName in self._dataUnitNames:
            dataUnitDescription = config[dataUnitName]
            requiredDependencies = ()
            optionalDependencies = ()
            table = None
            link = ()
            if 'dependencies' in dataUnitDescription:
                dependencies = dataUnitDescription['dependencies']
                if 'required' in dependencies:
                    requiredDependencies = (self[name] for name in dependencies['required'])
                if 'optional' in dependencies:
                    optionalDependencies = (self[name] for name in dependencies['optional'])
            if builder is not None:
                if 'link' in dataUnitDescription:
                    # Link names
                    link = tuple((linkDescription['name'] for linkDescription in dataUnitDescription['link']))
                    # Link columns that will become part of the Datasets table
                    for linkDescription in dataUnitDescription['link']:
                        self.links[linkDescription['name']] = builder.makeColumn(linkDescription)
                if 'tables' in dataUnitDescription:
                    for tableName, tableDescription in dataUnitDescription['tables'].items():
                        if tableName == dataUnitName:
                            # Primary table for this DataUnit
                            table = builder.addTable(tableName, tableDescription)
                        else:
                            # Secondary table
                            builder.addTable(tableName, tableDescription)
            dataUnit = DataUnit(name=dataUnitName,
                                requiredDependencies=requiredDependencies,
                                optionalDependencies=optionalDependencies,
                                table=table,
                                link=link)
            self[dataUnitName] = dataUnit

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
