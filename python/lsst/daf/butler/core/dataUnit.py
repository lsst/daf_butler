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

from .utils import ConnectedSet

__all__ = ("DataUnit", "DataUnitRegistry")


class DataUnit:
    """A discrete abstract unit of data that can be associated with
    metadata and used to label datasets.

    `DataUnit` instances represent concrete units such as e.g. `Camera`,
    `Sensor`, `Visit` and `SkyMap`.

    Attributes
    ----------
    requiredDependencies : `frozenset`
        Related `DataUnit` instances on which existence this `DataUnit`
        instance depends.
    optionalDependencies : `frozenset`
        Related `DataUnit` instances that may also be provided (and when they
        are, they must be kept in sync).
    dependencies : `frozenset`
        The union of `requiredDependencies` and `optionalDependencies`.
    table : `sqlalchemy.core.Table`, optional
        When not ``None`` the primary table entry corresponding to this
        `DataUnit`.
    """
    def __init__(self, name, requiredDependencies, optionalDependencies, table=None, linkColumns=None):
        self._name = name
        self._requiredDependencies = frozenset(requiredDependencies)
        self._optionalDependencies = frozenset(optionalDependencies)
        self._table = table
        self._linkColumns = linkColumns.copy() if linkColumns is not None else {}
        self._primaryKeyColumns = None

    def __repr__(self):
        return "DataUnit({})".format(self.name)

    @property
    def name(self):
        return self._name

    @property
    def requiredDependencies(self):
        return self._requiredDependencies

    @property
    def optionalDependencies(self):
        return self._optionalDependencies

    @property
    def dependencies(self):
        return self.requiredDependencies.union(self.optionalDependencies)

    @property
    def table(self):
        if hasattr(self, '_table'):
            return self._table
        else:
            return None

    @property
    def linkColumns(self):
        return self._linkColumns

    @property
    def primaryKeyColumns(self):
        if self._primaryKeyColumns is None:
            self._primaryKeyColumns = self.linkColumns.copy()
            for dependency in self.requiredDependencies:
                self._primaryKeyColumns.update(dependency.primaryKeyColumns)
        return self._primaryKeyColumns

    def validateId(self, value):
        """Check if id is valid.

        Raises
        ------
        ValueError
            If a value for a required dependency is missing.
        """
        for columnName in self.primaryKeyColumns:
            if columnName not in value:
                raise ValueError("Required primary-key column {} missing from value {}".format(
                    columnName, value))


class DataUnitRegistry:
    def __init__(self):
        self._dataUnitNames = None
        self._dataUnits = {}
        self.links = {}  # TODO move this

    def __len__(self):
        return len(self._dataUnits)

    def __getitem__(self, dataUnitName):
        return self._dataUnits[dataUnitName]

    def __setitem__(self, dataUnitName, dataUnit):
        assert isinstance(dataUnit, DataUnit)
        self._dataUnits[dataUnitName] = dataUnit

    def __iter__(self):
        yield from self._dataUnitNames

    def keys(self):
        yield from self._dataUnitNames

    def values(self):
        return (self[dataUnitName] for dataUnitName in self._dataUnitNames)

    def items(self):
        for dataUnitName in self._dataUnitNames:
            yield (dataUnitName, self[dataUnitName])

    @classmethod
    def fromConfig(cls, config, builder=None):
        dataUnitRegistry = cls()
        dataUnitRegistry.builder = builder
        dataUnitRegistry._initDataUnitNames(config)
        dataUnitRegistry._initDataUnits(config)
        return dataUnitRegistry

    def _initDataUnitNames(self, config):
        self._dataUnitNames = ConnectedSet(config)
        for dataUnitName, dataUnitDescription in config.items():
            if 'dependencies' in dataUnitDescription:
                dependencies = dataUnitDescription['dependencies']
                for category in ('required', 'optional'):
                    if category in dependencies:
                        for dependency in dependencies[category]:
                            self._dataUnitNames.connect(dependency, dataUnitName)

    def _initDataUnits(self, config):
        # Visit DataUnits in dependency order
        for dataUnitName in self._dataUnitNames:
            dataUnitDescription = config[dataUnitName]
            requiredDependencies = ()
            optionalDependencies = ()
            table = None
            linkColumns = {}
            if 'dependencies' in dataUnitDescription:
                dependencies = dataUnitDescription['dependencies']
                if 'required' in dependencies:
                    requiredDependencies = (self[name] for name in dependencies['required'])
                if 'optional' in dependencies:
                    optionalDependencies = (self[name] for name in dependencies['optional'])
            if self.builder is not None:
                if 'tables' in dataUnitDescription:
                    for tableName, tableDescription in dataUnitDescription['tables'].items():
                        if tableName == dataUnitName:
                            # Primary table for this DataUnit
                            table = self.builder.addTable(tableName, tableDescription)
                        else:
                            # Secondary table
                            self.builder.addTable(tableName, tableDescription)
                if 'link' in dataUnitDescription:
                    for dataUnitLinkDescription in dataUnitDescription['link']:
                        linkColumnName = dataUnitLinkDescription['name']
                        # Link column in Datasets table
                        linkColumnName = dataUnitLinkDescription['name']
                        self.links[linkColumnName] = self.builder.makeColumn(dataUnitLinkDescription)
                        # Link column in DataUnit table
                        if table is not None:
                            linkColumns[linkColumnName] = table.columns[linkColumnName]

            dataUnit = DataUnit(name=dataUnitName,
                                requiredDependencies=requiredDependencies,
                                optionalDependencies=optionalDependencies,
                                table=table,
                                linkColumns=linkColumns)
            self[dataUnitName] = dataUnit
