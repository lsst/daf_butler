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

from .utils import iterable
from .config import ConfigSubset
from sqlalchemy import Column, String, Integer, Boolean, LargeBinary, DateTime,\
    Float, ForeignKey, ForeignKeyConstraint, Table, MetaData
from .dataUnit import DataUnitRegistry

metadata = None  # Needed to make disabled test_hsc not fail on import

__all__ = ("SchemaConfig", "Schema", "SchemaBuilder")


class SchemaConfig(ConfigSubset):
    component = "schema"
    requiredKeys = ("version", "dataUnits")
    defaultConfigFile = "registry_schema.yaml"


class Schema:
    """The SQL schema for a Butler Registry.

    Parameters
    ----------
    config : `SchemaConfig` or `str`, optional
        Load configuration. Defaults will be read if ``config`` is not
        a `SchemaConfig`.

    Attributes
    ----------
    metadata : `sqlalchemy.MetaData`
        The sqlalchemy schema description.
    """
    def __init__(self, config=None):
        if config is None or not isinstance(config, SchemaConfig):
            config = SchemaConfig(config)
        self.config = config
        self.builder = SchemaBuilder()
        self.dataUnits = DataUnitRegistry.fromConfig(config['dataUnits'], self.builder)
        self.buildFromConfig(config)

    def buildFromConfig(self, config):
        for tableName, tableDescription in self.config['tables'].items():
            self.builder.addTable(tableName, tableDescription)
        datasetTable = self.builder.metadata.tables['Dataset']
        for linkColumn in self.dataUnits.links.values():
            datasetTable.append_column(linkColumn)
        self.metadata = self.builder.metadata


class SchemaBuilder:
    """Builds a Schema step-by-step.

    Attributes
    ----------
    metadata : `sqlalchemy.MetaData`
        The sqlalchemy schema description.
    """
    VALID_COLUMN_TYPES = {'string': String, 'int': Integer, 'float': Float,
                          'bool': Boolean, 'blob': LargeBinary, 'datetime': DateTime}

    def __init__(self):
        self.metadata = MetaData()

    def addTable(self, tableName, tableDescription):
        """Add a table to the schema metadata.

        Parameters
        ----------
        tableName : `str`
            Key of the table.
        tableDescription : `dict`
            Table description.

            Requires:
            - columns, a list of column descriptions
            - foreignKeys, a list of foreign-key constraint descriptions

        Raises
        ------
        ValueError
            If a table with the given name already exists.
        """
        if tableName in self.metadata.tables:
            raise ValueError("Table with name {} already exists".format(tableName))
        # Create a Table object (attaches itself to metadata)
        table = Table(tableName, self.metadata)
        if "columns" not in tableDescription:
            raise ValueError("No columns in table: {}".format(tableName))
        for columnDescription in tableDescription["columns"]:
            self.addColumn(tableName, columnDescription)
        if "foreignKeys" in tableDescription:
            for constraintDescription in tableDescription["foreignKeys"]:
                self.addForeignKeyConstraint(tableName, constraintDescription)
        return table

    def addColumn(self, tableName, columnDescription):
        """Add a column to a table.

        Parameters
        ----------
        tableName : `str`
            Key of the table.
        columnDescription : `dict`
            Description of the column to be created.
            Should always contain:
            - name, descriptive name
            - type, valid column type
            May contain:
            - nullable, entry can be null
            - primary_key, mark this column as primary key
            - foreign_key, link to other table
            - doc, docstring
        """
        table = self.metadata.tables[tableName]
        table.append_column(self.makeColumn(columnDescription))

    def addForeignKeyConstraint(self, tableName, constraintDescription):
        """Add a ForeignKeyConstraint to a table.

        Parameters
        ----------
        tableName : `str`
            Key of the table.
        constraintDescription : `dict`
            Description of the ForeignKeyConstraint to be created.
            Should always contain:
            - src, list of source column names
            - tgt, list of target column names
        """
        table = self.metadata.tables[tableName]
        table.append_constraint(self.makeForeignKeyConstraint(constraintDescription))

    def makeColumn(self, columnDescription):
        """Make a Column entry for addition to a Table.

        Parameters
        ----------
        columnDescription : `dict`
            Description of the column to be created.
            Should always contain:
            - name, descriptive name
            - type, valid column type
            May contain:
            - nullable, entry can be null
            - primary_key, mark this column as primary key
            - foreign_key, link to other table
            - doc, docstring

        Returns
        -------
        c : `sqlalchemy.Column`
            The created `Column` entry.

        Raises
        ------
        ValueError
            If the column description contains unsupported arguments
        """
        description = columnDescription.copy()
        # required
        columnName = description.pop("name")
        args = (columnName, self.VALID_COLUMN_TYPES[description.pop("type")])
        # foreign_key is special
        if "foreign_key" in description:
            args += (ForeignKey(description.pop("foreign_key")), )
        # additional optional arguments can be passed through directly
        kwargs = {}
        for opt in ("nullable", "primary_key", "doc"):
            if opt in description:
                value = description.pop(opt)
                kwargs[opt] = value
        if description:
            raise ValueError("Unhandled extra kwargs: {} for column: {}".format(description, columnName))
        return Column(*args, **kwargs)

    def makeForeignKeyConstraint(self, constraintDescription):
        """Make a ForeignKeyConstraint for addition to a Table.

        Parameters
        ----------
        constraintDescription : `dict`
            Description of the ForeignKeyConstraint to be created.
            Should always contain:
            - src, list of source column names
            - tgt, list of target column names
        """
        src = tuple(iterable(constraintDescription["src"]))
        tgt = tuple(iterable(constraintDescription["tgt"]))
        return ForeignKeyConstraint(src, tgt)
