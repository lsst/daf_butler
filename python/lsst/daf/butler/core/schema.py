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

import itertools

from .utils import iterable
from .views import makeView
from .config import ConfigSubset
from sqlalchemy import Column, String, Integer, Boolean, LargeBinary, DateTime,\
    Float, ForeignKeyConstraint, Table, MetaData

metadata = None  # Needed to make disabled test_hsc not fail on import

__all__ = ("SchemaConfig", "Schema", "SchemaBuilder")


class SchemaConfig(ConfigSubset):
    component = "schema"
    requiredKeys = ("version", "tables")
    defaultConfigFile = "schema.yaml"


class Schema:
    """The SQL schema for a Butler Registry.

    Parameters
    ----------
    dataUnits : `DataUnitsRegistry`
        Registry of all possible data units.
    config : `SchemaConfig` or `str`, optional
        Load configuration. Defaults will be read if ``config`` is not
        a `SchemaConfig`.

    Attributes
    ----------
    metadata : `sqlalchemy.MetaData`
        The sqlalchemy schema description.
    """
    def __init__(self, dataUnits, config=None):
        if config is None or not isinstance(config, SchemaConfig):
            config = SchemaConfig(config)
        self.config = config
        self.builder = SchemaBuilder()
        self.dataUnits = dataUnits
        self.buildFromConfig(config)

    def buildFromConfig(self, config):
        for tableName, tableDescription in self.config["tables"].items():
            self.builder.addTable(tableName, tableDescription)
        self.datasetTable = self.builder.metadata.tables["Dataset"]
        self._metadata = self.builder.metadata
        self._tables = self.builder.tables
        self._views = self.builder.views
        self.tables = {k: v for k, v in itertools.chain(self._tables.items(),
                                                        self._views.items())}


class SchemaBuilder:
    """Builds a Schema step-by-step.

    Attributes
    ----------
    metadata : `sqlalchemy.MetaData`
        The sqlalchemy schema description.
    tables : `dict`
        All created tables.
    views : `dict`
        All created views.
    """
    VALID_COLUMN_TYPES = {"string": String, "int": Integer, "float": Float, "region": LargeBinary,
                          "bool": Boolean, "blob": LargeBinary, "datetime": DateTime}

    def __init__(self):
        self.metadata = MetaData()
        self.tables = {}
        self.views = {}

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
        if "sql" in tableDescription:
            # This table can be materialized as a view
            table = makeView(tableName, self.metadata, selectable=tableDescription["sql"])
            self.views[tableName] = table
            view = True
        else:
            table = Table(tableName, self.metadata)
            self.tables[tableName] = table
            view = False
        if "columns" not in tableDescription:
            raise ValueError("No columns in table: {}".format(tableName))
        for columnDescription in tableDescription["columns"]:
            self.addColumn(table, columnDescription)
        if not view and "foreignKeys" in tableDescription:
            for constraintDescription in tableDescription["foreignKeys"]:
                self.addForeignKeyConstraint(table, constraintDescription)
        return table

    def addColumn(self, table, columnDescription):
        """Add a column to a table.

        Parameters
        ----------
        table : `sqlalchemy.Table`, `sqlalchemy.expression.TableClause` or `str`
            The table.
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
        if isinstance(table, str):
            table = self.metadata.tables[table]
        table.append_column(self.makeColumn(columnDescription))

    def addForeignKeyConstraint(self, table, constraintDescription):
        """Add a ForeignKeyConstraint to a table.

        Parameters
        ----------
        table : `sqlalchemy.Table` or `str`
            The table.
        constraintDescription : `dict`
            Description of the ForeignKeyConstraint to be created.
            Should always contain:
            - src, list of source column names
            - tgt, list of target column names
        """
        if isinstance(table, str):
            table = self.metadata.tables[table]
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
