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

__all__ = ("SchemaConfig", "Schema", "SchemaBuilder")

from base64 import b64encode, b64decode
from math import ceil

from .utils import iterable, stripIfNotNone
from .views import View
from .config import ConfigSubset
from sqlalchemy import Column, String, Integer, Boolean, LargeBinary, DateTime,\
    Float, ForeignKeyConstraint, Table, MetaData, TypeDecorator, UniqueConstraint,\
    Sequence

metadata = None  # Needed to make disabled test_hsc not fail on import


class Base64Bytes(TypeDecorator):
    """A SQLAlchemy custom type that maps Python `bytes` to a base64-encoded
    `sqlalchemy.String`.
    """

    impl = String

    def __init__(self, nbytes, *args, **kwds):
        length = 4*ceil(nbytes/3)
        TypeDecorator.__init__(self, *args, length=length, **kwds)
        self.nbytes = nbytes

    def process_bind_param(self, value, dialect):
        # 'value' is native `bytes`.  We want to encode that to base64 `bytes`
        # and then ASCII `str`, because `str` is what SQLAlchemy expects for
        # String fields.
        if value is None:
            return None
        if not isinstance(value, bytes):
            raise TypeError(
                f"Base64Bytes fields require 'bytes' values; got {value} with type {type(value)}"
            )
        return b64encode(value).decode('ascii')

    def process_result_value(self, value, dialect):
        # 'value' is a `str` that must be ASCII because it's base64-encoded.
        # We want to transform that to base64-encoded `bytes` and then
        # native `bytes`.
        return b64decode(value.encode('ascii')) if value is not None else None


class SchemaConfig(ConfigSubset):
    component = "schema"
    requiredKeys = ("version", "tables")
    defaultConfigFile = "schema.yaml"


class Schema:
    """The SQL schema for a Butler Registry.

    Parameters
    ----------
    config : `SchemaConfig` or `str`, optional
        Load configuration. Defaults will be read if ``config`` is not
        a `SchemaConfig`.
    limited : `bool`
        If `True`, ignore tables, views, and associated foreign keys whose
        config descriptions include a "limited" key set to `False`.

    Attributes
    ----------
    metadata : `sqlalchemy.MetaData`
        The sqlalchemy schema description.
    tables : `dict`
        A mapping from table or view name to the associated SQLAlchemy object.
        Note that this contains both true tables and views.
    views : `frozenset`
        The names of entries in ``tables`` that are actually implemented as
        views.
    """
    def __init__(self, config=None, limited=False):
        if config is None or not isinstance(config, SchemaConfig):
            config = SchemaConfig(config)
        builder = SchemaBuilder(config, limited=limited)
        self.metadata = builder.metadata
        self.views = frozenset(builder.views)
        self.tables = builder.tables


class SchemaBuilder:
    """Builds a Schema step-by-step.

    Parameters
    ----------
    config : `SchemaConfig`
        Configuration to parse.
    limited : `bool`
        If `True`, ignore tables, views, and associated foreign keys whose
        config descriptions include a "limited" key set to `False`.

    Attributes
    ----------
    metadata : `sqlalchemy.MetaData`
        The sqlalchemy schema description.
    tables : `dict`
        A mapping from table or view name to the associated SQLAlchemy object.
        Note that this contains both true tables and views.
    views : `set`
        The names of all entries in ``tables`` that are actually implemented as
        views.
    """
    VALID_COLUMN_TYPES = {"string": String, "int": Integer, "float": Float, "region": LargeBinary,
                          "bool": Boolean, "blob": LargeBinary, "datetime": DateTime,
                          "hash": Base64Bytes}

    def __init__(self, config, limited=False):
        self.config = config
        self.metadata = MetaData()
        self.tables = {}
        self.views = set()
        self._limited = limited
        for tableName, tableDescription in self.config["tables"].items():
            self.addTable(tableName, tableDescription)

    def isView(self, name):
        """Return True if the named table should be added / has been added as
        a view.

        Parameters
        ----------
        name : `str`
            Name of a table or view.  Does not need to have been added.

        Returns
        -------
        view : `bool`
            Whether the table should be added / has been added as a view.
        """
        if name in self.views:
            return True
        description = self.config["tables"][name]
        return "sql" in description and not description.get("materialize", False)

    def isIncluded(self, name):
        """Return True if the named table or view should be included in this
        schema.

        Parameters
        ----------
        name : `str`
            Name of a table or view.  Does not need to have been added.

        Returns
        -------
        included : `bool`
            Whether the table or view should be included in the schema.
        """
        if name in self.tables:
            return True
        description = self.config["tables"].get(name, None)
        if description is None:
            return False
        if self._limited:
            return description.get("limited", True)
        return True

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
            If a table with the given name already exists, or if the table
            name or one or more column names are not lowercase.
        """
        if tableName in self.metadata.tables:
            raise ValueError("Table with name {} already exists".format(tableName))
        if not tableName.islower():
            raise ValueError("Table name {} is not all lowercase.")
        if not self.isIncluded(tableName):
            return None
        doc = stripIfNotNone(tableDescription.get("doc", None))
        # Create a Table object (attaches itself to metadata)
        if self.isView(tableName):
            table = View(tableName, self.metadata, selectable=tableDescription["sql"], comment=doc,
                         info=tableDescription)
            self.tables[tableName] = table
            self.views.add(tableName)
        else:
            table = Table(tableName, self.metadata, comment=doc, info=tableDescription)
            self.tables[tableName] = table
        if "columns" not in tableDescription:
            raise ValueError("No columns in table: {}".format(tableName))
        for columnDescription in tableDescription["columns"]:
            self.addColumn(table, columnDescription)
        if "foreignKeys" in tableDescription:
            for constraintDescription in tableDescription["foreignKeys"]:
                self.addForeignKeyConstraint(table, constraintDescription)
        if "unique" in tableDescription:
            for columns in tableDescription["unique"]:
                table.append_constraint(UniqueConstraint(*columns))
        return table

    def addColumn(self, table, columnDescription):
        """Add a column to a table.

        Parameters
        ----------
        table : `sqlalchemy.Table`, `sqlalchemy.expression.TableClause`, `str`
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
            - length, length of the field
            - nbytes, length of decoded string (only for ``type=='hash'``)
            - doc, docstring
        """
        if isinstance(table, str):
            table = self.metadata.tables[table]
        table.append_column(self.makeColumn(columnDescription))

    def addForeignKeyConstraint(self, table, constraintDescription):
        """Add a ForeignKeyConstraint to a table.

        If the table or the ForeignKeyConstraint's target are views, or should
        not be included in this schema (because it is limited), does nothing.

        Parameters
        ----------
        table : `sqlalchemy.Table` or `str`
            The table.
        constraintDescription : `dict`
            Description of the ForeignKeyConstraint to be created.
            Should always contain:
            - src, list of source column names
            - tgt, list of target column names
            May also contain:
            - onDelete, one of "SET NULL" or "CASCADE".
        """
        if isinstance(table, str):
            table = self.metadata.tables[table]
        src, tgt, tgtTable, onDelete = self.normalizeForeignKeyConstraint(constraintDescription)
        if not self.isIncluded(table.name) or not self.isIncluded(tgtTable):
            return
        if self.isView(table.name) or self.isView(tgtTable):
            return
        table.append_constraint(ForeignKeyConstraint(src, tgt, ondelete=onDelete))

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
            - length, length of the field
            - autoinc, indicates column should auto increment
            - nbytes, length of decoded string (only for `type=='hash'`)
            - doc, docstring

        Returns
        -------
        c : `sqlalchemy.Column`
            The created `Column` entry.

        Raises
        ------
        ValueError
            If the column description contains unsupported arguments or if
            the column name is not lowercase.
        """
        description = columnDescription.copy()
        # required
        args = []
        # keeps track of if this column is intended to be auto incremented
        autoinc = False
        columnName = description.pop("name")
        if not columnName.islower():
            raise ValueError("Column name {} is not all lowercase.")
        args.append(columnName)
        columnType = self.VALID_COLUMN_TYPES[description.pop("type")]
        # extract kwargs for type object constructor, if any
        typeKwargs = {}
        for opt in ("length", "nbytes"):
            if opt in description:
                value = description.pop(opt)
                typeKwargs[opt] = value
        if typeKwargs:
            columnType = columnType(**typeKwargs)
        args.append(columnType)
        # check if autoinc is in the description and if it is, check if it is
        # set to true
        autoinc = description.pop("autoinc", False)
        if autoinc:
            # Generate a sequence to use for auto incrementing for
            # databases that do not support it natively, this will be
            # ignored by sqlalchemy for databases that do support it
            columnSequence = Sequence(columnName+"_seq", metadata=self.metadata)
            args.append(columnSequence)
        # extract kwargs for Column contructor.
        kwargs = {}
        for opt in ("nullable", "primary_key"):
            if opt in description:
                value = description.pop(opt)
                kwargs[opt] = value
        kwargs["comment"] = stripIfNotNone(description.pop("doc", None))
        # Note If auto incrementing some backends (such as postgres) MAY
        # require the following leaving this here for future reference:
        # kwargs["server_default"] = columnSequence.next_value()
        if description:
            raise ValueError("Unhandled extra kwargs: {} for column: {}".format(description, columnName))
        return Column(*args, **kwargs)

    def normalizeForeignKeyConstraint(self, constraintDescription):
        """Convert configuration for a ForeignKeyConstraint to standard form
        and return the target table.

        Parameters
        ----------
        constraintDescription : `dict`
            Description of the ForeignKeyConstraint to be created.
            Should always contain:

            - src, list of source column names or single source column name.
            - tgt, list of (table-qualified) target column names or single
              target column name.

            May also contain:

            - onDelete, one of "SET NULL" or "CASCADE".

        Returns
        -------
        src : `tuple`
            Sequence of field names in the local table.
        tgt : `tuple`
            Sequence of table-qualified field names in the remote table.
        tgtTable : `str`
            Name of the target table.
        onDelete : `str`, optional
            One of "SET NULL", "CASCADE", or `None`.
        """
        src = tuple(iterable(constraintDescription["src"]))
        tgt = tuple(iterable(constraintDescription["tgt"]))
        tgtTable, _ = tgt[0].split(".")
        assert all(t.split(".")[0] == tgtTable for t in tgt[1:])
        onDelete = constraintDescription.get("onDelete", None)
        return src, tgt, tgtTable, onDelete
