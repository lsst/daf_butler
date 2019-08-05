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
from __future__ import annotations

__all__ = ("SchemaConfig", "Schema")

from base64 import b64encode, b64decode
from math import ceil
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, Set, List, Union

from lsst.sphgeom import ConvexPolygon
from .views import View
from .config import ConfigSubset, Config
from .exceptions import ValidationError
from .utils import iterable, stripIfNotNone, NamedValueSet
from sqlalchemy import Column, String, Integer, Boolean, LargeBinary, DateTime,\
    Float, ForeignKeyConstraint, Table, MetaData, TypeDecorator, UniqueConstraint,\
    Sequence
from sqlalchemy.types import TypeEngine


class SchemaValidationError(ValidationError):
    """Exceptions used to indicate problems in Registry schema configuration.
    """

    @classmethod
    def translate(cls, caught, message):
        """A decorator that re-raises exceptions as `SchemaValidationError`.

        Decorated functions must be class or instance methods, with a
        ``config`` parameter as their first argument.  This will be passed
        to ``message.format()`` as a keyword argument, along with ``err``,
        the original exception.

        Parameters
        ----------
        caught : `type` (`Exception` subclass)
            The type of exception to catch.
        message : `str`
            A `str.format` string that may contain named placeholders for
            ``config``, ``err``, or any keyword-only argument accepted by
            the decorated function.
        """
        def decorate(func):
            def decorated(self, config, *args, **kwds):
                try:
                    return func(self, config, *args, **kwds)
                except caught as err:
                    raise cls(message.format(config=str(config), err=err))
            return decorated
        return decorate


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


class Base64Region(TypeDecorator):
    """A SQLAlchemy custom type that maps Python `sphgeom.ConvexPolygon` to a
    base64-encoded `sqlalchemy.LargeBinary`.
    """

    impl = LargeBinary

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return b64encode(value.encode())

    def process_result_value(self, value, dialect):
        return ConvexPolygon.decode(b64decode(value)) if value is not None else None


VALID_COLUMN_TYPES = {"string": String, "int": Integer, "float": Float, "region": Base64Region,
                      "bool": Boolean, "blob": LargeBinary, "datetime": DateTime, "hash": Base64Bytes}


@dataclass
class FieldSpec:
    """A struct-like class used to define a column in a logical Registry table.
    """

    name: str
    """Name of the column."""

    dtype: type
    """Type of the column; usually a `type` subclass provided by SQLAlchemy
    that defines both a Python type and a corresponding precise SQL type.
    """

    length: Optional[int] = None
    """Length of the type in the database, for variable-length types."""

    nbytes: Optional[int] = None
    """Natural length used for hash and encoded-region columns, to be converted
    into the post-encoding length.
    """

    primaryKey: bool = False
    """Whether this field is (part of) its table's primary key."""

    autoincrement: bool = False
    """Whether the database should insert automatically incremented values when
    no value is provided in an INSERT.
    """

    nullable: bool = True
    """Whether this field is allowed to be NULL."""

    doc: Optional[str] = None
    """Documentation for this field."""

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    @classmethod
    @SchemaValidationError.translate(KeyError, "Missing key {err} in column config '{config}'.")
    def fromConfig(cls, config: Config, **kwds) -> FieldSpec:
        """Create a `FieldSpec` from a subset of a `SchemaConfig`.

        Parameters
        ----------
        config: `Config`
            Configuration describing the column.  Nested configuration keys
            correspond to `FieldSpec` attributes.
        kwds
            Additional keyword arguments that provide defaults for values
            not present in config.

        Returns
        -------
        spec: `FieldSpec`
            Specification structure for the column.

        Raises
        ------
        SchemaValidationError
            Raised if configuration keys are missing or have invalid values.
        """
        dtype = VALID_COLUMN_TYPES.get(config["type"])
        if dtype is None:
            raise SchemaValidationError(f"Invalid field type string: '{config['type']}'.")
        if not config["name"].islower():
            raise SchemaValidationError(f"Column name '{config['name']}' is not all lowercase.")
        self = cls(name=config["name"], dtype=dtype, **kwds)
        self.length = config.get("length", self.length)
        self.nbytes = config.get("nbytes", self.nbytes)
        if self.length is not None and self.nbytes is not None:
            raise SchemaValidationError(f"Both length and nbytes provided for field '{self.name}'.")
        self.primaryKey = config.get("primaryKey", self.primaryKey)
        self.autoincrement = config.get("autoinc", self.autoincrement)
        self.nullable = config.get("nullable", False if self.primaryKey else self.nullable)
        self.doc = stripIfNotNone(config.get("doc", None))
        return self

    def getSizedColumnType(self) -> TypeEngine:
        """Return a sized version of the column type, utilizing either (or
        neither) of ``self.length`` and ``self.nbytes``.

        Returns
        -------
        dtype : `sqlalchemy.types.TypeEngine`
            A SQLAlchemy column type object.
        """
        if self.length is not None:
            return self.dtype(length=self.length)
        if self.nbytes is not None:
            return self.dtype(nbytes=self.nbytes)
        return self.dtype

    def toSqlAlchemy(self, tableName: str, schema: Schema) -> Column:
        """Construct a SQLAlchemy `Column` object from this specification.

        Parameters
        ----------
        tableName : `str`
            Name of the logical table to which this column belongs.
        schema : `Schema`
            Object represening the full schema.  May be modified in-place.

        Returns
        -------
        column : `sqlalchemy.Column`
            SQLAlchemy column object.
        """
        args = [self.name, self.getSizedColumnType()]
        if self.autoincrement:
            # Generate a sequence to use for auto incrementing for databases
            # that do not support it natively.  This will be ignored by
            # sqlalchemy for databases that do support it.
            args.append(Sequence(f"{tableName}_{self.name}_seq", metadata=schema.metadata))
        return Column(*args, nullable=self.nullable, primary_key=self.primaryKey, comment=self.doc)


@dataclass
class ForeignKeySpec:
    """A struct-like class used to define a foreign key constraint in a logical
    Registry table.
    """

    table: str
    """Name of the target table."""

    source: Tuple[str, ...]
    """Tuple of source table column names."""

    target: Tuple[str, ...]
    """Tuple of target table column names."""

    onDelete: Optional[str] = None
    """SQL clause indicating how to handle deletes to the target table.

    If not `None`, should be either "SET NULL" or "CASCADE".
    """

    @classmethod
    @SchemaValidationError.translate(KeyError, "Missing key {err} in foreignKey config '{config}'.")
    def fromConfig(cls, config: Config) -> ForeignKeySpec:
        """Create a `ForeignKeySpec` from a subset of a `SchemaConfig`.

        Parameters
        ----------
        config: `Config`
            Configuration describing the constraint.  Nested configuration keys
            correspond to `ForeignKeySpec` attributes.

        Returns
        -------
        spec: `ForeignKeySpec`
            Specification structure for the constraint.

        Raises
        ------
        SchemaValidationError
            Raised if configuration keys are missing or have invalid values.
        """
        return cls(table=config["table"],
                   source=tuple(iterable(config["source"])),
                   target=tuple(iterable(config["target"])),
                   onDelete=config.get("onDelete", None))

    def toSqlAlchemy(self, tableName: str, schema: Schema) -> ForeignKeyConstraint:
        """Construct a SQLAlchemy `ForeignKeyConstraint` corresponding to this
        specification.

        Parameters
        ----------
        tableName : `str`
            Name of the logical table to which this constraint belongs
            (the table for source columns).
        schema : `Schema`
            Object represening the full schema.  May be modified in-place.

        Returns
        -------
        constraint : `sqlalchemy.ForeignKeyConstraint`
            SQLAlchemy version of the foreign key constraint.
        """
        return ForeignKeyConstraint(self.source,
                                    [f"{self.table}.{col}" for col in self.target],
                                    name=f"{tableName}_{self.table}_fkey",
                                    ondelete=self.onDelete)


@dataclass
class TableSpec:
    """A struct-like class used to define a logical Registry table (which may
    also be implemented in the database as a view).
    """

    fields: NamedValueSet[FieldSpec]
    """Specifications for the columns in this table."""

    unique: Set[Tuple[str, ...]]
    """Non-primary-key unique constraints for the table."""

    foreignKeys: List[ForeignKeySpec]
    """Foreign key constraints for the table."""

    sql: Optional[str] = None
    """A SQL SELECT statement that can be used to define this logical table as
    a view.

    Should be `None` if this table's contents is not defined in terms of other
    tables.
    """

    doc: Optional[str] = None
    """Documentation for the table."""

    @classmethod
    @SchemaValidationError.translate(KeyError, "Missing key {err} in table config '{config}'.")
    def fromConfig(cls, config: Config) -> TableSpec:
        """Create a `ForeignKeySpec` from a subset of a `SchemaConfig`.

        Parameters
        ----------
        config: `Config`
            Configuration describing the constraint.  Nested configuration keys
            correspond to `TableSpec` attributes.

        Returns
        -------
        spec: `TableSpec`
            Specification structure for the table.

        Raises
        ------
        SchemaValidationError
            Raised if configuration keys are missing or have invalid values.
        """
        return cls(
            fields=NamedValueSet(FieldSpec.fromConfig(c) for c in config["columns"]),
            unique={tuple(u) for u in config.get("unique", ())},
            foreignKeys=[ForeignKeySpec.fromConfig(c) for c in config.get("foreignKeys", ())],
            sql=config.get("sql"),
            doc=stripIfNotNone(config.get("doc")),
        )

    def isView(self) -> bool:
        """Return `True` if this logical table should be implemented as some
        kind of view.
        """
        return self.sql is not None

    def toSqlAlchemy(self, tableName: str, schema: Schema) -> Union[Table, View]:
        """Construct a SQLAlchemy `Table` or `View` corresponding to this
        specification.

        This does not emit the actual DDL statements that would create the
        table or view in the database; it merely creates a SQLAlchemy
        representation of the table or view.

        Parameters
        ----------
        tableName : `str`
            Name of the logical table.
        schema : `Schema`
            Object represening the full schema.  Will be modified in-place.

        Returns
        -------
        table : `sqlalchemy.Table` or `View`
            A SQLAlchemy object representing the logical table.

        Notes
        -----
        This does *not* add foreign key constraints, as all tables must be
        created (in the SQLAlchemy metadata sense) before foreign keys can
        safely be created.  `addForeignKeys` must be called to complete this
        process.
        """
        if tableName in schema.metadata.tables:
            raise SchemaValidationError(f"Table with name '{tableName}' already exists.")
        if not tableName.islower():
            raise SchemaValidationError(f"Table name '{tableName}' is not all lowercase.")
        if self.isView():
            table = View(tableName, schema.metadata, selectable=self.sql, comment=self.doc, info=self)
            schema.views.add(tableName)
        else:
            table = Table(tableName, schema.metadata, comment=self.doc, info=self)
        schema.tables[tableName] = table
        for fieldSpec in self.fields:
            table.append_column(fieldSpec.toSqlAlchemy(tableName, schema.metadata))
        for columns in self.unique:
            table.append_constraint(UniqueConstraint(*columns))
        return table

    def addForeignKeys(self, tableName: str, schema: Schema):
        """Add SQLAlchemy foreign key constraints for this table to the
        corresponding entry in the given schema.

        Parameters
        ----------
        tableName : `str`
            Name of the logical table.
        schema : `Schema`
            Object represening the full schema.  Will be modified in-place.

        Notes
        -----
        This must be called after `toSqlAlchemy` has been called on all tables
        in the schema.
        """
        table = schema.tables[tableName]
        if not self.isView():
            for foreignKeySpec in self.foreignKeys:
                if foreignKeySpec.table not in schema.views:
                    table.append_constraint(foreignKeySpec.toSqlAlchemy(tableName, schema.metadata))


class SchemaConfig(ConfigSubset):
    component = "schema"
    requiredKeys = ("version", "tables")
    defaultConfigFile = "schema.yaml"

    def toSpec(self) -> Dict[str, TableSpec]:
        return {name: TableSpec.fromConfig(config) for name, config in self["tables"].items()}


class Schema:
    """The SQL schema for a Butler Registry.

    Parameters
    ----------
    spec : `dict`, optional
        Dictionary mapping `str` name to `TableSpec`, as returned by
        `SchemaConfig.toSpec`.  Will be constructed from default configuration
        if not provided.

    Attributes
    ----------
    metadata : `sqlalchemy.MetaData`
        The sqlalchemy schema description.
    tables : `dict`
        A mapping from table or view name to the associated SQLAlchemy object.
        Note that this contains both true tables and views.
    views : `set`
        The names of entries in ``tables`` that are actually implemented as
        views.
    """
    def __init__(self, spec=None):
        if spec is None:
            config = SchemaConfig()
            spec = config.toSpec()
        self.metadata = MetaData()
        self.views = set()
        self.tables = dict()
        # We ignore the return values of the `toSqlAlchemy` calls below because
        # they also mutate `self`, and that's all we need.  When we refactor
        # `SqlRegistry`'s dataset operations in the future, we'll probably
        # replace `Schema` with a simple `dict` and clean this up.
        for tableName, tableSpec in spec.items():
            tableSpec.toSqlAlchemy(tableName, self)
        for tableName, tableSpec in spec.items():
            tableSpec.addForeignKeys(tableName, self)
