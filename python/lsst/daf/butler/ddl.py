# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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
"""Classes for representing SQL data-definition language (DDL) in Python.

This include "CREATE TABLE" etc.

This provides an extra layer on top of SQLAlchemy's classes for these concepts,
because we need a level of indirection between logical tables and the actual
SQL, and SQLAlchemy's DDL classes always map 1-1 to SQL.

We've opted for the rather more obscure "ddl" as the name of this module
instead of "schema" because the latter is too overloaded; in most SQL
databases, a "schema" is also another term for a namespace.
"""
from __future__ import annotations

__all__ = (
    "TableSpec",
    "FieldSpec",
    "ForeignKeySpec",
    "IndexSpec",
    "Base64Bytes",
    "Base64Region",
    "AstropyTimeNsecTai",
    "GUID",
)

import functools
import logging
import uuid
from base64 import b64decode, b64encode
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from math import ceil
from typing import TYPE_CHECKING, Any

import astropy.time
import sqlalchemy
from lsst.sphgeom import Region, UnionRegion
from lsst.utils.iteration import ensure_iterable
from sqlalchemy.dialects.postgresql import UUID

from . import time_utils
from ._config import Config
from ._exceptions import ValidationError
from ._named import NamedValueSet
from .utils import stripIfNotNone

if TYPE_CHECKING:
    from .timespan_database_representation import TimespanDatabaseRepresentation


_LOG = logging.getLogger(__name__)


class SchemaValidationError(ValidationError):
    """Exceptions that indicate problems in Registry schema configuration."""

    @classmethod
    def translate(cls, caught: type[Exception], message: str) -> Callable:
        """Return decorator to re-raise exceptions as `SchemaValidationError`.

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

        def decorate(func: Callable) -> Callable:
            def decorated(self: Any, config: Config, *args: Any, **kwargs: Any) -> Any:
                try:
                    return func(self, config, *args, **kwargs)
                except caught as err:
                    raise cls(message.format(config=str(config), err=err)) from err

            return decorated

        return decorate


class Base64Bytes(sqlalchemy.TypeDecorator):
    """A SQLAlchemy custom type for Python `bytes`.

    Maps Python `bytes` to a base64-encoded `sqlalchemy.Text` field.

    Parameters
    ----------
    nbytes : `int` or `None`, optional
        Number of bytes.
    *args : `typing.Any`
        Parameters passed to base class constructor.
    **kwargs : `typing.Any`
        Keyword parameters passed to base class constructor.
    """

    impl = sqlalchemy.Text

    cache_ok = True

    def __init__(self, nbytes: int | None = None, *args: Any, **kwargs: Any):
        if nbytes is not None:
            length = 4 * ceil(nbytes / 3) if self.impl is sqlalchemy.String else None
        else:
            length = None
        super().__init__(*args, length=length, **kwargs)
        self.nbytes = nbytes

    def process_bind_param(self, value: bytes | None, dialect: sqlalchemy.engine.Dialect) -> str | None:
        # 'value' is native `bytes`.  We want to encode that to base64 `bytes`
        # and then ASCII `str`, because `str` is what SQLAlchemy expects for
        # String fields.
        if value is None:
            return None
        if not isinstance(value, bytes):
            raise TypeError(
                f"Base64Bytes fields require 'bytes' values; got '{value}' with type {type(value)}."
            )
        return b64encode(value).decode("ascii")

    def process_result_value(self, value: str | None, dialect: sqlalchemy.engine.Dialect) -> bytes | None:
        # 'value' is a `str` that must be ASCII because it's base64-encoded.
        # We want to transform that to base64-encoded `bytes` and then
        # native `bytes`.
        return b64decode(value.encode("ascii")) if value is not None else None

    @property
    def python_type(self) -> type[bytes]:
        return bytes


# create an alias, for use below to disambiguate between the built in
# sqlachemy type
LocalBase64Bytes = Base64Bytes


class Base64Region(Base64Bytes):
    """A SQLAlchemy custom type for Python `lsst.sphgeom.Region`.

    Maps Python `sphgeom.Region` to a base64-encoded `sqlalchemy.String`.
    """

    cache_ok = True  # have to be set explicitly in each class

    def process_bind_param(self, value: Region | None, dialect: sqlalchemy.engine.Dialect) -> str | None:
        if value is None:
            return None
        return super().process_bind_param(value.encode(), dialect)

    def process_result_value(self, value: str | None, dialect: sqlalchemy.engine.Dialect) -> Region | None:
        if value is None:
            return None
        return functools.reduce(
            UnionRegion,
            [
                # For some reason super() doesn't work here!
                Region.decode(Base64Bytes.process_result_value(self, union_member, dialect))
                for union_member in value.split(":")
            ],
        )

    @property
    def python_type(self) -> type[Region]:
        return Region

    @classmethod
    def union_aggregate(
        cls, column: sqlalchemy.ColumnElement[Base64Region]
    ) -> sqlalchemy.ColumnElement[Base64Region]:
        """Return a SQLAlchemy aggregate expression that computes the union of
        a set of unions.

        Parameters
        ----------
        column : `sqlalchemy.ColumnElement`
            SQLAlchemy column expression representing the regions to be
            combined.

        Returns
        -------
        union_column : `sqlalchemy.ColumnElement`
            SQLAlchemy column expression representing the union.
        """
        return sqlalchemy.cast(sqlalchemy.func.aggregate_strings(column, ":"), type_=Base64Region)


class AstropyTimeNsecTai(sqlalchemy.TypeDecorator):
    """A SQLAlchemy custom type for Python `astropy.time.Time`.

    Maps Python `astropy.time.Time` to a number of nanoseconds since Unix
    epoch in TAI scale.
    """

    impl = sqlalchemy.BigInteger

    cache_ok = True

    def process_bind_param(
        self, value: astropy.time.Time | None, dialect: sqlalchemy.engine.Dialect
    ) -> int | None:
        if value is None:
            return None
        if not isinstance(value, astropy.time.Time):
            raise TypeError(f"Unsupported type: {type(value)}, expected astropy.time.Time")
        value = time_utils.TimeConverter().astropy_to_nsec(value)
        return value

    def process_result_value(
        self, value: int | None, dialect: sqlalchemy.engine.Dialect
    ) -> astropy.time.Time | None:
        # value is nanoseconds since epoch, or None
        if value is None:
            return None
        value = time_utils.TimeConverter().nsec_to_astropy(value)
        return value


# TODO: sqlalchemy 2 has internal support for UUID:
# https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.Uuid
class GUID(sqlalchemy.TypeDecorator):
    """Platform-independent GUID type.

    Uses PostgreSQL's UUID type, otherwise uses CHAR(32), storing as
    stringified hex values.
    """

    impl = sqlalchemy.CHAR

    cache_ok = True

    def load_dialect_impl(self, dialect: sqlalchemy.Dialect) -> sqlalchemy.types.TypeEngine:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(sqlalchemy.CHAR(32))

    def process_bind_param(self, value: Any, dialect: sqlalchemy.Dialect) -> str | None:
        if value is None:
            return value

        # Coerce input to UUID type, in general having UUID on input is the
        # only thing that we want but there is code right now that uses ints.
        if isinstance(value, int):
            value = uuid.UUID(int=value)
        elif isinstance(value, bytes):
            value = uuid.UUID(bytes=value)
        elif isinstance(value, str):
            # hexstring
            value = uuid.UUID(hex=value)
        elif not isinstance(value, uuid.UUID):
            raise TypeError(f"Unexpected type of a bind value: {type(value)}")

        if dialect.name == "postgresql":
            return str(value)
        else:
            return f"{value.int:032x}"

    def process_result_value(
        self, value: str | uuid.UUID | None, dialect: sqlalchemy.Dialect
    ) -> uuid.UUID | None:
        if value is None or isinstance(value, uuid.UUID):
            # sqlalchemy 2 converts to UUID internally
            return value
        else:
            return uuid.UUID(hex=value)


VALID_CONFIG_COLUMN_TYPES = {
    "string": sqlalchemy.String,
    "int": sqlalchemy.BigInteger,
    "float": sqlalchemy.Float,
    "region": Base64Region,
    "bool": sqlalchemy.Boolean,
    "blob": sqlalchemy.LargeBinary,
    "datetime": AstropyTimeNsecTai,
    "hash": Base64Bytes,
    "uuid": GUID,
}


@dataclass
class FieldSpec:
    """A data class for defining a column in a logical `Registry` table."""

    name: str
    """Name of the column."""

    dtype: type
    """Type of the column; usually a `type` subclass provided by SQLAlchemy
    that defines both a Python type and a corresponding precise SQL type.
    """

    length: int | None = None
    """Length of the type in the database, for variable-length types."""

    nbytes: int | None = None
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
    """Whether this field is allowed to be NULL. If ``primaryKey`` is
    `True`, during construction this value will be forced to `False`."""

    default: Any = None
    """A server-side default value for this field.

    This is passed directly as the ``server_default`` argument to
    `sqlalchemy.schema.Column`.  It does _not_ go through SQLAlchemy's usual
    type conversion or quoting for Python literals, and should hence be used
    with care.  See the SQLAlchemy documentation for more information.
    """

    doc: str | None = None
    """Documentation for this field."""

    def __post_init__(self) -> None:
        if self.primaryKey:
            # Change the default to match primaryKey.
            self.nullable = False

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, FieldSpec):
            return self.name == other.name
        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(self.name)

    @classmethod
    @SchemaValidationError.translate(KeyError, "Missing key {err} in column config '{config}'.")
    def fromConfig(cls, config: Config, **kwargs: Any) -> FieldSpec:
        """Create a `FieldSpec` from a subset of a `SchemaConfig`.

        Parameters
        ----------
        config : `Config`
            Configuration describing the column.  Nested configuration keys
            correspond to `FieldSpec` attributes.
        **kwargs
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
        dtype = VALID_CONFIG_COLUMN_TYPES.get(config["type"])
        if dtype is None:
            raise SchemaValidationError(f"Invalid field type string: '{config['type']}'.")
        if not config["name"].islower():
            raise SchemaValidationError(f"Column name '{config['name']}' is not all lowercase.")
        self = cls(name=config["name"], dtype=dtype, **kwargs)
        self.length = config.get("length", self.length)
        self.nbytes = config.get("nbytes", self.nbytes)
        if self.length is not None and self.nbytes is not None:
            raise SchemaValidationError(f"Both length and nbytes provided for field '{self.name}'.")
        self.primaryKey = config.get("primaryKey", self.primaryKey)
        self.autoincrement = config.get("autoincrement", self.autoincrement)
        self.nullable = config.get("nullable", False if self.primaryKey else self.nullable)
        self.doc = stripIfNotNone(config.get("doc", None))
        return self

    @classmethod
    def for_region(cls, name: str = "region", nullable: bool = True, nbytes: int = 2048) -> FieldSpec:
        """Create a `FieldSpec` for a spatial region column.

        Parameters
        ----------
        name : `str`, optional
            Name for the field.
        nullable : `bool`, optional
            Whether NULL values are permitted.
        nbytes : `int`, optional
            Maximum number of bytes for serialized regions.  The actual column
            size will be larger to allow for base-64 encoding.

        Returns
        -------
        spec : `FieldSpec`
            Specification structure for a region column.
        """
        return cls(name, nullable=nullable, dtype=Base64Region, nbytes=nbytes)

    def isStringType(self) -> bool:
        """Indicate that this is a sqlalchemy.String field spec.

        Returns
        -------
        isString : `bool`
            The field refers to a `sqlalchemy.String` and not any other type.
            This can return `False` even if the object was created with a
            string type if it has been decided that it should be implemented
            as a `sqlalchemy.Text` type.
        """
        # For short strings retain them as strings
        if self.dtype is sqlalchemy.String and self.length and self.length <= 32:
            return True
        return False

    def getSizedColumnType(self) -> sqlalchemy.types.TypeEngine | type:
        """Return a sized version of the column type.

        Utilizes either (or neither) of ``self.length`` and ``self.nbytes``.

        Returns
        -------
        dtype : `sqlalchemy.types.TypeEngine`
            A SQLAlchemy column type object.
        """
        if self.length is not None:
            # Last chance check that we are only looking at possible String
            if self.dtype is sqlalchemy.String and not self.isStringType():
                return sqlalchemy.Text
            return self.dtype(length=self.length)
        if self.nbytes is not None:
            return self.dtype(nbytes=self.nbytes)
        return self.dtype

    def getPythonType(self) -> type:
        """Return the Python type associated with this field's (SQL) dtype.

        Returns
        -------
        type : `type`
            Python type associated with this field's (SQL) `dtype`.
        """
        # to construct these objects, nbytes keyword is needed
        if issubclass(self.dtype, LocalBase64Bytes):
            # satisfy mypy for something that must be true
            assert self.nbytes is not None
            return self.dtype(nbytes=self.nbytes).python_type
        else:
            return self.dtype().python_type  # type: ignore


@dataclass
class ForeignKeySpec:
    """Definition of a foreign key constraint in a logical `Registry` table."""

    table: str
    """Name of the target table."""

    source: tuple[str, ...]
    """Tuple of source table column names."""

    target: tuple[str, ...]
    """Tuple of target table column names."""

    onDelete: str | None = None
    """SQL clause indicating how to handle deletes to the target table.

    If not `None` (which indicates that a constraint violation exception should
    be raised), should be either "SET NULL" or "CASCADE".
    """

    addIndex: bool = True
    """If `True`, create an index on the columns of this foreign key in the
    source table.
    """

    @classmethod
    @SchemaValidationError.translate(KeyError, "Missing key {err} in foreignKey config '{config}'.")
    def fromConfig(cls, config: Config) -> ForeignKeySpec:
        """Create a `ForeignKeySpec` from a subset of a `SchemaConfig`.

        Parameters
        ----------
        config : `Config`
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
        return cls(
            table=config["table"],
            source=tuple(ensure_iterable(config["source"])),
            target=tuple(ensure_iterable(config["target"])),
            onDelete=config.get("onDelete", None),
        )


@dataclass(frozen=True)
class IndexSpec:
    """Specification of an index on table columns.

    Parameters
    ----------
    *columns : `str`
        Names of the columns to index.
    **kwargs : `Any`
        Additional keyword arguments to pass directly to
        `sqlalchemy.schema.Index` constructor. This could be used to provide
        backend-specific options, e.g. to create a ``GIST`` index in PostgreSQL
        one can pass ``postgresql_using="gist"``.
    """

    def __init__(self, *columns: str, **kwargs: Any):
        object.__setattr__(self, "columns", tuple(columns))
        object.__setattr__(self, "kwargs", kwargs)

    def __hash__(self) -> int:
        return hash(self.columns)

    columns: tuple[str, ...]
    """Column names to include in the index (`Tuple` [ `str` ])."""

    kwargs: dict[str, Any]
    """Additional keyword arguments passed directly to
    `sqlalchemy.schema.Index` constructor (`dict` [ `str`, `Any` ]).
    """


@dataclass
class TableSpec:
    """A data class used to define a table or table-like query interface.

    Parameters
    ----------
    fields : `~collections.abc.Iterable` [ `FieldSpec` ]
        Specifications for the columns in this table.
    unique : `~collections.abc.Iterable` [ `tuple` [ `str` ] ], optional
        Non-primary-key unique constraints for the table.
    indexes : `~collections.abc.Iterable` [ `IndexSpec` ], optional
        Indexes for the table.
    foreignKeys : `~collections.abc.Iterable` [ `ForeignKeySpec` ], optional
        Foreign key constraints for the table.
    exclusion : `~collections.abc.Iterable` [ `tuple` [ `str` or `type` ] ]
        Special constraints that prohibit overlaps between timespans over rows
        where other columns are equal.  These take the same form as unique
        constraints, but each tuple may contain a single
        `TimespanDatabaseRepresentation` subclass representing a timespan
        column.
    recycleIds : `bool`, optional
        If `True`, allow databases that might normally recycle autoincrement
        IDs to do so (usually better for performance) on any autoincrement
        field in this table.
    doc : `str`, optional
        Documentation for the table.
    """

    def __init__(
        self,
        fields: Iterable[FieldSpec],
        *,
        unique: Iterable[tuple[str, ...]] = (),
        indexes: Iterable[IndexSpec] = (),
        foreignKeys: Iterable[ForeignKeySpec] = (),
        exclusion: Iterable[tuple[str | type[TimespanDatabaseRepresentation], ...]] = (),
        recycleIds: bool = True,
        doc: str | None = None,
    ):
        self.fields = NamedValueSet(fields)
        self.unique = set(unique)
        self.indexes = set(indexes)
        self.foreignKeys = list(foreignKeys)
        self.exclusion = set(exclusion)
        self.recycleIds = recycleIds
        self.doc = doc

    fields: NamedValueSet[FieldSpec]
    """Specifications for the columns in this table."""

    unique: set[tuple[str, ...]]
    """Non-primary-key unique constraints for the table."""

    indexes: set[IndexSpec]
    """Indexes for the table."""

    foreignKeys: list[ForeignKeySpec]
    """Foreign key constraints for the table."""

    exclusion: set[tuple[str | type[TimespanDatabaseRepresentation], ...]]
    """Exclusion constraints for the table.

    Exclusion constraints behave mostly like unique constraints, but may
    contain a database-native Timespan column that is restricted to not overlap
    across rows (for identical combinations of any non-Timespan columns in the
    constraint).
    """

    recycleIds: bool = True
    """If `True`, allow databases that might normally recycle autoincrement IDs
    to do so (usually better for performance) on any autoincrement field in
    this table.
    """

    doc: str | None = None
    """Documentation for the table."""

    @classmethod
    @SchemaValidationError.translate(KeyError, "Missing key {err} in table config '{config}'.")
    def fromConfig(cls, config: Config) -> TableSpec:
        """Create a `ForeignKeySpec` from a subset of a `SchemaConfig`.

        Parameters
        ----------
        config : `Config`
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
            doc=stripIfNotNone(config.get("doc")),
        )
