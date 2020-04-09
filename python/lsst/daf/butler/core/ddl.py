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
"""Classes for representing SQL data-definition language (DDL; "CREATE TABLE",
etc.) in Python.

This provides an extra layer on top of SQLAlchemy's classes for these concepts,
because we need a level of indirection between logical tables and the actual
SQL, and SQLAlchemy's DDL classes always map 1-1 to SQL.

We've opted for the rather more obscure "ddl" as the name of this module
instead of "schema" because the latter is too overloaded; in most SQL
databases, a "schema" is also another term for a namespace.
"""
from __future__ import annotations

__all__ = ("TableSpec", "FieldSpec", "ForeignKeySpec", "Base64Bytes", "Base64Region",
           "AstropyTimeNsecTai")

from base64 import b64encode, b64decode
import logging
from math import ceil
from dataclasses import dataclass
from typing import Optional, Tuple, Sequence, Set

import sqlalchemy
import astropy.time

from lsst.sphgeom import ConvexPolygon
from .config import Config
from .exceptions import ValidationError
from . import time_utils
from .utils import iterable, stripIfNotNone, NamedValueSet


_LOG = logging.getLogger(__name__)


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


class Base64Bytes(sqlalchemy.TypeDecorator):
    """A SQLAlchemy custom type that maps Python `bytes` to a base64-encoded
    `sqlalchemy.String`.
    """

    impl = sqlalchemy.String

    def __init__(self, nbytes, *args, **kwds):
        length = 4*ceil(nbytes/3)
        super().__init__(*args, length=length, **kwds)
        self.nbytes = nbytes

    def process_bind_param(self, value, dialect):
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

    def process_result_value(self, value, dialect):
        # 'value' is a `str` that must be ASCII because it's base64-encoded.
        # We want to transform that to base64-encoded `bytes` and then
        # native `bytes`.
        return b64decode(value.encode("ascii")) if value is not None else None


class Base64Region(Base64Bytes):
    """A SQLAlchemy custom type that maps Python `sphgeom.ConvexPolygon` to a
    base64-encoded `sqlalchemy.String`.
    """

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return super().process_bind_param(value.encode(), dialect)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return ConvexPolygon.decode(super().process_result_value(value, dialect))


class AstropyTimeNsecTai(sqlalchemy.TypeDecorator):
    """A SQLAlchemy custom type that maps Python `astropy.time.Time` to a
    number of nanoseconds sunce Unix epoch in TAI scale.
    """

    impl = sqlalchemy.BigInteger

    def process_bind_param(self, value, dialect):
        # value is astropy.time.Time or None
        if value is None:
            return None
        if not isinstance(value, astropy.time.Time):
            raise TypeError(f"Unsupported type: {type(value)}, expected astropy.time.Time")
        value = time_utils.astropy_to_nsec(value)
        return value

    def process_result_value(self, value, dialect):
        # value is nanoseconds since epoch, or None
        if value is None:
            return None
        value = time_utils.nsec_to_astropy(value)
        return value


VALID_CONFIG_COLUMN_TYPES = {
    "string": sqlalchemy.String,
    "int": sqlalchemy.Integer,
    "float": sqlalchemy.Float,
    "region": Base64Region,
    "bool": sqlalchemy.Boolean,
    "blob": sqlalchemy.LargeBinary,
    "datetime": AstropyTimeNsecTai,
    "hash": Base64Bytes
}


@dataclass
class FieldSpec:
    """A struct-like class used to define a column in a logical `Registry`
    table.
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
        dtype = VALID_CONFIG_COLUMN_TYPES.get(config["type"])
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
        self.autoincrement = config.get("autoincrement", self.autoincrement)
        self.nullable = config.get("nullable", False if self.primaryKey else self.nullable)
        self.doc = stripIfNotNone(config.get("doc", None))
        return self

    def getSizedColumnType(self) -> sqlalchemy.types.TypeEngine:
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


@dataclass
class ForeignKeySpec:
    """A struct-like class used to define a foreign key constraint in a logical
    `Registry` table.
    """

    table: str
    """Name of the target table."""

    source: Tuple[str, ...]
    """Tuple of source table column names."""

    target: Tuple[str, ...]
    """Tuple of target table column names."""

    onDelete: Optional[str] = None
    """SQL clause indicating how to handle deletes to the target table.

    If not `None` (which indicates that a constraint violation exception should
    be raised), should be either "SET NULL" or "CASCADE".
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


@dataclass
class TableSpec:
    """A struct-like class used to define a table or table-like
    query interface.
    """

    fields: NamedValueSet[FieldSpec]
    """Specifications for the columns in this table."""

    unique: Set[Tuple[str, ...]] = frozenset()
    """Non-primary-key unique constraints for the table."""

    indexes: Set[Tuple[str, ...]] = frozenset()
    """Indexes for the table."""

    foreignKeys: Sequence[ForeignKeySpec] = tuple()
    """Foreign key constraints for the table."""

    doc: Optional[str] = None
    """Documentation for the table."""

    def __post_init__(self):
        self.fields = NamedValueSet(self.fields)
        self.unique = set(self.unique)
        self.indexes = set(self.indexes)
        self.foreignKeys = list(self.foreignKeys)

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
