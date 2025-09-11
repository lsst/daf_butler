# This file is part of butler4.
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

from __future__ import annotations

__all__ = (
    "COLLECTION_NAME_MAX_LENGTH",
    "BoolColumnSpec",
    "ColumnSpec",
    "ColumnType",
    "FloatColumnSpec",
    "HashColumnSpec",
    "IntColumnSpec",
    "RegionColumnSpec",
    "StringColumnSpec",
    "TimespanColumnSpec",
    "UUIDColumnSpec",
    "make_tuple_type_adapter",
)

import textwrap
import uuid
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Literal,
    TypeAlias,
    Union,
    final,
)

import astropy.time
import pyarrow as pa
import pydantic

from lsst.sphgeom import Region

from . import arrow_utils, ddl
from ._timespan import Timespan
from .pydantic_utils import SerializableBytesHex, SerializableRegion, SerializableTime

if TYPE_CHECKING:
    from .name_shrinker import NameShrinker

ColumnType: TypeAlias = Literal[
    "int",
    "string",
    "hash",
    "float",
    "datetime",
    "bool",
    "uuid",
    "timespan",
    "region",
    # The ingest_date column in the datasets table can be one of two column
    # types:
    # 1. TIMESTAMP column (which is not used anywhere else in the DB)
    # 2. Integer nanoseconds TAI (same as "datetime" column type)
    # Which it is depends on the database schema in use for the "datasets"
    # manager.  (v1 is TIMESTAMP, v2 is integer).  See makeStaticTableSpecs in
    # lsst.daf.butler.registry.datasets.byDimensions.tables.
    #
    # We don't know which it is until we go to resolve the query against
    # a database, so it has to be its own data type.
    "ingest_date",
]


COLLECTION_NAME_MAX_LENGTH = 64
# TODO: DM-42541 would bee a good opportunity to move this constant to a
# better home; this file is the least-bad home I can think of for now.  Note
# that actually changing the value is a (minor) schema change.


class ColumnValueSerializer(ABC):
    """Class that knows how to serialize and deserialize column values."""

    @abstractmethod
    def serialize(self, value: Any) -> Any:
        """Convert column value to something that can be serialized.

        Parameters
        ----------
        value : `Any`
            Column value to be serialized.

        Returns
        -------
        value : `Any`
            Column value in serializable format.
        """
        raise NotImplementedError

    @abstractmethod
    def deserialize(self, value: Any) -> Any:
        """Convert serialized value to column value.

        Parameters
        ----------
        value : `Any`
            Serialized column value.

        Returns
        -------
        value : `Any`
            Deserialized column value.
        """
        raise NotImplementedError


class _TypeAdapterColumnValueSerializer(ColumnValueSerializer):
    """Implementation of serializer that uses pydantic type adapter."""

    def __init__(self, type_adapter: pydantic.TypeAdapter):
        # Docstring inherited.
        self._type_adapter = type_adapter

    def serialize(self, value: Any) -> Any:
        # Docstring inherited.
        return value if value is None else self._type_adapter.dump_python(value)

    def deserialize(self, value: Any) -> Any:
        # Docstring inherited.
        return value if value is None else self._type_adapter.validate_python(value)


class _BaseColumnSpec(pydantic.BaseModel, ABC):
    """Base class for descriptions of table columns."""

    pytype: ClassVar[type]

    name: str = pydantic.Field(description="""Name of the column.""")

    doc: str = pydantic.Field(default="", description="Documentation for the column.")

    type: ColumnType

    nullable: bool = pydantic.Field(
        default=True,
        description="Whether the column may be ``NULL``.",
    )

    def to_sql_spec(self, name_shrinker: NameShrinker | None = None, **kwargs: Any) -> ddl.FieldSpec:
        """Convert this specification to a SQL-specific one.

        Parameters
        ----------
        name_shrinker : `NameShrinker`, optional
            Object that should be used to shrink the field name to ensure it
            fits within database-specific limits.
        **kwargs
            Forwarded to `ddl.FieldSpec`.

        Returns
        -------
        sql_spec : `ddl.FieldSpec`
            A SQL-specific version of this specification.
        """
        name = self.name
        if name_shrinker is not None:
            name = name_shrinker.shrink(name)
        return ddl.FieldSpec(name=name, dtype=ddl.VALID_CONFIG_COLUMN_TYPES[self.type], **kwargs)

    @abstractmethod
    def to_arrow(self) -> arrow_utils.ToArrow:
        """Return an object that converts values of this column to a column in
        an Arrow table.

        Returns
        -------
        converter : `arrow_utils.ToArrow`
            A converter object with schema information in Arrow form.
        """
        raise NotImplementedError()

    def serializer(self) -> ColumnValueSerializer:
        """Return object that converts values of this column to or from
        serializable format.

        Returns
        -------
        serializer : `ColumnValueSerializer`
            A converter instance.
        """
        return _TypeAdapterColumnValueSerializer(pydantic.TypeAdapter(self.annotated_type))

    def display(self, level: int = 0, tab: str = "  ") -> list[str]:
        """Return a human-reader-focused string description of this column as
        a list of lines.

        Parameters
        ----------
        level : `int`
            Number of indentation tabs for the first line.
        tab : `str`
            Characters to duplicate ``level`` times to form the actual indent.

        Returns
        -------
        lines : `list` [ `str` ]
            Display lines.
        """
        lines = [f"{tab * level}{self.name}: {self.type}"]
        if self.doc:
            indent = tab * (level + 1)
            lines.extend(
                textwrap.wrap(
                    self.doc,
                    initial_indent=indent,
                    subsequent_indent=indent,
                )
            )
        return lines

    def __str__(self) -> str:
        return "\n".join(self.display())

    @property
    def annotated_type(self) -> Any:
        """Return a Pydantic-friendly type annotation for this column type.

        Since this is a runtime object and most type annotations must be
        static, this is really only useful for `pydantic.TypeAdapter`
        construction and dynamic `pydantic.create_model` construction.
        """
        base = self._get_base_annotated_type()
        if self.nullable:
            return base | None
        return base

    @abstractmethod
    def _get_base_annotated_type(self) -> Any:
        """Return the base annotated type (not taking into account `nullable`)
        for this column type.
        """
        raise NotImplementedError()


def make_tuple_type_adapter(
    columns: Iterable[ColumnSpec],
) -> pydantic.TypeAdapter[tuple[Any, ...]]:
    """Return a `pydantic.TypeAdapter` for a `tuple` with types defined by an
    iterable of `ColumnSpec` objects.

    Parameters
    ----------
    columns : `~collections.abc.Iterable` [ `ColumnSpec` ]
        Iterable of column specifications.

    Returns
    -------
    adapter : `pydantic.TypeAdapter`
        A Pydantic type adapter for the `tuple` representation of a row with
        the given columns.
    """
    # Static type-checkers don't like this runtime use of static-typing
    # constructs, but that's how Pydantic works.
    return pydantic.TypeAdapter(tuple[*[spec.annotated_type for spec in columns]])  # type: ignore


@final
class IntColumnSpec(_BaseColumnSpec):
    """Description of an integer column."""

    pytype: ClassVar[type] = int

    type: Literal["int"] = "int"

    def to_arrow(self) -> arrow_utils.ToArrow:
        # Docstring inherited.
        return arrow_utils.ToArrow.for_primitive(self.name, pa.uint64(), nullable=self.nullable)

    def _get_base_annotated_type(self) -> Any:
        # Docstring inherited.
        return pydantic.StrictInt


@final
class StringColumnSpec(_BaseColumnSpec):
    """Description of a string column."""

    pytype: ClassVar[type] = str

    type: Literal["string"] = "string"

    length: int
    """Maximum length of strings."""

    def to_sql_spec(self, name_shrinker: NameShrinker | None = None, **kwargs: Any) -> ddl.FieldSpec:
        # Docstring inherited.
        return super().to_sql_spec(length=self.length, name_shrinker=name_shrinker, **kwargs)

    def to_arrow(self) -> arrow_utils.ToArrow:
        # Docstring inherited.
        return arrow_utils.ToArrow.for_primitive(self.name, pa.string(), nullable=self.nullable)

    def _get_base_annotated_type(self) -> Any:
        # Docstring inherited.
        return pydantic.StrictStr


@final
class HashColumnSpec(_BaseColumnSpec):
    """Description of a hash digest."""

    pytype: ClassVar[type] = bytes

    type: Literal["hash"] = "hash"

    nbytes: int
    """Number of bytes for the hash."""

    def to_sql_spec(self, name_shrinker: NameShrinker | None = None, **kwargs: Any) -> ddl.FieldSpec:
        # Docstring inherited.
        return super().to_sql_spec(nbytes=self.nbytes, name_shrinker=name_shrinker, **kwargs)

    def to_arrow(self) -> arrow_utils.ToArrow:
        # Docstring inherited.
        return arrow_utils.ToArrow.for_primitive(
            self.name,
            # The size for Arrow binary columns is a fixed size, not a maximum
            # as in SQL, so we use a variable-size column.
            pa.binary(),
            nullable=self.nullable,
        )

    def _get_base_annotated_type(self) -> Any:
        # Docstring inherited.
        return SerializableBytesHex


@final
class FloatColumnSpec(_BaseColumnSpec):
    """Description of a float column."""

    pytype: ClassVar[type] = float

    type: Literal["float"] = "float"

    def to_arrow(self) -> arrow_utils.ToArrow:
        # Docstring inherited.
        assert self.nullable is not None, "nullable=None should be resolved by validators"
        return arrow_utils.ToArrow.for_primitive(self.name, pa.float64(), nullable=self.nullable)

    def _get_base_annotated_type(self) -> Any:
        # Docstring inherited.
        return pydantic.StrictFloat


@final
class BoolColumnSpec(_BaseColumnSpec):
    """Description of a bool column."""

    pytype: ClassVar[type] = bool

    type: Literal["bool"] = "bool"

    def to_arrow(self) -> arrow_utils.ToArrow:
        # Docstring inherited.
        return arrow_utils.ToArrow.for_primitive(self.name, pa.bool_(), nullable=self.nullable)

    def _get_base_annotated_type(self) -> Any:
        # Docstring inherited.
        return pydantic.StrictBool


@final
class UUIDColumnSpec(_BaseColumnSpec):
    """Description of a UUID column."""

    pytype: ClassVar[type] = uuid.UUID

    type: Literal["uuid"] = "uuid"

    def to_arrow(self) -> arrow_utils.ToArrow:
        # Docstring inherited.
        assert self.nullable is not None, "nullable=None should be resolved by validators"
        return arrow_utils.ToArrow.for_uuid(self.name, nullable=self.nullable)

    def _get_base_annotated_type(self) -> Any:
        # Docstring inherited.
        return uuid.UUID


@final
class RegionColumnSpec(_BaseColumnSpec):
    """Description of a region column."""

    name: str = "region"

    pytype: ClassVar[type] = Region

    type: Literal["region"] = "region"

    nbytes: int = 2048
    """Number of bytes for the encoded region."""

    def to_arrow(self) -> arrow_utils.ToArrow:
        # Docstring inherited.
        assert self.nullable is not None, "nullable=None should be resolved by validators"
        return arrow_utils.ToArrow.for_region(self.name, nullable=self.nullable)

    def _get_base_annotated_type(self) -> Any:
        # Docstring inherited.
        return SerializableRegion


@final
class TimespanColumnSpec(_BaseColumnSpec):
    """Description of a timespan column."""

    name: str = "timespan"

    pytype: ClassVar[type] = Timespan

    type: Literal["timespan"] = "timespan"

    def to_arrow(self) -> arrow_utils.ToArrow:
        # Docstring inherited.
        return arrow_utils.ToArrow.for_timespan(self.name, nullable=self.nullable)

    def _get_base_annotated_type(self) -> Any:
        # Docstring inherited.
        return Timespan


@final
class DateTimeColumnSpec(_BaseColumnSpec):
    """Description of a time column, stored as integer TAI nanoseconds since
    1970-01-01 and represented in Python via `astropy.time.Time`.
    """

    pytype: ClassVar[type] = astropy.time.Time

    type: Literal["datetime"] = "datetime"

    def to_arrow(self) -> arrow_utils.ToArrow:
        # Docstring inherited.
        assert self.nullable is not None, "nullable=None should be resolved by validators"
        return arrow_utils.ToArrow.for_datetime(self.name, nullable=self.nullable)

    def _get_base_annotated_type(self) -> Any:
        # Docstring inherited.
        return SerializableTime


ColumnSpec = Annotated[
    Union[
        IntColumnSpec,
        StringColumnSpec,
        HashColumnSpec,
        FloatColumnSpec,
        BoolColumnSpec,
        UUIDColumnSpec,
        RegionColumnSpec,
        TimespanColumnSpec,
        DateTimeColumnSpec,
    ],
    pydantic.Field(discriminator="type"),
]
