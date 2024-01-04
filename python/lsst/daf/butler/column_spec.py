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
    "ColumnSpec",
    "IntColumnSpec",
    "StringColumnSpec",
    "HashColumnSpec",
    "FloatColumnSpec",
    "BoolColumnSpec",
    "RegionColumnSpec",
    "TimespanColumnSpec",
)

import textwrap
from abc import ABC, abstractmethod
from typing import Annotated, Any, ClassVar, Literal, Union, final

import pyarrow as pa
import pydantic
from lsst.sphgeom import Region

from . import arrow_utils, ddl
from ._timespan import Timespan


class _BaseColumnSpec(pydantic.BaseModel, ABC):
    """Base class for descriptions of table columns."""

    name: str = pydantic.Field(description="""Name of the column.""")

    doc: str = pydantic.Field(default="", description="Documentation for the column.")

    type: str

    nullable: bool = pydantic.Field(
        default=True,
        description="Whether the column may be ``NULL``.",
    )

    def to_sql_spec(self, **kwargs: Any) -> ddl.FieldSpec:
        """Convert this specification to a SQL-specific one.

        Parameters
        ----------
        **kwargs
            Forwarded to `ddl.FieldSpec`.

        Returns
        -------
        sql_spec : `ddl.FieldSpec`
            A SQL-specific version of this specification.
        """
        return ddl.FieldSpec(name=self.name, dtype=ddl.VALID_CONFIG_COLUMN_TYPES[self.type], **kwargs)

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


@final
class IntColumnSpec(_BaseColumnSpec):
    """Description of an integer column."""

    pytype: ClassVar[type] = int

    type: Literal["int"] = "int"

    def to_arrow(self) -> arrow_utils.ToArrow:
        # Docstring inherited.
        return arrow_utils.ToArrow.for_primitive(self.name, pa.uint64(), nullable=self.nullable)


@final
class StringColumnSpec(_BaseColumnSpec):
    """Description of a string column."""

    pytype: ClassVar[type] = str

    type: Literal["string"] = "string"

    length: int
    """Maximum length of strings."""

    def to_sql_spec(self, **kwargs: Any) -> ddl.FieldSpec:
        # Docstring inherited.
        return super().to_sql_spec(length=self.length, **kwargs)

    def to_arrow(self) -> arrow_utils.ToArrow:
        # Docstring inherited.
        return arrow_utils.ToArrow.for_primitive(self.name, pa.string(), nullable=self.nullable)


@final
class HashColumnSpec(_BaseColumnSpec):
    """Description of a hash digest."""

    pytype: ClassVar[type] = bytes

    type: Literal["hash"] = "hash"

    nbytes: int
    """Number of bytes for the hash."""

    def to_sql_spec(self, **kwargs: Any) -> ddl.FieldSpec:
        # Docstring inherited.
        return super().to_sql_spec(nbytes=self.nbytes, **kwargs)

    def to_arrow(self) -> arrow_utils.ToArrow:
        # Docstring inherited.
        return arrow_utils.ToArrow.for_primitive(
            self.name,
            # The size for Arrow binary columns is a fixed size, not a maximum
            # as in SQL, so we use a variable-size column.
            pa.binary(),
            nullable=self.nullable,
        )


@final
class FloatColumnSpec(_BaseColumnSpec):
    """Description of a float column."""

    pytype: ClassVar[type] = float

    type: Literal["float"] = "float"

    def to_arrow(self) -> arrow_utils.ToArrow:
        # Docstring inherited.
        assert self.nullable is not None, "nullable=None should be resolved by validators"
        return arrow_utils.ToArrow.for_primitive(self.name, pa.float64(), nullable=self.nullable)


@final
class BoolColumnSpec(_BaseColumnSpec):
    """Description of a bool column."""

    pytype: ClassVar[type] = bool

    type: Literal["bool"] = "bool"

    def to_arrow(self) -> arrow_utils.ToArrow:
        # Docstring inherited.
        return arrow_utils.ToArrow.for_primitive(self.name, pa.bool_(), nullable=self.nullable)


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


@final
class TimespanColumnSpec(_BaseColumnSpec):
    """Description of a timespan column."""

    name: str = "timespan"

    pytype: ClassVar[type] = Timespan

    type: Literal["timespan"] = "timespan"

    def to_arrow(self) -> arrow_utils.ToArrow:
        # Docstring inherited.
        return arrow_utils.ToArrow.for_timespan(self.name, nullable=self.nullable)


ColumnSpec = Annotated[
    Union[
        IntColumnSpec,
        StringColumnSpec,
        HashColumnSpec,
        FloatColumnSpec,
        BoolColumnSpec,
        RegionColumnSpec,
        TimespanColumnSpec,
    ],
    pydantic.Field(discriminator="type"),
]
