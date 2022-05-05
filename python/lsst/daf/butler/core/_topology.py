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

__all__ = (
    "TopologicalSpace",
    "TopologicalFamily",
    "TopologicalRelationshipEndpoint",
    "TopologicalExtentDatabaseRepresentation",
)

import enum
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Dict, Generic, Mapping, Optional, Type, TypeVar

import sqlalchemy
from lsst.utils.classes import immutable

from . import ddl
from .named import NamedValueAbstractSet


@enum.unique
class TopologicalSpace(enum.Enum):
    """Enumeration of continuous-variable relationships for dimensions.

    Most dimension relationships are discrete, in that they are regular foreign
    key relationships between tables.  Those connected to a
    `TopologicalSpace` are not - a row in a table instead occupies some
    region in a continuous-variable space, and topological operators like
    "overlaps" between regions in that space define the relationships between
    rows.
    """

    SPATIAL = enum.auto()
    """The (spherical) sky, using `lsst.sphgeom.Region` objects to represent
    those regions in memory.
    """

    TEMPORAL = enum.auto()
    """Time, using `Timespan` instances (with TAI endpoints) to represent
    intervals in memory.
    """


@immutable
class TopologicalFamily(ABC):
    """A grouping of `TopologicalRelationshipEndpoint` objects.

    These regions form a hierarchy in which one endpoint's rows always contain
    another's in a predefined way.

    This hierarchy means that endpoints in the same family do not generally
    have to be have to be related using (e.g.) overlaps; instead, the regions
    from one "best" endpoint from each family are related to the best endpoint
    from each other family in a query.

    Parameters
    ----------
    name : `str`
        Unique string identifier for this family.
    category : `TopologicalSpace`
        Space in which the regions of this family live.
    """

    def __init__(
        self,
        name: str,
        space: TopologicalSpace,
    ):
        self.name = name
        self.space = space

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, TopologicalFamily):
            return self.space == other.space and self.name == other.name
        return False

    def __hash__(self) -> int:
        return hash(self.name)

    def __contains__(self, other: TopologicalRelationshipEndpoint) -> bool:
        return other.topology.get(self.space) == self

    @abstractmethod
    def choose(
        self, endpoints: NamedValueAbstractSet[TopologicalRelationshipEndpoint]
    ) -> TopologicalRelationshipEndpoint:
        """Select the best member of this family to use.

        These are to be used in a query join or data ID when more than one
        is present.

        Usually this should correspond to the most fine-grained region.

        Parameters
        ----------
        endpoints : `NamedValueAbstractSet` [`TopologicalRelationshipEndpoint`]
            Endpoints to choose from.  May include endpoints that are not
            members of this family (which should be ignored).

        Returns
        -------
        best : `TopologicalRelationshipEndpoint`
            The best endpoint that is both a member of ``self`` and in
            ``endpoints``.
        """
        raise NotImplementedError()

    name: str
    """Unique string identifier for this family (`str`).
    """

    space: TopologicalSpace
    """Space in which the regions of this family live (`TopologicalSpace`).
    """


@immutable
class TopologicalRelationshipEndpoint(ABC):
    """Representation of a logical table that can participate in overlap joins.

    An abstract base class whose instances represent a logical table that
    may participate in overlap joins defined by a `TopologicalSpace`.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Return unique string identifier for this endpoint (`str`)."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def topology(self) -> Mapping[TopologicalSpace, TopologicalFamily]:
        """Return the relationship families to which this endpoint belongs.

        It is keyed by the category for that family.
        """
        raise NotImplementedError()

    @property
    def spatial(self) -> Optional[TopologicalFamily]:
        """Return this endpoint's `~TopologicalSpace.SPATIAL` family."""
        return self.topology.get(TopologicalSpace.SPATIAL)

    @property
    def temporal(self) -> Optional[TopologicalFamily]:
        """Return this endpoint's `~TopologicalSpace.TEMPORAL` family."""
        return self.topology.get(TopologicalSpace.TEMPORAL)


_S = TypeVar("_S", bound="TopologicalExtentDatabaseRepresentation")
_R = TypeVar("_R")


class TopologicalExtentDatabaseRepresentation(Generic[_R]):
    """Mapping of in-memory representation of a region to DB representation.

    An abstract base class whose subclasses provide a mapping from the
    in-memory representation of a `TopologicalSpace` region to a
    database-storage representation, and whose instances act like a
    SQLAlchemy-based column expression.
    """

    NAME: ClassVar[str]
    """Name to use for this logical column in tables (`str`).

    If the representation actually uses multiple columns, this will just be
    part of the names of those columns.  Queries (and tables that represent
    materialized queries) may use a different name (via the ``name`` parameters
    to various methods) in order to disambiguate between the regions associated
    with different tables.
    """

    SPACE: ClassVar[TopologicalSpace]
    """Topological space where regions represented by this class exist.
    """

    @classmethod
    @abstractmethod
    def makeFieldSpecs(
        cls, nullable: bool, name: Optional[str] = None, **kwargs: Any
    ) -> tuple[ddl.FieldSpec, ...]:
        """Make objects that reflect the fields that must be added to table.

        Makes one or more `ddl.FieldSpec` objects that reflect the fields
        that must be added to a table for this representation.

        Parameters
        ----------
        nullable : `bool`
            If `True`, the region is permitted to be logically ``NULL``
            (mapped to `None` in Python), though the correspoding value(s) in
            the database are implementation-defined.  Nullable region fields
            default to NULL, while others default to (-∞, ∞).
        name : `str`, optional
            Name for the logical column; a part of the name for multi-column
            representations.  Defaults to ``cls.NAME``.
        **kwargs
            Keyword arguments are forwarded to the `ddl.FieldSpec` constructor
            for all fields; implementations only provide the ``name``,
            ``dtype``, and ``default`` arguments themselves.

        Returns
        -------
        specs : `tuple` [ `ddl.FieldSpec` ]
            Field specification objects; length of the tuple is
            subclass-dependent, but is guaranteed to match the length of the
            return values of `getFieldNames` and `update`.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def getFieldNames(cls, name: Optional[str] = None) -> tuple[str, ...]:
        """Return the actual field names used by this representation.

        Parameters
        ----------
        name : `str`, optional
            Name for the logical column; a part of the name for multi-column
            representations.  Defaults to ``cls.NAME``.

        Returns
        -------
        names : `tuple` [ `str` ]
            Field name(s).  Guaranteed to be the same as the names of the field
            specifications returned by `makeFieldSpecs`.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def update(
        cls, extent: Optional[_R], name: Optional[str] = None, result: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Add a region to a dictionary.

        This region represents a database row in this representation.

        Parameters
        ----------
        extent
            An instance of the region type this class provides a database
            representation for, or `None` for ``NULL``.
        name : `str`, optional
            Name for the logical column; a part of the name for multi-column
            representations.  Defaults to ``cls.NAME``.
        result : `dict` [ `str`, `Any` ], optional
            A dictionary representing a database row that fields should be
            added to, or `None` to create and return a new one.

        Returns
        -------
        result : `dict` [ `str`, `Any` ]
            A dictionary containing this representation of a region.  Exactly
            the `dict` passed as ``result`` if that is not `None`.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def extract(cls, mapping: Mapping[str, Any], name: Optional[str] = None) -> Optional[_R]:
        """Extract a region from a dictionary.

        This region represents a database row in this representation.

        Parameters
        ----------
        mapping : `Mapping` [ `str`, `Any` ]
            A dictionary representing a database row containing a `Timespan`
            in this representation.  Should have key(s) equal to the return
            value of `getFieldNames`.
        name : `str`, optional
            Name for the logical column; a part of the name for multi-column
            representations.  Defaults to ``cls.NAME``.

        Returns
        -------
        region
            Python representation of the region.
        """
        raise NotImplementedError()

    @classmethod
    def hasExclusionConstraint(cls) -> bool:
        """Return `True` if this representation supports exclusion constraints.

        Returns
        -------
        supported : `bool`
            If `True`, defining a constraint via `ddl.TableSpec.exclusion` that
            includes the fields of this representation is allowed.
        """
        return False

    @classmethod
    @abstractmethod
    def from_columns(
        cls: Type[_S], columns: sqlalchemy.sql.ColumnCollection, name: Optional[str] = None
    ) -> _S:
        """Construct representation of a column in the table or subquery.

        Constructs an instance that represents a logical column (which may
        actually be backed by multiple columns) in the given table or subquery.

        Parameters
        ----------
        columns : `sqlalchemy.sql.ColumnCollections`
            SQLAlchemy container for raw columns.
        name : `str`, optional
            Name for the logical column; a part of the name for multi-column
            representations.  Defaults to ``cls.NAME``.

        Returns
        -------
        representation : `TopologicalExtentDatabaseRepresentation`
            Object representing a logical column.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def name(self) -> str:
        """Return base logical name for the topological extent (`str`).

        If the representation uses only one actual column, this should be the
        full name of the column.  In other cases it is an unspecified subset of
        the column names.
        """
        raise NotImplementedError()

    @abstractmethod
    def isNull(self) -> sqlalchemy.sql.ColumnElement:
        """Return expression that tests where region is ``NULL``.

        Returns a SQLAlchemy expression that tests whether this region is
        logically ``NULL``.

        Returns
        -------
        isnull : `sqlalchemy.sql.ColumnElement`
            A boolean SQLAlchemy expression object.
        """
        raise NotImplementedError()

    @abstractmethod
    def flatten(self, name: Optional[str] = None) -> tuple[sqlalchemy.sql.ColumnElement, ...]:
        """Return the actual column(s) that comprise this logical column.

        Parameters
        ----------
        name : `str`, optional
            If provided, a name for the logical column that should be used to
            label the columns.  If not provided, the columns' native names will
            be used.

        Returns
        -------
        columns : `tuple` [ `sqlalchemy.sql.ColumnElement` ]
            The true column or columns that back this object.
        """
        raise NotImplementedError()
