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

from abc import ABC, abstractmethod
import enum
from typing import (
    Any,
    ClassVar,
    Iterator,
    Mapping,
    Optional,
    Type,
    TypeVar,
)

import sqlalchemy

from .named import NamedValueAbstractSet
from .utils import immutable


@enum.unique
class TopologicalSpace(enum.Enum):
    """Enumeration of the different categories of continuous-variable
    relationships supported by the dimensions system.

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
    """A grouping of `TopologicalRelationshipEndpoint` objects whose regions
    form a hierarchy in which one endpoint's rows always contain another's in a
    predefined way.

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
    def choose(self, endpoints: NamedValueAbstractSet[TopologicalRelationshipEndpoint]
               ) -> TopologicalRelationshipEndpoint:
        """Select the best member of this family to use in a query join or
        data ID when more than one is present.

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
    """An abstract base class whose instances represent a logical table that
    may participate in overlap joins defined by a `TopologicalSpace`.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique string identifier for this endpoint (`str`).
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def topology(self) -> Mapping[TopologicalSpace, TopologicalFamily]:
        """The relationship families to which this endpoint belongs, keyed
        by the category for that family.
        """
        raise NotImplementedError()

    @property
    def spatial(self) -> Optional[TopologicalFamily]:
        """This endpoint's `~TopologicalSpace.SPATIAL` family.
        """
        return self.topology.get(TopologicalSpace.SPATIAL)

    @property
    def temporal(self) -> Optional[TopologicalFamily]:
        """This endpoint's `~TopologicalSpace.TEMPORAL` family.
        """
        return self.topology.get(TopologicalSpace.TEMPORAL)


_S = TypeVar("_S", bound="TopologicalExtentDatabaseRepresentation")


class TopologicalExtentDatabaseRepresentation(ABC):
    """An abstract base class whose subclasses provide a mapping from the
    in-memory representation of a `TopologicalSpace` region to a
    database-storage representation, and whose instances act like a
    SQLAlchemy-based column expression.
    """

    NAME: ClassVar[str]
    """Default name for this type of logical column (`str`).
    """

    @classmethod
    @abstractmethod
    def fromSelectable(cls: Type[_S], selectable: sqlalchemy.sql.FromClause,
                       name: Optional[str] = None) -> _S:
        """Construct an instance that represents a logical column (which may
        actually be backed by multiple columns) in the given table or subquery.

        Parameters
        ----------
        selectable : `sqlalchemy.sql.FromClause`
            SQLAlchemy object representing a table or subquery.
        name : `str`, optional
            Logical name for this possibly-compound field.

        Returns
        -------
        representation : `TopologicalExtentDatabaseRepresentation`
            Object representing a logical column.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def name(self) -> str:
        """Base logical name for the topological extent (`str`).

        If the representation uses only one actual column, this should be the
        full name of the column.  In other cases it is an unspecified subset of
        the column names.
        """
        raise NotImplementedError()

    @abstractmethod
    def overlaps(self: _S, other: _S) -> sqlalchemy.sql.ColumnElement:
        """Return a boolean SQLAlchemy column expression representing an
        overlap test between this logical column and another of the same type.

        Parameters
        ----------
        other : ``type(self)``
            Another instance of the exact same type as ``self``.

        Returns
        -------
        expression : `sqlalchemy.sql.ColumnElement`
            A boolean SQLAlchemy expression.
        """
        raise NotImplementedError()

    @abstractmethod
    def flatten(self, name: Optional[str]) -> Iterator[sqlalchemy.sql.ColumnElement]:
        """Return the actual column or columns that comprise this logical
        column.

        Parameters
        ----------
        name : `str`, optional
            If provided, a name for the logical column that should be used to
            label the columns.  If not provided, the columns' native names will
            be used.

        Returns
        -------
        columns : `Iterator` [ `sqlalchemy.sql.ColumnElement` ]
            The true column or columns that back this object.
        """
        raise NotImplementedError()
