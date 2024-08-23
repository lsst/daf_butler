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

from __future__ import annotations

__all__ = (
    "TopologicalSpace",
    "TopologicalFamily",
    "TopologicalRelationshipEndpoint",
)

import enum
from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from lsst.utils.classes import immutable

if TYPE_CHECKING:
    from .dimensions import DimensionGroup
    from .queries.tree import ColumnReference


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
    space : `TopologicalSpace`
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

    def __repr__(self) -> str:
        return self.name

    @abstractmethod
    def choose(self, dimensions: DimensionGroup) -> TopologicalRelationshipEndpoint:
        """Select the best member of this family to use.

        These are to be used in a query join or data ID when more than one
        is present.

        Usually this should correspond to the most fine-grained region.

        Parameters
        ----------
        dimensions : `DimensionGroup`
            Dimensions to choose from, if this is a dimension-based topological
            family.

        Returns
        -------
        best : `TopologicalRelationshipEndpoint`
            The best endpoint from this family for these dimensions.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_column_reference(self, endpoint: TopologicalRelationshipEndpoint) -> ColumnReference:
        """Create a column reference to the generalized region column for the
        given endpoint.

        Parameters
        ----------
        endpoint : `TopologicalRelationshipEndpoint`
            Endpoint to create a column reference to.

        Returns
        -------
        column : `.queries.tree.ColumnReference`
            Column reference.
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
    def spatial(self) -> TopologicalFamily | None:
        """Return this endpoint's `~TopologicalSpace.SPATIAL` family."""
        return self.topology.get(TopologicalSpace.SPATIAL)

    @property
    def temporal(self) -> TopologicalFamily | None:
        """Return this endpoint's `~TopologicalSpace.TEMPORAL` family."""
        return self.topology.get(TopologicalSpace.TEMPORAL)
