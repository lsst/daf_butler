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

__all__ = ("QueryBackend",)

from abc import ABC, abstractmethod
from collections.abc import Set
from typing import TYPE_CHECKING, Any

from .._collectionType import CollectionType

if TYPE_CHECKING:
    from ...core import DimensionUniverse
    from ..interfaces import CollectionRecord
    from ..managers import RegistryManagerInstances


class QueryBackend(ABC):
    """An interface for constructing and evaluating the
    `~lsst.daf.relation.Relation` objects that comprise registry queries.

    This ABC is expected to have a concrete subclass for each concrete registry
    type.
    """

    @property
    @abstractmethod
    def managers(self) -> RegistryManagerInstances:
        """A struct containing the manager instances that back a SQL registry.

        Notes
        -----
        This property is a temporary interface that will be removed in favor of
        new methods once the manager and storage classes have been integrated
        with `~lsst.daf.relation.Relation`.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def universe(self) -> DimensionUniverse:
        """Definition of all dimensions and dimension elements for this
        registry.
        """
        raise NotImplementedError()

    @abstractmethod
    def resolve_collection_wildcard(
        self,
        expression: Any,
        *,
        collection_types: Set[CollectionType] = CollectionType.all(),
        done: set[str] | None = None,
        flatten_chains: bool = True,
        include_chains: bool | None = None,
    ) -> list[CollectionRecord]:
        """Return the collection records that match a wildcard expression.

        Parameters
        ----------
        expression
            Names and/or patterns for collections; will be passed to
            `CollectionWildcard.from_expression`.
        collection_types : `collections.abc.Set` [ `CollectionType` ], optional
            If provided, only yield collections of these types.
        done : `set` [ `str` ], optional
            A set of collection names that should be skipped, updated to
            include all processed collection names on return.
        flatten_chains : `bool`, optional
            If `True` (default) recursively yield the child collections of
            `~CollectionType.CHAINED` collections.
        include_chains : `bool`, optional
            If `False`, return records for `~CollectionType.CHAINED`
            collections themselves.  The default is the opposite of
            ``flattenChains``: either return records for CHAINED collections or
            their children, but not both.

        Returns
        ------
        records : `list` [ `CollectionRecord` ]
            Matching collection records.
        """
        raise NotImplementedError()
