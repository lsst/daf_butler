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

__all__ = ("SqlQueryBackend",)

from collections.abc import Set
from typing import TYPE_CHECKING, Any

from .._collectionType import CollectionType
from ._query_backend import QueryBackend

if TYPE_CHECKING:
    from ...core import DatasetType, DimensionUniverse
    from ..interfaces import CollectionRecord, Database
    from ..managers import RegistryManagerInstances


class SqlQueryBackend(QueryBackend):
    """An implementation of `QueryBackend` for `SqlRegistry`.

    Parameters
    ----------
    db : `Database`
        Object that abstracts the database engine.
    managers : `RegistryManagerInstances`
        Struct containing the manager objects that back a `SqlRegistry`.
    """

    def __init__(
        self,
        db: Database,
        managers: RegistryManagerInstances,
    ):
        self._db = db
        self._managers = managers

    @property
    def managers(self) -> RegistryManagerInstances:
        # Docstring inherited.
        return self._managers

    @property
    def universe(self) -> DimensionUniverse:
        # Docstring inherited.
        return self._managers.dimensions.universe

    def resolve_collection_wildcard(
        self,
        expression: Any,
        *,
        collection_types: Set[CollectionType] = CollectionType.all(),
        done: set[str] | None = None,
        flatten_chains: bool = True,
        include_chains: bool | None = None,
    ) -> list[CollectionRecord]:
        # Docstring inherited.
        return self._managers.collections.resolve_wildcard(
            expression,
            collection_types=collection_types,
            done=done,
            flatten_chains=flatten_chains,
            include_chains=include_chains,
        )

    def resolve_dataset_type_wildcard(
        self,
        expression: Any,
        components: bool | None = None,
        missing: list[str] | None = None,
        explicit_only: bool = False,
    ) -> dict[DatasetType, list[str | None]]:
        # Docstring inherited.
        return self._managers.datasets.resolve_wildcard(expression, components, missing, explicit_only)
