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

__all__ = ("DirectButlerCollections",)

from collections.abc import Iterable

from lsst.utils.iteration import ensure_iterable

from .._butler_collections import ButlerCollections
from ..registry.sql_registry import SqlRegistry


class DirectButlerCollections(ButlerCollections):
    """Implementation of ButlerCollections for DirectButler.

    Parameters
    ----------
    registry : `~lsst.daf.butler.registry.sql_registry.SqlRegistry`
        Registry object used to work with the collections database.
    """

    def __init__(self, registry: SqlRegistry):
        self._registry = registry

    def extend_chain(self, parent_collection_name: str, child_collection_names: str | Iterable[str]) -> None:
        return self._registry._managers.collections.extend_collection_chain(
            parent_collection_name, list(ensure_iterable(child_collection_names))
        )

    def prepend_chain(self, parent_collection_name: str, child_collection_names: str | Iterable[str]) -> None:
        return self._registry._managers.collections.prepend_collection_chain(
            parent_collection_name, list(ensure_iterable(child_collection_names))
        )

    def redefine_chain(
        self, parent_collection_name: str, child_collection_names: str | Iterable[str]
    ) -> None:
        self._registry._managers.collections.update_chain(
            parent_collection_name, list(ensure_iterable(child_collection_names))
        )

    def remove_from_chain(
        self, parent_collection_name: str, child_collection_names: str | Iterable[str]
    ) -> None:
        return self._registry._managers.collections.remove_from_collection_chain(
            parent_collection_name, list(ensure_iterable(child_collection_names))
        )
