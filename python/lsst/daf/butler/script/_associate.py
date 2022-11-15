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

from collections.abc import Iterable

from .._butler import Butler
from ..registry import CollectionType
from .queryDatasets import QueryDatasets


def associate(
    repo: str,
    collection: str,
    dataset_type: Iterable[str],
    collections: Iterable[str],
    where: str | None,
    find_first: bool,
) -> None:
    """Add existing datasets to a CHAINED collection."""

    butler = Butler(repo, writeable=True)

    butler.registry.registerCollection(collection, CollectionType.TAGGED)

    results = QueryDatasets(
        butler=butler,
        glob=dataset_type,
        collections=collections,
        where=where,
        find_first=find_first,
        show_uri=False,
        repo=None,
    )

    butler.registry.associate(collection, results.getDatasets())
