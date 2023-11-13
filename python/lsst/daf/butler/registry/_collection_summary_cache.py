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

__all__ = ("CollectionSummaryCache",)

from collections.abc import Iterable, Mapping
from typing import Any

from ._collection_summary import CollectionSummary


class CollectionSummaryCache:
    """Cache for collection summaries.

    Notes
    -----
    This class stores `CollectionSummary` records indexed by collection keys.
    For cache to be usable the records that are given to `update` method have
    to include all dataset types, i.e. the query that produces records should
    not be constrained by dataset type.
    """

    def __init__(self) -> None:
        self._cache: dict[Any, CollectionSummary] = {}

    def update(self, summaries: Mapping[Any, CollectionSummary]) -> None:
        """Add records to the cache.

        Parameters
        ----------
        summaries : `~collections.abc.Mapping` [`Any`, `CollectionSummary`]
            Summary records indexed by collection key, records must include all
            dataset types.
        """
        self._cache.update(summaries)

    def find_summaries(self, keys: Iterable[Any]) -> tuple[dict[Any, CollectionSummary], set[Any]]:
        """Return summary records given a set of keys.

        Parameters
        ----------
        keys : `~collections.abc.Iterable` [`Any`]
            Sequence of collection keys.

        Returns
        -------
        summaries : `dict` [`Any`, `CollectionSummary`]
            Dictionary of summaries indexed by collection keys, includes
            records found in the cache.
        missing_keys : `set` [`Any`]
            Collection keys that are not present in the cache.
        """
        found = {}
        not_found = set()
        for key in keys:
            if (summary := self._cache.get(key)) is not None:
                found[key] = summary
            else:
                not_found.add(key)
        return found, not_found
