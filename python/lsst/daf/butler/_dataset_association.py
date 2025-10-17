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

__all__ = ("DatasetAssociation",)

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ._collection_type import CollectionType
from ._dataset_ref import DatasetRef
from ._dataset_type import DatasetType
from ._timespan import Timespan

if TYPE_CHECKING:
    from ._butler_collections import CollectionInfo
    from .queries._general_query_results import GeneralQueryResults


@dataclass(frozen=True, eq=True)
class DatasetAssociation:
    """Class representing the membership of a dataset in a single collection.

    One dataset is associated with one collection, possibly including
    a timespan.
    """

    __slots__ = ("ref", "collection", "timespan")

    ref: DatasetRef
    """Resolved reference to a dataset (`DatasetRef`).
    """

    collection: str
    """Name of a collection (`str`).
    """

    timespan: Timespan | None
    """Validity range of the dataset if this is a `~CollectionType.CALIBRATION`
    collection (`Timespan` or `None`).
    """

    @classmethod
    def from_query_result(
        cls,
        result: GeneralQueryResults,
        dataset_type: DatasetType,
        collection_info: Mapping[str, CollectionInfo] | None = None,
    ) -> Iterator[DatasetAssociation]:
        """Construct dataset associations from the result of general query.

        Parameters
        ----------
        result : `~lsst.daf.butler.queries.GeneralQueryResults`
            General query result returned by
            `Query.general <lsst.daf.butler.queries.Query.general>` method. The
            result has to include "dataset_id", "run", "collection", and
            "timespan" dataset fields for ``dataset_type``.
        dataset_type : `DatasetType`
            Dataset type, query has to include this dataset type.
        collection_info : `~collections.abc.Mapping` \
                [`str`, `CollectionInfo`], optional
            Mapping from collection name to information about it for all
            collections that may appear in the query results.  If not provided,
            timespans for `~CollectionType.RUN` and `~CollectionType.TAGGED`
            collections will be bounded, instead of `None`; this is actually
            more consistent with how those timespans are used elsewhere in the
            query system, but is a change from how `DatasetAssocation` has
            historically worked.
        """
        timespan_key = f"{dataset_type.name}.timespan"
        collection_key = f"{dataset_type.name}.collection"
        for _, refs, row_dict in result.iter_tuples(dataset_type):
            collection = row_dict[collection_key]
            timespan = row_dict[timespan_key]
            if (
                collection_info is not None
                and collection_info[collection].type is not CollectionType.CALIBRATION
            ):
                # This behavior is for backwards compatibility only; in most
                # contexts it makes sense to consider the timespan of a RUN
                # or TAGGED collection to be unbounded, not None, and that's
                # what the query results we're iterating over do.
                timespan = None
            yield DatasetAssociation(refs[0], collection, timespan)

    def __lt__(self, other: Any) -> bool:
        # Allow sorting of associations
        if not isinstance(other, type(self)):
            return NotImplemented

        return (self.ref, self.collection, self.timespan) < (other.ref, other.collection, other.timespan)
