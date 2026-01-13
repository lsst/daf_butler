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

from collections import defaultdict
from collections.abc import Iterable

from ..dimensions import (
    DataCoordinate,
    DimensionDataAttacher,
    DimensionGroup,
    DimensionUniverse,
)
from ..dimensions.record_cache import DimensionRecordCache
from ..queries import QueryFactoryFunction


def expand_data_ids(
    data_ids: Iterable[DataCoordinate],
    universe: DimensionUniverse,
    query_func: QueryFactoryFunction,
    cache: DimensionRecordCache | None,
) -> list[DataCoordinate]:
    """Expand the given data IDs to look up implied dimension values and attach
    dimension records.

    Parameters
    ----------
    data_ids : `~collections.abc.Iterable` [ `DataCoordinate` ]
        Data coordinates to be expanded.
    universe : `DimensionUniverse`
        Dimension universe associated with the given ``data_ids`` values.
    query_func : QueryFactoryFunction
        Function used to set up a Butler query context for looking up required
        information from the database.
    cache : `DimensionRecordCache` | None
        Cache containing already-known dimension records.  May be `None` if a
        cache is not available.

    Returns
    -------
    expanded : `list` [ `DataCoordinate` ]
        List of `DataCoordinate` instances in the same order as the input
        values.  It is guaranteed that each `DataCoordinate` has
        ``hasRecords()=True`` and ``hasFull()=True``.
    """
    output = list(data_ids)

    grouped_by_dimensions: defaultdict[DimensionGroup, list[int]] = defaultdict(list)
    for i, data_id in enumerate(data_ids):
        if not data_id.hasRecords():
            grouped_by_dimensions[data_id.dimensions].append(i)

    if not grouped_by_dimensions:
        # All given DataCoordinate values are already expanded.
        return output

    attacher = DimensionDataAttacher(
        cache=cache,
        dimensions=DimensionGroup.union(*grouped_by_dimensions.keys(), universe=universe),
    )
    for dimensions, indexes in grouped_by_dimensions.items():
        with query_func() as query:
            expanded = attacher.attach(dimensions, (output[index] for index in indexes), query)
            for index, data_id in zip(indexes, expanded):
                output[index] = data_id

    return output
