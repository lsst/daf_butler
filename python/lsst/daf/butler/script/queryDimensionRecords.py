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

import logging
from operator import attrgetter
from typing import Any

from astropy.table import Table
from lsst.sphgeom import Region

from .._butler import Butler
from .._timespan import Timespan

_LOG = logging.getLogger(__name__)


def queryDimensionRecords(
    repo: str,
    element: str,
    datasets: tuple[str, ...],
    collections: tuple[str, ...],
    where: str,
    order_by: tuple[str, ...],
    limit: int,
    offset: int,
) -> Table | None:
    """Query dimension records.

    Parameters
    ----------
    repo : `str`
        Butler location.
    element : `str`
        Name of relevant dimension record.
    datasets : `tuple` of `str`
        Dataset types to restrict query by.
    collections : `~collections.abc.Iterable` of `str`
        Collections to search.
    where : `str`
        Query string.
    order_by : `tuple` of `str`
        Columns to order results by.
    limit : `int`
        Maximum number of results.
    offset : `int`
        Offset into the results.

    Notes
    -----
    Docstring for supported parameters is the same as
    `~lsst.daf.butler.Registry.queryDimensionRecords` except for ``no_check``,
    which is the inverse of ``check``.
    """
    if offset:
        raise NotImplementedError("--offset is no longer supported.  It will be removed after v28.")

    butler = Butler.from_config(repo, without_datastore=True)

    with butler.query() as query:

        if datasets:
            query_collections = collections or "*"
            dataset_types = butler.registry.queryDatasetTypes(datasets)
            collections_info = butler.collections.query_info(
                query_collections, include_summary=True, summary_datasets=dataset_types
            )
            dataset_type_collections = butler.collections._group_by_dataset_type(
                {dt.name for dt in dataset_types}, collections_info
            )

            if not dataset_type_collections:
                return None

            for dt, dt_collections in dataset_type_collections.items():
                query = query.join_dataset_search(dt, collections=dt_collections)

        query_results = query.dimension_records(element)

        if where:
            query_results = query_results.where(where)
        if order_by:
            query_results = query_results.order_by(*order_by)
        query_limit = abs(limit)
        warn_limit = False
        if limit != 0:
            if limit < 0:
                query_limit += 1
                warn_limit = True

            query_results = query_results.limit(query_limit)

        records = list(query_results)
        if warn_limit and len(records) == query_limit:
            records.pop(-1)
            _LOG.warning("More data IDs are available than the request limit of %d", abs(limit))

        if not records:
            return None

    if not order_by:
        # use the dataId to sort the rows if not ordered already
        records.sort(key=attrgetter("dataId"))

    # order the columns the same as the record's `field.names`, and add units
    # to timespans
    keys = records[0].fields.names
    headers = ["timespan (TAI)" if name == "timespan" else name for name in records[0].fields.names]

    def conform(v: Any) -> Any:
        match v:
            case Timespan():
                v = str(v)
            case bytes():
                v = "0x" + v.hex()
            case Region():
                v = "(elided)"
        return v

    return Table([[conform(getattr(record, key, None)) for record in records] for key in keys], names=headers)
