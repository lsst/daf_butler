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

__all__ = ("transferDatasets",)

import itertools
import logging

from .._butler import Butler
from .queryDatasets import QueryDatasets

log = logging.getLogger(__name__)


def transferDatasets(
    source: str,
    dest: str,
    dataset_type: tuple[str, ...],
    collections: tuple[str, ...],
    where: str,
    find_first: bool,
    limit: int,
    order_by: tuple[str, ...],
    transfer: str,
    register_dataset_types: bool,
    transfer_dimensions: bool = True,
) -> int:
    """Transfer datasets from run in source to dest.

    Parameters
    ----------
    source : `str`
        URI string of the source Butler repo.
    dest : `str`
        URI string of the destination Butler repo.
    dataset_type : `tuple` of `str`
        Dataset type names. An empty tuple implies all dataset types.
    collections : `tuple` of `str`
        Names of collection globs to match. An empty tuple implies all
        collections.
    where : `str`
        Query modification string.
    find_first : `bool`
        Whether only the first match should be used.
    limit : `int`
        Limit the number of results to be returned. A value of 0 means
        unlimited. A negative value is used to specify a cap where a warning
        is issued if that cap is hit.
    order_by : `tuple` of `str`
        Dimensions to use for sorting results. If no ordering is given the
        results of ``limit`` are undefined and default sorting of the resulting
        datasets will be applied. It is an error if the requested ordering
        is inconsistent with the dimensions of the dataset type being queried.
    transfer : `str`
        Transfer mode to use when placing artifacts in the destination.
    register_dataset_types : `bool`
        Indicate whether missing dataset types should be registered.
    transfer_dimensions : `bool`
        Indicate whether dimensions should be transferred along with
        datasets. It can be more efficient to disable this if it is known
        that all dimensions exist.
    """
    source_butler = Butler.from_config(source, writeable=False)
    dest_butler = Butler.from_config(dest, writeable=True)

    dataset_type_expr = dataset_type or "*"
    collections_expr: tuple[str, ...] = collections or ("*",)

    query = QueryDatasets(
        butler=source_butler,
        glob=dataset_type_expr,
        collections=collections_expr,
        where=where,
        find_first=find_first,
        limit=limit,
        order_by=order_by,
        show_uri=False,
        with_dimension_records=True,
    )
    # Place results in a set to remove duplicates (which should not exist
    # in new query system)
    source_refs_set = set(itertools.chain(*query.getDatasets()))

    transferred = dest_butler.transfer_from(
        source_butler,
        source_refs_set,
        transfer=transfer,
        register_dataset_types=register_dataset_types,
        transfer_dimensions=transfer_dimensions,
    )
    return len(transferred)
