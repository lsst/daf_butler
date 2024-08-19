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

import logging

from lsst.daf.butler import DatasetRef

from .._butler import Butler

log = logging.getLogger(__name__)


def transferDatasets(
    source: str,
    dest: str,
    dataset_type: tuple[str, ...],
    collections: tuple[str, ...],
    where: str,
    find_first: bool,
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

    dataset_type_expr = dataset_type or ...
    collections_expr: tuple[str, ...] = collections or ("*",)

    dataset_types = [dt.name for dt in source_butler.registry.queryDatasetTypes(dataset_type_expr)]
    source_refs: list[DatasetRef] = []
    with source_butler._query() as query:
        query_collections_info = source_butler.collections.x_query_info(
            collections_expr, include_summary=True
        )
        query_collections = [info.name for info in query_collections_info]
        dataset_types = list(
            source_butler.collections._filter_dataset_types(dataset_types, query_collections_info)
        )
        # Loop over dataset types and accumulate.
        for dt in dataset_types:
            results = query.datasets(dt, collections=query_collections, find_first=find_first)
            if where:
                results = results.where(where)
            # Need dimension records in case new datastore records have to
            # be derived.
            source_refs.extend(results.with_dimension_records())

    # Place results in a set to remove duplicates
    source_refs_set = set(source_refs)

    transferred = dest_butler.transfer_from(
        source_butler,
        source_refs_set,
        transfer=transfer,
        register_dataset_types=register_dataset_types,
        transfer_dimensions=transfer_dimensions,
    )
    return len(transferred)
