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
from collections.abc import Iterable, Iterator, Mapping
from typing import Any, NamedTuple

from lsst.utils.iteration import ensure_iterable

from ._butler import Butler
from ._dataset_ref import DatasetRef
from ._exceptions import InvalidQueryError, MissingDatasetTypeError
from .dimensions import DataId
from .utils import has_globs

_LOG = logging.getLogger(__name__)


class DatasetsPage(NamedTuple):
    """A single page of results from ``query_all_datasets``."""

    dataset_type: str
    data: list[DatasetRef]


def query_all_datasets(
    butler: Butler,
    *,
    collections: list[str],
    name: str | Iterable[str] = "*",
    find_first: bool = True,
    data_id: DataId | None = None,
    where: str = "",
    bind: Mapping[str, Any] | None = None,
    with_dimension_records: bool = False,
    limit: int | None = None,
    order_by: Iterable[str] | str | None = None,
    **kwargs: Any,
) -> Iterator[DatasetsPage]:
    """Query for dataset refs from multiple types simultaneously.

    Parameters
    ----------
    butler : `Butler`
        Butler instance to use for executing queries.
    collections :  `list` [ `str` ]
        The collections to search, in order.  If not provided
        or `None`, the default collection search path for this butler is
        used.
    name : `str` or `~collections.abc.Iterable` [ `str` ], optional
        Names or name patterns (glob-style) that returned dataset type
        names must match.  If an iterable, items are OR'd together.  The
        default is to include all dataset types in the given collections.
    find_first : `bool`, optional
        If `True` (default), for each result data ID, only yield one
        `DatasetRef` of each `DatasetType`, from the first collection in
        which a dataset of that dataset type appears (according to the
        order of ``collections`` passed in).
    data_id : `dict` or `DataCoordinate`, optional
        A data ID whose key-value pairs are used as equality constraints in
        the query.
    where : `str`, optional
        A string expression similar to a SQL WHERE clause.  May involve any
        column of a dimension table or (as a shortcut for the primary key
        column of a dimension table) dimension name.  See
        :ref:`daf_butler_dimension_expressions` for more information.
    bind : `~collections.abc.Mapping`, optional
        Mapping containing literal values that should be injected into the
        ``where`` expression, keyed by the identifiers they replace. Values
        of collection type can be expanded in some cases; see
        :ref:`daf_butler_dimension_expressions_identifiers` for more
        information.
    with_dimension_records : `bool`, optional
        If `True` (default is `False`) then returned data IDs will have
        dimension records.
    limit : `int` or `None`, optional
        Upper limit on the number of returned records. `None` can be used
        if no limit is wanted. A limit of ``0`` means that the query will
        be executed and validated but no results will be returned.
    order_by : `~collections.abc.Iterable` [`str`] or `str`, optional
        Names of the columns/dimensions to use for ordering returned data
        IDs. Column name can be prefixed with minus (``-``) to use
        descending ordering.  Results are ordered only within each dataset
        type, they are not globally ordered across all results.
    **kwargs
        Additional keyword arguments are forwarded to
        `DataCoordinate.standardize` when processing the ``data_id``
        argument (and may be used to provide a constraining data ID even
        when the ``data_id`` argument is `None`).

    Raises
    ------
    MissingDatasetTypeError
        When no dataset types match ``name``, or an explicit (non-glob)
        dataset type in ``name`` does not exist.
    InvalidQueryError
        If the parameters to the query are inconsistent or malformed.

    Returns
    -------
    pages : `~collections.abc.Iterator` [ `DatasetsPage` ]
        `DatasetRef` results matching the given query criteria, grouped by
        dataset type.
    """
    if find_first and has_globs(collections):
        raise InvalidQueryError("Can not use wildcards in collections when find_first=True")

    dataset_type_query = list(ensure_iterable(name))
    dataset_type_collections = _filter_collections_and_dataset_types(butler, collections, dataset_type_query)

    for dt, filtered_collections in sorted(dataset_type_collections.items()):
        _LOG.debug("Querying dataset type %s", dt)
        results = butler.query_datasets(
            dt,
            collections=filtered_collections,
            find_first=find_first,
            with_dimension_records=with_dimension_records,
            data_id=data_id,
            order_by=order_by,
            explain=False,
            where=where,
            bind=bind,
            limit=limit,
            **kwargs,
        )

        # Track how much of the limit has been used up by each query.
        if limit is not None:
            limit -= len(results)

        yield DatasetsPage(dataset_type=dt, data=results)

        if limit is not None and limit <= 0:
            break


def _filter_collections_and_dataset_types(
    butler: Butler, collections: list[str], dataset_type_query: list[str]
) -> Mapping[str, list[str]]:
    """For each dataset type matching the query, filter down the given
    collections to only those that might actually contain datasets of the given
    type.

    Parameters
    ----------
    collections
        List of collection names or collection search globs.
    dataset_type_query
        List of dataset type names or search globs.

    Returns
    -------
    mapping
        Mapping from dataset type name to list of collections that contain that
        dataset type.

    Notes
    -----
    Because collection summaries are an approximation, some of the returned
    collections may not actually contain datasets of the expected type.
    """
    missing_types: list[str] = []
    dataset_types = set(butler.registry.queryDatasetTypes(dataset_type_query, missing=missing_types))
    if len(dataset_types) == 0:
        raise MissingDatasetTypeError(f"No dataset types found for query {dataset_type_query}")
    if len(missing_types) > 0:
        raise MissingDatasetTypeError(f"Dataset types not found: {missing_types}")

    # Expand the collections query and include summary information.
    query_collections_info = butler.collections.query_info(
        collections,
        include_summary=True,
        flatten_chains=True,
        include_chains=False,
        summary_datasets=dataset_types,
    )

    # Only iterate over dataset types that are relevant for the query.
    dataset_type_names = {dataset_type.name for dataset_type in dataset_types}
    dataset_type_collections = butler.collections._group_by_dataset_type(
        dataset_type_names, query_collections_info
    )
    n_dataset_types = len(dataset_types)
    if (n_filtered := len(dataset_type_collections)) != n_dataset_types:
        _LOG.debug("Filtered %d dataset types down to %d", n_dataset_types, n_filtered)
    else:
        _LOG.debug("Processing %d dataset type%s", n_dataset_types, "" if n_dataset_types == 1 else "s")

    return dataset_type_collections
