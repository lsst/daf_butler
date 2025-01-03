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

import dataclasses
import logging
from collections.abc import Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, NamedTuple

from lsst.utils.iteration import ensure_iterable

from ._dataset_ref import DatasetRef
from ._exceptions import InvalidQueryError, MissingDatasetTypeError
from .dimensions import DataId, DataIdValue
from .queries import Query
from .utils import has_globs

if TYPE_CHECKING:
    from ._butler import Butler


_LOG = logging.getLogger(__name__)


class DatasetsPage(NamedTuple):
    """A single page of results from ``query_all_datasets``."""

    dataset_type: str
    data: list[DatasetRef]


@dataclasses.dataclass(frozen=True)
class QueryAllDatasetsParameters:
    """These are the parameters passed to `Butler.query_all_datasets` and have
    the same meaning as that function unless noted below.
    """

    collections: Sequence[str]
    name: Sequence[str]
    find_first: bool
    data_id: DataId
    where: str
    bind: Mapping[str, Any]
    limit: int | None
    """
    Upper limit on the number of returned records. `None` can be used
    if no limit is wanted. A limit of ``0`` means that the query will
    be executed and validated but no results will be returned.

    (This cannot be negative, contrary to the `Butler.query_all_datasets`
    equivalent.)
    """
    with_dimension_records: bool
    kwargs: dict[str, DataIdValue] = dataclasses.field(default_factory=dict)


def query_all_datasets(
    butler: Butler, query: Query, args: QueryAllDatasetsParameters
) -> Iterator[DatasetsPage]:
    """Query for dataset refs from multiple types simultaneously.

    Parameters
    ----------
    butler : `Butler`
        Butler instance to use for executing queries.
    query : `Query`
        Query context object to use for executing queries.
    args : `QueryAllDatasetsParameters`
        Arguments describing the query to be performed.

    Raises
    ------
    MissingDatasetTypeError
        When no dataset types match ``name``, or an explicit (non-glob)
        dataset type in ``name`` does not exist.
    InvalidQueryError
        If the parameters to the query are inconsistent or malformed.
    MissingCollectionError
        If a given collection is not found.

    Returns
    -------
    pages : `~collections.abc.Iterator` [ `DatasetsPage` ]
        `DatasetRef` results matching the given query criteria, grouped by
        dataset type.
    """
    if args.find_first and has_globs(args.collections):
        raise InvalidQueryError("Can not use wildcards in collections when find_first=True")

    dataset_type_query = list(ensure_iterable(args.name))

    with butler.registry.caching_context():
        dataset_type_collections = _filter_collections_and_dataset_types(
            butler, args.collections, dataset_type_query
        )

        limit = args.limit
        for dt, filtered_collections in sorted(dataset_type_collections.items()):
            _LOG.debug("Querying dataset type %s", dt)
            results = (
                query.datasets(dt, filtered_collections, find_first=args.find_first)
                .where(args.data_id, args.where, args.kwargs, bind=args.bind)
                .limit(limit)
            )
            if args.with_dimension_records:
                results = results.with_dimension_records()

            for page in results._iter_pages():
                if limit is not None:
                    # Track how much of the limit has been used up by each
                    # query.
                    limit -= len(page)

                yield DatasetsPage(dataset_type=dt, data=page)

            if limit is not None and limit <= 0:
                break


def _filter_collections_and_dataset_types(
    butler: Butler, collections: Sequence[str], dataset_type_query: Sequence[str]
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
