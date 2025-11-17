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

from collections.abc import Iterable
from fnmatch import fnmatch
from typing import Literal

from astropy.table import Column, Table, hstack, vstack

from .._butler import Butler
from .._butler_collections import CollectionInfo
from .._collection_type import CollectionType


def _parseDatasetTypes(dataset_types: frozenset[str] | list[str] | None) -> list[str]:
    """Parse dataset types from a collection info object or a list of strings.

    Parameters
    ----------
    dataset_types : `frozenset`[`str`] | `list`[`str`] | `None`
        The dataset types to parse. If `None`, an empty list is returned.
        If a `frozenset` or `list` is provided, it is returned as a list.

    Returns
    -------
    dataset_types : `list`[`str`]
        The parsed dataset types.
    """
    return [""] if not dataset_types else list(dataset_types)


def _getTable(
    repo: str,
    glob: Iterable[str],
    collection_type: Iterable[CollectionType],
    inverse: bool,
    show_dataset_types: bool = False,
    exclude_dataset_types: Iterable[str] | None = None,
) -> Table:
    """Run queryCollections and return the results in Table form.

    Only lists the first child (or parent if `inverse` is `True`) in the
    description column.

    Parameters
    ----------
    repo : `str`
        The Butler repository location.
    glob : `collections.abc.Iterable` of `str`
        Wildcard to pass to ``queryCollections``.
    collection_type
        Same as `queryCollections`
    inverse : `bool`
        True if parent CHAINED datasets of each dataset should be listed in the
        description column, False if children of CHAINED datasets should be
        listed.
    show_dataset_types : `bool`, optional
        If `True`, also show the dataset types present within each collection.
    exclude_dataset_types : `~collections.abc.Iterable` [ `str` ], optional
        A glob-style iterable of dataset types to exclude.
        Only has an effect if `show_dataset_types` is True.

    Returns
    -------
    collections : `astropy.table.Table`
        Same as `queryCollections`
    """
    typeCol = "Type"
    descriptionCol = "Parents" if inverse else "Children"
    table = Table(
        names=("Name", typeCol, descriptionCol),
        dtype=(str, str, str),
    )
    if show_dataset_types:
        table.add_column(Column(name="Dataset Types", dtype=str))

    with Butler.from_config(repo) as butler:

        def addDatasetTypes(collection_table: Table, collection: str, dataset_types: list[str]) -> Table:
            if dataset_types[0] == "":
                cinfo = butler.collections.get_info(collection, include_summary=True)
                dataset_types = _parseDatasetTypes(cinfo.dataset_types)
            if exclude_dataset_types:
                dataset_types = [
                    dt
                    for dt in dataset_types
                    if not any(fnmatch(dt, pattern) for pattern in exclude_dataset_types)
                ]
                dataset_types = _parseDatasetTypes(dataset_types)
            types_table = Table({"Dataset Types": sorted(dataset_types)}, dtype=(str,))
            collection_table = hstack([collection_table, types_table]).filled("")
            return collection_table

        def addCollection(info: CollectionInfo, relation: str) -> None:
            try:
                info_relatives = getattr(info, relation)
            except AttributeError:
                info_relatives = []
            # Parent results can be returned in a non-deterministic order, so
            # sort them to make the output deterministic.
            if relation == "parents":
                info_relatives = sorted(info_relatives)
            if info_relatives:
                collection_table = Table([[info.name], [info.type.name]], names=("Name", typeCol))
                description_table = Table(names=(descriptionCol,), dtype=(str,))
                for info_relative in info_relatives:
                    relative_table = Table([[info_relative]], names=(descriptionCol,))
                    if show_dataset_types:
                        relative_table = addDatasetTypes(relative_table, info_relative, [""])
                    description_table = vstack([description_table, relative_table])
                collection_table = hstack([collection_table, description_table]).filled("")
                for row in collection_table:
                    table.add_row(row)
            else:
                collection_table = Table(
                    [[info.name], [info.type.name], [""]], names=("Name", typeCol, descriptionCol)
                )
                if show_dataset_types:
                    collection_table = addDatasetTypes(collection_table, info.name, [""])
                for row in collection_table:
                    table.add_row(row)

        collections = sorted(
            butler.collections.query_info(
                glob or "*",
                collection_types=frozenset(collection_type),
                include_parents=inverse,
                include_summary=show_dataset_types,
            )
        )
        if inverse:
            for info in collections:
                addCollection(info, "parents")
            # If none of the datasets has a parent dataset then remove the
            # description column.
            if not any(c for c in table[descriptionCol]):
                del table[descriptionCol]
        else:
            for info in collections:
                if info.type == CollectionType.CHAINED:
                    addCollection(info, "children")
                else:
                    addCollection(info, "self")
            # If there aren't any CHAINED datasets in the results then remove
            # the description column.
            if not any(columnVal == CollectionType.CHAINED.name for columnVal in table[typeCol]):
                del table[descriptionCol]

        return table


def _getTree(
    repo: str,
    glob: Iterable[str],
    collection_type: Iterable[CollectionType],
    inverse: bool,
    show_dataset_types: bool = False,
    exclude_dataset_types: Iterable[str] | None = None,
) -> Table:
    """Run queryCollections and return the results in a table representing tree
    form.

    Recursively lists children (or parents if `inverse` is `True`)

    Parameters
    ----------
    repo : `str`
        Butler repository location.
    glob : `collections.abc.Iterable` of `str`
        Wildcards to pass to ``queryCollections``.
    collection_type
        Same as `queryCollections`
    inverse : `bool`
        True if parent CHAINED datasets of each dataset should be listed in the
        description column, False if children of CHAINED datasets should be
        listed.
    show_dataset_types : `bool`, optional
        If `True`, also show the dataset types present within each collection.
    exclude_dataset_types : `~collections.abc.Iterable` [ `str` ], optional
        A glob-style iterable of dataset types to exclude.
        Only has an effect if `show_dataset_types` is True.

    Returns
    -------
    collections : `astropy.table.Table`
        Same as `queryCollections`
    """
    table = Table(
        names=("Name", "Type"),
        dtype=(str, str),
    )
    if show_dataset_types:
        table.add_column(Column(name="Dataset Types", dtype=str))

    with Butler.from_config(repo, without_datastore=True) as butler:

        def addCollection(info: CollectionInfo, level: int = 0) -> None:
            collection_table = Table([["  " * level + info.name], [info.type.name]], names=["Name", "Type"])
            if show_dataset_types:
                if info.type == CollectionType.CHAINED:
                    collection_table = hstack(
                        [collection_table, Table([[""] * len(collection_table)], names=["Dataset Types"])]
                    )
                else:
                    dataset_types = _parseDatasetTypes(info.dataset_types)
                    if exclude_dataset_types:
                        dataset_types = [
                            dt
                            for dt in dataset_types
                            if not any(fnmatch(dt, pattern) for pattern in exclude_dataset_types)
                        ]
                        dataset_types = _parseDatasetTypes(dataset_types)
                    dataset_types_table = Table({"Dataset Types": sorted(dataset_types)}, dtype=(str,))
                    collection_table = hstack([collection_table, dataset_types_table]).filled("")
            for row in collection_table:
                table.add_row(row)

            if inverse:
                assert info.parents is not None  # For mypy.
                for pname in sorted(info.parents):
                    pinfo = butler.collections.get_info(
                        pname, include_parents=inverse, include_summary=show_dataset_types
                    )
                    addCollection(pinfo, level + 1)
            else:
                if info.type == CollectionType.CHAINED:
                    for name in info.children:
                        cinfo = butler.collections.get_info(name, include_summary=show_dataset_types)
                        addCollection(cinfo, level + 1)

        collections = butler.collections.query_info(
            glob or "*",
            collection_types=frozenset(collection_type),
            include_parents=inverse,
            include_summary=show_dataset_types,
        )
        for collection in sorted(collections):
            addCollection(collection)
        return table


def _getList(
    repo: str,
    glob: Iterable[str],
    collection_type: Iterable[CollectionType],
    flatten_chains: bool,
    show_dataset_types: bool = False,
    exclude_dataset_types: Iterable[str] | None = None,
) -> Table:
    """Return collection results as a table representing a flat list of
    collections.

    Parameters
    ----------
    repo : `str`
        Butler repository location.
    glob : `collections.abc.Iterable` of `str`
        Wildcards to pass to ``queryCollections``.
    collection_type
        Same as `queryCollections`
    flatten_chains : `bool`
        If `True`, flatten the tree of CHAINED datasets.
    show_dataset_types : `bool`, optional
        If `True`, also show the dataset types present within each collection.
    exclude_dataset_types : `~collections.abc.Iterable` [ `str` ], optional
        A glob-style iterable of dataset types to exclude.
        Only has an effect if `show_dataset_types` is True.

    Returns
    -------
    collections : `astropy.table.Table`
        Same as `queryCollections`
    """
    table = Table(
        names=("Name", "Type"),
        dtype=(str, str),
    )
    if show_dataset_types:
        table.add_column(Column(name="Dataset Types", dtype=str))

    with Butler.from_config(repo) as butler:

        def addCollection(info: CollectionInfo) -> None:
            collection_table = Table([[info.name], [info.type.name]], names=["Name", "Type"])
            if show_dataset_types:
                dataset_types = _parseDatasetTypes(info.dataset_types)
                if exclude_dataset_types:
                    dataset_types = [
                        dt
                        for dt in dataset_types
                        if not any(fnmatch(dt, pattern) for pattern in exclude_dataset_types)
                    ]
                    dataset_types = _parseDatasetTypes(dataset_types)
                dataset_types_table = Table({"Dataset Types": sorted(dataset_types)}, dtype=(str,))
                collection_table = hstack([collection_table, dataset_types_table]).filled("")
            for row in collection_table:
                table.add_row(row)

        collections = list(
            butler.collections.query_info(
                glob or "*",
                collection_types=frozenset(collection_type),
                flatten_chains=flatten_chains,
                include_summary=show_dataset_types,
            )
        )
        for collection in collections:
            addCollection(collection)

        return table


def queryCollections(
    repo: str,
    glob: Iterable[str],
    collection_type: Iterable[CollectionType],
    chains: Literal["INVERSE-TABLE", "TABLE", "TREE", "INVERSE-TREE", "FLATTEN", "NO-CHILDREN"],
    show_dataset_types: bool = False,
    exclude_dataset_types: Iterable[str] | None = None,
) -> Table:
    """Get the collections whose names match an expression.

    Parameters
    ----------
    repo : `str`
        URI to the location of the repo or URI to a config file describing the
        repo and its location.
    glob : `~collections.abc.Iterable` [`str`]
        A list of glob-style search string that fully or partially identify
        the dataset type names to search for.
    collection_type : `~collections.abc.Iterable` [ `CollectionType` ], \
            optional
        If provided, only return collections of these types.
    chains : `str`
        Affects contents and formatting of results, see
        ``cli.commands.query_collections``.
    show_dataset_types : `bool`, optional
        If `True`, include the dataset types present within each collection.
    exclude_dataset_types : `~collections.abc.Iterable` [ `str` ], optional
        A glob-style iterable of dataset types to exclude.
        Only has an effect if `show_dataset_types` is True.

    Returns
    -------
    collections : `astropy.table.Table`
        A table containing information about collections.
    """
    if (inverse := chains == "INVERSE-TABLE") or chains == "TABLE":
        return _getTable(repo, glob, collection_type, inverse, show_dataset_types, exclude_dataset_types)
    elif (inverse := chains == "INVERSE-TREE") or chains == "TREE":
        return _getTree(repo, glob, collection_type, inverse, show_dataset_types, exclude_dataset_types)
    elif chains == "FLATTEN" or chains == "NO-CHILDREN":
        flatten = chains == "FLATTEN"
        return _getList(repo, glob, collection_type, flatten, show_dataset_types, exclude_dataset_types)
    raise RuntimeError(f"Value for --chains not recognized: {chains}")
