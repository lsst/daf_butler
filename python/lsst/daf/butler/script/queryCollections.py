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

from astropy.table import Table

from .._butler import Butler
from .._butler_collections import CollectionInfo
from .._collection_type import CollectionType


def _getTable(
    repo: str,
    glob: Iterable[str],
    collection_type: Iterable[CollectionType],
    inverse: bool,
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
    butler = Butler.from_config(repo)
    collections = sorted(
        butler.collections.query_info(
            glob or "*", collection_types=frozenset(collection_type), include_parents=inverse
        )
    )
    if inverse:
        for info in collections:
            if info.parents:
                first = True
                for parentName in sorted(info.parents):
                    table.add_row((info.name if first else "", info.type.name if first else "", parentName))
                    first = False
            else:
                table.add_row((info.name, info.type.name, ""))
        # If none of the datasets has a parent dataset then remove the
        # description column.
        if not any(c for c in table[descriptionCol]):
            del table[descriptionCol]
    else:
        for info in collections:
            if info.type == CollectionType.CHAINED:
                if info.children:
                    first = True
                    for child in info.children:
                        table.add_row((info.name if first else "", info.type.name if first else "", child))
                        first = False
                else:
                    table.add_row((info.name, info.type.name, ""))
            else:
                table.add_row((info.name, info.type.name, ""))
        # If there aren't any CHAINED datasets in the results then remove the
        # description column.
        if not any(columnVal == CollectionType.CHAINED.name for columnVal in table[typeCol]):
            del table[descriptionCol]

    return table


def _getTree(
    repo: str,
    glob: Iterable[str],
    collection_type: Iterable[CollectionType],
    inverse: bool,
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

    Returns
    -------
    collections : `astropy.table.Table`
        Same as `queryCollections`
    """
    table = Table(
        names=("Name", "Type"),
        dtype=(str, str),
    )
    butler = Butler.from_config(repo, without_datastore=True)

    def addCollection(info: CollectionInfo, level: int = 0) -> None:
        table.add_row(("  " * level + info.name, info.type.name))
        if inverse:
            assert info.parents is not None  # For mypy.
            for pname in sorted(info.parents):
                pinfo = butler.collections.get_info(pname, include_parents=inverse)
                addCollection(pinfo, level + 1)
        else:
            if info.type == CollectionType.CHAINED:
                for name in info.children:
                    cinfo = butler.collections.get_info(name)
                    addCollection(cinfo, level + 1)

    collections = butler.collections.query_info(
        glob or "*", collection_types=frozenset(collection_type), include_parents=inverse
    )
    for collection in sorted(collections):
        addCollection(collection)
    return table


def _getFlatten(
    repo: str,
    glob: Iterable[str],
    collection_type: Iterable[CollectionType],
) -> Table:
    butler = Butler.from_config(repo)
    collections = list(
        butler.collections.query_info(
            glob or "*", collection_types=frozenset(collection_type), flatten_chains=True
        )
    )
    names = [c.name for c in collections]
    types = [c.type.name for c in collections]
    return Table((names, types), names=("Name", "Type"))


def queryCollections(
    repo: str,
    glob: Iterable[str],
    collection_type: Iterable[CollectionType],
    chains: str,
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
        Must be one of "FLATTEN", "TABLE", or "TREE" (case sensitive).
        Affects contents and formatting of results, see
        ``cli.commands.query_collections``.

    Returns
    -------
    collections : `astropy.table.Table`
        A table containing information about collections.
    """
    if (inverse := chains == "INVERSE-TABLE") or chains == "TABLE":
        return _getTable(repo, glob, collection_type, inverse)
    elif (inverse := chains == "INVERSE-TREE") or chains == "TREE":
        return _getTree(repo, glob, collection_type, inverse)
    elif chains == "FLATTEN":
        return _getFlatten(repo, glob, collection_type)
    raise RuntimeError(f"Value for --chains not recognized: {chains}")
