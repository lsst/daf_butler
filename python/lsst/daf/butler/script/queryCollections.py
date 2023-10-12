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
from ..registry import CollectionType


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
    repo
    glob
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
    names = sorted(
        butler.registry.queryCollections(collectionTypes=frozenset(collection_type), expression=glob or ...)
    )
    if inverse:
        for name in names:
            type = butler.registry.getCollectionType(name)
            parentNames = butler.registry.getCollectionParentChains(name)
            if parentNames:
                first = True
                for parentName in sorted(parentNames):
                    table.add_row((name if first else "", type.name if first else "", parentName))
                    first = False
            else:
                table.add_row((name, type.name, ""))
        # If none of the datasets has a parent dataset then remove the
        # description column.
        if not any(c for c in table[descriptionCol]):
            del table[descriptionCol]
    else:
        for name in names:
            type = butler.registry.getCollectionType(name)
            if type == CollectionType.CHAINED:
                children = butler.registry.getCollectionChain(name)
                if children:
                    first = True
                    for child in sorted(children):
                        table.add_row((name if first else "", type.name if first else "", child))
                        first = False
                else:
                    table.add_row((name, type.name, ""))
            else:
                table.add_row((name, type.name, ""))
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
    repo
    glob
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

    def addCollection(name: str, level: int = 0) -> None:
        collectionType = butler.registry.getCollectionType(name)
        table.add_row(("  " * level + name, collectionType.name))
        if inverse:
            parentNames = butler.registry.getCollectionParentChains(name)
            for pname in sorted(parentNames):
                addCollection(pname, level + 1)
        else:
            if collectionType == CollectionType.CHAINED:
                childNames = butler.registry.getCollectionChain(name)
                for name in childNames:
                    addCollection(name, level + 1)

    collections = butler.registry.queryCollections(
        collectionTypes=frozenset(collection_type), expression=glob or ...
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
    collectionNames = list(
        butler.registry.queryCollections(
            collectionTypes=frozenset(collection_type), flattenChains=True, expression=glob or ...
        )
    )

    collectionTypes = [butler.registry.getCollectionType(c).name for c in collectionNames]
    return Table((collectionNames, collectionTypes), names=("Name", "Type"))


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
    inverse : `bool`
        If true, show what CHAINED collections the dataset is a member of
        (instead of what datasets any CHAINED collection contains)

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
