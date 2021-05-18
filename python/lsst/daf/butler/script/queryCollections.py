# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

from astropy.table import Table
import itertools
from numpy import array

from .. import Butler


def queryCollections(repo, glob, collection_type, chains):
    """Get the collections whose names match an expression.

    Parameters
    ----------
    repo : `str`
        URI to the location of the repo or URI to a config file describing the
        repo and its location.
    glob : iterable [`str`]
        A list of glob-style search string that fully or partially identify
        the dataset type names to search for.
    collection_type : `Iterable` [ `CollectionType` ], optional
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
    butler = Butler(repo)

    if not glob:
        glob = ...

    if chains == "TABLE":
        collectionNames = list(butler.registry.queryCollections(collectionTypes=frozenset(collection_type),
                                                                expression=glob))
        collectionTypes = [butler.registry.getCollectionType(c).name for c in collectionNames]
        collectionDefinitions = [str(butler.registry.getCollectionChain(name)) if colType == "CHAINED" else ""
                                 for name, colType in zip(collectionNames, collectionTypes)]

        # Only add a definition column if at least one definition row is
        # populated:
        if any(collectionDefinitions):
            return Table((collectionNames, collectionTypes, collectionDefinitions),
                         names=("Name", "Type", "Definition"))
        return Table((collectionNames, collectionTypes), names=("Name", "Type"))
    elif chains == "TREE":
        def getCollections(collectionName, nesting=0):
            """Get a list of the name and type of the passed-in collection,
            and its child collections, if it is a CHAINED collection. Child
            collection names are indended from their parents by adding spaces
            before the collection name.

            Parameters
            ----------
            collectionName : `str`
                The name of the collection to get.
            nesting : `int`
                The amount of indent to apply before each collection.

            Returns
            -------
            collections : `list` [`tuple` [`str`, `str`]]
                Tuples of the collection name and its type. Starts with the
                passed-in collection, and if it is a CHAINED collection, each
                of its children follows, and so on.
            """
            def nested(val):
                stepDepth = 2
                return " " * (stepDepth * nesting) + val

            collectionType = butler.registry.getCollectionType(collectionName).name
            if collectionType == "CHAINED":
                # Get the child collections of the chained collection:
                childCollections = list(butler.registry.getCollectionChain(collectionName))

                # Fill in the child collections of the chained collection:
                collections = itertools.chain(*[getCollections(child, nesting + 1)
                                                for child in childCollections])

                # Insert the chained (parent) collection at the beginning of
                # the list, and return the list:
                return [(nested(collectionName), "CHAINED")] + list(collections)
            else:
                return [(nested(collectionName), collectionType)]

        collectionNameIter = butler.registry.queryCollections(collectionTypes=frozenset(collection_type),
                                                              expression=glob)
        collections = list(itertools.chain(*[getCollections(name) for name in collectionNameIter]))
        return Table(array(collections), names=("Name", "Type"))
    elif chains == "FLATTEN":
        collectionNames = list(butler.registry.queryCollections(collectionTypes=frozenset(collection_type),
                                                                flattenChains=True,
                                                                expression=glob))
        collectionTypes = [butler.registry.getCollectionType(c).name for c in collectionNames]
        return Table((collectionNames,
                      collectionTypes),
                     names=("Name", "Type"))
    raise RuntimeError(f"Value for --chains not recognized: {chains}")
