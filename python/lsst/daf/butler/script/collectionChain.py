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

from .. import Butler, CollectionType
from ..registry import MissingCollectionError


def collectionChain(repo, parent, children, doc, flatten):
    """Get the collections whose names match an expression.

    Parameters
    ----------
    repo : `str`
        URI to the location of the repo or URI to a config file describing the
        repo and its location.
    parent: `str`
        Name of the chained collection to update. Will be created if it
        does not exist already.
    children: iterable of `str`
        Names of the children to be included in the chain.
    doc : `str`
        If the chained collection is being created, the documentation string
        that will be associated with it.
    flatten : `str`
        If `True`, recursively flatten out any nested
        `~CollectionType.CHAINED` collections in ``children`` first.
    """

    butler = Butler(repo, writeable=True)

    try:
        butler.registry.getCollectionType(parent)
    except MissingCollectionError:
        # Create it
        if not doc:
            doc = None
        butler.registry.registerCollection(parent, CollectionType.CHAINED, doc)

    butler.registry.setCollectionChain(parent, children, flatten=flatten)
