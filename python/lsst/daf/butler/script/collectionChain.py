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


def collectionChain(repo, mode, parent, children, doc, flatten, pop):
    """Get the collections whose names match an expression.

    Parameters
    ----------
    repo : `str`
        URI to the location of the repo or URI to a config file describing the
        repo and its location.
    mode : `str`
        Update mode for this chain. Options are:
        'redefine': Create or modify ``parent`` to be defined by the supplied
        ``children``.
        'remove': Modify existing chain to remove ``children`` from it.
        'extend': Modify existing chain to add ``children`` to the end of it.
        This is the same as 'redefine' if the chain does not exist.
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
    pop : `bool`
        Pop the first collection off the chain. Can not be used if ``children``
        are given. The ``parent`` collection must exist.
    """
    butler = Butler(repo, writeable=True)

    if pop:
        if children:
            raise RuntimeError("Can not provide children if using 'pop'")

        children = butler.registry.getCollectionChain(parent)[1:]
        butler.registry.setCollectionChain(parent, children)
        return

    # All other options require children.
    if not children:
        raise RuntimeError("Must provide children when defining a collection chain.")

    try:
        butler.registry.getCollectionType(parent)
    except MissingCollectionError:
        # Create it -- but only if mode is redefine or extend
        if mode in ("redefine", "extend"):
            if not doc:
                doc = None
            butler.registry.registerCollection(parent, CollectionType.CHAINED, doc)
        else:
            raise RuntimeError(f"Mode '{mode}' requires that the collection exists "
                               f"but collection '{parent}' is not known to this registry") from None

    current = list(butler.registry.getCollectionChain(parent))

    if mode == "redefine":
        # Given children are what we want.
        pass
    elif mode == "extend":
        current.extend(children)
        children = current
    elif mode == "remove":
        for child in children:
            current.remove(child)
        children = current
    else:
        raise ValueError(f"Unrecognized update mode: '{mode}'")

    butler.registry.setCollectionChain(parent, children, flatten=flatten)
