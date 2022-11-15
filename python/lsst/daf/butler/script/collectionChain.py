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

from collections.abc import Iterable

from .._butler import Butler
from ..registry import CollectionType, MissingCollectionError


def collectionChain(
    repo: str, mode: str, parent: str, children: Iterable[str], doc: str | None, flatten: bool
) -> tuple[str, ...]:
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
        'prepend': Add the given ``children`` to the beginning of the chain.
        'extend': Modify existing chain to add ``children`` to the end of it.
        'pop': Pop a numbered element off the chain. Defaults to popping
        the first element (0). ``children`` must be integers if given.
        Both 'prepend' and 'extend' are the same as 'redefine' if the chain
        does not exist.
    parent: `str`
        Name of the chained collection to update. Will be created if it
        does not exist already.
    children: iterable of `str`
        Names of the children to be included in the chain.
    doc : `str`
        If the chained collection is being created, the documentation string
        that will be associated with it.
    flatten : `bool`
        If `True`, recursively flatten out any nested
        `~CollectionType.CHAINED` collections in ``children`` first.

    Returns
    -------
    chain : `tuple` of `str`
        The collections in the chain following this command.
    """
    butler = Butler(repo, writeable=True)

    # Every mode needs children except pop.
    if not children and mode != "pop":
        raise RuntimeError(f"Must provide children when defining a collection chain in mode {mode}.")

    try:
        butler.registry.getCollectionType(parent)
    except MissingCollectionError:
        # Create it -- but only if mode can work with empty chain.
        if mode in ("redefine", "extend", "prepend"):
            if not doc:
                doc = None
            butler.registry.registerCollection(parent, CollectionType.CHAINED, doc)
        else:
            raise RuntimeError(
                f"Mode '{mode}' requires that the collection exists "
                f"but collection '{parent}' is not known to this registry"
            ) from None

    current = list(butler.registry.getCollectionChain(parent))

    if mode == "redefine":
        # Given children are what we want.
        pass
    elif mode == "prepend":
        children = tuple(children) + tuple(current)
    elif mode == "extend":
        current.extend(children)
        children = current
    elif mode == "remove":
        for child in children:
            current.remove(child)
        children = current
    elif mode == "pop":
        if children:
            n_current = len(current)

            def convert_index(i: int) -> int:
                """Convert negative index to positive."""
                if i >= 0:
                    return i
                return n_current + i

            # For this mode the children should be integers.
            # Convert negative integers to positive ones to allow
            # sorting.
            indices = [convert_index(int(child)) for child in children]

            # Reverse sort order so we can remove from the end first
            indices = list(reversed(sorted(indices)))

        else:
            # Nothing specified, pop from the front of the chain.
            indices = [0]

        for i in indices:
            current.pop(i)

        children = current
    else:
        raise ValueError(f"Unrecognized update mode: '{mode}'")

    butler.registry.setCollectionChain(parent, children, flatten=flatten)

    return tuple(butler.registry.getCollectionChain(parent))
