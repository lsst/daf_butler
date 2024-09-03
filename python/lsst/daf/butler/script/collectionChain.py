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

from .._butler import Butler
from .._collection_type import CollectionType
from ..registry import MissingCollectionError


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
    parent : `str`
        Name of the chained collection to update. Will be created if it
        does not exist already.
    children : iterable of `str`
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
    butler = Butler.from_config(repo, writeable=True, without_datastore=True)

    # Every mode needs children except pop.
    if not children and mode != "pop":
        raise RuntimeError(f"Must provide children when defining a collection chain in mode {mode}.")

    try:
        butler.collections.get_info(parent)
    except MissingCollectionError:
        # Create it -- but only if mode can work with empty chain.
        if mode in ("redefine", "extend", "prepend"):
            if not doc:
                doc = None
            butler.collections.register(parent, CollectionType.CHAINED, doc)
        else:
            raise RuntimeError(
                f"Mode '{mode}' requires that the collection exists "
                f"but collection '{parent}' is not known to this registry"
            ) from None

    if flatten:
        if mode not in ("redefine", "prepend", "extend"):
            raise RuntimeError(f"'flatten' flag is not allowed for {mode}")
        children = butler.collections.query(children, flatten_chains=True)

    _modify_collection_chain(butler, mode, parent, children)

    return butler.collections.get_info(parent).children


def _modify_collection_chain(butler: Butler, mode: str, parent: str, children: Iterable[str]) -> None:
    if mode == "prepend":
        butler.collections.prepend_chain(parent, children)
    elif mode == "redefine":
        butler.collections.redefine_chain(parent, children)
    elif mode == "remove":
        butler.collections.remove_from_chain(parent, children)
    elif mode == "pop":
        children_to_pop = _find_children_to_pop(butler, parent, children)
        butler.collections.remove_from_chain(parent, children_to_pop)
    elif mode == "extend":
        butler.collections.extend_chain(parent, children)
    else:
        raise ValueError(f"Unrecognized update mode: '{mode}'")


def _find_children_to_pop(butler: Butler, parent: str, children: Iterable[str]) -> list[str]:
    """Find the names of the children in the parent collection corresponding to
    the given indexes.
    """
    children = list(children)
    collection_info = butler.collections.get_info(parent)
    current = collection_info.children
    n_current = len(current)
    if children:

        def convert_index(i: int) -> int:
            """Convert negative index to positive."""
            if i >= 0:
                return i
            return n_current + i

        # For this mode the children should be integers.
        # Convert negative integers to positive ones to allow
        # sorting.
        indices = [convert_index(int(child)) for child in children]
    else:
        # Nothing specified, pop from the front of the chain.
        indices = [0]

    for index in indices:
        if index >= n_current:
            raise IndexError(
                f"Index {index} is out of range.  Parent collection {parent} has {n_current} children."
            )

    return [current[i] for i in indices]
