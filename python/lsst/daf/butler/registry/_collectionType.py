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

__all__ = [
    "CollectionType",
]

import enum
from typing import FrozenSet, Iterable, Optional


class CollectionType(enum.IntEnum):
    """Enumeration used to label different types of collections."""

    RUN = 1
    """A ``RUN`` collection (also just called a 'run') is the initial
    collection a dataset is inserted into and the only one it can never be
    removed from.

    Within a particular run, there may only be one dataset with a particular
    dataset type and data ID.
    """

    TAGGED = 2
    """Datasets can be associated with and removed from ``TAGGED`` collections
    arbitrarily.

    Within a particular tagged collection, there may only be one dataset with
    a particular dataset type and data ID.
    """

    CHAINED = 3
    """A ``CHAINED`` collection is simply an ordered list of other collections
    to be searched.  These may include other ``CHAINED`` collections.
    """

    CALIBRATION = 4
    """A ``CALIBRATION`` collection operates like a ``TAGGED`` collection, but
    also associates each dataset with a validity range as well.  Queries
    against calibration collections must include a timestamp as an input.

    It is difficult (perhaps impossible) to enforce a constraint that there be
    one dataset with a particular dataset type and data ID at any particular
    timestamp in the database, so higher-level tools that populate calibration
    collections are expected to maintain that invariant instead.
    """

    @classmethod
    def all(cls) -> FrozenSet[CollectionType]:
        """Return a `frozenset` containing all members."""
        return frozenset(cls.__members__.values())

    @classmethod
    def from_name(cls, name: str) -> CollectionType:
        """Return the `CollectionType` given its name.

        Parameters
        ----------
        name : `str`
            Name of the collection type. Case insensitive.

        Returns
        -------
        collection_type : `CollectionType`
            The matching collection type.

        Raises
        ------
        KeyError
            Raised if the name does not match a collection type.
        """
        name = name.upper()
        try:
            return CollectionType.__members__[name]
        except KeyError:
            raise KeyError(f"Collection type of '{name}' not known to Butler.") from None

    @classmethod
    def from_names(cls, names: Optional[Iterable[str]]) -> FrozenSet[CollectionType]:
        """Return a `frozenset` containing the `CollectionType` instances
        corresponding to the names.

        Parameters
        ----------
        names : iterable of `str`, or `None`
            Names of collection types. Case insensitive. If `None` or empty,
            all collection types will be returned.

        Returns
        -------
        types : `frozenset` of `CollectionType`
            The matching types.

        Raises
        ------
        KeyError
            Raised if the name does not correspond to a collection type.
        """
        if not names:
            return CollectionType.all()

        # Convert to real collection types
        return frozenset({CollectionType.from_name(name) for name in names})
