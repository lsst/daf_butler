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

"""Code relating to constraints based on `DatasetRef`, `DatasetType`, or
`StorageClass`.
"""

from __future__ import annotations

__all__ = ("Constraints", "ConstraintsConfig", "ConstraintsValidationError")

import logging
from typing import TYPE_CHECKING

from .._config import Config
from .._config_support import LookupKey, processLookupConfigList
from .._exceptions import ValidationError

if TYPE_CHECKING:
    from .._dataset_ref import DatasetRef
    from .._dataset_type import DatasetType
    from .._storage_class import StorageClass
    from ..dimensions import DimensionUniverse

log = logging.getLogger(__name__)


class ConstraintsValidationError(ValidationError):
    """Thrown when a constraints list has mutually exclusive definitions."""

    pass


class ConstraintsConfig(Config):
    """Configuration information for `Constraints`."""

    pass


class Constraints:
    """Determine whether an entity is allowed to be handled.

    Supported entities are `DatasetRef`, `DatasetType`, or `StorageClass`.

    Parameters
    ----------
    config : `ConstraintsConfig` or `str`
        Load configuration.  If `None` then this is equivalent to having
        no restrictions.
    universe : `DimensionUniverse`
        The set of all known dimensions, used to normalize any lookup keys
        involving dimensions.
    """

    matchAllKey = LookupKey("all")
    """Configuration key associated with matching everything."""

    def __init__(self, config: ConstraintsConfig | str | None, *, universe: DimensionUniverse):
        # Default is to accept all and reject nothing
        self._accept = set()
        self._reject = set()

        if config is not None:
            self.config = ConstraintsConfig(config)

            if "accept" in self.config:
                self._accept = processLookupConfigList(self.config["accept"], universe=universe)
            if "reject" in self.config:
                self._reject = processLookupConfigList(self.config["reject"], universe=universe)

        if self.matchAllKey in self._accept and self.matchAllKey in self._reject:
            raise ConstraintsValidationError(
                "Can not explicitly accept 'all' and reject 'all' in one configuration"
            )

    def __str__(self) -> str:
        # Standard stringification
        if not self._accept and not self._reject:
            return "Accepts: all"

        accepts = ", ".join(str(k) for k in self._accept)
        rejects = ", ".join(str(k) for k in self._reject)
        return f"Accepts: {accepts}; Rejects: {rejects}"

    def isAcceptable(self, entity: DatasetRef | DatasetType | StorageClass) -> bool:
        """Check whether the supplied entity will be acceptable.

        Parameters
        ----------
        entity : `DatasetType`, `DatasetRef`, or `StorageClass`
            Instance to use to look in constraints table.
            The entity itself reports the `LookupKey` that is relevant.

        Returns
        -------
        allowed : `bool`
            `True` if the entity is allowed.
        """
        # Get the names to use for lookup
        names = set(entity._lookupNames())

        # Test if this entity is explicitly mentioned for accept/reject
        isExplicitlyAccepted = bool(names & self._accept)

        if isExplicitlyAccepted:
            return True

        isExplicitlyRejected = bool(names & self._reject)

        if isExplicitlyRejected:
            return False

        # Now look for wildcard match -- we have to also check for dataId
        # overrides

        # Generate a new set of lookup keys that use the wildcard name
        # but the supplied dimensions
        wildcards = {k.clone(name=self.matchAllKey.name) for k in names}

        isWildcardAccepted = bool(wildcards & self._accept)
        isWildcardRejected = bool(wildcards & self._reject)

        if isWildcardRejected:
            return False

        # If all the wildcard and explicit rejections have failed then
        # if the accept list is empty, or if a wildcard acceptance worked
        # we can accept, else reject
        if isWildcardAccepted or not self._accept:
            return True

        return False

    def getLookupKeys(self) -> set[LookupKey]:
        """Retrieve the look up keys for all the constraints entries.

        Returns
        -------
        keys : `set` of `LookupKey`
            The keys available for determining constraints.  Does not include
            the special "all" lookup key.
        """
        all = self._accept | self._accept
        return {a for a in all if a.name != self.matchAllKey.name}
