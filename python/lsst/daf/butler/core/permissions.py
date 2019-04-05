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

"""Code relating to control of permissions based on DatasetType."""

__all__ = ("Permissions", "PermissionsValidationError", "PermissionsConfig")

import logging
from .config import Config
from .configSupport import LookupKey, processLookupConfigList, normalizeLookupKeys
from .exceptions import ValidationError

log = logging.getLogger(__name__)


class PermissionsValidationError(ValidationError):
    """Exception thrown when a permissions list is not consistent with the
    associated `DatasetType`."""
    pass


class PermissionsConfig(Config):
    """Configuration information for `Permissions`"""
    pass


class Permissions:
    """Control whether a `DatasetType` is allowed to be handled.

    Parameters
    ----------
    config : `PermisssionsConfig` or `str`
        Load configuration.  If `None` then this is equivalent to having
        no restrictions.
    universe : `DimensionUniverse`, optional
        The set of all known dimensions. If not `None`, any look up keys
        involving dimensions will be normalized.  Normalization only happens
        once.
    """

    matchAllKey = LookupKey("all")
    """Configuration key associated with matching everything."""

    def __init__(self, config, universe=None):
        # Default is to accept all and reject nothing
        self.normalized = False
        self._accept = set()
        self._reject = set()

        if config is not None:
            self.config = PermissionsConfig(config)

            if "accept" in self.config:
                self._accept = processLookupConfigList(self.config["accept"])
            if "reject" in self.config:
                self._reject = processLookupConfigList(self.config["reject"])

        if self.matchAllKey in self._accept and self.matchAllKey in self._reject:
            raise PermissionsValidationError("Can not explicitly accept 'all' and reject 'all'"
                                             " in one configuration")

        # Normalize all the dimensions given the supplied universe
        self.normalizeDimensions(universe)

    def __str__(self):
        # Standard stringification
        if not self._accept and not self._reject:
            return "Accepts: all"

        accepts = ", ".join(str(k) for k in self._accept)
        rejects = ", ".join(str(k) for k in self._reject)
        return f"Accepts: {accepts}; Rejects: {rejects}"

    def hasPermission(self, entity):
        """Check whether the supplied entity has permission for whatever
        this `Permissions` class is associated with.

        Parameters
        ----------
        entity : `DatasetType`, `DatasetRef`, or `StorageClass`
            Instance to use to look in permissions table.
            The entity itself reports the `LookupKey` that is relevant.

        Returns
        -------
        allowed : `bool`
            `True` if the entity is allowed.
        """

        # normalize the registry if not already done and we have access
        # to a universe
        if not self.normalized:
            try:
                universe = entity.dimensions.universe
            except AttributeError:
                pass
            else:
                self.normalizeDimensions(universe)

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

    def normalizeDimensions(self, universe):
        """Normalize permission lookups that use dimensions.

        Parameters
        ----------
        universe : `DimensionUniverse`
            The set of all known dimensions. If `None`, returns without
            action.

        Notes
        -----
        Goes through all permission lookups, and for keys that include
        dimensions, rewrites those keys to use a verified set of
        dimensions.

        Returns without action if the keys have already been
        normalized.

        Raises
        ------
        ValueError
            Raised if a key exists where a dimension is not part of
            the ``universe``.
        """
        if self.normalized:
            return

        # Normalize as a dict to reuse the existing infrastructure
        for attr in ("_accept", "_reject"):
            temp = {k: None for k in getattr(self, attr)}
            normalizeLookupKeys(temp, universe)
            setattr(self, attr, set(temp))

        self.normalized = True
