#
# LSST Data Management System
#
# Copyright 2008-2017  AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#

import os
import urllib


class Location(object):
    """Identifies a location in the `Datastore`.

    Attributes
    ----------
    uri
    path
    """

    __slots__ = ("_datastoreRoot", "_uri")

    def __init__(self, datastoreRoot, uri):
        self._datastoreRoot = datastoreRoot
        self._uri = urllib.parse.urlparse(uri)

    @property
    def uri(self):
        """URI corresponding to location.
        """
        return self._uri.geturl()

    @property
    def path(self):
        """Path corresponding to location.

        This path includes the root of the `Datastore`.
        """
        return os.path.join(self._datastoreRoot, self._uri.path.lstrip("/"))


class LocationFactory(object):
    """Factory for `Location` instances.
    """

    def __init__(self, datastoreRoot):
        """Constructor

        Parameters
        ----------
        datastoreRoot : `str`
            Root directory of the `Datastore` in the filesystem.
        """
        self._datastoreRoot = datastoreRoot

    def fromUri(self, uri):
        """Factory function to create a `Location` from a URI.

        Parameters
        ----------
        uri : `str`
            A valid Universal Resource Identifier.

        Returns
        location : `Location`
            The equivalent `Location`.
        """
        return Location(self._datastoreRoot, uri)

    def fromPath(self, path):
        """Factory function to create a `Location` from a POSIX path.

        Parameters
        ----------
        path : `str`
            A standard POSIX path, relative to the `Datastore` root.

        Returns
        location : `Location`
            The equivalent `Location`.
        """
        uri = urllib.parse.urljoin("file://", path)
        return self.fromUri(uri)
