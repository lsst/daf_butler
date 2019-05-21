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

__all__ = ("Location", "LocationFactory")

import os
import os.path
import urllib
import posixpath


class Location:
    """Identifies a location in the `Datastore`.
    """

    __slots__ = ("_datastoreRoot", "_uri")

    def __init__(self, datastoreRoot, uri):
        self._datastoreRoot = datastoreRoot
        self._uri = urllib.parse.urlparse(uri)

    def __str__(self):
        return self.uri

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
        return self._uri.path

    @property
    def pathInStore(self):
        """Path corresponding to location relative to `Datastore` root.
        """
        return self._uri.path.split(self._datastoreRoot)[-1].lstrip(os.sep)

    def updateExtension(self, ext):
        """Update the file extension associated with this `Location`.

        Parameters
        ----------
        ext : `str`
            New extension. If an empty string is given any extension will
            be removed. If `None` is given there will be no change.
        """
        if ext is None:
            return
        path, _ = os.path.splitext(self._uri.path)

        # Ensure that we have a leading "." on file extension (and we do not
        # try to modify the empty string)
        if ext and not ext.startswith("."):
            ext = "." + ext

        parts = list(self._uri)
        parts[2] = path + ext
        self._uri = urllib.parse.urlparse(urllib.parse.urlunparse(parts))


class LocationFactory:
    """Factory for `Location` instances.
    """

    def __init__(self, datastoreRoot):
        """Constructor

        Parameters
        ----------
        datastoreRoot : `str`
            Root location of the `Datastore` in the filesystem. Assumed
            relative to current directory unless absolute.
        """
        self._datastoreRoot = os.path.abspath(os.path.expanduser(datastoreRoot))

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
        if uri is None or not isinstance(uri, str):
            raise ValueError("URI must be a string and not {}".format(uri))

        parsed = urllib.parse.urlparse(uri)
        if parsed.scheme == 'file' and parsed.netloc:
            # joining with an empty string produces an '/' we don't want
            if not parsed.path:
                path = posixpath.join(self._datastoreRoot, parsed.netloc)
            else:
                path = posixpath.join(self._datastoreRoot, parsed.netloc,
                                      parsed.path.lstrip('/'))
            # scheme, netloc, path, query, fragment, user, pass, host, port
            uri = urllib.parse.urlunparse((parsed.scheme, '', path, '', '', parsed.fragment))
            parsed = urllib.parse.urlparse(uri)
        if posixpath.commonprefix([parsed.path, self._datastoreRoot]) != self._datastoreRoot:
            raise ValueError((f'URI {uri} does not share the same datastore root '
                              f'used by this Location factory: {self._datastoreRoot}.'))

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
        uri = 'file://' + path
        return self.fromUri(uri)
