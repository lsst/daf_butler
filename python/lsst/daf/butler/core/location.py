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

import os
import os.path
import urllib

__all__ = ("Location", "LocationFactory")


class Location(object):
    """Identifies a location in the `Datastore`.
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

    @property
    def component_path(self):
        """Path corresponding to location of component of composite.

        This is used in cases where a composite is broken into separate
        files, combining the root file path with the component name.
        When reading a file from the URI, it is possible that the file
        will not exist if the composite was written as a single entity
        and the URI fragment is referring to a location inside that entity.

        This path includes the root of the `Datastore`. Returns the empty
        string if no fragment has been specified as part of the URI.
        """
        if not self.fragment:
            return ''

        # We need to insert the fragment before the file extension because
        # some I/O libraries insist on a particular extension.
        filepath, extension = os.path.splitext(self.path)
        return "{}#{}{}".format(filepath, self.fragment, extension)

    @property
    def fragment(self):
        """URI fragment associated with this location.
        """
        return self._uri.fragment

    def paths(self):
        """Return path and component path.

        Returns
        -------
        path : `tuple`
            Tuple of the path and the component path.
            The component path can be an empty string.
        """
        return (self.path, self.component_path)

    def preferredPath(self):
        """Returns component path, if set, else the path.

        Returns
        -------
            Path for component in composite if set, else the default path.
        """
        return self.component_path if self.component_path else self.path

    def componentUri(self, componentName):
        """Returns URI of the named component."""
        parts = list(self._uri)
        parts[5] = componentName
        return urllib.parse.urlunparse(parts)

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


class LocationFactory(object):
    """Factory for `Location` instances.
    """

    def __init__(self, datastoreRoot):
        """Constructor

        Parameters
        ----------
        datastoreRoot : `str`
            Root location of the `Datastore` in the filesystem.
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
        if uri is None or not isinstance(uri, str):
            raise ValueError("URI must be a string and not {}".format(uri))
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
