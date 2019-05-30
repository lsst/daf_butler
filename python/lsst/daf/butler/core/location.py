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
from pathlib import Path

if os.sep == posixpath.sep:
    IS_POSIX = True
else:
    IS_POSIX = False


def os2posix(path):
    """Convert a local path description to a POSIX path description.

    Parameters
    ----------
    path : `str`
        Path using the local path separator.

    Returns
    -------
    posix : `str`
        Path using POSIX path separator
    """
    if IS_POSIX:
        return path

    # Split on OS path separator but have to restore root directory
    # Untested on Windows
    ospaths = path.split(os.sep)
    if ospaths[0] == "":
        ospaths[0] = Path(path).root

    return posixpath.join(*ospaths)


def posix2os(path):
    """Convert a POSIX path description to a local path description.

    Parameters
    ----------
    path : `str`
        Path using the POSIX path separator.

    Returns
    -------
    ospath : `str`
        Path using OS path separator
    """
    if IS_POSIX:
        return path

    # Have to restore the root directory after splitting
    posixpaths = path.split(posixpath.sep)
    if posixpaths[0] == "":
        posixpaths[0] = posixpath.sep
    return os.path.join(*posixpaths)


class Location:
    """Identifies a location within the `Datastore`.

    Parameters
    ----------
    datastoreRootUri : `urllib.parse.ParseResult`
        Base URI for this datastore, must include an absolute path.
    path : `str`
        Relative path within datastore.  Assumed to be using the local
        path separator if a ``file`` scheme is being used for the URI,
        else a POSIX separator.
    """

    __slots__ = ("_datastoreRootUri", "_path")

    def __init__(self, datastoreRootUri, path):
        if not isinstance(datastoreRootUri, urllib.parse.ParseResult):
            raise ValueError("Datastore root must be a URI ParseResult instance")

        if not posixpath.isabs(datastoreRootUri.path):
            raise ValueError(f"Supplied URI must be an absolute path (given {datastoreRootUri}).")

        if not datastoreRootUri.scheme:
            datastoreRootUri = datastoreRootUri._replace(scheme="file")

        self._datastoreRootUri = datastoreRootUri

        if self._datastoreRootUri.scheme == "file":
            pathModule = os.path
        else:
            pathModule = posixpath

        if pathModule.isabs(path):
            raise ValueError("Path within datastore must be relative not absolute")

        self._path = path

    def __str__(self):
        return self.uri

    @property
    def uri(self):
        """URI corresponding to fully-specified location in datastore.
        """
        uriPath = os2posix(self.path)
        return self._datastoreRootUri._replace(path=uriPath).geturl()

    @property
    def path(self):
        """Path corresponding to location.

        This path includes the root of the `Datastore`, but does not include
        non-path components of the root URI.  If a file URI scheme is being
        used the path will be returned with the local OS path separator.
        """
        if self._datastoreRootUri.scheme != "file":
            return posixpath.join(self._datastoreRootUri.path, self.pathInStore)
        else:
            return os.path.normpath(os.path.join(posix2os(self._datastoreRootUri.path), self.pathInStore))

    @property
    def pathInStore(self):
        """Path corresponding to location relative to `Datastore` root.

        Uses the same path separator as supplied to the object constructor.
        """
        return self._path

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

        path, _ = os.path.splitext(self.pathInStore)

        # Ensure that we have a leading "." on file extension (and we do not
        # try to modify the empty string)
        if ext and not ext.startswith("."):
            ext = "." + ext

        self._path = path + ext


class LocationFactory:
    """Factory for `Location` instances.

    The factory is constructed from the root location of the datastore.
    This location can be a path on the file system (absolute or relative)
    or as a URI.

    Parameters
    ----------
    datastoreRoot : `str`
        Root location of the `Datastore` either as a path in the local
        filesystem or as a URI.  File scheme URIs can be used. If a local
        filesystem path is used without URI scheme, it will be converted
        to an absolute path and any home directory indicators expanded.
        If a file scheme is used with a relative path, the path will
        be treated as a posixpath but then converted to an absolute path.
    """

    def __init__(self, datastoreRoot):
        parsed = urllib.parse.urlparse(datastoreRoot)
        parsed = self._fixupFileUri(parsed)
        self._datastoreRootUri = parsed

    @staticmethod
    def _fixupFileUri(parsed, root=None):
        """Fix up relative paths in file URI instances.

        Parameters
        ----------
        parsed : `~urllib.parse.ParseResult`
            The result from parsing a URI using `urllib.parse`.
        root : `str`, optional
            Path to use as root when converting relative to absolute.
            If `None`, it will be the current working directory.

        Returns
        -------
        modified : `~urllib.parse.ParseResult`
            Update result if a file URI is being handled.

        Notes
        -----
        If a file URI is supplied with a relative path, the returned result
        will be converted to an absolute path.  If the supplied result has
        no scheme defined, ``file`` scheme will be assumed and the path
        will be converted to an absolute path.

        Relative paths are explicitly not supported by RFC8089 but `urllib`
        does accept URIs of the form `file:relative/path.ext`, but they need
        to be turned into absolute paths before they can be used.
        """
        if not parsed.scheme or parsed.scheme == "file":

            if root is None:
                root = os.path.abspath(os.path.curdir)

            if not parsed.scheme:
                # if there was no scheme this is a local OS file path
                # which can support tilde expansion.
                expandedPath = os.path.expanduser(parsed.path)
            elif parsed.scheme == "file":
                # file URI implies POSIX path separators so split as posix,
                # then join as os, and convert to abspath. Do not handle
                # home directories since `file` scheme is explicitly documented
                # to not do tilde expansion.
                expandedPath = posix2os(parsed.path)
            else:
                raise RuntimeError("Unexpectedly got confused by URI scheme")

            if not os.path.isabs(expandedPath):
                expandedPath = os.path.join(root, expandedPath)

            # Recalculate the path by converting from os path to posixpath
            uriPath = os2posix(os.path.normpath(expandedPath))

            # ParseResult is a NamedTuple so _replace is standard API
            parsed = parsed._replace(path=uriPath, scheme="file")

        return parsed

    def __str__(self):
        return f"{self.__class__.__name__}@{self._datastoreRootUri.geturl()}"

    def fromPath(self, path):
        """Factory function to create a `Location` from a POSIX path.

        Parameters
        ----------
        path : `str`
            A standard POSIX path, relative to the `Datastore` root.

        Returns
        -------
        location : `Location`
            The equivalent `Location`.
        """
        if os.path.isabs(path):
            raise ValueError("LocationFactory path must be relative to datastore, not absolute.")
        return Location(self._datastoreRootUri, path)
