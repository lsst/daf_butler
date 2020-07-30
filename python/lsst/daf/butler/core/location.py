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

__all__ = ("Location", "LocationFactory")

import os
import os.path
import posixpath
import types

from typing import (
    Optional,
    Union,
)

from ._butlerUri import ButlerURI


class Location:
    """Identifies a location within the `Datastore`.

    Parameters
    ----------
    datastoreRootUri : `ButlerURI` or `str`
        Base URI for this datastore, must include an absolute path.
    path : `str`
        Relative path within datastore.  Assumed to be using the local
        path separator if a ``file`` scheme is being used for the URI,
        else a POSIX separator.
    """

    __slots__ = ("_datastoreRootUri", "_path")

    def __init__(self, datastoreRootUri: Union[ButlerURI, str], path: str):
        if isinstance(datastoreRootUri, str):
            datastoreRootUri = ButlerURI(datastoreRootUri, forceDirectory=True)
        elif not isinstance(datastoreRootUri, ButlerURI):
            raise ValueError("Datastore root must be a ButlerURI instance")

        if not posixpath.isabs(datastoreRootUri.path):
            raise ValueError(f"Supplied URI must be an absolute path (given {datastoreRootUri}).")

        self._datastoreRootUri = datastoreRootUri

        pathModule: types.ModuleType
        if not self._datastoreRootUri.scheme:
            pathModule = os.path
        else:
            pathModule = posixpath

        # mypy can not work out that these modules support isabs
        if pathModule.isabs(path):  # type: ignore
            raise ValueError("Path within datastore must be relative not absolute")

        self._path = path

    def __str__(self) -> str:
        return self.uri

    def __repr__(self) -> str:
        uri = self._datastoreRootUri.geturl()
        path = self._path
        return f"{self.__class__.__name__}({uri!r}, {path!r})"

    @property
    def uri(self) -> str:
        """URI string corresponding to fully-specified location in datastore.
        """
        return self._datastoreRootUri.join(self._path).geturl()

    @property
    def path(self) -> str:
        """Path corresponding to location.

        This path includes the root of the `Datastore`, but does not include
        non-path components of the root URI.  Paths will not include URI
        quoting. If a file URI scheme is being used the path will be returned
        with the local OS path separator.
        """
        # Create new full URI to location
        full = self._datastoreRootUri.join(self._path)
        try:
            return full.ospath
        except AttributeError:
            return full.unquoted_path

    @property
    def pathInStore(self) -> str:
        """Path corresponding to location relative to `Datastore` root.

        Uses the same path separator as supplied to the object constructor.
        """
        return self._path

    @property
    def netloc(self) -> str:
        """The URI network location."""
        return self._datastoreRootUri.netloc

    @property
    def relativeToPathRoot(self) -> str:
        """Returns the path component of the URI relative to the network
        location.

        Effectively, this is the path property with POSIX separator stripped
        from the left hand side of the path.  Will be unquoted.
        """
        full = self._datastoreRootUri.join(self._path)
        return full.relativeToPathRoot

    def updateExtension(self, ext: Optional[str]) -> None:
        """Update the file extension associated with this `Location`.

        All file extensions are replaced.

        Parameters
        ----------
        ext : `str`
            New extension. If an empty string is given any extension will
            be removed. If `None` is given there will be no change.
        """
        if ext is None:
            return

        # Get the extension and remove it from the path if one is found
        # .fits.gz counts as one extension do not use os.path.splitext
        current = self.getExtension()
        path = self.pathInStore
        if current:
            path = path[:-len(current)]

        # Ensure that we have a leading "." on file extension (and we do not
        # try to modify the empty string)
        if ext and not ext.startswith("."):
            ext = "." + ext

        self._path = path + ext

    def getExtension(self) -> str:
        """Return the file extension(s) associated with this location.

        Returns
        -------
        ext : `str`
            The file extension (including the ``.``). Can be empty string
            if there is no file extension. Will return all file extensions
            as a single extension such that ``file.fits.gz`` will return
            a value of ``.fits.gz``.
        """
        return ButlerURI(self.path).getExtension()


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

    def __init__(self, datastoreRoot: str):
        self._datastoreRootUri = ButlerURI(datastoreRoot, forceAbsolute=True,
                                           forceDirectory=True)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}@{self._datastoreRootUri}"

    @property
    def netloc(self) -> str:
        """Returns the network location of root location of the `Datastore`."""
        return self._datastoreRootUri.netloc

    def fromPath(self, path: str) -> Location:
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
