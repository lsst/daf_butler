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

from typing import (
    Optional,
    Union,
)

from ._butlerUri import ButlerURI


class Location:
    """Identifies a location within the `Datastore`.

    Parameters
    ----------
    datastoreRootUri : `ButlerURI` or `str` or `None`
        Base URI for this datastore, must include an absolute path.
        If `None` the `path` must correspond to an absolute URI.
    path : `ButlerURI` or `str`
        Relative path within datastore.  Assumed to be using the local
        path separator if a ``file`` scheme is being used for the URI,
        else a POSIX separator. Can be a full URI if the root URI is `None`.
        Can also be a schemeless URI if it refers to a relative path.
    """

    __slots__ = ("_datastoreRootUri", "_path", "_uri")

    def __init__(self, datastoreRootUri: Union[None, ButlerURI, str],
                 path: Union[ButlerURI, str]):
        # Be careful not to force a relative local path to absolute path
        path_uri = ButlerURI(path, forceAbsolute=False)

        if isinstance(datastoreRootUri, str):
            datastoreRootUri = ButlerURI(datastoreRootUri, forceDirectory=True)
        elif datastoreRootUri is None:
            if not path_uri.isabs():
                raise ValueError(f"No datastore root URI given but path '{path}' was not absolute URI.")
        elif not isinstance(datastoreRootUri, ButlerURI):
            raise ValueError("Datastore root must be a ButlerURI instance")

        if datastoreRootUri is not None and not datastoreRootUri.isabs():
            raise ValueError(f"Supplied root URI must be an absolute path (given {datastoreRootUri}).")

        self._datastoreRootUri = datastoreRootUri

        # if the root URI is not None the path must not be absolute since
        # it is required to be within the root.
        if datastoreRootUri is not None:
            if path_uri.isabs():
                raise ValueError(f"Path within datastore must be relative not absolute, got {path_uri}")

        self._path = path_uri

        # Internal cache of the full location as a ButlerURI
        self._uri: Optional[ButlerURI] = None

        # Check that the resulting URI is inside the datastore
        # This can go wrong if we were given ../dir as path
        if self._datastoreRootUri is not None:
            pathInStore = self.uri.relative_to(self._datastoreRootUri)
            if pathInStore is None:
                raise ValueError(f"Unexpectedly {path} jumps out of {self._datastoreRootUri}")

    def __str__(self) -> str:
        return str(self.uri)

    def __repr__(self) -> str:
        uri = self._datastoreRootUri
        path = self._path
        return f"{self.__class__.__name__}({uri!r}, {path.path!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Location):
            return NotImplemented
        # Compare the combined URI rather than how it is apportioned
        return self.uri == other.uri

    @property
    def uri(self) -> ButlerURI:
        """URI corresponding to fully-specified location in datastore.
        """
        if self._uri is None:
            root = self._datastoreRootUri
            if root is None:
                uri = self._path
            else:
                uri = root.join(self._path)
            self._uri = uri
        return self._uri

    @property
    def path(self) -> str:
        """Path corresponding to location.

        This path includes the root of the `Datastore`, but does not include
        non-path components of the root URI.  Paths will not include URI
        quoting. If a file URI scheme is being used the path will be returned
        with the local OS path separator.
        """
        full = self.uri
        try:
            return full.ospath
        except AttributeError:
            return full.unquoted_path

    @property
    def pathInStore(self) -> ButlerURI:
        """Path corresponding to location relative to `Datastore` root.

        Uses the same path separator as supplied to the object constructor.
        Can be an absolute URI if that is how the location was configured.
        """
        return self._path

    @property
    def netloc(self) -> str:
        """The URI network location."""
        return self.uri.netloc

    @property
    def relativeToPathRoot(self) -> str:
        """Returns the path component of the URI relative to the network
        location.

        Effectively, this is the path property with POSIX separator stripped
        from the left hand side of the path.  Will be unquoted.
        """
        return self.uri.relativeToPathRoot

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

        self._path = self._path.updatedExtension(ext)

        # Clear the URI cache so it can be recreated with the new path
        self._uri = None

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
        return self.uri.getExtension()


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

    def __init__(self, datastoreRoot: Union[ButlerURI, str]):
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
