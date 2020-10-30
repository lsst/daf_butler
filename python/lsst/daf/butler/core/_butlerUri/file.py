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

import os
import os.path
import shutil
import urllib
import posixpath
import copy
import logging

__all__ = ('ButlerFileURI',)

from typing import (
    TYPE_CHECKING,
    cast,
    Optional,
    Tuple,
    Union,
)

from ..utils import safeMakeDir
from .utils import NoTransaction, os2posix, posix2os
from ._butlerUri import ButlerURI


if TYPE_CHECKING:
    from ..datastore import DatastoreTransaction


log = logging.getLogger(__name__)


class ButlerFileURI(ButlerURI):
    """URI for explicit ``file`` scheme."""

    transferModes = ("copy", "link", "symlink", "hardlink", "relsymlink", "auto", "move")
    transferDefault: str = "link"

    @property
    def ospath(self) -> str:
        """Path component of the URI localized to current OS.

        Will unquote URI path since a formal URI must include the quoting.
        """
        return urllib.parse.unquote(posix2os(self._uri.path))

    def exists(self) -> bool:
        # Uses os.path.exists so if there is a soft link that points
        # to a file that no longer exists this will return False
        return os.path.exists(self.ospath)

    def size(self) -> int:
        if not os.path.isdir(self.ospath):
            stat = os.stat(self.ospath)
            sz = stat.st_size
        else:
            sz = 0
        return sz

    def remove(self) -> None:
        """Remove the resource."""
        os.remove(self.ospath)

    def as_local(self) -> Tuple[str, bool]:
        """Return the local path of the file.

        Returns
        -------
        path : `str`
            The local path to this file.
        temporary : `bool`
            Always returns `False` (this is not a temporary file).
        """
        return self.ospath, False

    def _force_to_file(self) -> ButlerFileURI:
        """Force a schemeless URI to a file URI and returns a new URI.

        Returns
        -------
        file : `ButlerFileURI`
            A copy of the URI using file scheme. If already a file scheme
            the copy will be identical.

        Raises
        ------
        ValueError
            Raised if this URI is schemeless and relative path and so can
            not be forced to file absolute path without context.
        """
        # This is always a file scheme so always return copy
        return copy.copy(self)

    def relative_to(self, other: ButlerURI) -> Optional[str]:
        """Return the relative path from this URI to the other URI.

        Parameters
        ----------
        other : `ButlerURI`
            URI to use to calculate the relative path. Must be a parent
            of this URI.

        Returns
        -------
        subpath : `str`
            The sub path of this URI relative to the supplied other URI.
            Returns `None` if there is no parent child relationship.
            Scheme and netloc must match but for file URIs schemeless
            is also used. If this URI is a relative URI but the other is
            absolute, it is assumed to be in the parent completely unless it
            starts with ".." (in which case the path is combined and tested).
            If both URIs are relative, the relative paths are compared
            for commonality.

        Notes
        -----
        By definition a relative path will be relative to the enclosing
        absolute parent URI. It will be returned unchanged if it does not
        use a parent directory specification.
        """
        # We know self is a file so check the other. Anything other than
        # file or schemeless means by definition these have no paths in common
        if other.scheme and other.scheme != "file":
            return None

        # for case where both URIs are relative use the normal logic
        # where a/b/c.txt and a/b/ returns c.txt.
        if not self.isabs() and not other.isabs():
            return super().relative_to(other)

        # if we have a relative path convert it to absolute
        # relative to the supplied parent.  This is solely to handle
        # the case where the relative path includes ".." but somehow
        # then goes back inside the directory of the parent
        if not self.isabs():
            childUri = other.join(self.path)
            return childUri.relative_to(other)

        # By this point if the schemes are identical we can use the
        # base class implementation.
        if self.scheme == other.scheme:
            return super().relative_to(other)

        # if one is schemeless and the other is not the base implementation
        # will fail so we need to fix that -- they are both absolute so
        # forcing to file is fine.
        # Use a cast to convince mypy that other has to be a ButlerFileURI
        # in order to get to this part of the code.
        return self._force_to_file().relative_to(cast(ButlerFileURI, other)._force_to_file())

    def read(self, size: int = -1) -> bytes:
        # Docstring inherits
        with open(self.ospath, "rb") as fh:
            return fh.read(size)

    def write(self, data: bytes, overwrite: bool = True) -> None:
        dir = os.path.dirname(self.ospath)
        if not os.path.exists(dir):
            safeMakeDir(dir)
        if overwrite:
            mode = "wb"
        else:
            mode = "xb"
        with open(self.ospath, mode) as f:
            f.write(data)

    def mkdir(self) -> None:
        if not os.path.exists(self.ospath):
            safeMakeDir(self.ospath)
        elif not os.path.isdir(self.ospath):
            raise FileExistsError(f"URI {self} exists but is not a directory!")

    def transfer_from(self, src: ButlerURI, transfer: str,
                      overwrite: bool = False,
                      transaction: Optional[Union[DatastoreTransaction, NoTransaction]] = None) -> None:
        """Transfer the current resource to a local file.

        Parameters
        ----------
        src : `ButlerURI`
            Source URI.
        transfer : `str`
            Mode to use for transferring the resource. Supports the following
            options: copy, link, symlink, hardlink, relsymlink.
        overwrite : `bool`, optional
            Allow an existing file to be overwritten. Defaults to `False`.
        transaction : `DatastoreTransaction`, optional
            If a transaction is provided, undo actions will be registered.
        """
        # Fail early to prevent delays if remote resources are requested
        if transfer not in self.transferModes:
            raise ValueError(f"Transfer mode '{transfer}' not supported by URI scheme {self.scheme}")

        log.debug(f"Transferring {src} [exists: {src.exists()}] -> "
                  f"{self} [exists: {self.exists()}] (transfer={transfer})")

        # We do not have to special case ButlerFileURI here because
        # as_local handles that.
        local_src, is_temporary = src.as_local()

        # Default transfer mode depends on whether we have a temporary
        # file or not.
        if transfer == "auto":
            transfer = self.transferDefault if not is_temporary else "copy"

        # Follow soft links
        local_src = os.path.realpath(os.path.normpath(local_src))

        if not os.path.exists(local_src):
            raise FileNotFoundError(f"Source URI {src} does not exist")

        # All the modes involving linking use "link" somewhere
        if "link" in transfer and is_temporary:
            raise RuntimeError("Can not use local file system transfer mode"
                               f" {transfer} for remote resource ({src})")

        # For temporary files we can own them
        requested_transfer = transfer
        if is_temporary and transfer == "copy":
            transfer = "move"

        # The output location should not exist
        dest_exists = self.exists()
        if not overwrite and dest_exists:
            raise FileExistsError(f"Destination path '{self}' already exists. Transfer "
                                  f"from {src} cannot be completed.")

        # Make the path absolute (but don't follow links since that
        # would possibly cause us to end up in the wrong place if the
        # file existed already as a soft link)
        newFullPath = os.path.abspath(self.ospath)
        outputDir = os.path.dirname(newFullPath)
        if not os.path.isdir(outputDir):
            # Must create the directory -- this can not be rolled back
            # since another transfer running concurrently may
            # be relying on this existing.
            safeMakeDir(outputDir)

        if transaction is None:
            # Use a no-op transaction to reduce code duplication
            transaction = NoTransaction()

        # For links the OS doesn't let us overwrite so if something does
        # exist we have to remove it before we do the actual "transfer" below
        if "link" in transfer and overwrite and dest_exists:
            try:
                self.remove()
            except Exception:
                # If this fails we ignore it since it's a problem
                # that will manifest immediately below with a more relevant
                # error message
                pass

        if transfer == "move":
            with transaction.undoWith(f"move from {local_src}", shutil.move, newFullPath, local_src):
                shutil.move(local_src, newFullPath)
        elif transfer == "copy":
            with transaction.undoWith(f"copy from {local_src}", os.remove, newFullPath):
                shutil.copy(local_src, newFullPath)
        elif transfer == "link":
            # Try hard link and if that fails use a symlink
            with transaction.undoWith(f"link to {local_src}", os.remove, newFullPath):
                try:
                    os.link(local_src, newFullPath)
                except OSError:
                    # Read through existing symlinks
                    os.symlink(local_src, newFullPath)
        elif transfer == "hardlink":
            with transaction.undoWith(f"hardlink to {local_src}", os.remove, newFullPath):
                os.link(local_src, newFullPath)
        elif transfer == "symlink":
            # Read through existing symlinks
            with transaction.undoWith(f"symlink to {local_src}", os.remove, newFullPath):
                os.symlink(local_src, newFullPath)
        elif transfer == "relsymlink":
            # This is a standard symlink but using a relative path
            # Need the directory name to give to relative root
            # A full file path confuses it into an extra ../
            newFullPathRoot = os.path.dirname(newFullPath)
            relPath = os.path.relpath(local_src, newFullPathRoot)
            with transaction.undoWith(f"relsymlink to {local_src}", os.remove, newFullPath):
                os.symlink(relPath, newFullPath)
        else:
            raise NotImplementedError("Transfer type '{}' not supported.".format(transfer))

        # This was an explicit move requested from a remote resource
        # try to remove that resource. We check is_temporary because
        # the local file would have been moved by shutil.move already.
        if requested_transfer == "move" and is_temporary:
            # Transactions do not work here
            src.remove()

        if is_temporary and os.path.exists(local_src):
            # This should never happen since we have moved it above
            os.remove(local_src)

    @staticmethod
    def _fixupPathUri(parsed: urllib.parse.ParseResult, root: Optional[Union[str, ButlerURI]] = None,
                      forceAbsolute: bool = False,
                      forceDirectory: bool = False) -> Tuple[urllib.parse.ParseResult, bool]:
        """Fix up relative paths in URI instances.

        Parameters
        ----------
        parsed : `~urllib.parse.ParseResult`
            The result from parsing a URI using `urllib.parse`.
        root : `str` or `ButlerURI`, optional
            Path to use as root when converting relative to absolute.
            If `None`, it will be the current working directory. This
            is a local file system path, or a file URI.  It is only used if
            a file-scheme is used incorrectly with a relative path.
        forceAbsolute : `bool`, ignored
            Has no effect for this subclass. ``file`` URIs are always
            absolute.
        forceDirectory : `bool`, optional
            If `True` forces the URI to end with a separator, otherwise given
            URI is interpreted as is.

        Returns
        -------
        modified : `~urllib.parse.ParseResult`
            Update result if a URI is being handled.
        dirLike : `bool`
            `True` if given parsed URI has a trailing separator or
            forceDirectory is True. Otherwise `False`.

        Notes
        -----
        Relative paths are explicitly not supported by RFC8089 but `urllib`
        does accept URIs of the form ``file:relative/path.ext``. They need
        to be turned into absolute paths before they can be used.  This is
        always done regardless of the ``forceAbsolute`` parameter.
        """
        # assume we are not dealing with a directory like URI
        dirLike = False

        # file URI implies POSIX path separators so split as POSIX,
        # then join as os, and convert to abspath. Do not handle
        # home directories since "file" scheme is explicitly documented
        # to not do tilde expansion.
        sep = posixpath.sep

        # For local file system we can explicitly check to see if this
        # really is a directory. The URI might point to a location that
        # does not exists yet but all that matters is if it is a directory
        # then we make sure use that fact. No need to do the check if
        # we are already being told.
        if not forceDirectory and posixpath.isdir(parsed.path):
            forceDirectory = True

        # For an absolute path all we need to do is check if we need
        # to force the directory separator
        if posixpath.isabs(parsed.path):
            if forceDirectory:
                if not parsed.path.endswith(sep):
                    parsed = parsed._replace(path=parsed.path+sep)
                dirLike = True
            return copy.copy(parsed), dirLike

        # Relative path so must fix it to be compliant with the standard

        # Replacement values for the URI
        replacements = {}

        if root is None:
            root = os.path.abspath(os.path.curdir)
        elif isinstance(root, ButlerURI):
            if root.scheme and root.scheme != "file":
                raise RuntimeError(f"The override root must be a file URI not {root.scheme}")
            root = os.path.abspath(root.ospath)

        replacements["path"] = posixpath.normpath(posixpath.join(os2posix(root), parsed.path))

        # normpath strips trailing "/" so put it back if necessary
        # Acknowledge that trailing separator exists.
        if forceDirectory or (parsed.path.endswith(sep) and not replacements["path"].endswith(sep)):
            replacements["path"] += sep
            dirLike = True

        # ParseResult is a NamedTuple so _replace is standard API
        parsed = parsed._replace(**replacements)

        if parsed.params or parsed.query:
            log.warning("Additional items unexpectedly encountered in file URI: %s", parsed.geturl())

        return parsed, dirLike
