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

__all__ = ("ButlerURI",)

import contextlib
import os
import os.path
import shutil
import urllib
import pkg_resources
import posixpath
from pathlib import Path, PurePath, PurePosixPath
import requests
import tempfile
import copy
import logging
import re

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    cast,
    Iterator,
    Optional,
    Tuple,
    Type,
    Union,
)

from .utils import safeMakeDir

if TYPE_CHECKING:
    try:
        import boto3
    except ImportError:
        pass
    from .datastore import DatastoreTransaction


log = logging.getLogger(__name__)

# Determine if the path separator for the OS looks like POSIX
IS_POSIX = os.sep == posixpath.sep

# Root path for this operating system
OS_ROOT_PATH = Path().resolve().root

# Regex for looking for URI escapes
ESCAPES_RE = re.compile(r"%[A-F0-9]{2}")


def os2posix(ospath: str) -> str:
    """Convert a local path description to a POSIX path description.

    Parameters
    ----------
    ospath : `str`
        Path using the local path separator.

    Returns
    -------
    posix : `str`
        Path using POSIX path separator
    """
    if IS_POSIX:
        return ospath

    posix = PurePath(ospath).as_posix()

    # PurePath strips trailing "/" from paths such that you can no
    # longer tell if a path is meant to be referring to a directory
    # Try to fix this.
    if ospath.endswith(os.sep) and not posix.endswith(posixpath.sep):
        posix += posixpath.sep

    return posix


def posix2os(posix: Union[PurePath, str]) -> str:
    """Convert a POSIX path description to a local path description.

    Parameters
    ----------
    posix : `str`, `PurePath`
        Path using the POSIX path separator.

    Returns
    -------
    ospath : `str`
        Path using OS path separator
    """
    if IS_POSIX:
        return str(posix)

    posixPath = PurePosixPath(posix)
    paths = list(posixPath.parts)

    # Have to convert the root directory after splitting
    if paths[0] == posixPath.root:
        paths[0] = OS_ROOT_PATH

    # Trailing "/" is stripped so we need to add back an empty path
    # for consistency
    if str(posix).endswith(posixpath.sep):
        paths.append("")

    return os.path.join(*paths)


class NoTransaction:
    """A simple emulation of the `DatastoreTransaction` class.

    Does nothing.
    """

    def __init__(self) -> None:
        return

    @contextlib.contextmanager
    def undoWith(self, name: str, undoFunc: Callable, *args: Any, **kwargs: Any) -> Iterator[None]:
        """No-op context manager to replace `DatastoreTransaction`
        """
        yield None


class ButlerURI:
    """Convenience wrapper around URI parsers.

    Provides access to URI components and can convert file
    paths into absolute path URIs. Scheme-less URIs are treated as if
    they are local file system paths and are converted to absolute URIs.

    A specialist subclass is created for each supported URI scheme.

    Parameters
    ----------
    uri : `str` or `urllib.parse.ParseResult`
        URI in string form.  Can be scheme-less if referring to a local
        filesystem path.
    root : `str`, optional
        When fixing up a relative path in a ``file`` scheme or if scheme-less,
        use this as the root. Must be absolute.  If `None` the current
        working directory will be used.
    forceAbsolute : `bool`, optional
        If `True`, scheme-less relative URI will be converted to an absolute
        path using a ``file`` scheme. If `False` scheme-less URI will remain
        scheme-less and will not be updated to ``file`` or absolute path.
    forceDirectory: `bool`, optional
        If `True` forces the URI to end with a separator, otherwise given URI
        is interpreted as is.
    """

    _pathLib: Type[PurePath] = PurePosixPath
    """Path library to use for this scheme."""

    _pathModule = posixpath
    """Path module to use for this scheme."""

    transferModes: Tuple[str, ...] = ("copy", "auto", "move")
    """Transfer modes supported by this implementation.

    Move is special in that it is generally a copy followed by an unlink.
    Whether that unlink works depends critically on whether the source URI
    implements unlink. If it does not the move will be reported as a failure.
    """

    transferDefault: str = "copy"
    """Default mode to use for transferring if ``auto`` is specified."""

    quotePaths = True
    """True if path-like elements modifying a URI should be quoted.

    All non-schemeless URIs have to internally use quoted paths. Therefore
    if a new file name is given (e.g. to updateFile or join) a decision must
    be made whether to quote it to be consistent.
    """

    # This is not an ABC with abstract methods because the __new__ being
    # a factory confuses mypy such that it assumes that every constructor
    # returns a ButlerURI and then determines that all the abstract methods
    # are still abstract. If they are not marked abstract but just raise
    # mypy is fine with it.

    # mypy is confused without this
    _uri: urllib.parse.ParseResult

    def __new__(cls, uri: Union[str, urllib.parse.ParseResult, ButlerURI],
                root: Optional[str] = None, forceAbsolute: bool = True,
                forceDirectory: bool = False) -> ButlerURI:
        parsed: urllib.parse.ParseResult
        dirLike: bool
        subclass: Optional[Type] = None

        # Record if we need to post process the URI components
        # or if the instance is already fully configured
        if isinstance(uri, str):
            # Since local file names can have special characters in them
            # we need to quote them for the parser but we can unquote
            # later. Assume that all other URI schemes are quoted.
            # Since sometimes people write file:/a/b and not file:///a/b
            # we should not quote in the explicit case of file:
            if "://" not in uri and not uri.startswith("file:"):
                if ESCAPES_RE.search(uri):
                    log.warning("Possible double encoding of %s", uri)
                else:
                    uri = urllib.parse.quote(uri)
            parsed = urllib.parse.urlparse(uri)
        elif isinstance(uri, urllib.parse.ParseResult):
            parsed = copy.copy(uri)
        elif isinstance(uri, ButlerURI):
            parsed = copy.copy(uri._uri)
            dirLike = uri.dirLike
            # No further parsing required and we know the subclass
            subclass = type(uri)
        else:
            raise ValueError(f"Supplied URI must be string, ButlerURI, or ParseResult but got '{uri!r}'")

        if subclass is None:
            # Work out the subclass from the URI scheme
            if not parsed.scheme:
                subclass = ButlerSchemelessURI
            elif parsed.scheme == "file":
                subclass = ButlerFileURI
            elif parsed.scheme == "s3":
                subclass = ButlerS3URI
            elif parsed.scheme.startswith("http"):
                subclass = ButlerHttpURI
            elif parsed.scheme == "resource":
                # Rules for scheme names disasllow pkg_resource
                subclass = ButlerPackageResourceURI
            elif parsed.scheme == "mem":
                # in-memory datastore object
                subclass = ButlerInMemoryURI
            else:
                raise NotImplementedError(f"No URI support for scheme: '{parsed.scheme}'"
                                          " in {parsed.geturl()}")

            parsed, dirLike = subclass._fixupPathUri(parsed, root=root,
                                                     forceAbsolute=forceAbsolute,
                                                     forceDirectory=forceDirectory)

            # It is possible for the class to change from schemeless
            # to file so handle that
            if parsed.scheme == "file":
                subclass = ButlerFileURI

        # Now create an instance of the correct subclass and set the
        # attributes directly
        self = object.__new__(subclass)
        self._uri = parsed
        self.dirLike = dirLike
        return self

    @property
    def scheme(self) -> str:
        """The URI scheme (``://`` is not part of the scheme)."""
        return self._uri.scheme

    @property
    def netloc(self) -> str:
        """The URI network location."""
        return self._uri.netloc

    @property
    def path(self) -> str:
        """The path component of the URI."""
        return self._uri.path

    @property
    def unquoted_path(self) -> str:
        """The path component of the URI with any URI quoting reversed."""
        return urllib.parse.unquote(self._uri.path)

    @property
    def ospath(self) -> str:
        """Path component of the URI localized to current OS."""
        raise AttributeError(f"Non-file URI ({self}) has no local OS path.")

    @property
    def relativeToPathRoot(self) -> str:
        """Returns path relative to network location.

        Effectively, this is the path property with posix separator stripped
        from the left hand side of the path.

        Always unquotes.
        """
        p = self._pathLib(self.path)
        relToRoot = str(p.relative_to(p.root))
        if self.dirLike and not relToRoot.endswith("/"):
            relToRoot += "/"
        return urllib.parse.unquote(relToRoot)

    @property
    def fragment(self) -> str:
        """The fragment component of the URI."""
        return self._uri.fragment

    @property
    def params(self) -> str:
        """Any parameters included in the URI."""
        return self._uri.params

    @property
    def query(self) -> str:
        """Any query strings included in the URI."""
        return self._uri.query

    def geturl(self) -> str:
        """Return the URI in string form.

        Returns
        -------
        url : `str`
            String form of URI.
        """
        return self._uri.geturl()

    def split(self) -> Tuple[ButlerURI, str]:
        """Splits URI into head and tail. Equivalent to os.path.split where
        head preserves the URI components.

        Returns
        -------
        head: `ButlerURI`
            Everything leading up to tail, expanded and normalized as per
            ButlerURI rules.
        tail : `str`
            Last `self.path` component. Tail will be empty if path ends on a
            separator. Tail will never contain separators. It will be
            unquoted.
        """
        head, tail = self._pathModule.split(self.path)
        headuri = self._uri._replace(path=head)

        # The file part should never include quoted metacharacters
        tail = urllib.parse.unquote(tail)

        # Schemeless is special in that it can be a relative path
        # We need to ensure that it stays that way. All other URIs will
        # be absolute already.
        forceAbsolute = self._pathModule.isabs(self.path)
        return ButlerURI(headuri, forceDirectory=True, forceAbsolute=forceAbsolute), tail

    def basename(self) -> str:
        """Returns the base name, last element of path, of the URI. If URI ends
        on a slash returns an empty string. This is the second element returned
        by split().

        Equivalent of os.path.basename().

        Returns
        -------
        tail : `str`
            Last part of the path attribute. Trail will be empty if path ends
            on a separator.
        """
        return self.split()[1]

    def dirname(self) -> ButlerURI:
        """Returns a ButlerURI containing all the directories of the path
        attribute.

        Equivalent of os.path.dirname()

        Returns
        -------
        head : `ButlerURI`
            Everything except the tail of path attribute, expanded and
            normalized as per ButlerURI rules.
        """
        return self.split()[0]

    def replace(self, **kwargs: Any) -> ButlerURI:
        """Replace components in a URI with new values and return a new
        instance.

        Returns
        -------
        new : `ButlerURI`
            New `ButlerURI` object with updated values.
        """
        return self.__class__(self._uri._replace(**kwargs))

    def updateFile(self, newfile: str) -> None:
        """Update in place the final component of the path with the supplied
        file name.

        Parameters
        ----------
        newfile : `str`
            File name with no path component.

        Notes
        -----
        Updates the URI in place.
        Updates the ButlerURI.dirLike attribute. The new file path will
        be quoted if necessary.
        """
        if self.quotePaths:
            newfile = urllib.parse.quote(newfile)
        dir, _ = self._pathModule.split(self.path)
        newpath = self._pathModule.join(dir, newfile)

        self.dirLike = False
        self._uri = self._uri._replace(path=newpath)

    def getExtension(self) -> str:
        """Return the file extension(s) associated with this URI path.

        Returns
        -------
        ext : `str`
            The file extension (including the ``.``). Can be empty string
            if there is no file extension. Will return all file extensions
            as a single extension such that ``file.fits.gz`` will return
            a value of ``.fits.gz``.
        """
        extensions = self._pathLib(self.path).suffixes
        return "".join(extensions)

    def join(self, path: str) -> ButlerURI:
        """Create a new `ButlerURI` with additional path components including
        a file.

        Parameters
        ----------
        path : `str`
            Additional file components to append to the current URI. Assumed
            to include a file at the end. Will be quoted depending on the
            associated URI scheme.

        Returns
        -------
        new : `ButlerURI`
            New URI with any file at the end replaced with the new path
            components.

        Notes
        -----
        Schemeless URIs assume local path separator but all other URIs assume
        POSIX separator if the supplied path has directory structure. It
        may be this never becomes a problem but datastore templates assume
        POSIX separator is being used.
        """
        new = self.dirname()  # By definition a directory URI

        # new should be asked about quoting, not self, since dirname can
        # change the URI scheme for schemeless -> file
        if new.quotePaths:
            path = urllib.parse.quote(path)

        newpath = self._pathModule.normpath(self._pathModule.join(new.path, path))
        new._uri = new._uri._replace(path=newpath)
        # Declare the new URI not be dirLike unless path ended in /
        if not path.endswith(self._pathModule.sep):
            new.dirLike = False
        return new

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
            Scheme and netloc must match.
        """
        if self.scheme != other.scheme or self.netloc != other.netloc:
            return None

        enclosed_path = self._pathLib(self.relativeToPathRoot)
        parent_path = other.relativeToPathRoot
        subpath: Optional[str]
        try:
            subpath = str(enclosed_path.relative_to(parent_path))
        except ValueError:
            subpath = None
        else:
            subpath = urllib.parse.unquote(subpath)
        return subpath

    def exists(self) -> bool:
        """Indicate that the resource is available.

        Returns
        -------
        exists : `bool`
            `True` if the resource exists.
        """
        raise NotImplementedError()

    def remove(self) -> None:
        """Remove the resource."""
        raise NotImplementedError()

    def isabs(self) -> bool:
        """Indicate that the resource is fully specified.

        For non-schemeless URIs this is always true.

        Returns
        -------
        isabs : `bool`
            `True` in all cases except schemeless URI.
        """
        return True

    def as_local(self) -> Tuple[str, bool]:
        """Return the location of the (possibly remote) resource in the
        local file system.

        Returns
        -------
        path : `str`
            If this is a remote resource, it will be a copy of the resource
            on the local file system, probably in a temporary directory.
            For a local resource this should be the actual path to the
            resource.
        is_temporary : `bool`
            Indicates if the local path is a temporary file or not.
        """
        raise NotImplementedError()

    def read(self, size: int = -1) -> bytes:
        """Open the resource and return the contents in bytes.

        Parameters
        ----------
        size : `int`, optional
            The number of bytes to read. Negative or omitted indicates
            that all data should be read.
        """
        raise NotImplementedError()

    def write(self, data: bytes, overwrite: bool = True) -> None:
        """Write the supplied bytes to the new resource.

        Parameters
        ----------
        data : `bytes`
            The bytes to write to the resource. The entire contents of the
            resource will be replaced.
        overwrite : `bool`, optional
            If `True` the resource will be overwritten if it exists. Otherwise
            the write will fail.
        """
        raise NotImplementedError()

    def mkdir(self) -> None:
        """For a dir-like URI, create the directory resource if it does not
        already exist.
        """
        raise NotImplementedError()

    def __str__(self) -> str:
        return self.geturl()

    def __repr__(self) -> str:
        return f'ButlerURI("{self.geturl()}")'

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ButlerURI):
            return False
        return self.geturl() == other.geturl()

    def __copy__(self) -> ButlerURI:
        # Implement here because the __new__ method confuses things
        return type(self)(str(self))

    def __deepcopy__(self, memo: Any) -> ButlerURI:
        # Implement here because the __new__ method confuses things
        return self.__copy__()

    def __getnewargs__(self) -> Tuple:
        return (str(self),)

    @staticmethod
    def _fixupPathUri(parsed: urllib.parse.ParseResult, root: Optional[str] = None,
                      forceAbsolute: bool = False,
                      forceDirectory: bool = False) -> Tuple[urllib.parse.ParseResult, bool]:
        """Correct any issues with the supplied URI.

        Parameters
        ----------
        parsed : `~urllib.parse.ParseResult`
            The result from parsing a URI using `urllib.parse`.
        root : `str`, ignored
            Not used by the this implementation since all URIs are
            absolute except for those representing the local file system.
        forceAbsolute : `bool`, ignored.
            Not used by this implementation. URIs are generally always
            absolute.
        forceDirectory : `bool`, optional
            If `True` forces the URI to end with a separator, otherwise given
            URI is interpreted as is. Specifying that the URI is conceptually
            equivalent to a directory can break some ambiguities when
            interpreting the last element of a path.

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

        AWS S3 differentiates between keys with trailing POSIX separators (i.e
        `/dir` and `/dir/`) whereas POSIX does not neccessarily.

        Scheme-less paths are normalized.
        """
        # assume we are not dealing with a directory like URI
        dirLike = False

        # URI is dir-like if explicitly stated or if it ends on a separator
        endsOnSep = parsed.path.endswith(posixpath.sep)
        if forceDirectory or endsOnSep:
            dirLike = True
            # only add the separator if it's not already there
            if not endsOnSep:
                parsed = parsed._replace(path=parsed.path+posixpath.sep)

        return parsed, dirLike

    def transfer_from(self, src: ButlerURI, transfer: str,
                      overwrite: bool = False,
                      transaction: Optional[Union[DatastoreTransaction, NoTransaction]] = None) -> None:
        """Transfer the current resource to a new location.

        Parameters
        ----------
        src : `ButlerURI`
            Source URI.
        transfer : `str`
            Mode to use for transferring the resource. Generically there are
            many standard options: copy, link, symlink, hardlink, relsymlink.
            Not all URIs support all modes.
        overwrite : `bool`, optional
            Allow an existing file to be overwritten. Defaults to `False`.
        transaction : `DatastoreTransaction`, optional
            A transaction object that can (depending on implementation)
            rollback transfers on error.  Not guaranteed to be implemented.

        Notes
        -----
        Conceptually this is hard to scale as the number of URI schemes
        grow.  The destination URI is more important than the source URI
        since that is where all the transfer modes are relevant (with the
        complication that "move" deletes the source).

        Local file to local file is the fundamental use case but every
        other scheme has to support "copy" to local file (with implicit
        support for "move") and copy from local file.
        All the "link" options tend to be specific to local file systems.

        "move" is a "copy" where the remote resource is deleted at the end.
        Whether this works depends on the source URI rather than the
        destination URI.  Reverting a move on transaction rollback is
        expected to be problematic if a remote resource was involved.
        """
        raise NotImplementedError(f"No transfer modes supported by URI scheme {self.scheme}")


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
    def _fixupPathUri(parsed: urllib.parse.ParseResult, root: Optional[str] = None,
                      forceAbsolute: bool = False,
                      forceDirectory: bool = False) -> Tuple[urllib.parse.ParseResult, bool]:
        """Fix up relative paths in URI instances.

        Parameters
        ----------
        parsed : `~urllib.parse.ParseResult`
            The result from parsing a URI using `urllib.parse`.
        root : `str`, optional
            Path to use as root when converting relative to absolute.
            If `None`, it will be the current working directory. This
            is a local file system path, not a URI.  It is only used if
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


class ButlerS3URI(ButlerURI):
    """S3 URI"""

    @property
    def client(self) -> boto3.client:
        """Client object to address remote resource."""
        # Defer import for circular dependencies
        from .s3utils import getS3Client
        return getS3Client()

    def exists(self) -> bool:
        # s3utils itself imports ButlerURI so defer this import
        from .s3utils import s3CheckFileExists
        exists, _ = s3CheckFileExists(self, client=self.client)
        return exists

    def remove(self) -> None:
        """Remove the resource."""

        # https://github.com/boto/boto3/issues/507 - there is no
        # way of knowing if the file was actually deleted except
        # for checking all the keys again, reponse is  HTTP 204 OK
        # response all the time
        self.client.delete(Bucket=self.netloc, Key=self.relativeToPathRoot)

    def read(self, size: int = -1) -> bytes:
        args = {}
        if size > 0:
            args["Range"] = f"bytes=0-{size-1}"
        try:
            response = self.client.get_object(Bucket=self.netloc,
                                              Key=self.relativeToPathRoot,
                                              **args)
        except (self.client.exceptions.NoSuchKey, self.client.exceptions.NoSuchBucket) as err:
            raise FileNotFoundError(f"No such resource: {self}") from err
        body = response["Body"].read()
        response["Body"].close()
        return body

    def write(self, data: bytes, overwrite: bool = True) -> None:
        if not overwrite:
            if self.exists():
                raise FileExistsError(f"Remote resource {self} exists and overwrite has been disabled")
        self.client.put_object(Bucket=self.netloc, Key=self.relativeToPathRoot,
                               Body=data)

    def mkdir(self) -> None:
        # Defer import for circular dependencies
        from .s3utils import bucketExists
        if not bucketExists(self.netloc):
            raise ValueError(f"Bucket {self.netloc} does not exist for {self}!")

        if not self.dirLike:
            raise ValueError("Can not create a 'directory' for file-like URI {self}")

        # don't create S3 key when root is at the top-level of an Bucket
        if not self.path == "/":
            self.client.put_object(Bucket=self.netloc, Key=self.relativeToPathRoot)

    def as_local(self) -> Tuple[str, bool]:
        """Download object from S3 and place in temporary directory.

        Returns
        -------
        path : `str`
            Path to local temporary file.
        temporary : `bool`
            Always returns `True`. This is always a temporary file.
        """
        with tempfile.NamedTemporaryFile(suffix=self.getExtension(), delete=False) as tmpFile:
            self.client.download_fileobj(self.netloc, self.relativeToPathRoot, tmpFile)
        return tmpFile.name, True

    def transfer_from(self, src: ButlerURI, transfer: str = "copy",
                      overwrite: bool = False,
                      transaction: Optional[Union[DatastoreTransaction, NoTransaction]] = None) -> None:
        """Transfer the current resource to an S3 bucket.

        Parameters
        ----------
        src : `ButlerURI`
            Source URI.
        transfer : `str`
            Mode to use for transferring the resource. Supports the following
            options: copy.
        overwrite : `bool`, optional
            Allow an existing file to be overwritten. Defaults to `False`.
        transaction : `DatastoreTransaction`, optional
            Currently unused.
        """
        # Fail early to prevent delays if remote resources are requested
        if transfer not in self.transferModes:
            raise ValueError(f"Transfer mode '{transfer}' not supported by URI scheme {self.scheme}")

        log.debug(f"Transferring {src} [exists: {src.exists()}] -> "
                  f"{self} [exists: {self.exists()}] (transfer={transfer})")

        if not overwrite and self.exists():
            raise FileExistsError(f"Destination path '{self}' already exists.")

        if transfer == "auto":
            transfer = self.transferDefault

        if isinstance(src, type(self)):
            # Looks like an S3 remote uri so we can use direct copy
            # note that boto3.resource.meta.copy is cleverer than the low
            # level copy_object
            copy_source = {
                "Bucket": src.netloc,
                "Key": src.relativeToPathRoot,
            }
            self.client.copy_object(CopySource=copy_source, Bucket=self.netloc, Key=self.relativeToPathRoot)
        else:
            # Use local file and upload it
            local_src, is_temporary = src.as_local()

            # resource.meta.upload_file seems like the right thing
            # but we have a low level client
            with open(local_src, "rb") as fh:
                self.client.put_object(Bucket=self.netloc,
                                       Key=self.relativeToPathRoot, Body=fh)
            if is_temporary:
                os.remove(local_src)

        # This was an explicit move requested from a remote resource
        # try to remove that resource
        if transfer == "move":
            # Transactions do not work here
            src.remove()


class ButlerPackageResourceURI(ButlerURI):
    """URI referring to a Python package resource.

    These URIs look like: ``resource://lsst.daf.butler/configs/file.yaml``
    where the network location is the Python package and the path is the
    resource name.
    """

    def exists(self) -> bool:
        """Check that the python resource exists."""
        return pkg_resources.resource_exists(self.netloc, self.relativeToPathRoot)

    def read(self, size: int = -1) -> bytes:
        with pkg_resources.resource_stream(self.netloc, self.relativeToPathRoot) as fh:
            return fh.read(size)


class ButlerHttpURI(ButlerURI):
    """General HTTP(S) resource."""

    @property
    def session(self) -> requests.Session:
        """Client object to address remote resource."""
        from .webdavutils import getHttpSession, isWebdavEndpoint
        if isWebdavEndpoint(self):
            log.debug("%s looks like a Webdav endpoint.", self.geturl())
            return getHttpSession()
        
        log.debug("%s looks like a standard HTTP endpoint.", self.geturl())
        return requests.Session()

    def exists(self) -> bool:
        """Check that a remote HTTP resource exists."""
        log.debug("Checking if resource exists: %s", self.geturl())
        r = self.session.head(self.geturl())

        return True if r.status_code == 200 else False

    def mkdir(self) -> None:

        if not self.exists():
            log.debug("Creating new directory: %s", self.geturl())
            r = self.session.request('MKCOL', self.geturl())
            if r.status_code != 201:
                raise ValueError(f"Can not create directory {self}, status code: {r.status_code}")

    def remove(self) -> None:
        """Remove the resource."""
        log.debug("Removing resource: %s", self.geturl())
        r = self.session.delete(self.geturl())
        if r.status_code not in [200, 202, 204]:
            raise FileNotFoundError(f"Unable to delete resource {self}; status code: {r.status_code}")

    def as_local(self) -> Tuple[str, bool]:
        """Download object over HTTP and place in temporary directory.

        Returns
        -------
        path : `str`
            Path to local temporary file.
        temporary : `bool`
            Always returns `True`. This is always a temporary file.
        """
        log.debug("Downloading remote resource as local file: %s", self.geturl())
        r = self.session.get(self.geturl(), stream=True)
        if r.status_code != 200:
            raise FileNotFoundError(f"Unable to download resource {self}; status code: {r.status_code}")
        with tempfile.NamedTemporaryFile(suffix=self.getExtension(), delete=False) as tmpFile:
            for chunk in r.iter_content():
                tmpFile.write(chunk)
        return tmpFile.name, True

    def read(self, size: int = -1) -> bytes:
        log.debug("Reading from remote resource: %s", self.geturl())
        stream = True if size > 0 else False
        r = self.session.get(self.geturl(), stream=stream)
        if not stream:
            return r.content
        else:
            return next(r.iter_content(chunk_size=size))

    def write(self, data: bytes, overwrite: bool = True) -> None:
        log.debug("Writing to remote resource: %s", self.geturl())
        if not overwrite:
            if self.exists():
                raise FileExistsError(f"Remote resource {self} exists and overwrite has been disabled")
        self.session.put(self.geturl(), data=data)

    def transfer_from(self, src: ButlerURI, transfer: str = "copy",
                      transaction: Optional[Union[DatastoreTransaction, NoTransaction]] = None) -> None:
        """Transfer the current resource to a Webdav repository.

        Parameters
        ----------
        src : `ButlerURI`
            Source URI.
        transfer : `str`
            Mode to use for transferring the resource. Supports the following
            options: copy.
        transaction : `DatastoreTransaction`, optional
            Currently unused.
        """
        # Fail early to prevent delays if remote resources are requested
        if transfer not in self.transferModes:
            raise ValueError(f"Transfer mode '{transfer}' not supported by URI scheme {self.scheme}")

        log.debug(f"Transferring {src} [exists: {src.exists()}] -> "
                  f"{self} [exists: {self.exists()}] (transfer={transfer})")

        if self.exists():
            raise FileExistsError(f"Destination path '{self}' already exists.")

        if transfer == "auto":
            transfer = self.transferDefault

        if isinstance(src, type(self)):
            if transfer == "move":
                self.session.request('MOVE', src.geturl(), headers={'Destination': self.geturl()})
            else:
                self.session.request('COPY', src.geturl(), headers={'Destination': self.geturl()})
        else:
            # Use local file and upload it
            local_src, is_temporary = src.as_local()
            files = {'file': open(local_src, 'rb')}
            self.session.post(self.geturl(), files=files)
            if is_temporary:
                os.remove(local_src)


class ButlerInMemoryURI(ButlerURI):
    """Internal in-memory datastore URI (`mem://`).

    Not used for any real purpose other than indicating that the dataset
    is in memory.
    """

    def exists(self) -> bool:
        """Test for existence and always return False."""
        return True

    def as_local(self) -> Tuple[str, bool]:
        raise RuntimeError(f"Do not know how to retrieve data for URI '{self}'")


class ButlerSchemelessURI(ButlerFileURI):
    """Scheme-less URI referring to the local file system"""

    _pathLib = PurePath
    _pathModule = os.path
    quotePaths = False

    @property
    def ospath(self) -> str:
        """Path component of the URI localized to current OS."""
        return self.path

    def isabs(self) -> bool:
        """Indicate that the resource is fully specified.

        For non-schemeless URIs this is always true.

        Returns
        -------
        isabs : `bool`
            `True` if the file is absolute, `False` otherwise.
        """
        return os.path.isabs(self.ospath)

    def _force_to_file(self) -> ButlerFileURI:
        """Force a schemeless URI to a file URI and returns a new URI.

        This will include URI quoting of the path.

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
        if not self.isabs():
            raise RuntimeError(f"Internal error: Can not force {self} to absolute file URI")
        uri = self._uri._replace(scheme="file", path=urllib.parse.quote(os2posix(self.path)))
        # mypy really wants a ButlerFileURI to be returned here
        return ButlerURI(uri, forceDirectory=self.dirLike)  # type: ignore

    @staticmethod
    def _fixupPathUri(parsed: urllib.parse.ParseResult, root: Optional[str] = None,
                      forceAbsolute: bool = False,
                      forceDirectory: bool = False) -> Tuple[urllib.parse.ParseResult, bool]:
        """Fix up relative paths for local file system.

        Parameters
        ----------
        parsed : `~urllib.parse.ParseResult`
            The result from parsing a URI using `urllib.parse`.
        root : `str`, optional
            Path to use as root when converting relative to absolute.
            If `None`, it will be the current working directory. This
            is a local file system path, not a URI.
        forceAbsolute : `bool`, optional
            If `True`, scheme-less relative URI will be converted to an
            absolute path using a ``file`` scheme. If `False` scheme-less URI
            will remain scheme-less and will not be updated to ``file`` or
            absolute path.
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

        Scheme-less paths are normalized.
        """
        # assume we are not dealing with a directory URI
        dirLike = False

        # Replacement values for the URI
        replacements = {}

        if root is None:
            root = os.path.abspath(os.path.curdir)

        # this is a local OS file path which can support tilde expansion.
        # we quoted it in the constructor so unquote here
        expandedPath = os.path.expanduser(urllib.parse.unquote(parsed.path))

        # Ensure that this becomes a file URI if it is already absolute
        if os.path.isabs(expandedPath):
            replacements["scheme"] = "file"
            # Keep in OS form for now to simplify later logic
            replacements["path"] = os.path.normpath(expandedPath)
        elif forceAbsolute:
            # This can stay in OS path form, do not change to file
            # scheme.
            replacements["path"] = os.path.normpath(os.path.join(root, expandedPath))
        else:
            # No change needed for relative local path staying relative
            # except normalization
            replacements["path"] = os.path.normpath(expandedPath)
            # normalization of empty path returns "." so we are dirLike
            if expandedPath == "":
                dirLike = True

        # normpath strips trailing "/" which makes it hard to keep
        # track of directory vs file when calling replaceFile

        # For local file system we can explicitly check to see if this
        # really is a directory. The URI might point to a location that
        # does not exists yet but all that matters is if it is a directory
        # then we make sure use that fact. No need to do the check if
        # we are already being told.
        if not forceDirectory and os.path.isdir(replacements["path"]):
            forceDirectory = True

        # add the trailing separator only if explicitly required or
        # if it was stripped by normpath. Acknowledge that trailing
        # separator exists.
        endsOnSep = expandedPath.endswith(os.sep) and not replacements["path"].endswith(os.sep)
        if (forceDirectory or endsOnSep or dirLike):
            dirLike = True
            if not replacements["path"].endswith(os.sep):
                replacements["path"] += os.sep

        if "scheme" in replacements:
            # This is now meant to be a URI path so force to posix
            # and quote
            replacements["path"] = urllib.parse.quote(os2posix(replacements["path"]))

        # ParseResult is a NamedTuple so _replace is standard API
        parsed = parsed._replace(**replacements)

        if parsed.params or parsed.fragment or parsed.query:
            log.warning("Additional items unexpectedly encountered in schemeless URI: %s", parsed.geturl())

        return parsed, dirLike
