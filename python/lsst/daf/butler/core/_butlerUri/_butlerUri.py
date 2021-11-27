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

import contextlib
import concurrent.futures
import urllib.parse
import posixpath
import copy
import logging
import re
import shutil
import tempfile
import os

from random import Random
from pathlib import Path, PurePath, PurePosixPath

__all__ = ('ButlerURI',)

from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Iterator,
    List,
    Dict,
    Optional,
    Tuple,
    Type,
    Union,
)

from .utils import NoTransaction

if TYPE_CHECKING:
    from ..datastore import DatastoreTransaction


log = logging.getLogger(__name__)

# Regex for looking for URI escapes
ESCAPES_RE = re.compile(r"%[A-F0-9]{2}")

# Precomputed escaped hash
ESCAPED_HASH = urllib.parse.quote("#")

# Maximum number of worker threads for parallelized operations.
# If greater than 10, be aware that this number has to be consistent
# with connection pool sizing (for example in urllib3).
MAX_WORKERS = 10


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
    root : `str` or `ButlerURI`, optional
        When fixing up a relative path in a ``file`` scheme or if scheme-less,
        use this as the root. Must be absolute.  If `None` the current
        working directory will be used. Can be a file URI.
    forceAbsolute : `bool`, optional
        If `True`, scheme-less relative URI will be converted to an absolute
        path using a ``file`` scheme. If `False` scheme-less URI will remain
        scheme-less and will not be updated to ``file`` or absolute path.
    forceDirectory: `bool`, optional
        If `True` forces the URI to end with a separator, otherwise given URI
        is interpreted as is.
    isTemporary : `bool`, optional
        If `True` indicates that this URI points to a temporary resource.
        The default is `False`, unless ``uri`` is already a `ButlerURI`
        instance and ``uri.isTemporary is True``.
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
    if a new file name is given (e.g. to updatedFile or join) a decision must
    be made whether to quote it to be consistent.
    """

    isLocal = False
    """If `True` this URI refers to a local file."""

    # This is not an ABC with abstract methods because the __new__ being
    # a factory confuses mypy such that it assumes that every constructor
    # returns a ButlerURI and then determines that all the abstract methods
    # are still abstract. If they are not marked abstract but just raise
    # mypy is fine with it.

    # mypy is confused without these
    _uri: urllib.parse.ParseResult
    isTemporary: bool
    dirLike: bool

    def __new__(cls, uri: Union[str, urllib.parse.ParseResult, ButlerURI, Path],
                root: Optional[Union[str, ButlerURI]] = None, forceAbsolute: bool = True,
                forceDirectory: bool = False, isTemporary: Optional[bool] = None) -> ButlerURI:
        """Create and return new specialist ButlerURI subclass."""
        parsed: urllib.parse.ParseResult
        dirLike: bool = False
        subclass: Optional[Type[ButlerURI]] = None

        if isinstance(uri, os.PathLike):
            uri = str(uri)

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
                    # Special case hash since we must support fragments
                    # even in schemeless URIs -- although try to only replace
                    # them in file part and not directory part
                    if ESCAPED_HASH in uri:
                        dirpos = uri.rfind("/")
                        # Do replacement after this /
                        uri = uri[:dirpos+1] + uri[dirpos+1:].replace(ESCAPED_HASH, "#")

            parsed = urllib.parse.urlparse(uri)
        elif isinstance(uri, urllib.parse.ParseResult):
            parsed = copy.copy(uri)
            # If we are being instantiated with a subclass, rather than
            # ButlerURI, ensure that that subclass is used directly.
            # This could lead to inconsistencies if this constructor
            # is used externally outside of the ButlerURI.replace() method.
            #   ButlerS3URI(urllib.parse.urlparse("file://a/b.txt"))
            # will be a problem.
            # This is needed to prevent a schemeless absolute URI become
            # a file URI unexpectedly when calling updatedFile or
            # updatedExtension
            if cls is not ButlerURI:
                parsed, dirLike = cls._fixDirectorySep(parsed, forceDirectory)
                subclass = cls

        elif isinstance(uri, ButlerURI):
            # Since ButlerURI is immutable we can return the argument
            # unchanged if it already agrees with forceDirectory and
            # isTemporary.
            # It might be safe to use uri.replace to change these to match
            # the arguments, but that seems more likely to paper over logic
            # errors than do something useful, so we just raise.
            if forceDirectory and not uri.dirLike:
                raise RuntimeError(f"{uri} is already a file-like ButlerURI; cannot force it to directory.")
            if isTemporary is not None and isTemporary != uri.isTemporary:
                raise RuntimeError(
                    f"{uri} is already a {'temporary' if uri.isTemporary else 'permanent'} "
                    f"ButlerURI; cannot make it {'temporary' if isTemporary else 'permanent'}."
                )
            return uri
        else:
            raise ValueError("Supplied URI must be string, Path, "
                             f"ButlerURI, or ParseResult but got '{uri!r}'")

        if subclass is None:
            # Work out the subclass from the URI scheme
            if not parsed.scheme:
                from .schemeless import ButlerSchemelessURI
                subclass = ButlerSchemelessURI
            elif parsed.scheme == "file":
                from .file import ButlerFileURI
                subclass = ButlerFileURI
            elif parsed.scheme == "s3":
                from .s3 import ButlerS3URI
                subclass = ButlerS3URI
            elif parsed.scheme.startswith("http"):
                from .http import ButlerHttpURI
                subclass = ButlerHttpURI
            elif parsed.scheme == "resource":
                # Rules for scheme names disallow pkg_resource
                from .packageresource import ButlerPackageResourceURI
                subclass = ButlerPackageResourceURI
            elif parsed.scheme == "mem":
                # in-memory datastore object
                from .mem import ButlerInMemoryURI
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
                from .file import ButlerFileURI
                subclass = ButlerFileURI

        # Now create an instance of the correct subclass and set the
        # attributes directly
        self = object.__new__(subclass)
        self._uri = parsed
        self.dirLike = dirLike
        if isTemporary is None:
            isTemporary = False
        self.isTemporary = isTemporary
        return self

    @property
    def scheme(self) -> str:
        """Return the URI scheme.

        Notes
        -----
        (``://`` is not part of the scheme).
        """
        return self._uri.scheme

    @property
    def netloc(self) -> str:
        """Return the URI network location."""
        return self._uri.netloc

    @property
    def path(self) -> str:
        """Return the path component of the URI."""
        return self._uri.path

    @property
    def unquoted_path(self) -> str:
        """Return path component of the URI with any URI quoting reversed."""
        return urllib.parse.unquote(self._uri.path)

    @property
    def ospath(self) -> str:
        """Return the path component of the URI localized to current OS."""
        raise AttributeError(f"Non-file URI ({self}) has no local OS path.")

    @property
    def relativeToPathRoot(self) -> str:
        """Return path relative to network location.

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
    def is_root(self) -> bool:
        """Return whether this URI points to the root of the network location.

        This means that the path components refers to the top level.
        """
        relpath = self.relativeToPathRoot
        if relpath == "./":
            return True
        return False

    @property
    def fragment(self) -> str:
        """Return the fragment component of the URI."""
        return self._uri.fragment

    @property
    def params(self) -> str:
        """Return any parameters included in the URI."""
        return self._uri.params

    @property
    def query(self) -> str:
        """Return any query strings included in the URI."""
        return self._uri.query

    def geturl(self) -> str:
        """Return the URI in string form.

        Returns
        -------
        url : `str`
            String form of URI.
        """
        return self._uri.geturl()

    def root_uri(self) -> ButlerURI:
        """Return the base root URI.

        Returns
        -------
        uri : `ButlerURI`
            root URI.
        """
        return self.replace(path="", forceDirectory=True)

    def split(self) -> Tuple[ButlerURI, str]:
        """Split URI into head and tail.

        Returns
        -------
        head: `ButlerURI`
            Everything leading up to tail, expanded and normalized as per
            ButlerURI rules.
        tail : `str`
            Last `self.path` component. Tail will be empty if path ends on a
            separator. Tail will never contain separators. It will be
            unquoted.

        Notes
        -----
        Equivalent to `os.path.split()` where head preserves the URI
        components.
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
        """Return the base name, last element of path, of the URI.

        Returns
        -------
        tail : `str`
            Last part of the path attribute. Trail will be empty if path ends
            on a separator.

        Notes
        -----
        If URI ends on a slash returns an empty string. This is the second
        element returned by `split()`.

        Equivalent of `os.path.basename()``.
        """
        return self.split()[1]

    def dirname(self) -> ButlerURI:
        """Return the directory component of the path as a new `ButlerURI`.

        Returns
        -------
        head : `ButlerURI`
            Everything except the tail of path attribute, expanded and
            normalized as per ButlerURI rules.

        Notes
        -----
        Equivalent of `os.path.dirname()`.
        """
        return self.split()[0]

    def parent(self) -> ButlerURI:
        """Return a `ButlerURI` of the parent directory.

        Returns
        -------
        head : `ButlerURI`
            Everything except the tail of path attribute, expanded and
            normalized as per `ButlerURI` rules.

        Notes
        -----
        For a file-like URI this will be the same as calling `dirname()`.
        """
        # When self is file-like, return self.dirname()
        if not self.dirLike:
            return self.dirname()
        # When self is dir-like, return its parent directory,
        # regardless of the presence of a trailing separator
        originalPath = self._pathLib(self.path)
        parentPath = originalPath.parent
        return self.replace(path=str(parentPath), forceDirectory=True)

    def replace(self, forceDirectory: bool = False, isTemporary: bool = False, **kwargs: Any) -> ButlerURI:
        """Return new `ButlerURI` with specified components replaced.

        Parameters
        ----------
        forceDirectory : `bool`, optional
            Parameter passed to ButlerURI constructor to force this
            new URI to be dir-like.
        isTemporary : `bool`, optional
            Indicate that the resulting URI is temporary resource.
        **kwargs
            Components of a `urllib.parse.ParseResult` that should be
            modified for the newly-created `ButlerURI`.

        Returns
        -------
        new : `ButlerURI`
            New `ButlerURI` object with updated values.

        Notes
        -----
        Does not, for now, allow a change in URI scheme.
        """
        # Disallow a change in scheme
        if "scheme" in kwargs:
            raise ValueError(f"Can not use replace() method to change URI scheme for {self}")
        return self.__class__(self._uri._replace(**kwargs), forceDirectory=forceDirectory,
                              isTemporary=isTemporary)

    def updatedFile(self, newfile: str) -> ButlerURI:
        """Return new URI with an updated final component of the path.

        Parameters
        ----------
        newfile : `str`
            File name with no path component.

        Returns
        -------
        updated : `ButlerURI`

        Notes
        -----
        Forces the ButlerURI.dirLike attribute to be false. The new file path
        will be quoted if necessary.
        """
        if self.quotePaths:
            newfile = urllib.parse.quote(newfile)
        dir, _ = self._pathModule.split(self.path)
        newpath = self._pathModule.join(dir, newfile)

        updated = self.replace(path=newpath)
        updated.dirLike = False
        return updated

    def updatedExtension(self, ext: Optional[str]) -> ButlerURI:
        """Return a new `ButlerURI` with updated file extension.

        All file extensions are replaced.

        Parameters
        ----------
        ext : `str` or `None`
            New extension. If an empty string is given any extension will
            be removed. If `None` is given there will be no change.

        Returns
        -------
        updated : `ButlerURI`
            URI with the specified extension. Can return itself if
            no extension was specified.
        """
        if ext is None:
            return self

        # Get the extension
        current = self.getExtension()

        # Nothing to do if the extension already matches
        if current == ext:
            return self

        # Remove the current extension from the path
        # .fits.gz counts as one extension do not use os.path.splitext
        path = self.path
        if current:
            path = path[:-len(current)]

        # Ensure that we have a leading "." on file extension (and we do not
        # try to modify the empty string)
        if ext and not ext.startswith("."):
            ext = "." + ext

        return self.replace(path=path + ext)

    def getExtension(self) -> str:
        """Return the file extension(s) associated with this URI path.

        Returns
        -------
        ext : `str`
            The file extension (including the ``.``). Can be empty string
            if there is no file extension. Usually returns only the last
            file extension unless there is a special extension modifier
            indicating file compression, in which case the combined
            extension (e.g. ``.fits.gz``) will be returned.
        """
        special = {".gz", ".bz2", ".xz", ".fz"}

        # Get the file part of the path so as not to be confused by
        # "." in directory names.
        basename = self.basename()
        extensions = self._pathLib(basename).suffixes

        if not extensions:
            return ""

        ext = extensions.pop()

        # Multiple extensions, decide whether to include the final two
        if extensions and ext in special:
            ext = f"{extensions[-1]}{ext}"

        return ext

    def join(self, path: Union[str, ButlerURI], isTemporary: Optional[bool] = None,
             forceDirectory: bool = False) -> ButlerURI:
        """Return new `ButlerURI` with additional path components.

        Parameters
        ----------
        path : `str`, `ButlerURI`
            Additional file components to append to the current URI. Assumed
            to include a file at the end. Will be quoted depending on the
            associated URI scheme. If the path looks like a URI with a scheme
            referring to an absolute location, it will be returned
            directly (matching the behavior of `os.path.join()`). It can
            also be a `ButlerURI`.
        isTemporary : `bool`, optional
            Indicate that the resulting URI represents a temporary resource.
            Default is ``self.isTemporary``.
        forceDirectory : `bool`, optional
            If `True` forces the URI to end with a separator, otherwise given
            URI is interpreted as is.

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

        If an absolute `ButlerURI` is given for ``path`` is is assumed that
        this should be returned directly. Giving a ``path`` of an absolute
        scheme-less URI is not allowed for safety reasons as it may indicate
        a mistake in the calling code.

        Raises
        ------
        ValueError
            Raised if the ``path`` is an absolute scheme-less URI. In that
            situation it is unclear whether the intent is to return a
            ``file`` URI or it was a mistake and a relative scheme-less URI
            was meant.
        RuntimeError
            Raised this attempts to join a temporary URI to a non-temporary
            URI.
        """
        if isTemporary is None:
            isTemporary = self.isTemporary
        elif not isTemporary and self.isTemporary:
            raise RuntimeError("Cannot join temporary URI to non-temporary URI.")
        # If we have a full URI in path we will use it directly
        # but without forcing to absolute so that we can trap the
        # expected option of relative path.
        path_uri = ButlerURI(path, forceAbsolute=False, forceDirectory=forceDirectory,
                             isTemporary=isTemporary)
        if path_uri.scheme:
            # Check for scheme so can distinguish explicit URIs from
            # absolute scheme-less URIs.
            return path_uri

        if path_uri.isabs():
            # Absolute scheme-less path.
            raise ValueError(f"Can not join absolute scheme-less {path_uri!r} to another URI.")

        # If this was originally a ButlerURI extract the unquoted path from it.
        # Otherwise we use the string we were given to allow "#" to appear
        # in the filename if given as a plain string.
        if not isinstance(path, str):
            path = path_uri.unquoted_path

        new = self.dirname()  # By definition a directory URI

        # new should be asked about quoting, not self, since dirname can
        # change the URI scheme for schemeless -> file
        if new.quotePaths:
            path = urllib.parse.quote(path)

        newpath = self._pathModule.normpath(self._pathModule.join(new.path, path))

        # normpath can strip trailing / so we force directory if the supplied
        # path ended with a /
        return new.replace(path=newpath,
                           forceDirectory=(forceDirectory or path.endswith(self._pathModule.sep)),
                           isTemporary=isTemporary)

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
        # Scheme-less absolute other is treated as if it's a file scheme.
        # Scheme-less relative other can only return non-None if self
        # is also scheme-less relative and that is handled specifically
        # in a subclass.
        if not other.scheme and other.isabs():
            other = other.abspath()

        # Scheme-less self is handled elsewhere.
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

    @classmethod
    def mexists(cls, uris: Iterable[ButlerURI]) -> Dict[ButlerURI, bool]:
        """Check for existence of multiple URIs at once.

        Parameters
        ----------
        uris : iterable of `ButlerURI`
            The URIs to test.

        Returns
        -------
        existence : `dict` of [`ButlerURI`, `bool`]
            Mapping of original URI to boolean indicating existence.
        """
        exists_executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
        future_exists = {exists_executor.submit(uri.exists): uri for uri in uris}

        results: Dict[ButlerURI, bool] = {}
        for future in concurrent.futures.as_completed(future_exists):
            uri = future_exists[future]
            try:
                exists = future.result()
            except Exception:
                exists = False
            results[uri] = exists
        return results

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

    def abspath(self) -> ButlerURI:
        """Return URI using an absolute path.

        Returns
        -------
        abs : `ButlerURI`
            Absolute URI. For non-schemeless URIs this always returns itself.
            Schemeless URIs are upgraded to file URIs.
        """
        return self

    def _as_local(self) -> Tuple[str, bool]:
        """Return the location of the (possibly remote) resource as local file.

        This is a helper function for `as_local` context manager.

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

    @contextlib.contextmanager
    def as_local(self) -> Iterator[ButlerURI]:
        """Return the location of the (possibly remote) resource as local file.

        Yields
        ------
        local : `ButlerURI`
            If this is a remote resource, it will be a copy of the resource
            on the local file system, probably in a temporary directory.
            For a local resource this should be the actual path to the
            resource.

        Notes
        -----
        The context manager will automatically delete any local temporary
        file.

        Examples
        --------
        Should be used as a context manager:

        .. code-block:: py

           with uri.as_local() as local:
               ospath = local.ospath
        """
        if self.dirLike:
            raise TypeError(f"Directory-like URI {self} cannot be fetched as local.")
        local_src, is_temporary = self._as_local()
        local_uri = ButlerURI(local_src, isTemporary=is_temporary)

        try:
            yield local_uri
        finally:
            # The caller might have relocated the temporary file
            if is_temporary and local_uri.exists():
                local_uri.remove()

    @classmethod
    @contextlib.contextmanager
    def temporary_uri(cls, prefix: Optional[ButlerURI] = None,
                      suffix: Optional[str] = None) -> Iterator[ButlerURI]:
        """Create a temporary file-like URI.

        Parameters
        ----------
        prefix : `ButlerURI`, optional
            Prefix to use. Without this the path will be formed as a local
            file URI in a temporary directory. Ensuring that the prefix
            location exists is the responsibility of the caller.
        suffix : `str`, optional
            A file suffix to be used. The ``.`` should be included in this
            suffix.  Passing a string that ends with ``/`` causes a
            directory-like to be returned even if ``forceDirectory`` is
            `False`.
        forceDirectory : `bool`, optional
            If `True`, return a directory-like URI instead of a file-like URI.

        Yields
        ------
        uri : `ButlerURI`
            The temporary URI. Will be removed when the context is completed.
        """
        use_tempdir = False
        if prefix is None:
            prefix = ButlerURI(tempfile.mkdtemp(), forceDirectory=True, isTemporary=True)
            # Record that we need to delete this directory. Can not rely
            # on isTemporary flag since an external prefix may have that
            # set as well.
            use_tempdir = True

        # Need to create a randomized file name. For consistency do not
        # use mkstemp for local and something else for remote. Additionally
        # this method does not create the file to prevent name clashes.
        characters = "abcdefghijklmnopqrstuvwxyz0123456789_"
        rng = Random()
        tempname = "".join(rng.choice(characters) for _ in range(16))
        if suffix:
            tempname += suffix
        temporary_uri = prefix.join(tempname, isTemporary=True)
        if temporary_uri.dirLike:
            # If we had a safe way to clean up a remote temporary directory, we
            # could support this.
            raise NotImplementedError("temporary_uri cannot be used to create a temporary directory.")
        try:
            yield temporary_uri
        finally:
            if use_tempdir:
                shutil.rmtree(prefix.ospath, ignore_errors=True)
            else:
                try:
                    # It's okay if this does not work because the user removed
                    # the file.
                    temporary_uri.remove()
                except FileNotFoundError:
                    pass

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
        """For a dir-like URI, create the directory resource if needed."""
        raise NotImplementedError()

    def isdir(self) -> bool:
        """Return True if this URI looks like a directory, else False."""
        return self.dirLike

    def size(self) -> int:
        """For non-dir-like URI, return the size of the resource.

        Returns
        -------
        sz : `int`
            The size in bytes of the resource associated with this URI.
            Returns 0 if dir-like.
        """
        raise NotImplementedError()

    def __str__(self) -> str:
        """Convert the URI to its native string form."""
        return self.geturl()

    def __repr__(self) -> str:
        """Return string representation suitable for evaluation."""
        return f'ButlerURI("{self.geturl()}")'

    def __eq__(self, other: Any) -> bool:
        """Compare supplied object with this `ButlerURI`."""
        if not isinstance(other, ButlerURI):
            return NotImplemented
        return self.geturl() == other.geturl()

    def __hash__(self) -> int:
        """Return hash of this object."""
        return hash(str(self))

    def __copy__(self) -> ButlerURI:
        """Copy constructor.

        Object is immutable so copy can return itself.
        """
        # Implement here because the __new__ method confuses things
        return self

    def __deepcopy__(self, memo: Any) -> ButlerURI:
        """Deepcopy the object.

        Object is immutable so copy can return itself.
        """
        # Implement here because the __new__ method confuses things
        return self

    def __getnewargs__(self) -> Tuple:
        """Support pickling."""
        return (str(self),)

    @classmethod
    def _fixDirectorySep(cls, parsed: urllib.parse.ParseResult,
                         forceDirectory: bool = False) -> Tuple[urllib.parse.ParseResult, bool]:
        """Ensure that a path separator is present on directory paths.

        Parameters
        ----------
        parsed : `~urllib.parse.ParseResult`
            The result from parsing a URI using `urllib.parse`.
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
        """
        # assume we are not dealing with a directory like URI
        dirLike = False

        # Directory separator
        sep = cls._pathModule.sep

        # URI is dir-like if explicitly stated or if it ends on a separator
        endsOnSep = parsed.path.endswith(sep)
        if forceDirectory or endsOnSep:
            dirLike = True
            # only add the separator if it's not already there
            if not endsOnSep:
                parsed = parsed._replace(path=parsed.path+sep)

        return parsed, dirLike

    @classmethod
    def _fixupPathUri(cls, parsed: urllib.parse.ParseResult, root: Optional[Union[str, ButlerURI]] = None,
                      forceAbsolute: bool = False,
                      forceDirectory: bool = False) -> Tuple[urllib.parse.ParseResult, bool]:
        """Correct any issues with the supplied URI.

        Parameters
        ----------
        parsed : `~urllib.parse.ParseResult`
            The result from parsing a URI using `urllib.parse`.
        root : `str` or `ButlerURI`, ignored
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
        return cls._fixDirectorySep(parsed, forceDirectory)

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

    def walk(self, file_filter: Optional[Union[str, re.Pattern]] = None) -> Iterator[Union[List,
                                                                                           Tuple[ButlerURI,
                                                                                                 List[str],
                                                                                                 List[str]]]]:
        """Walk the directory tree returning matching files and directories.

        Parameters
        ----------
        file_filter : `str` or `re.Pattern`, optional
            Regex to filter out files from the list before it is returned.

        Yields
        ------
        dirpath : `ButlerURI`
            Current directory being examined.
        dirnames : `list` of `str`
            Names of subdirectories within dirpath.
        filenames : `list` of `str`
            Names of all the files within dirpath.
        """
        raise NotImplementedError()

    @classmethod
    def findFileResources(cls, candidates: Iterable[Union[str, ButlerURI]],
                          file_filter: Optional[str] = None,
                          grouped: bool = False) -> Iterator[Union[ButlerURI, Iterator[ButlerURI]]]:
        """Get all the files from a list of values.

        Parameters
        ----------
        candidates : iterable [`str` or `ButlerURI`]
            The files to return and directories in which to look for files to
            return.
        file_filter : `str`, optional
            The regex to use when searching for files within directories.
            By default returns all the found files.
        grouped : `bool`, optional
            If `True` the results will be grouped by directory and each
            yielded value will be an iterator over URIs. If `False` each
            URI will be returned separately.

        Yields
        ------
        found_file: `ButlerURI`
            The passed-in URIs and URIs found in passed-in directories.
            If grouping is enabled, each of the yielded values will be an
            iterator yielding members of the group. Files given explicitly
            will be returned as a single group at the end.

        Notes
        -----
        If a value is a file it is yielded immediately. If a value is a
        directory, all the files in the directory (recursively) that match
        the regex will be yielded in turn.
        """
        fileRegex = None if file_filter is None else re.compile(file_filter)

        singles = []

        # Find all the files of interest
        for location in candidates:
            uri = ButlerURI(location)
            if uri.isdir():
                for found in uri.walk(fileRegex):
                    if not found:
                        # This means the uri does not exist and by
                        # convention we ignore it
                        continue
                    root, dirs, files = found
                    if not files:
                        continue
                    if grouped:
                        yield (root.join(name) for name in files)
                    else:
                        for name in files:
                            yield root.join(name)
            else:
                if grouped:
                    singles.append(uri)
                else:
                    yield uri

        # Finally, return any explicitly given files in one group
        if grouped and singles:
            yield iter(singles)
