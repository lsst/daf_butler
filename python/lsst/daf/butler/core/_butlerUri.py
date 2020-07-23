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

import os
import os.path
import shutil
import urllib
import posixpath
from pathlib import Path, PurePath, PurePosixPath
import requests
import tempfile
import copy

from typing import (
    TYPE_CHECKING,
    Any,
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

# Determine if the path separator for the OS looks like POSIX
IS_POSIX = os.sep == posixpath.sep

# Root path for this operating system
OS_ROOT_PATH = Path().resolve().root


def os2posix(ospath: str) -> str:
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
    posix : `str`
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

    transferModes: Tuple[str, ...] = ("copy", "auto")
    """Transfer modes supported by this implementation."""

    transferDefault: str = "copy"
    """Default mode to use for transferring if ``auto`` is specified."""

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
            else:
                subclass = ButlerGenericURI

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
    def ospath(self) -> str:
        """Path component of the URI localized to current OS."""
        raise AttributeError(f"Non-file URI ({self}) has no local OS path.")

    @property
    def relativeToPathRoot(self) -> str:
        """Returns path relative to network location.

        Effectively, this is the path property with posix separator stripped
        from the left hand side of the path.
        """
        p = self._pathLib(self.path)
        relToRoot = str(p.relative_to(p.root))
        if self.dirLike and not relToRoot.endswith("/"):
            relToRoot += "/"
        return relToRoot

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
            separator. Tail will never contain separators.
        """
        head, tail = self._pathModule.split(self.path)
        headuri = self._uri._replace(path=head)

        # Schemeless is special in that it can be a relative path
        # We need to ensure that it stays that way. All other URIs will
        # be absolute already.
        forceAbsolute = self._pathModule.isabs(self.path)
        return self.__class__(headuri, forceDirectory=True, forceAbsolute=forceAbsolute), tail

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
        Updates the ButlerURI.dirLike attribute.
        """
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
            to include a file at the end.

        Returns
        -------
        new : `ButlerURI`
            New URI with any file at the end replaced with the new path
            components.

        Notes
        -----
        File URIs assume the path component is local file system. All other
        URIs assume POSIX separators.
        """
        new = self.dirname()
        # Assume path is posix
        newpath = posixpath.join(new.path, path)
        new._uri = self._uri._replace(path=newpath)
        return new

    def exists(self) -> bool:
        """Indicate that the resource is available.

        Returns
        -------
        exists : `bool`
            `True` if the resource exists.
        """
        raise NotImplementedError()

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

    def transfer_from(self, src: ButlerURI, transfer: str) -> None:
        """Transfer the current resource to a new location.

        Parameters
        ----------
        src : `ButlerURI`
            Source URI.
        transfer : `str`
            Mode to use for transferring the resource. Generically there are
            many standard options: copy, link, symlink, hardlink, relsymlink.
            Not all URIs support all modes.

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
        """
        raise NotImplementedError(f"No transfer modes supported by URI scheme {self.scheme}")


class ButlerFileURI(ButlerURI):
    """URI for explicit ``file`` scheme."""

    transferModes = ("copy", "link", "symlink", "hardlink", "relsymlink", "auto")
    transferDefault: str = "link"

    @property
    def ospath(self) -> str:
        """Path component of the URI localized to current OS."""
        return posix2os(self._uri.path)

    def exists(self) -> bool:
        return os.path.exists(self.ospath)

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

    def read(self, size: int = -1) -> bytes:
        # Docstring inherits
        with open(self.ospath, "rb") as fh:
            return fh.read(size)

    def transfer_from(self, src: ButlerURI, transfer: str) -> None:
        """Transfer the current resource to a local file.

        Parameters
        ----------
        src : `ButlerURI`
            Source URI.
        transfer : `str`
            Mode to use for transferring the resource. Supports the following
            options: copy, link, symlink, hardlink, relsymlink.

        Notes
        -----
        "move" is currently disabled. For the general case it is the riskiest
        option to implement.
        """
        if transfer == "move":
            raise ValueError("move transfers not currently supported")

        # Fail early to prevent delays if remote resources are requested
        if transfer not in self.transferModes:
            raise ValueError(f"Transfer mode '{transfer}' not supported by URI scheme {self.scheme}")

        # We do not have to special case ButlerFileURI here because
        # as_local handles that.
        local_src, is_temporary = src.as_local()

        # Default transfer mode depends on whether we have a temporary
        # file or not.
        if transfer == "auto":
            transfer = self.transferDefault if not is_temporary else "copy"

        # Follow soft links
        local_src = os.path.realpath(local_src)

        # All the modes involving linking use "link" somewhere
        if "link" in transfer and is_temporary:
            raise RuntimeError("Can not use local file system transfer mode"
                               f" {transfer} for remote resource ({src})")

        # For temporary files we can own them
        if is_temporary and transfer == "copy":
            transfer = "move"

        newFullPath = os.path.realpath(self.ospath)
        if os.path.exists(newFullPath):
            raise FileExistsError(f"Destination path '{newFullPath}' already exists.")
        outputDir = os.path.dirname(newFullPath)
        if not os.path.isdir(outputDir):
            # Must create the directory
            safeMakeDir(outputDir)

        if transfer == "move":
            shutil.move(local_src, newFullPath)
        elif transfer == "copy":
            shutil.copy(local_src, newFullPath)
        elif transfer == "link":
            # Try hard link and if that fails use a symlink
            try:
                os.link(local_src, newFullPath)
            except OSError:
                # Read through existing symlinks
                os.symlink(local_src, newFullPath)
        elif transfer == "hardlink":
            os.link(local_src, newFullPath)
        elif transfer == "symlink":
            # Read through existing symlinks
            os.symlink(local_src, newFullPath)
        elif transfer == "relsymlink":
            # This is a standard symlink but using a relative path
            # Need the directory name to give to relative root
            # A full file path confuses it into an extra ../
            newFullPathRoot, _ = os.path.split(newFullPath)
            relPath = os.path.relpath(local_src, newFullPathRoot)
            os.symlink(relPath, newFullPath)
        else:
            raise NotImplementedError("Transfer type '{}' not supported.".format(transfer))

        if is_temporary and os.path.exists(local_src):
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

        # For an absolute path all we need to do is check if we need
        # to force the directory separator
        if posixpath.isabs(parsed.path):
            if forceDirectory and not parsed.path.endswith(sep):
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

    def read(self, size: int = -1) -> bytes:
        args = {}
        if size > 0:
            args["Range"] = f"bytes=0-{size-1}"
        response = self.client.get_object(Bucket=self.netloc,
                                          Key=self.relativeToPathRoot,
                                          **args)
        return response["Body"].read()

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

    def transfer_from(self, src: ButlerURI, transfer: str = "copy") -> None:
        """Transfer the current resource to an S3 bucket.

        Parameters
        ----------
        src : `ButlerURI`
            Source URI.
        transfer : `str`
            Mode to use for transferring the resource. Supports the following
            options: copy.

        Notes
        -----
        "move" is currently disabled. For the general case it is the riskiest
        option to implement.
        """
        # Fail early to prevent delays if remote resources are requested
        if transfer not in self.transferModes:
            raise ValueError(f"Transfer mode '{transfer}' not supported by URI scheme {self.scheme}")

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


class ButlerHttpURI(ButlerURI):
    """General HTTP(S) resource."""

    def exists(self) -> bool:
        """Check that a remote HTTP resource exists."""
        header = requests.head(self.geturl())
        return True if header.status_code == 200 else False

    def as_local(self) -> Tuple[str, bool]:
        """Download object over HTTP and place in temporary directory.

        Returns
        -------
        path : `str`
            Path to local temporary file.
        temporary : `bool`
            Always returns `True`. This is always a temporary file.
        """
        r = requests.get(self.geturl(), stream=True)
        if r.status_code != 200:
            raise FileNotFoundError(f"Unable to download resource {self}; status code: {r.status_code}")
        with tempfile.NamedTemporaryFile(suffix=self.getExtension(), delete=False) as tmpFile:
            for chunk in r.iter_content():
                tmpFile.write(chunk)
        return tmpFile.name, True

    def read(self, size: int = -1) -> bytes:
        # Docstring inherits
        stream = True if size > 0 else False
        r = requests.get(self.geturl(), stream=stream)
        if not stream:
            return r.content
        else:
            return next(r.iter_content(chunk_size=size))


class ButlerGenericURI(ButlerURI):
    """Generic URI with a defined scheme"""

    def exists(self) -> bool:
        """Test for existence and always return False."""
        return False

    def as_local(self) -> Tuple[str, bool]:
        raise RuntimeError(f"Do not know how to retrieve data for URI '{self}'")


class ButlerSchemelessURI(ButlerFileURI):
    """Scheme-less URI referring to the local file system"""

    _pathLib = PurePath
    _pathModule = os.path

    @property
    def ospath(self) -> str:
        """Path component of the URI localized to current OS."""
        return self.path

    def join(self, path: str) -> ButlerURI:
        """Create a new `ButlerURI` with additional path components including
        a file.

        Parameters
        ----------
        path : `str`
            Additional file components to append to the current URI. Assumed
            to include a file at the end.

        Returns
        -------
        new : `ButlerURI`
            New URI with any file at the end replaced with the new path
            components.

        Notes
        -----
        File URIs assume the path component is local file system. All other
        URIs assume POSIX separators.
        """
        new = self.dirname()
        # Assume os path completely
        newpath = os.path.join(new.path, path)
        new._uri = self._uri._replace(path=newpath)
        return new

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
        expandedPath = os.path.expanduser(parsed.path)

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

        # add the trailing separator only if explicitly required or
        # if it was stripped by normpath. Acknowledge that trailing
        # separator exists.
        endsOnSep = expandedPath.endswith(os.sep) and not replacements["path"].endswith(os.sep)
        if (forceDirectory or endsOnSep or dirLike):
            dirLike = True
            replacements["path"] += os.sep

        if "scheme" in replacements:
            # This is now meant to be a URI path so force to posix
            replacements["path"] = os2posix(replacements["path"])

        # ParseResult is a NamedTuple so _replace is standard API
        parsed = parsed._replace(**replacements)

        return parsed, dirLike
