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
import urllib.parse
import os.path
import logging

__all__ = ('ButlerSchemelessURI',)

from pathlib import PurePath

from typing import (
    Optional,
    Tuple,
    Union,
)

from .file import ButlerFileURI
from .utils import os2posix
from ._butlerUri import ButlerURI

log = logging.getLogger(__name__)


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

    @classmethod
    def _fixupPathUri(cls, parsed: urllib.parse.ParseResult, root: Optional[Union[str, ButlerURI]] = None,
                      forceAbsolute: bool = False,
                      forceDirectory: bool = False) -> Tuple[urllib.parse.ParseResult, bool]:
        """Fix up relative paths for local file system.

        Parameters
        ----------
        parsed : `~urllib.parse.ParseResult`
            The result from parsing a URI using `urllib.parse`.
        root : `str` or `ButlerURI`, optional
            Path to use as root when converting relative to absolute.
            If `None`, it will be the current working directory. This
            is a local file system path, or a file URI.
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

        Scheme-less paths are normalized and environment variables are
        expanded.
        """
        # assume we are not dealing with a directory URI
        dirLike = False

        # Replacement values for the URI
        replacements = {}

        if root is None:
            root = os.path.abspath(os.path.curdir)
        elif isinstance(root, ButlerURI):
            if root.scheme and root.scheme != "file":
                raise RuntimeError(f"The override root must be a file URI not {root.scheme}")
            root = os.path.abspath(root.ospath)

        # this is a local OS file path which can support tilde expansion.
        # we quoted it in the constructor so unquote here
        expandedPath = os.path.expanduser(urllib.parse.unquote(parsed.path))

        # We might also be receiving a path containing environment variables
        # so expand those here
        expandedPath = os.path.expandvars(expandedPath)

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

        # We do allow fragment but do not expect params or query to be
        # specified for schemeless
        if parsed.params or parsed.query:
            log.warning("Additional items unexpectedly encountered in schemeless URI: %s", parsed.geturl())

        return parsed, dirLike
