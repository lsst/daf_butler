# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

__all__ = ("ButlerRepoIndex",)

import os
from typing import ClassVar

from lsst.resources import ResourcePath

from ._config import Config


class ButlerRepoIndex:
    """Index of all known butler repositories.

    The index of butler repositories is found by looking for a
    configuration file at the URI pointed at by the environment
    variable ``$DAF_BUTLER_REPOSITORY_INDEX``. The configuration file
    is a simple dictionary lookup of the form:

    .. code-block:: yaml

       label1: uri1
       label2: uri2

    and can be in YAML or JSON format. The content of the file will be
    cached.
    """

    index_env_var: ClassVar[str] = "DAF_BUTLER_REPOSITORY_INDEX"
    """The name of the environment variable to read to locate the index."""

    _cache: ClassVar[dict[ResourcePath, Config]] = {}
    """Cache of indexes. In most scenarios only one index will be found
    and the environment will not change. In tests this may not be true."""

    _most_recent_failure: ClassVar[str] = ""
    """Cache of the most recent failure when reading an index. Reset on
    every read."""

    @classmethod
    def _read_repository_index(cls, index_uri: ResourcePath) -> Config:
        """Read the repository index from the supplied URI.

        Parameters
        ----------
        index_uri : `lsst.resources.ResourcePath`
            URI of the repository index.

        Returns
        -------
        repo_index : `Config`
            The index found at this URI.

        Raises
        ------
        FileNotFoundError
            Raised if the URI does not exist.

        Notes
        -----
        Does check the cache before reading the file.
        """
        if index_uri in cls._cache:
            return cls._cache[index_uri]

        try:
            repo_index = Config(index_uri)
        except FileNotFoundError as e:
            # More explicit error message.
            raise FileNotFoundError(f"Butler repository index file not found at {index_uri}.") from e
        except Exception as e:
            raise RuntimeError(
                f"Butler repository index file at {index_uri} could not be read: {type(e).__qualname__} {e}"
            ) from e
        cls._cache[index_uri] = repo_index

        return repo_index

    @classmethod
    def _get_index_uri(cls) -> ResourcePath:
        """Find the URI to the repository index.

        Returns
        -------
        index_uri : `lsst.resources.ResourcePath`
            URI to the repository index.

        Raises
        ------
        KeyError
            Raised if the location of the index could not be determined.
        """
        index_uri = os.environ.get(cls.index_env_var)
        if index_uri is None:
            raise KeyError(f"No repository index defined in environment variable {cls.index_env_var}")
        return ResourcePath(index_uri)

    @classmethod
    def _read_repository_index_from_environment(cls) -> Config:
        """Look in environment for index location and read it.

        Returns
        -------
        repo_index : `Config`
            The index found in the environment.
        """
        cls._most_recent_failure = ""
        try:
            index_uri = cls._get_index_uri()
        except KeyError as e:
            cls._most_recent_failure = str(e)
            raise
        try:
            repo_index = cls._read_repository_index(index_uri)
        except Exception as e:
            cls._most_recent_failure = str(e)
            raise
        return repo_index

    @classmethod
    def get_known_repos(cls) -> set[str]:
        """Retrieve the list of known repository labels.

        Returns
        -------
        repos : `set` of `str`
            All the known labels. Can be empty if no index can be found.
        """
        try:
            repo_index = cls._read_repository_index_from_environment()
        except Exception:
            return set()
        return set(repo_index)

    @classmethod
    def get_failure_reason(cls) -> str:
        """Return possible reason for failure to return repository index.

        Returns
        -------
        reason : `str`
            If there is a problem reading the repository index, this will
            contain a string with an explanation. Empty string if everything
            worked.

        Notes
        -----
        The value returned is only reliable if called immediately after a
        failure. The most recent failure reason is reset every time an attempt
        is made to request a label and so the reason can be out of date.
        """
        return cls._most_recent_failure

    @classmethod
    def get_repo_uri(cls, label: str, return_label: bool = False) -> ResourcePath:
        """Look up the label in a butler repository index.

        Parameters
        ----------
        label : `str`
            Label of the Butler repository to look up.
        return_label : `bool`, optional
            If ``label`` cannot be found in the repository index (either
            because index is not defined or ``label`` is not in the index) and
            ``return_label`` is `True` then return ``ResourcePath(label)``.
            If ``return_label`` is `False` (default) then an exception will be
            raised instead.

        Returns
        -------
        uri : `lsst.resources.ResourcePath`
            URI to the Butler repository associated with the given label or
            default value if it is provided.

        Raises
        ------
        KeyError
            Raised if the label is not found in the index, or if an index
            is not defined, and ``return_label`` is `False`.
        FileNotFoundError
            Raised if an index is defined in the environment but it
            can not be found.
        """
        try:
            repo_index = cls._read_repository_index_from_environment()
        except Exception:
            if return_label:
                return ResourcePath(label, forceAbsolute=False)
            raise

        repo_uri = repo_index.get(label)
        if repo_uri is None:
            if return_label:
                return ResourcePath(label, forceAbsolute=False)
            # This should not raise since it worked earlier.
            try:
                index_uri = str(cls._get_index_uri())
            except KeyError:
                index_uri = "<environment variable not defined>"
            raise KeyError(f"Label '{label}' not known to repository index at {index_uri}")
        return ResourcePath(repo_uri)
