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

__all__ = ("ButlerRepoIndex",)

import os
from typing import ClassVar, Dict, Set

from lsst.resources import ResourcePath, ResourcePathExpression

from .core import Config


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

    _cache: ClassVar[Dict[ResourcePath, Config]] = {}
    """Cache of indexes. In most scenarios only one index will be found
    and the environment will not change. In tests this may not be true."""

    @classmethod
    def _read_repository_index(cls, index_uri: ResourcePathExpression) -> Config:
        """Read the repository index from the supplied URI.

        Parameters
        ----------
        index_uri : `lsst.resources.ResourcePathExpression`
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
        # Force the given value to a ResourcePath so that it can be used
        # as an index into the cache consistently.
        uri = ResourcePath(index_uri)

        if index_uri in cls._cache:
            return cls._cache[uri]

        repo_index = Config(uri)
        cls._cache[uri] = repo_index

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
            raise KeyError(f"No repository index defined in enviroment variable {cls.index_env_var}")
        return ResourcePath(index_uri)

    @classmethod
    def _read_repository_index_from_environment(cls) -> Config:
        """Look in environment for index location and read it.

        Returns
        -------
        repo_index : `Config`
            The index found in the environment.
        """
        index_uri = cls._get_index_uri()
        return cls._read_repository_index(index_uri)

    @classmethod
    def get_known_repos(cls) -> Set[str]:
        """Retrieve the list of known repository labels.

        Returns
        -------
        repos : `set` of `str`
            All the known labels. Can be empty if no index can be found.
        """
        try:
            repo_index = cls._read_repository_index_from_environment()
        except (FileNotFoundError, KeyError):
            return set()
        return set(repo_index)

    @classmethod
    def get_repo_uri(cls, label: str) -> ResourcePath:
        """Look up the label in a butler repository index.

        Parameters
        ----------
        label : `str`
            Label of the Butler repository to look up.

        Returns
        -------
        uri : `lsst.resources.ResourcePath`
            URI to the Butler repository associated with the given label.

        Raises
        ------
        KeyError
            Raised if the label is not found in the index, or if an index
            can not be found at all.
        FileNotFoundError
            Raised if an index is defined in the environment but it
            can not be found.
        """
        repo_index = cls._read_repository_index_from_environment()
        repo_uri = repo_index.get(label)
        if repo_uri is None:
            # This should not raise since it worked earlier.
            try:
                index_uri = str(cls._get_index_uri())
            except KeyError:
                index_uri = "<environment variable not defined>"
            raise KeyError(f"Label '{label}' not known to repository index at {index_uri}")
        return ResourcePath(repo_uri)
