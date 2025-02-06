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
from typing import Any, ClassVar

import yaml
from pydantic import TypeAdapter, ValidationError

from lsst.resources import ResourcePath

from ._config import Config
from ._utilities.thread_safe_cache import ThreadSafeCache


class ButlerRepoIndex:
    """Index of all known butler repositories.

    The index of butler repositories can be configured in two ways:

    1. By setting the environment variable ``DAF_BUTLER_REPOSITORY_INDEX`` to
    the URI of a configuration file.
    2. By setting the environment variable ``DAF_BUTLER_REPOSITORIES`` to the
    contents of the configuration file as a string.

    In either case, the configuration is a simple dictionary lookup of the
    form:

    .. code-block:: yaml

       label1: uri1
       label2: uri2

    and can be in YAML or JSON format. The content of the file will be
    cached.
    """

    index_env_var: ClassVar[str] = "DAF_BUTLER_REPOSITORY_INDEX"
    """The name of the environment variable containing the URI of the index
    configuration file.
    """
    repositories_env_var: ClassVar[str] = "DAF_BUTLER_REPOSITORIES"
    """The name of the environment variable containing the configuration
    directly as a string.
    """

    _cache: ClassVar[ThreadSafeCache[str, dict[str, str]]] = ThreadSafeCache()
    """Cache of indexes. In most scenarios only one index will be found
    and the environment will not change. In tests this may not be true."""

    _most_recent_failure: ClassVar[str] = ""
    """Cache of the most recent failure when reading an index. Reset on
    every read."""

    @classmethod
    def _read_repository_index(cls, index_uri: str) -> dict[str, str]:
        """Read the repository index from the supplied URI.

        Parameters
        ----------
        index_uri : `str`
            URI of the repository index.

        Returns
        -------
        repo_index : `dict` [ `str` , `str` ]
            The index found at this URI.

        Raises
        ------
        FileNotFoundError
            Raised if the URI does not exist.

        Notes
        -----
        Does check the cache before reading the file.
        """
        config = cls._cache.get(index_uri)
        if config is not None:
            return config

        try:
            repo_index = cls._validate_configuration(Config(index_uri))
        except FileNotFoundError as e:
            # More explicit error message.
            raise FileNotFoundError(f"Butler repository index file not found at {index_uri}.") from e
        except Exception as e:
            raise RuntimeError(
                f"Butler repository index file at {index_uri} could not be read: {type(e).__qualname__} {e}"
            ) from e
        repo_index = cls._cache.set_or_get(index_uri, repo_index)

        return repo_index

    @classmethod
    def _read_repository_index_from_environment(cls) -> dict[str, str]:
        """Look in environment for index location and read it.

        Returns
        -------
        repo_index : `dict` [ `str` , `str` ]
            The index found in the environment.
        """
        cls._most_recent_failure = ""
        try:
            index_uri = os.getenv(cls.index_env_var)
            direct_configuration = os.getenv(cls.repositories_env_var)

            if index_uri and direct_configuration:
                raise RuntimeError(
                    f"Only one of the environment variables {cls.repositories_env_var} and"
                    f" {cls.index_env_var} should be set."
                )

            if direct_configuration:
                return cls._validate_configuration(yaml.safe_load(direct_configuration))

            if index_uri:
                return cls._read_repository_index(index_uri)

            raise RuntimeError(
                "No repository index defined.  Neither of the environment variables"
                f" {cls.repositories_env_var} or {cls.index_env_var} was set."
            )
        except Exception as e:
            cls._most_recent_failure = str(e)
            raise

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
            raise KeyError(f"Label '{label}' not known to repository index")
        return ResourcePath(repo_uri)

    @classmethod
    def _validate_configuration(cls, obj: Any) -> dict[str, str]:
        try:
            return TypeAdapter(dict[str, str]).validate_python(obj)
        except ValidationError as e:
            raise ValueError("Repository index not in expected format") from e
