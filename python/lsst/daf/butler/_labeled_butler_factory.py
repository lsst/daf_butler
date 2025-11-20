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

__all__ = ("LabeledButlerFactory", "LabeledButlerFactoryProtocol")

from collections.abc import Mapping
from typing import Protocol

from lsst.resources import ResourcePathExpression

from ._butler import Butler
from ._butler_config import ButlerConfig, ButlerType
from ._butler_repo_index import ButlerRepoIndex
from ._utilities.named_locks import NamedLocks
from ._utilities.thread_safe_cache import ThreadSafeCache


class LabeledButlerFactoryProtocol(Protocol):
    """Callable to retrieve a butler from a label."""

    def __call__(self, label: str) -> Butler: ...


class LabeledButlerFactory:
    """Factory for efficiently instantiating Butler instances from the
    repository index file.  This is intended for use from long-lived services
    that want to instantiate a separate Butler instance for each end user
    request.

    Parameters
    ----------
    repositories : `~collections.abc.Mapping` [`str`, `str`], optional
        Keys are arbitrary labels, and values are URIs to Butler configuration
        files.  If not provided, defaults to the global repository index
        configured by the ``DAF_BUTLER_REPOSITORY_INDEX`` environment variable
        --  see `ButlerRepoIndex`.

    Notes
    -----
    This interface is currently considered experimental and is subject to
    change.

    For each label in the repository index, caches shared state to allow fast
    instantiation of new instances.

    Instance methods on this class are threadsafe -- a single instance of
    `LabeledButlerFactory` can be used concurrently by multiple threads. It is
    NOT safe for a single `Butler` instance returned by this factory to be used
    concurrently by multiple threads.  However, separate `Butler` instances can
    safely be used by separate threads.
    """

    def __init__(self, repositories: Mapping[str, str] | None = None) -> None:
        if repositories is None:
            self._repositories = None
        else:
            self._repositories = dict(repositories)

        self._factories = ThreadSafeCache[str, _ButlerFactory]()
        self._initialization_locks = NamedLocks()

        # This may be overridden by unit tests.
        self._preload_unsafe_direct_butler_caches = True

    def bind(self, access_token: str | None) -> LabeledButlerFactoryProtocol:
        """Create a callable factory function for generating Butler instances
        with out needing to specify access tokans again.

        Parameters
        ----------
        access_token : `str` or `None`
            An optional access token to use for authentication with the Butler.

        Returns
        -------
        bound : `LabeledButlerFactoryProtocol`
            A callable that takes a label as input and returns a Butler
            instance.
        """

        def create(label: str) -> Butler:
            return self.create_butler(label=label, access_token=access_token)

        return create

    def create_butler(self, *, label: str, access_token: str | None) -> Butler:
        """Create a Butler instance.

        Parameters
        ----------
        label : `str`
            Label of the repository to instantiate, from the ``repositories``
            parameter to the `LabeledButlerFactory` constructor or the global
            repository index file.
        access_token : `str` | `None`
            Gafaelfawr access token used to authenticate to a Butler server.
            This is required for any repositories configured to use
            `RemoteButler`.  If you only use `DirectButler`, this may be
            `None`.

        Raises
        ------
        KeyError
            Raised if the label is not found in the index.

        Notes
        -----
        For a service making requests on behalf of end users, the access token
        should normally be a "delegated" token so that access permissions are
        based on the end user instead of the service. See
        https://gafaelfawr.lsst.io/user-guide/gafaelfawringress.html#requesting-delegated-tokens
        """
        factory = self._get_or_create_butler_factory(label)
        return factory.create_butler(access_token)

    def close(self) -> None:
        """Reset the factory cache, and release any resources associated with
        the cached instances.
        """
        factories = self._factories.clear()
        for factory in factories.values():
            factory.close()

    def _get_or_create_butler_factory(self, label: str) -> _ButlerFactory:
        # We maintain a separate lock per label.  We only want to instantiate
        # one factory function per label, because creating the factory sets up
        # shared state that should only exist once per repository.  However, we
        # don't want other repositories' instance creation to block on one
        # repository that is slow to initialize.
        with self._initialization_locks.lock(label):
            if (factory := self._factories.get(label)) is not None:
                return factory

            factory = self._create_butler_factory_function(label)
            return self._factories.set_or_get(label, factory)

    def _create_butler_factory_function(self, label: str) -> _ButlerFactory:
        config_uri = self._get_config_uri(label)
        config = ButlerConfig(config_uri)
        butler_type = config.get_butler_type()

        match butler_type:
            case ButlerType.DIRECT:
                return _DirectButlerFactory(config, self._preload_unsafe_direct_butler_caches)
            case ButlerType.REMOTE:
                return _RemoteButlerFactory(config)
            case _:
                raise TypeError(f"Unknown butler type '{butler_type}' for label '{label}'")

    def _get_config_uri(self, label: str) -> ResourcePathExpression:
        if self._repositories is None:
            return ButlerRepoIndex.get_repo_uri(label)
        else:
            config_uri = self._repositories.get(label)
            if config_uri is None:
                raise KeyError(f"Unknown repository label '{label}'")
            return config_uri


class _ButlerFactory(Protocol):
    def create_butler(self, access_token: str | None) -> Butler: ...
    def close(self) -> None: ...


class _DirectButlerFactory(_ButlerFactory):
    def __init__(self, config: ButlerConfig, preload_unsafe_caches: bool) -> None:
        import lsst.daf.butler.direct_butler

        # Create a 'template' Butler that will be cloned when callers request
        # an instance.
        self._butler = Butler.from_config(config)
        assert isinstance(self._butler, lsst.daf.butler.direct_butler.DirectButler)

        # Load caches so that data is available in cloned instances without
        # needing to refetch it from the database for every instance.
        self._butler._preload_cache(load_dimension_record_cache=preload_unsafe_caches)

    def create_butler(self, access_token: str | None) -> Butler:
        # Access token is ignored because DirectButler does not use Gafaelfawr
        # authentication.
        return self._butler.clone()

    def close(self) -> None:
        self._butler.close()


class _RemoteButlerFactory(_ButlerFactory):
    def __init__(self, config: ButlerConfig) -> None:
        import lsst.daf.butler.remote_butler._factory

        self._factory = lsst.daf.butler.remote_butler._factory.RemoteButlerFactory.create_factory_from_config(
            config
        )

    def create_butler(self, access_token: str | None) -> Butler:
        if access_token is None:
            raise ValueError("Access token is required to connect to a Butler server")
        return self._factory.create_butler_for_access_token(access_token)

    def close(self) -> None:
        pass
