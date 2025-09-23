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

from typing import Annotated

from fastapi import Depends, Header, HTTPException
from safir.dependencies.gafaelfawr import auth_delegated_token_dependency
from safir.dependencies.logger import logger_dependency as safir_logger_dependency
from structlog.stdlib import BoundLogger

from lsst.daf.butler import LabeledButlerFactory

from ._config import load_config
from ._factory import Factory
from ._gafaelfawr import GafaelfawrClient, GafaelfawrGroupAuthorizer

_butler_factory: LabeledButlerFactory | None = None
_authorizer: GafaelfawrGroupAuthorizer | None = None


async def butler_factory_dependency() -> LabeledButlerFactory:
    """Return a global LabeledButlerFactory instance.  This will be used to
    construct internal DirectButler instances for interacting with the Butler
    repositories we are serving.
    """
    global _butler_factory
    if _butler_factory is None:
        config = load_config()
        repositories = {k: v.config_uri for k, v in config.repositories.items()}
        _butler_factory = LabeledButlerFactory(repositories)
    return _butler_factory


async def authorizer_dependency() -> GafaelfawrGroupAuthorizer:
    """Instantiate a client for checking group membership via Gafaelfawr."""
    global _authorizer
    if _authorizer is None:
        config = load_config()
        authorized_groups = {k: v.authorized_groups for k, v in config.repositories.items()}
        client = GafaelfawrClient(str(config.gafaelfawr_url))
        _authorizer = GafaelfawrGroupAuthorizer(client, authorized_groups)

    return _authorizer


async def user_name_dependency(x_auth_request_user: Annotated[str | None, Header()] = None) -> str | None:
    """Retrieve the user name from Gafaelfawr authentication headers.

    Parameters
    ----------
    x_auth_request_user : FastAPI header
        Header provided by FastAPI.

    Returns
    -------
    user_name : `str` | `None`
        The user name, if Gafaelfawr is available on this environment.  `None`
        if Gafaelfawr is not available.
    """
    if x_auth_request_user is None and load_config().gafaelfawr_enabled:
        raise HTTPException(status_code=403, detail="Required X-Auth-Request-User header was not provided")

    return x_auth_request_user


async def repository_authorization_dependency(
    repository: str,
    user_name: Annotated[str | None, Depends(user_name_dependency)],
    user_token: Annotated[str, Depends(auth_delegated_token_dependency)],
    authorizer: Annotated[GafaelfawrGroupAuthorizer, Depends(authorizer_dependency)],
) -> None:
    """Restrict access to specific repositories based on the user's membership
    in Gafaelfawr groups.

    Parameters
    ----------
    repository : `str`
        Butler repository that is being accessed.
    user_name : `str`
        Name of the user accessing the repository, from Gafaelfawr headers.
    user_token : `str`
        Delegated token for the user accessing the repository, from Gafaelfawr
        headers.  Used for retrieving group membership information about the
        user from Gafaelfawr.
    authorizer : `GafaelfawrGroupAuthorizer`
        Authorization client that will be used to verify group membership.
    """
    assert user_name is not None, "Gafaelfawr user name header should have been populated."
    if not await authorizer.is_user_authorized_for_repository(
        repository=repository, user_name=user_name, user_token=user_token
    ):
        raise HTTPException(
            status_code=403,
            detail=f"User {user_name} does not have permission to access Butler repository '{repository}'",
        )


async def logger_dependency(
    logger: Annotated[BoundLogger, Depends(safir_logger_dependency)],
    repository: str,
    user_name: Annotated[str | None, Depends(user_name_dependency)],
    x_auth_request_service: Annotated[str | None, Header()] = None,
) -> BoundLogger:
    """Return a logger with additional bound context information.

    Parameters
    ----------
    logger : `structlog.stdlib.BoundLogger`
        Logger provided by Safir.
    repository : `str`
        Butler repository that is being accessed.
    user_name : `str` or `None`
        Name of the user accessing the repository, from Gafaelfawr headers.
    x_auth_request_service : `str` or `None`
        Name of the service being used to access the repository, from
        Gafaelfawr headers.
    """
    return logger.bind(
        butler_repo=repository, requester={"username": user_name, "service": x_auth_request_service}
    )


async def factory_dependency(
    repository: str,
    butler_factory: Annotated[LabeledButlerFactory, Depends(butler_factory_dependency)],
) -> Factory:
    """Return Factory object for injection into FastAPI.

    Parameters
    ----------
    repository : `str`
        Label of the repository for lookup from the repository index.
    butler_factory : `LabeledButlerFactory`
        Factory for instantiating DirectButlers.
    """
    return Factory(butler_factory=butler_factory, repository=repository)


def reset_dependency_caches() -> None:
    """Clear caches used by dependencies.  Unit tests should call this after
    changing the configuration, to allow objects to be re-created with the
    new configuration.
    """
    global _butler_factory
    global _authorizer

    _butler_factory = None
    _authorizer = None
