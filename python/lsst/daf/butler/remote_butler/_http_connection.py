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

__all__ = ("RemoteButlerHttpConnection", "parse_model")

from collections.abc import Mapping
from typing import TypeVar
from uuid import uuid4

import httpx
from lsst.daf.butler import __version__
from pydantic import BaseModel, ValidationError

from .._exceptions import create_butler_user_error
from ._authentication import get_authentication_headers
from .server_models import CLIENT_REQUEST_ID_HEADER_NAME, ERROR_STATUS_CODE, ErrorResponseModel

_AnyPydanticModel = TypeVar("_AnyPydanticModel", bound=BaseModel)
"""Generic type variable that accepts any Pydantic model class."""


class RemoteButlerHttpConnection:
    """HTTP connection to a Butler server.

    Parameters
    ----------
    http_client : `httpx.Client`
        HTTP connection pool we will use to connect to the server.
    server_url : `str`
        URL of the Butler server we will connect to.
    access_token : `str`
        Rubin Science Platform Gafaelfawr access token that will be used to
        authenticate with the server.
    """

    def __init__(self, http_client: httpx.Client, server_url: str, access_token: str) -> None:
        self._client = http_client
        self.server_url = server_url
        self._access_token = access_token

        auth_headers = get_authentication_headers(access_token)
        headers = {"user-agent": f"RemoteButler/{__version__}"}

        self._headers = auth_headers | headers

    def post(self, path: str, model: BaseModel) -> httpx.Response:
        """Send a POST request to the Butler server.

        Parameters
        ----------
        path : `str`
            A relative path to an endpoint.
        model : `pydantic.BaseModel`
            Pydantic model containing the request body to be sent to the
            server.

        Returns
        -------
        response: `~httpx.Response`
            The response from the server.

        Raises
        ------
        ButlerUserError
            If the server explicitly returned a user-facing error response.
        ButlerServerError
            If there is an issue communicating with the server.
        """
        json = model.model_dump_json().encode("utf-8")
        return self._send_request(
            "POST",
            path,
            content=json,
            headers={"content-type": "application/json"},
        )

    def get(self, path: str, params: Mapping[str, str | bool] | None = None) -> httpx.Response:
        """Send a GET request to the Butler server.

        Parameters
        ----------
        path : `str`
            A relative path to an endpoint.
        params : `~collections.abc.Mapping` [ `str` , `str` | `bool` ]
            Query parameters included in the request URL.

        Returns
        -------
        response: `~httpx.Response`
            The response from the server.

        Raises
        ------
        ButlerUserError
            If the server explicitly returned a user-facing error response.
        ButlerServerError
            If there is an issue communicating with the server.
        """
        return self._send_request("GET", path, params=params)

    def _get_url(self, path: str, version: str = "v1") -> str:
        """Form the complete path to an endpoint on the server.

        Parameters
        ----------
        path : `str`
            The relative path to the server endpoint.
        version : `str`, optional
            Version string to prepend to path. Defaults to "v1".

        Returns
        -------
        path : `str`
            The full path to the endpoint.
        """
        slash = "" if self.server_url.endswith("/") else "/"
        return f"{self.server_url}{slash}{version}/{path}"

    def _send_request(
        self,
        method: str,
        path: str,
        *,
        content: bytes | None = None,
        params: Mapping[str, str | bool] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        """Send an HTTP request to the Butler server with authentication
        headers and a request ID.

        If the server returns a user-facing error detail, raises an exception
        with the message as a subclass of ButlerUserError.
        """
        url = self._get_url(path)

        request_id = str(uuid4())
        request_headers = {CLIENT_REQUEST_ID_HEADER_NAME: request_id}
        request_headers.update(self._headers)
        if headers is not None:
            request_headers.update(headers)

        try:
            response = self._client.request(
                method, url, content=content, params=params, headers=request_headers
            )

            if response.status_code == ERROR_STATUS_CODE:
                # Raise an exception that the server has forwarded to the
                # client.
                model = _try_to_parse_model(response, ErrorResponseModel)
                if model is not None:
                    exc = create_butler_user_error(model.error_type, model.detail)
                    exc.add_note(f"Client request ID: {request_id}")
                    raise exc
                # If model is None, server sent an expected error code, but the
                # body wasn't in the expected JSON format.  This likely means
                # some HTTP thing between us and the server is misbehaving.

            response.raise_for_status()
            return response
        except httpx.HTTPError as e:
            raise ButlerServerError(request_id) from e


def parse_model(response: httpx.Response, model: type[_AnyPydanticModel]) -> _AnyPydanticModel:
    """Deserialize a Pydantic model from the body of an HTTP response.

    Parameters
    ----------
    response : `~httpx.Response`
        An HTTP response object.
    model : `type` [ ``pydantic.BaseModel`` ]
        A Pydantic model class that will be used to parse the response body.

    Returns
    -------
    response_model : ``pydantic.BaseModel``
        An instance of the Pydantic model class loaded from the response body.
    """
    return model.model_validate_json(response.content)


def _try_to_parse_model(response: httpx.Response, model: type[_AnyPydanticModel]) -> _AnyPydanticModel | None:
    """Attempt to deserialize a Pydantic model from the body of an HTTP
    response.  Returns `None` if the content could not be parsed as JSON or
    failed validation against the model.
    """
    try:
        return parse_model(response, model)
    except (ValueError, ValidationError):
        return None


class ButlerServerError(RuntimeError):
    """Exception returned when there is an error communicating with the Butler
    server.

    Parameters
    ----------
    client_request_id : `str`
        Request ID to include in the exception message.
    """

    def __init__(self, client_request_id: str):
        super().__init__(f"Error while communicating with Butler server.  Request ID: {client_request_id}")
