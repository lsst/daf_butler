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

import time
import urllib.parse
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TypeVar
from uuid import uuid4

import httpx
from pydantic import BaseModel, ValidationError

from lsst.daf.butler import __version__

from ._authentication import get_authentication_headers
from ._errors import deserialize_butler_user_error
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
        request = self._build_post_request(path, model)
        return self._send_request(request)

    @contextmanager
    def post_with_stream_response(self, path: str, model: BaseModel) -> Iterator[httpx.Response]:
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
        request = self._build_post_request(path, model)
        with self._send_request_with_stream_response(request) as response:
            yield response

    def _build_post_request(self, path: str, model: BaseModel) -> _Request:
        json = model.model_dump_json().encode("utf-8")
        return self._build_request(
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
        request = self._build_request("GET", path, params=params)
        return self._send_request(request)

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

    def _build_request(
        self,
        method: str,
        path: str,
        *,
        content: bytes | None = None,
        params: Mapping[str, str | bool] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> _Request:
        url = self._get_url(path)

        request_id = str(uuid4())
        request_headers = {CLIENT_REQUEST_ID_HEADER_NAME: request_id}
        request_headers.update(self._headers)
        if headers is not None:
            request_headers.update(headers)

        return _Request(
            request=self._client.build_request(
                method, url, content=content, params=params, headers=request_headers
            ),
            request_id=request_id,
        )

    def _send_request(self, request: _Request) -> httpx.Response:
        """Send an HTTP request to the Butler server with authentication
        headers and a request ID.

        If the server returns a user-facing error detail, raises an exception
        with the message as a subclass of ButlerUserError.
        """
        try:
            response = self._send_with_retries(request, stream=False)
            self._handle_http_status(response, request.request_id)
            return response
        except httpx.HTTPError as e:
            raise ButlerServerError(request.request_id) from e

    @contextmanager
    def _send_request_with_stream_response(self, request: _Request) -> Iterator[httpx.Response]:
        try:
            response = self._send_with_retries(request, stream=True)
            try:
                self._handle_http_status(response, request.request_id)
                yield response
            finally:
                response.close()
        except httpx.HTTPError as e:
            raise ButlerServerError(request.request_id) from e

    def _send_with_retries(self, request: _Request, stream: bool) -> httpx.Response:
        max_retry_time_seconds = 120
        start_time = time.time()
        while True:
            response = self._client.send(request.request, stream=stream)
            retry = _needs_retry(response)
            time_remaining = max_retry_time_seconds - (time.time() - start_time)
            if retry.retry and time_remaining > 0:
                if stream:
                    response.close()
                sleep_time = min(time_remaining, retry.delay_seconds)
                time.sleep(sleep_time)
            else:
                return response

    def _handle_http_status(self, response: httpx.Response, request_id: str) -> None:
        if response.status_code == ERROR_STATUS_CODE:
            # Raise an exception that the server has forwarded to the
            # client.
            model = _try_to_parse_model(response, ErrorResponseModel)
            if model is not None:
                exc = deserialize_butler_user_error(model)
                exc.add_note(f"Client request ID: {request_id}")
                raise exc
            # If model is None, server sent an expected error code, but
            # the body wasn't in the expected JSON format.  This likely
            # means some HTTP thing between us and the server is
            # misbehaving.

        response.raise_for_status()


@dataclass(frozen=True)
class _Retry:
    retry: bool
    delay_seconds: int


def _needs_retry(response: httpx.Response) -> _Retry:
    # Handle a 503 Service Unavailable, sent by the server if it is
    # overloaded, or a 429, sent by the server if the client
    # triggers a rate limit.
    if response.status_code == 503 or response.status_code == 429:
        # Only retry if the server has instructed us to do so by sending a
        # Retry-After header.
        retry_after = response.headers.get("retry-after")
        if retry_after is not None:
            try:
                # The HTTP standard also allows a date string here, but the
                # Butler server only sends integer seconds.
                delay_seconds = int(retry_after)
                return _Retry(True, delay_seconds)
            except ValueError:
                pass

    return _Retry(False, 0)


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
    return model.model_validate_json(response.read())


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


def quote_path_variable(path: str) -> str:  # numpydoc ignore=PR01
    """URL encode a string to make it suitable for inserting as a segment of
    the path part of a URL.
    """
    return urllib.parse.quote(path, safe="")


@dataclass(frozen=True)
class _Request:
    request: httpx.Request
    request_id: str
