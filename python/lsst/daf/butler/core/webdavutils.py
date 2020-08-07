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

__all__ = ("getHttpSession", "isWebdavEndpoint", "webdavCheckFileExists",
           "folderExists", "webdavDeleteFile", "getFileURL")

import os
import requests
import logging

from typing import (
    Optional,
    Tuple,
    Union,
)

from .location import ButlerURI, Location

log = logging.getLogger(__name__)


def getHttpSession() -> requests.Session:
    """Create a requests.Session pre-configured with environment variable data

    Returns
    -------
    s : `requests.Session`
        An http session used to execute requests.

    Notes
    -----
    The WEBDAV_AUTH_METHOD must be set to obtain a session.
    Depending on the chosen method, additional
    environment variables are required:

    X509: must set WEBDAV_PROXY_CERT
    (path to proxy certificate used to authenticate requests)

    TOKEN: must set WEBDAV_BEARER_TOKEN
    (bearer token used to authenticate requests, as a single string)

    NB: requests will read CA certificates in the REQUESTS_CA_BUNDLE env variable.
    It must be manually exported according to the system CA directory.
    """
    s = requests.Session()
    log.debug("Creating new HTTP session")

    try:
        env_auth_method = os.environ['WEBDAV_AUTH_METHOD']
    except KeyError:
        raise KeyError("Environment variable WEBDAV_AUTH_METHOD is not set, please use values X509 or TOKEN")

    if env_auth_method == "X509":
        try:
            proxy_cert = os.environ['WEBDAV_PROXY_CERT']
        except KeyError:
            raise KeyError("Environment variable WEBDAV_PROXY_CERT is not set")
        s.cert = (proxy_cert, proxy_cert)
    elif env_auth_method == "TOKEN":
        try:
            bearer_token = os.environ['WEBDAV_BEARER_TOKEN']
        except KeyError:
            raise KeyError("Environment variable WEBDAV_BEARER_TOKEN is not set")
        s.headers = {'Authorization': 'Bearer ' + bearer_token}
    else:
        raise ValueError("Environment variable WEBDAV_AUTH_METHOD must be set to X509 or TOKEN")

    # s.verify = False
    log.debug("Session configured and ready to use")

    return s


def webdavCheckFileExists(path: Union[Location, ButlerURI, str],
                          session: Optional[requests.Session] = None) -> Tuple[bool, int]:
    """Check that a remote HTTP resource exists."""
    if session is None:
        session = getHttpSession()

    filepath = getFileURL(path)

    r = session.head(filepath)
    return (True, r.headers['Content-Length']) if r.status_code == 200 else (False, -1)


def webdavDeleteFile(path: Union[Location, ButlerURI, str],
                     session: Optional[requests.Session] = None) -> None:
    """Check that a remote HTTP resource exists."""
    if session is None:
        session = getHttpSession()

    filepath = getFileURL(path)

    r = session.delete(filepath)
    if r.status_code not in [200, 202, 204]:
        raise FileNotFoundError(f"Unable to delete resource {filepath}; status code: {r.status_code}")


def folderExists(path: Union[Location, ButlerURI, str],
                 session: Optional[requests.Session] = None) -> bool:
    """Check if the Webdav repository at a given URL actually exists.

    Parameters
    ----------
    folderName : `str`
        Name of the Webdav folder
    session : `requests.Session`, optional
        Session object to query.

    Returns
    -------
    exists : `bool`
        True if it exists, False if no folder is found.
    """
    if session is None:
        session = getHttpSession()

    filepath = getFileURL(path)

    r = session.head(filepath)
    return True if r.status_code == 200 else False


def isWebdavEndpoint(path: Union[Location, ButlerURI, str]) -> bool:

    filepath = getFileURL(path)

    r = requests.options(filepath)
    return True if 'DAV' in r.headers else False


def getFileURL(path: Union[Location, ButlerURI, str]) -> str:

    if isinstance(path, str):
        filepath = path
    elif isinstance(path, ButlerURI):
        filepath = path.geturl()
    elif isinstance(path, Location):
        filepath = path.uri.geturl()

    return filepath
