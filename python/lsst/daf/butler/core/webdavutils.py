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
           "folderExists", "webdavDeleteFile")

import os
import requests
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

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
    session : `requests.Session`
        An http session used to execute requests.

    Notes
    -----
    The WEBDAV_AUTH_METHOD must be set to obtain a session.
    Depending on the chosen method, additional
    environment variables are required:

    X509: must set WEBDAV_PROXY_CERT
    (path to proxy certificate used to authenticate requests)

    TOKEN: must set WEBDAV_TOKEN_FILE
    (file which contains the bearer token used to
    authenticate requests, as a simple string)

    NB: requests will read CA certificates in REQUESTS_CA_BUNDLE
    It must be manually exported according to the system CA directory.
    """

    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])

    session = requests.Session()
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))

    log.debug("Creating new HTTP session...")

    try:
        env_auth_method = os.environ['WEBDAV_AUTH_METHOD']
    except KeyError:
        raise KeyError("Environment variable WEBDAV_AUTH_METHOD is not set, please use values X509 or TOKEN")

    if env_auth_method == "X509":
        try:
            proxy_cert = os.environ['WEBDAV_PROXY_CERT']
        except KeyError:
            raise KeyError("Environment variable WEBDAV_PROXY_CERT is not set")
        log.debug("... using x509 authentication.")
        session.cert = (proxy_cert, proxy_cert)
    elif env_auth_method == "TOKEN":
        refreshToken(session)
        log.debug("... using bearer-token authentication.")

    else:
        raise ValueError("Environment variable WEBDAV_AUTH_METHOD must be set to X509 or TOKEN")

    log.debug("Session configured and ready.")

    return session


def refreshToken(session: requests.Session) -> None:
    """Set or update the 'Authorization' header of the session,
    configure bearer token authentication, with the value of the token
    fetched from WEBDAV_TOKEN_FILE

    Parameters
    ----------
    session : `requests.Session`
        Session on which bearer token authentication must be configured
    """
    try:
        token_path = os.environ['WEBDAV_TOKEN_FILE']
        if not os.path.isfile(token_path):
            raise FileNotFoundError(f"No token file: {token_path}")

        bearer_token = open(os.environ['WEBDAV_TOKEN_FILE'], 'r').read().replace('\n', '')
        log.debug("Using token %s", bearer_token)
    except KeyError:
        raise KeyError("Environment variable WEBDAV_TOKEN_FILE is not set")

    session.headers.update({'Authorization': 'Bearer ' + bearer_token})


def webdavCheckFileExists(path: Union[Location, ButlerURI, str],
                          session: Optional[requests.Session] = None) -> Tuple[bool, int]:
    """Check that a remote HTTP resource exists.

    Parameters
    ----------
    path : `Location`, `ButlerURI` or `str`
        Location or ButlerURI containing the bucket name and filepath.
    session : `requests.Session`, optional
        Session object to query.

    Returns
    -------
    exists : `bool`
        True if resource exists, False otherwise.
    size : `int`
        Size of the resource, if it exists, in bytes, otherwise -1
    """
    if session is None:
        session = getHttpSession()

    filepath = _getFileURL(path)

    log.debug("Checking if file exists: %s", filepath)

    r = session.head(filepath)
    return (True, int(r.headers['Content-Length'])) if r.status_code == 200 else (False, -1)


def webdavDeleteFile(path: Union[Location, ButlerURI, str],
                     session: Optional[requests.Session] = None) -> None:
    """Remove a remote HTTP resource.
    Raises a FileNotFoundError if the resource does not exist or on failure.

    Parameters
    ----------
    path : `Location`, `ButlerURI` or `str`
        Location or ButlerURI containing the bucket name and filepath.
    session : `requests.Session`, optional
        Session object to query.
    """
    if session is None:
        session = getHttpSession()

    filepath = _getFileURL(path)

    log.debug("Removing file: %s", filepath)
    r = session.delete(filepath)
    if r.status_code not in [200, 202, 204]:
        raise FileNotFoundError(f"Unable to delete resource {filepath}; status code: {r.status_code}")


def folderExists(path: Union[Location, ButlerURI, str],
                 session: Optional[requests.Session] = None) -> bool:
    """Check if the Webdav repository at a given URL actually exists.

    Parameters
    ----------
    path : `Location`, `ButlerURI` or `str`
        Location or ButlerURI containing the bucket name and filepath.
    session : `requests.Session`, optional
        Session object to query.

    Returns
    -------
    exists : `bool`
        True if it exists, False if no folder is found.
    """
    if session is None:
        session = getHttpSession()

    filepath = _getFileURL(path)

    log.debug("Checking if folder exists: %s", filepath)
    r = session.head(filepath)
    return True if r.status_code == 200 else False


def isWebdavEndpoint(path: Union[Location, ButlerURI, str]) -> bool:
    """Check whether the remote HTTP endpoint implements Webdav features.

    Parameters
    ----------
    path : `Location`, `ButlerURI` or `str`
        Location or ButlerURI containing the bucket name and filepath.

    Returns
    -------
    isWebdav : `bool`
        True if the endpoint implements Webdav, False if it doesn't.
    """
    filepath = _getFileURL(path)

    log.debug("Detecting HTTP endpoint type...")
    r = requests.options(filepath)
    return True if 'DAV' in r.headers else False


def _getFileURL(path: Union[Location, ButlerURI, str]) -> str:
    """Returns the absolute URL of the resource as a string.

    Parameters
    ----------
    path : `Location`, `ButlerURI` or `str`
        Location or ButlerURI containing the bucket name and filepath.

    Returns
    -------
    filepath : `str`
        The fully qualified URL of the resource.
    """
    if isinstance(path, Location):
        filepath = path.uri.geturl()
    else:
        filepath = ButlerURI(path).geturl()
    return filepath
