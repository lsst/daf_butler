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

__all__ = ("getHttpSession", "getWebdavClient", "webdavCheckFileExists", "folderExists")

import os
import requests
import urllib3
import logging

from typing import (
    Optional,
    Tuple,
    Union,
)

try:
    import webdav3.client as wc
except ImportError:
    wc = None

from webdav3.exceptions import WebDavException

from .location import ButlerURI, Location

log = logging.getLogger(__name__)
urllib3.disable_warnings()


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

    s.verify = False
    log.debug("Session configured and ready to use")

    return s


def getWebdavClient() -> wc.Client:
    """Create a Webdav client with the specified endpoint

    Returns
    -------
    client : `webdav3.client`
        A client of the Webdav service.

    Notes
    -----
    The endpoint URL is from the environment variable WEBDAV_ENDPOINT_URL
    (which must be set).
    The WEBDAV_AUTH_METHOD must also be set to obtain a client.

    Depending on the chosen method, additional
    environment variables are required:

    X509: must set WEBDAV_PROXY_CERT
    (path to proxy certificate used to authenticate requests)

    TOKEN: must set WEBDAV_BEARER_TOKEN
    (bearer token used to authenticate requests, as a single string)
    """
    log.debug("Creating new Webdav client")
    if wc is None:
        raise ModuleNotFoundError("Could not find webdav.client. "
                                  "Are you sure it is installed?")

    try:
        env_auth_method = os.environ['WEBDAV_AUTH_METHOD']
    except KeyError:
        raise KeyError("Environment variable WEBDAV_AUTH_METHOD is not set, please use values X509 or TOKEN")

    try:
        env_webdav_endpoint = os.environ['WEBDAV_ENDPOINT_URL']
    except KeyError:
        raise KeyError("Environment variable WEBDAV_ENDPOINT_URL is not set")

    if env_auth_method == "X509":
        try:
            env_webdav_cert = os.environ['WEBDAV_PROXY_CERT']
        except KeyError:
            raise KeyError("Environment variable WEBDAV_PROXY_CERT is not set")
        options = {
            'webdav_hostname': env_webdav_endpoint,
            'cert_path': env_webdav_cert,
            'key_path': env_webdav_cert,
            'verbose': False
        }
    elif env_auth_method == "TOKEN":
        try:
            env_webdav_token = os.environ['WEBDAV_BEARER_TOKEN']
        except KeyError:
            raise KeyError("Environment variable WEBDAV_BEARER_TOKEN is not set")
        options = {
            'webdav_hostname': env_webdav_endpoint,
            'webdav_token': env_webdav_token,
            'verbose': False
        }
    else:
        raise ValueError("Environment variable WEBDAV_AUTH_METHOD must be set to X509 or TOKEN")

    try:
        client = wc.Client(options)
    except WebDavException as exception:
        raise ValueError(f"Failure to create webdav client, \
                            please check your WEBDAV_ENDPOINT_URL \
                            and other environment variables") from exception

    client.verify = False
    log.debug("Webdav client configured and ready to use")

    return client


def webdavCheckFileExists(path: Union[Location, ButlerURI, str],
                          client: Optional[wc.Client] = None) -> Tuple[bool, int]:
    """Returns (True, filesize) if file exists in the Webdav repository
    and (False, -1) if the file is not found.

    Parameters
    ----------
    path : `Location`, `ButlerURI` or `str`
        Location or ButlerURI containing the endpoint URL and filepath.
    client : `webdav3.client`, optional
        Webdav Client object to query.

    Returns
    -------
    exists : `bool`
        True if file exists, False otherwise.
    size : `int`
        Size of the file, if file exists, in bytes, otherwise -1

    Notes
    -----
    Webdav Paths are sensitive to leading and trailing path separators.
    """
    if wc is None:
        raise ModuleNotFoundError("Could not find webdav3.client. "
                                  "Are you sure it is installed?")

    if client is None:
        client = getWebdavClient()

    filepath = path.relativeToPathRoot

    if client.check(filepath):
        try:
            size = client.info(filepath)["size"]
        except WebDavException as exception:
            raise ValueError(f"Failed to retrieve size of file, please check your permissions") from exception
        log.debug("File %s exists with size %s", filepath, size)
        return (True, size)

    log.debug("File %s does not exist", filepath)
    return (False, -1)


def folderExists(folderName: str, client: Optional[wc.Client] = None) -> bool:
    """Check if the Webdav repository with the given name actually exists.

    Parameters
    ----------
    folderName : `str`
        Name of the Webdav folder
    client : `webdav3.client`, optional
        Webdav Client object to query.

    Returns
    -------
    exists : `bool`
        True if it exists, False if no folder is found.
    """
    if wc is None:
        raise ModuleNotFoundError("Could not find webdav3.client. "
                                  "Are you sure it is installed?")

    if client is None:
        client = getWebdavClient()

    exists = client.check(folderName)
    log.debug("Folder %s exists: %s", folderName, exists)

    return exists
