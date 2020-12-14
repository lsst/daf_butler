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

import os
import os.path
import requests
import tempfile
import logging

__all__ = ('ButlerHttpURI', )

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from typing import (
    TYPE_CHECKING,
    Optional,
    Tuple,
    Union,
)

from .utils import NoTransaction
from ._butlerUri import ButlerURI
from ..location import Location

if TYPE_CHECKING:
    from ..datastore import DatastoreTransaction

log = logging.getLogger(__name__)


def getHttpSession() -> requests.Session:
    """Create a requests.Session pre-configured with environment variable data

    Returns
    -------
    session : `requests.Session`
        An http session used to execute requests.

    Notes
    -----
    The following environment variables must be set:
    - LSST_BUTLER_WEBDAV_CA_BUNDLE: the directory where CA
        certificates are stored if you intend to use HTTPS to
        communicate with the endpoint.
    - LSST_BUTLER_WEBDAV_AUTH: which authentication method to use.
        Possible values are X509 and TOKEN
    - (X509 only) LSST_BUTLER_WEBDAV_PROXY_CERT: path to proxy
        certificate used to authenticate requests
    - (TOKEN only) LSST_BUTLER_WEBDAV_TOKEN_FILE: file which
        contains the bearer token used to authenticate requests
    - (OPTIONAL) LSST_BUTLER_WEBDAV_EXPECT100: if set, we will add an
        "Expect: 100-Continue" header in all requests. This is required
        on certain endpoints where requests redirection is made.
    """

    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])

    session = requests.Session()
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))

    log.debug("Creating new HTTP session...")

    try:
        env_auth_method = os.environ['LSST_BUTLER_WEBDAV_AUTH']
    except KeyError:
        raise KeyError("Environment variable LSST_BUTLER_WEBDAV_AUTH is not set, "
                       "please use values X509 or TOKEN")

    if env_auth_method == "X509":
        log.debug("... using x509 authentication.")
        try:
            proxy_cert = os.environ['LSST_BUTLER_WEBDAV_PROXY_CERT']
        except KeyError:
            raise KeyError("Environment variable LSST_BUTLER_WEBDAV_PROXY_CERT is not set")
        session.cert = (proxy_cert, proxy_cert)
    elif env_auth_method == "TOKEN":
        log.debug("... using bearer-token authentication.")
        refreshToken(session)
    else:
        raise ValueError("Environment variable LSST_BUTLER_WEBDAV_AUTH must be set to X509 or TOKEN")

    ca_bundle = None
    try:
        ca_bundle = os.environ['LSST_BUTLER_WEBDAV_CA_BUNDLE']
    except KeyError:
        log.warning("Environment variable LSST_BUTLER_WEBDAV_CA_BUNDLE is not set: "
                    "HTTPS requests will fail. If you intend to use HTTPS, please "
                    "export this variable.")

    session.verify = ca_bundle
    log.debug("Session configured and ready.")

    return session

def _defaultTimeout() -> float:
    timeout = 20
    return timeout


def expect100() -> bool:
    # This header is required for request redirection, in dCache for example
    if "LSST_BUTLER_WEBDAV_EXPECT100" in os.environ:
        log.debug("Expect: 100-Continue header enabled.")
        return True
    return False


def isTokenAuth() -> bool:
    """Returns the status of bearer-token authentication.

    Returns
    -------
    isTokenAuth : `bool`
        True if LSST_BUTLER_WEBDAV_AUTH is set to TOKEN, False otherwise.
    """
    try:
        env_auth_method = os.environ['LSST_BUTLER_WEBDAV_AUTH']
    except KeyError:
        raise KeyError("Environment variable LSST_BUTLER_WEBDAV_AUTH is not set, "
                       "please use values X509 or TOKEN")

    if env_auth_method == "TOKEN":
        return True
    return False


def refreshToken(session: requests.Session) -> None:
    """Set or update the 'Authorization' header of the session,
    configure bearer token authentication, with the value fetched
    from LSST_BUTLER_WEBDAV_TOKEN_FILE

    Parameters
    ----------
    session : `requests.Session`
        Session on which bearer token authentication must be configured
    """
    try:
        token_path = os.environ['LSST_BUTLER_WEBDAV_TOKEN_FILE']
        if not os.path.isfile(token_path):
            raise FileNotFoundError(f"No token file: {token_path}")
        with open(os.environ['LSST_BUTLER_WEBDAV_TOKEN_FILE'], "r") as fh:
            bearer_token = fh.read().replace('\n', '')
    except KeyError:
        raise KeyError("Environment variable LSST_BUTLER_WEBDAV_TOKEN_FILE is not set")

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

    r = session.head(filepath, timeout=_defaultTimeout())
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
    r = session.delete(filepath, timeout=_defaultTimeout())
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
    r = session.head(filepath, timeout=_defaultTimeout())
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
    ca_bundle = None
    try:
        ca_bundle = os.environ['LSST_BUTLER_WEBDAV_CA_BUNDLE']
    except KeyError:
        log.warning("Environment variable LSST_BUTLER_WEBDAV_CA_BUNDLE is not set: "
                    "HTTPS requests will fail. If you intend to use HTTPS, please "
                    "export this variable.")
    filepath = _getFileURL(path)

    log.debug("Detecting HTTP endpoint type...")
    r = requests.options(filepath, verify=ca_bundle)
    return True if 'DAV' in r.headers else False


def finalurl(r: requests.Response) -> str:
    """Check whether the remote HTTP endpoint redirects to a different
    endpoint, and return the final destination of the request.
    This is needed when using PUT operations, to avoid starting
    to send the data to the endpoint, before having to send it again once
    the 307 redirect response is received, and thus wasting bandwidth.

    Parameters
    ----------
    r : `requests.Response`
        An HTTP response received when requesting the endpoint

    Returns
    -------
    destination_url: `string`
        The final destination to which requests must be sent.
    """
    destination_url = r.url
    if r.status_code == 307:
        destination_url = r.headers['Location']
        log.debug("Request redirected to %s", destination_url)
    return destination_url


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


class ButlerHttpURI(ButlerURI):
    """General HTTP(S) resource."""
    _session = requests.Session()
    _sessionInitialized = False

    @property
    def session(self) -> requests.Session:
        """Client object to address remote resource."""
        if ButlerHttpURI._sessionInitialized:
            if isTokenAuth():
                refreshToken(ButlerHttpURI._session)
            return ButlerHttpURI._session

        baseURL = self.scheme + "://" + self.netloc

        if isWebdavEndpoint(baseURL):
            log.debug("%s looks like a Webdav endpoint.", baseURL)
            s = getHttpSession()
        else:
            raise RuntimeError(f"Only Webdav endpoints are supported; got base URL '{baseURL}'.")

        ButlerHttpURI._session = s
        ButlerHttpURI._sessionInitialized = True
        return s

    def exists(self) -> bool:
        """Check that a remote HTTP resource exists."""
        log.debug("Checking if resource exists: %s", self.geturl())
        r = self.session.head(self.geturl(), timeout=_defaultTimeout())

        return True if r.status_code == 200 else False

    def size(self) -> int:
        if self.dirLike:
            return 0
        r = self.session.head(self.geturl(), timeout=_defaultTimeout())
        if r.status_code == 200:
            return int(r.headers['Content-Length'])
        else:
            raise FileNotFoundError(f"Resource {self} does not exist")

    def mkdir(self) -> None:
        """For a dir-like URI, create the directory resource if it does not
        already exist.
        """
        if not self.dirLike:
            raise ValueError(f"Can not create a 'directory' for file-like URI {self}")

        if not self.exists():
            # We need to test the absence of the parent directory,
            # but also if parent URL is different from self URL,
            # otherwise we could be stuck in a recursive loop
            # where self == parent
            if not self.parent().exists() and self.parent().geturl() != self.geturl():
                self.parent().mkdir()
            log.debug("Creating new directory: %s", self.geturl())
            r = self.session.request("MKCOL", self.geturl(), timeout=_defaultTimeout())
            if r.status_code != 201:
                if r.status_code == 405:
                    log.debug("Can not create directory: %s may already exist: skipping.", self.geturl())
                else:
                    raise ValueError(f"Can not create directory {self}, status code: {r.status_code}")

    def remove(self) -> None:
        """Remove the resource."""
        log.debug("Removing resource: %s", self.geturl())
        r = self.session.delete(self.geturl(), timeout=_defaultTimeout())
        if r.status_code not in [200, 202, 204]:
            raise FileNotFoundError(f"Unable to delete resource {self}; status code: {r.status_code}")

    def _as_local(self) -> Tuple[str, bool]:
        """Download object over HTTP and place in temporary directory.

        Returns
        -------
        path : `str`
            Path to local temporary file.
        temporary : `bool`
            Always returns `True`. This is always a temporary file.
        """
        log.debug("Downloading remote resource as local file: %s", self.geturl())
        r = self.session.get(self.geturl(), stream=True, timeout=_defaultTimeout())
        if r.status_code != 200:
            raise FileNotFoundError(f"Unable to download resource {self}; status code: {r.status_code}")
        with tempfile.NamedTemporaryFile(suffix=self.getExtension(), delete=False) as tmpFile:
            for chunk in r.iter_content():
                tmpFile.write(chunk)
        return tmpFile.name, True

    def read(self, size: int = -1) -> bytes:
        """Open the resource and return the contents in bytes.

        Parameters
        ----------
        size : `int`, optional
            The number of bytes to read. Negative or omitted indicates
            that all data should be read.
        """
        log.debug("Reading from remote resource: %s", self.geturl())
        stream = True if size > 0 else False
        r = self.session.get(self.geturl(), stream=stream, timeout=_defaultTimeout())
        if r.status_code != 200:
            raise FileNotFoundError(f"Unable to read resource {self}; status code: {r.status_code}")
        if not stream:
            return r.content
        else:
            return next(r.iter_content(chunk_size=size))

    def write(self, data: bytes, overwrite: bool = True) -> None:
        """Write the supplied bytes to the new resource.

        Parameters
        ----------
        data : `bytes`
            The bytes to write to the resource. The entire contents of the
            resource will be replaced.
        overwrite : `bool`, optional
            If `True` the resource will be overwritten if it exists. Otherwise
            the write will fail.
        """
        log.debug("Writing to remote resource: %s", self.geturl())
        if not overwrite:
            if self.exists():
                raise FileExistsError(f"Remote resource {self} exists and overwrite has been disabled")
        dest_url = finalurl(self._emptyPut())
        r = self.session.put(dest_url, data=data, timeout=_defaultTimeout())
        if r.status_code not in [201, 202, 204]:
            raise ValueError(f"Can not write file {self}, status code: {r.status_code}")

    def transfer_from(self, src: ButlerURI, transfer: str = "copy",
                      overwrite: bool = False,
                      transaction: Optional[Union[DatastoreTransaction, NoTransaction]] = None) -> None:
        """Transfer the current resource to a Webdav repository.

        Parameters
        ----------
        src : `ButlerURI`
            Source URI.
        transfer : `str`
            Mode to use for transferring the resource. Supports the following
            options: copy.
        transaction : `DatastoreTransaction`, optional
            Currently unused.
        """
        # Fail early to prevent delays if remote resources are requested
        if transfer not in self.transferModes:
            raise ValueError(f"Transfer mode {transfer} not supported by URI scheme {self.scheme}")

        log.debug(f"Transferring {src} [exists: {src.exists()}] -> "
                  f"{self} [exists: {self.exists()}] (transfer={transfer})")

        if self.exists():
            raise FileExistsError(f"Destination path {self} already exists.")

        if transfer == "auto":
            transfer = self.transferDefault

        if isinstance(src, type(self)):
            if transfer == "move":
                r = self.session.request("MOVE", src.geturl(), headers={"Destination": self.geturl()}, timeout=_defaultTimeout())
                log.debug("Running move via MOVE HTTP request.")
            else:
                r = self.session.request("COPY", src.geturl(), headers={"Destination": self.geturl()}, timeout=_defaultTimeout())
                log.debug("Running copy via COPY HTTP request.")
        else:
            # Use local file and upload it
            with src.as_local() as local_uri:
                with open(local_uri.ospath, "rb") as f:
                    dest_url = finalurl(self._emptyPut())
                    r = self.session.put(dest_url, data=f, timeout=_defaultTimeout())
            log.debug("Uploading URI %s to %s via local file", src, self)

        if r.status_code not in [201, 202, 204]:
            raise ValueError(f"Can not transfer file {self}, status code: {r.status_code}")

        # This was an explicit move requested from a remote resource
        # try to remove that resource
        if transfer == "move":
            # Transactions do not work here
            src.remove()

    def _emptyPut(self) -> requests.Response:
        """Send an empty PUT request to current URL. This is used to detect
        if redirection is enabled before sending actual data.

        Returns
        -------
        response : `requests.Response`
            HTTP Response from the endpoint.
        """
        if expect100():
            return self.session.put(self.geturl(), data=None,
                                    headers={"Expect": "100-continue", "Content-Length": "0"},
                                    allow_redirects=False, timeout=_defaultTimeout())
        return self.session.put(self.geturl(), data=None,
                                headers={"Content-Length": "0"}, allow_redirects=False, timeout=_defaultTimeout())
