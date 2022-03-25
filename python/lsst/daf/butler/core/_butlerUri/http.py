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

import functools
import logging
import os
import os.path
import random
import stat
import tempfile

import requests

__all__ = ("ButlerHttpURI",)

from typing import TYPE_CHECKING, BinaryIO, Optional, Tuple, Union

from requests.adapters import HTTPAdapter
from requests.auth import AuthBase
from urllib3.util.retry import Retry

from ..utils import time_this
from ._butlerUri import ButlerURI
from .utils import NoTransaction

if TYPE_CHECKING:
    from ..datastore import DatastoreTransaction

log = logging.getLogger(__name__)


# Default timeouts for all HTTP requests, in seconds.
DEFAULT_TIMEOUT_CONNECT = 60
DEFAULT_TIMEOUT_READ = 300

# Allow for network timeouts to be set in the environment.
TIMEOUT = (
    int(os.environ.get("LSST_HTTP_TIMEOUT_CONNECT", DEFAULT_TIMEOUT_CONNECT)),
    int(os.environ.get("LSST_HTTP_TIMEOUT_READ", DEFAULT_TIMEOUT_READ)),
)

# Should we send a "Expect: 100-continue" header on PUT requests?
# The "Expect: 100-continue" header is used by some servers (e.g. dCache)
# as an indication that the client knows how to handle redirects to
# the specific server that will actually receive the data for PUT
# requests.
_SEND_EXPECT_HEADER_ON_PUT = "LSST_HTTP_PUT_SEND_EXPECT_HEADER" in os.environ


class BearerTokenAuth(AuthBase):
    """Attach a bearer token 'Authorization' header to each request.

    Parameters
    ----------
    token : `str`
        Can be either the path to a local protected file which contains the
        value of the token or the token itself.
    """

    def __init__(self, token: str):
        self._token = self._path = None
        self._mtime: float = -1.0
        if not token:
            return

        self._token = token
        if os.path.isfile(token):
            self._path = os.path.abspath(token)
            if not _is_protected(self._path):
                raise PermissionError(
                    f"Bearer token file at {self._path} must be protected for access only by its owner"
                )
            self._refresh()

    def _refresh(self) -> None:
        """Read the token file (if any) if its modification time is more recent
        than the last time we read it.
        """
        if not self._path:
            return

        if (mtime := os.stat(self._path).st_mtime) > self._mtime:
            log.debug("Reading bearer token file at %s", self._path)
            self._mtime = mtime
            with open(self._path) as f:
                self._token = f.read().rstrip("\n")

    def __call__(self, req: requests.PreparedRequest) -> requests.PreparedRequest:
        if self._token:
            self._refresh()
            req.headers["Authorization"] = f"Bearer {self._token}"
        return req


class SessionStore:
    """Cache a single reusable HTTP client session per enpoint."""

    def __init__(self) -> None:
        # The key of the dictionary is a root URI and the value is the
        # session
        self._sessions: dict[str, requests.Session] = {}

    def get(self, rpath: ButlerHttpURI, persist: bool = True) -> requests.Session:
        """Retrieve a session for accessing the remote resource at rpath.

        Parameters
        ----------
        rpath : `ButlerHttpURI`
            URL to a resource at the remote server for which a session is to
            be retrieved.

        persist : `bool`
            if `True`, make the network connection with the front end server
            of the endpoint  persistent. Connections to the backend servers
            are persisted.

        Notes
        -----
        Once a session is created for a given endpoint it is cached and
        returned every time a session is requested for any path under that same
        endpoint. For instance, a single session will be cached and shared
        for paths "https://www.example.org/path/to/file" and
        "https://www.example.org/any/other/path".

        Note that "https://www.example.org" and "https://www.example.org:12345"
        will have different sessions since the port number is not identical.

        In order to configure the session, some environment variables are
        inspected:

        - LSST_HTTP_CACERT_BUNDLE: path to a .pem file containing the CA
            certificates to trust when verifying the server's certificate.

        - LSST_HTTP_AUTH_BEARER_TOKEN: value of a bearer token or path to a
            local file containing a bearer token to be used as the client
            authentication mechanism with all requests.
            The permissions of the token file must be set so that only its
            owner can access it.
            If initialized, takes precedence over LSST_HTTP_AUTH_CLIENT_CERT
            and LSST_HTTP_AUTH_CLIENT_KEY.

        - LSST_HTTP_AUTH_CLIENT_CERT: path to a .pem file which contains the
            client certificate for authenticating to the server.
            If initialized, the variable LSST_HTTP_AUTH_CLIENT_KEY must also be
            initialized with the path of the client private key file.
            The permissions of the client private key must be set so that only
            its owner can access it, at least for reading.
        """
        root_uri = str(rpath.root_uri())
        if root_uri not in self._sessions:
            # We don't have yet a session for this endpoint: create a new one
            self._sessions[root_uri] = self._make_session(rpath, persist)
        return self._sessions[root_uri]

    def _make_session(self, rpath: ButlerHttpURI, persist: bool) -> requests.Session:
        """Make a new session configured from values from the environment."""
        session = requests.Session()
        root_uri = str(rpath.root_uri())
        log.debug(
            "Creating new HTTP session for endpoint %s (persist connection=%s)...",
            root_uri,
            persist,
        )

        retries = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=5.0 + random.random(),
            status=3,
            status_forcelist=[429, 500, 502, 503, 504],
        )

        # Persist a single connection to the front end server, if required
        num_connections = 1 if persist else 0
        session.mount(
            root_uri,
            HTTPAdapter(
                pool_connections=1,
                pool_maxsize=num_connections,
                pool_block=False,
                max_retries=retries,
            ),
        )

        # Prevent persisting connections to back-end servers which may vary
        # from request to request. Systematically persisting connections to
        # those servers may exhaust their capabilities when there are thousands
        # of simultaneous clients
        session.mount(
            f"{rpath.scheme}://",
            HTTPAdapter(
                pool_connections=1,
                pool_maxsize=0,
                pool_block=False,
                max_retries=retries,
            ),
        )

        # Should we use a specific CA cert bundle for authenticating the
        # server?
        session.verify = True
        if ca_bundle := os.getenv("LSST_HTTP_CACERT_BUNDLE"):
            session.verify = ca_bundle
        else:
            log.debug(
                "Environment variable LSST_HTTP_CACERT_BUNDLE is not set: "
                "if you would need to verify the remote server's certificate "
                "issued by specific certificate authorities please consider "
                "initializing this variable."
            )

        # Should we use bearer tokens for client authentication?
        if token := os.getenv("LSST_HTTP_AUTH_BEARER_TOKEN"):
            log.debug("... using bearer token authentication")
            session.auth = BearerTokenAuth(token)
            return session

        # Should we instead use client certificate and private key? If so, both
        # LSST_HTTP_AUTH_CLIENT_CERT and LSST_HTTP_AUTH_CLIENT_KEY must be
        # initialized.
        client_cert = os.getenv("LSST_HTTP_AUTH_CLIENT_CERT")
        client_key = os.getenv("LSST_HTTP_AUTH_CLIENT_KEY")
        if client_cert and client_key:
            if not _is_protected(client_key):
                raise PermissionError(
                    f"Private key file at {client_key} must be protected for access only by its owner"
                )
            log.debug("... using client certificate authentication.")
            session.cert = (client_cert, client_key)
            return session

        if client_cert:
            # Only the client certificate was provided.
            raise ValueError(
                "Environment variable LSST_HTTP_AUTH_CLIENT_KEY must be set to client private key file path"
            )

        if client_key:
            # Only the client private key was provided.
            raise ValueError(
                "Environment variable LSST_HTTP_AUTH_CLIENT_CERT must be set to client certificate file path"
            )

        log.debug(
            "Neither LSST_HTTP_AUTH_BEARER_TOKEN nor (LSST_HTTP_AUTH_CLIENT_CERT and "
            "LSST_HTTP_AUTH_CLIENT_KEY) are initialized. Client authentication is disabled."
        )
        return session


@functools.lru_cache
def _is_webdav_endpoint(path: Union[ButlerURI, str]) -> bool:
    """Check whether the remote HTTP endpoint implements Webdav features.

    Parameters
    ----------
    path : `ButlerURI` or `str`
        URL to the resource to be checked.
        Should preferably refer to the root since the status is shared
        by all paths in that server.

    Returns
    -------
    isWebdav : `bool`
        True if the endpoint implements Webdav, False if it doesn't.
    """
    if (ca_cert_bundle := os.getenv("LSST_HTTP_CACERT_BUNDLE")) is None:
        log.warning(
            "Environment variable LSST_HTTP_CACERT_BUNDLE is not set: "
            "some HTTPS requests may fail if remote server presents a "
            "certificate issued by an unknown certificate authority."
        )

    log.debug("Detecting HTTP endpoint type for '%s'...", path)
    verify: Union[bool, str] = ca_cert_bundle if ca_cert_bundle else True
    resp = requests.options(str(path), verify=verify)
    return "DAV" in resp.headers


# Tuple (path, block_size) pointing to the location of a local directory
# to save temporary files and the block size of the underlying file system
_TMPDIR: Optional[Tuple[str, int]] = None


def _get_temp_dir() -> Tuple[str, int]:
    """Return the temporary directory path and block size.
    This function caches its results in _TMPDIR.
    """
    global _TMPDIR
    if _TMPDIR:
        return _TMPDIR

    # Use the value of environment variables 'LSST_RESOURCES_TMPDIR' or
    # 'TMPDIR', if defined. Otherwise use current working directory
    tmpdir = os.getcwd()
    for dir in (os.getenv(v) for v in ("LSST_RESOURCES_TMPDIR", "TMPDIR")):
        if dir and os.path.isdir(dir):
            tmpdir = dir
            break

    # Compute the block size as 256 blocks of typical size
    # (i.e. 4096 bytes) or 10 times the file system block size,
    # whichever is higher. This is a reasonable compromise between
    # using memory for buffering and the number of system calls
    # issued to read from or write to temporary files
    fsstats = os.statvfs(tmpdir)
    return (_TMPDIR := (tmpdir, max(10 * fsstats.f_bsize, 256 * 4096)))


class ButlerHttpURI(ButlerURI):
    """General HTTP(S) resource."""

    _is_webdav: Optional[bool] = None
    _sessions_store = SessionStore()
    _put_sessions_store = SessionStore()

    @property
    def session(self) -> requests.Session:
        """Client session to address remote resource for all HTTP methods but
        PUT.
        """
        if hasattr(self, "_session"):
            return self._session

        self._session: requests.Session = self._sessions_store.get(self)
        return self._session

    @property
    def put_session(self) -> requests.Session:
        """Client session for uploading data to the remote resource."""
        if hasattr(self, "_put_session"):
            return self._put_session

        self._put_session: requests.Session = self._put_sessions_store.get(self)
        return self._put_session

    @property
    def is_webdav_endpoint(self) -> bool:
        """Check if the current endpoint implements WebDAV features.

        This is stored per URI but cached by root so there is
        only one check per hostname.
        """
        if self._is_webdav is not None:
            return self._is_webdav

        self._is_webdav = _is_webdav_endpoint(self.root_uri())
        return self._is_webdav

    def exists(self) -> bool:
        """Check that a remote HTTP resource exists."""
        log.debug("Checking if resource exists: %s", self.geturl())
        resp = self.session.head(self.geturl(), timeout=TIMEOUT)
        return resp.status_code == 200

    def size(self) -> int:
        """Return the size of the remote resource in bytes."""
        if self.dirLike:
            return 0

        resp = self.session.head(self.geturl(), timeout=TIMEOUT)
        if resp.status_code != 200:
            raise FileNotFoundError(f"Resource {self} does not exist")
        return int(resp.headers["Content-Length"])

    def mkdir(self) -> None:
        """Create the directory resource if it does not already exist."""
        # Only available on WebDAV backends
        if not self.is_webdav_endpoint:
            raise NotImplementedError(
                "Endpoint does not implement WebDAV functionality"
            )

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
            r = self.session.request("MKCOL", self.geturl(), timeout=TIMEOUT)
            if r.status_code != 201:
                if r.status_code == 405:
                    log.debug(
                        "Can not create directory: %s may already exist: skipping.",
                        self.geturl(),
                    )
                else:
                    raise ValueError(
                        f"Can not create directory {self}, status code: {r.status_code}"
                    )

    def remove(self) -> None:
        """Remove the resource."""
        log.debug("Removing resource: %s", self.geturl())
        r = self.session.delete(self.geturl(), timeout=TIMEOUT)
        if r.status_code not in [200, 202, 204]:
            raise FileNotFoundError(
                f"Unable to delete resource {self}; status code: {r.status_code}"
            )

    def _as_local(self) -> Tuple[str, bool]:
        """Download object over HTTP and place in temporary directory.

        Returns
        -------
        path : `str`
            Path to local temporary file.
        temporary : `bool`
            Always returns `True`. This is always a temporary file.
        """
        r = self.session.get(self.geturl(), stream=True, timeout=TIMEOUT)
        if r.status_code != 200:
            raise FileNotFoundError(
                f"Unable to download resource {self}; status code: {r.status_code}"
            )
        tmpdir, buffering = _get_temp_dir()
        with tempfile.NamedTemporaryFile(
            suffix=self.getExtension(), buffering=buffering, dir=tmpdir, delete=False
        ) as tmpFile:
            with time_this(
                log,
                msg="Downloading %s [length=%s] to local file %s [chunk_size=%d]",
                args=(self, r.headers.get("Content-Length"), tmpFile.name, buffering),
            ):
                for chunk in r.iter_content(chunk_size=buffering):
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
        with time_this(log, msg="Read from remote resource %s", args=(self,)):
            r = self.session.get(self.geturl(), stream=stream, timeout=TIMEOUT)
        if r.status_code != 200:
            raise FileNotFoundError(
                f"Unable to read resource {self}; status code: {r.status_code}"
            )
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
                raise FileExistsError(
                    f"Remote resource {self} exists and overwrite has been disabled"
                )
        with time_this(
            log, msg="Write to remote %s (%d bytes)", args=(self, len(data))
        ):
            self._do_put(data=data)

    def transfer_from(
        self,
        src: ButlerURI,
        transfer: str = "copy",
        overwrite: bool = False,
        transaction: Optional[Union[DatastoreTransaction, NoTransaction]] = None,
    ) -> None:
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
            raise ValueError(
                f"Transfer mode {transfer} not supported by URI scheme {self.scheme}"
            )

        # Existence checks cost time so do not call this unless we know
        # that debugging is enabled.
        if log.isEnabledFor(logging.DEBUG):
            log.debug(
                "Transferring %s [exists: %s] -> %s [exists: %s] (transfer=%s)",
                src,
                src.exists(),
                self,
                self.exists(),
                transfer,
            )

        if self.exists():
            raise FileExistsError(f"Destination path {self} already exists.")

        if transfer == "auto":
            transfer = self.transferDefault

        if isinstance(src, type(self)):
            # Only available on WebDAV backends
            if not self.is_webdav_endpoint:
                raise NotImplementedError(
                    "Endpoint does not implement WebDAV functionality"
                )

            with time_this(
                log, msg="Transfer from %s to %s directly", args=(src, self)
            ):
                method = "MOVE" if transfer == "move" else "COPY"
                log.debug("%s from %s to %s", method, src.geturl(), self.geturl())
                resp = self.session.request(
                    method,
                    src.geturl(),
                    headers={"Destination": self.geturl()},
                    timeout=TIMEOUT,
                )
                if resp.status_code not in [201, 202, 204]:
                    raise ValueError(
                        f"Can not transfer file {self}, status code: {resp.status_code}"
                    )
        else:
            # Use local file and upload it
            with src.as_local() as local_uri:
                with open(local_uri.ospath, "rb") as f:
                    with time_this(
                        log,
                        msg="Transfer from %s to %s via local file",
                        args=(src, self),
                    ):
                        self._do_put(data=f)

            # This was an explicit move requested from a remote resource
            # try to remove that resource
            if transfer == "move":
                # Transactions do not work here
                src.remove()

    def _do_put(self, data: Union[BinaryIO, bytes]) -> None:
        """Perform an HTTP PUT request taking into account redirection."""
        final_url = self.geturl()
        if _SEND_EXPECT_HEADER_ON_PUT:
            # Do a PUT request with an empty body and retrieve the final
            # destination URL returned by the server.
            headers = {"Content-Length": "0", "Expect": "100-continue"}
            resp = self.put_session.put(
                final_url,
                data=None,
                headers=headers,
                allow_redirects=False,
                timeout=TIMEOUT,
            )
            if resp.is_redirect or resp.is_permanent_redirect:
                final_url = resp.headers["Location"]
                log.debug(
                    "PUT request to %s redirected to %s", self.geturl(), final_url
                )

        # Send data to its final destination.
        resp = self.put_session.put(final_url, data=data, timeout=TIMEOUT)
        if resp.status_code not in [201, 202, 204]:
            raise ValueError(
                f"Can not write file {self}, status code: {resp.status_code}"
            )


def _is_protected(filepath: str) -> bool:
    """Return true if the permissions of file at filepath only allow for access
    by its owner.

    Parameters
    ----------
    filepath : `str`
        Path of a local file.
    """
    if not os.path.isfile(filepath):
        return False
    mode = stat.S_IMODE(os.stat(filepath).st_mode)
    owner_accessible = bool(mode & stat.S_IRWXU)
    group_accessible = bool(mode & stat.S_IRWXG)
    other_accessible = bool(mode & stat.S_IRWXO)
    return owner_accessible and not group_accessible and not other_accessible
