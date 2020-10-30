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

from typing import (
    TYPE_CHECKING,
    Optional,
    Tuple,
    Union,
)

from .utils import NoTransaction
from ._butlerUri import ButlerURI

if TYPE_CHECKING:
    from ..datastore import DatastoreTransaction

log = logging.getLogger(__name__)


class ButlerHttpURI(ButlerURI):
    """General HTTP(S) resource."""
    _session = requests.Session()
    _sessionInitialized = False

    @property
    def session(self) -> requests.Session:
        """Client object to address remote resource."""
        from ..webdavutils import refreshToken, isTokenAuth, getHttpSession, isWebdavEndpoint
        if ButlerHttpURI._sessionInitialized:
            if isTokenAuth():
                refreshToken(ButlerHttpURI._session)
            return ButlerHttpURI._session

        baseURL = self.scheme + "://" + self.netloc

        if isWebdavEndpoint(baseURL):
            log.debug("%s looks like a Webdav endpoint.", baseURL)
            s = getHttpSession()

        ButlerHttpURI._session = s
        ButlerHttpURI._sessionInitialized = True
        return s

    def exists(self) -> bool:
        """Check that a remote HTTP resource exists."""
        log.debug("Checking if resource exists: %s", self.geturl())
        r = self.session.head(self.geturl())

        return True if r.status_code == 200 else False

    def size(self) -> int:
        if self.dirLike:
            return 0
        r = self.session.head(self.geturl())
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
            r = self.session.request("MKCOL", self.geturl())
            if r.status_code != 201:
                if r.status_code == 405:
                    log.debug("Can not create directory: %s may already exist: skipping.", self.geturl())
                else:
                    raise ValueError(f"Can not create directory {self}, status code: {r.status_code}")

    def remove(self) -> None:
        """Remove the resource."""
        log.debug("Removing resource: %s", self.geturl())
        r = self.session.delete(self.geturl())
        if r.status_code not in [200, 202, 204]:
            raise FileNotFoundError(f"Unable to delete resource {self}; status code: {r.status_code}")

    def as_local(self) -> Tuple[str, bool]:
        """Download object over HTTP and place in temporary directory.

        Returns
        -------
        path : `str`
            Path to local temporary file.
        temporary : `bool`
            Always returns `True`. This is always a temporary file.
        """
        log.debug("Downloading remote resource as local file: %s", self.geturl())
        r = self.session.get(self.geturl(), stream=True)
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
        r = self.session.get(self.geturl(), stream=stream)
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
        from ..webdavutils import finalurl
        log.debug("Writing to remote resource: %s", self.geturl())
        if not overwrite:
            if self.exists():
                raise FileExistsError(f"Remote resource {self} exists and overwrite has been disabled")
        dest_url = finalurl(self._emptyPut())
        r = self.session.put(dest_url, data=data)
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
        from ..webdavutils import finalurl
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
                r = self.session.request("MOVE", src.geturl(), headers={"Destination": self.geturl()})
                log.debug("Running move via MOVE HTTP request.")
            else:
                r = self.session.request("COPY", src.geturl(), headers={"Destination": self.geturl()})
                log.debug("Running copy via COPY HTTP request.")
        else:
            # Use local file and upload it
            local_src, is_temporary = src.as_local()
            f = open(local_src, "rb")
            dest_url = finalurl(self._emptyPut())
            r = self.session.put(dest_url, data=f)
            f.close()
            if is_temporary:
                os.remove(local_src)
            log.debug("Running transfer from a local copy of the file.")

        if r.status_code not in [201, 202, 204]:
            raise ValueError(f"Can not transfer file {self}, status code: {r.status_code}")

    def _emptyPut(self) -> requests.Response:
        """Send an empty PUT request to current URL. This is used to detect
        if redirection is enabled before sending actual data.

        Returns
        -------
        response : `requests.Response`
            HTTP Response from the endpoint.
        """
        return self.session.put(self.geturl(), data=None,
                                headers={"Content-Length": "0"}, allow_redirects=False)
