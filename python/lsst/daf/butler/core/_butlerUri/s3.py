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

import logging
import re
import tempfile

__all__ = ('ButlerS3URI',)

from typing import (
    TYPE_CHECKING,
    Optional,
    Any,
    Callable,
    Iterator,
    List,
    Tuple,
    Union,
)

from ..utils import time_this
from .utils import NoTransaction
from ._butlerUri import ButlerURI
from .s3utils import getS3Client, s3CheckFileExists, bucketExists

from botocore.exceptions import ClientError
from http.client import ImproperConnectionState, HTTPException
from urllib3.exceptions import RequestError, HTTPError

if TYPE_CHECKING:
    try:
        import boto3
    except ImportError:
        pass
    from ..datastore import DatastoreTransaction

# https://pypi.org/project/backoff/
try:
    import backoff
except ImportError:
    class Backoff():
        @staticmethod
        def expo(func: Callable, *args: Any, **kwargs: Any) -> Callable:
            return func

        @staticmethod
        def on_exception(func: Callable, *args: Any, **kwargs: Any) -> Callable:
            return func

    backoff = Backoff

# settings for "backoff" retry decorators. these retries are belt-and-
# suspenders along with the retries built into Boto3, to account for
# semantic differences in errors between S3-like providers.
retryable_io_errors = (
    # http.client
    ImproperConnectionState, HTTPException,
    # urllib3.exceptions
    RequestError, HTTPError,
    # built-ins
    TimeoutError, ConnectionError)
retryable_client_errors = (
    # botocore.exceptions
    ClientError,
    # built-ins
    PermissionError)
all_retryable_errors = retryable_client_errors + retryable_io_errors
max_retry_time = 60


log = logging.getLogger(__name__)


class ButlerS3URI(ButlerURI):
    """S3 URI implementation class."""

    @property
    def client(self) -> boto3.client:
        """Client object to address remote resource."""
        # Defer import for circular dependencies
        return getS3Client()

    @backoff.on_exception(backoff.expo, retryable_client_errors, max_time=max_retry_time)
    def exists(self) -> bool:
        """Check that the S3 resource exists."""
        if self.is_root:
            # Only check for the bucket since the path is irrelevant
            return bucketExists(self.netloc)
        exists, _ = s3CheckFileExists(self, client=self.client)
        return exists

    @backoff.on_exception(backoff.expo, retryable_client_errors, max_time=max_retry_time)
    def size(self) -> int:
        """Return the size of the resource in bytes."""
        if self.dirLike:
            return 0
        exists, sz = s3CheckFileExists(self, client=self.client)
        if not exists:
            raise FileNotFoundError(f"Resource {self} does not exist")
        return sz

    @backoff.on_exception(backoff.expo, retryable_client_errors, max_time=max_retry_time)
    def remove(self) -> None:
        """Remove the resource."""
        # https://github.com/boto/boto3/issues/507 - there is no
        # way of knowing if the file was actually deleted except
        # for checking all the keys again, reponse is  HTTP 204 OK
        # response all the time
        self.client.delete_object(Bucket=self.netloc, Key=self.relativeToPathRoot)

    @backoff.on_exception(backoff.expo, all_retryable_errors, max_time=max_retry_time)
    def read(self, size: int = -1) -> bytes:
        """Read the contents of the resource."""
        args = {}
        if size > 0:
            args["Range"] = f"bytes=0-{size-1}"
        try:
            response = self.client.get_object(Bucket=self.netloc,
                                              Key=self.relativeToPathRoot,
                                              **args)
        except (self.client.exceptions.NoSuchKey, self.client.exceptions.NoSuchBucket) as err:
            raise FileNotFoundError(f"No such resource: {self}") from err
        with time_this(log, msg="Read from %s", args=(self,)):
            body = response["Body"].read()
        response["Body"].close()
        return body

    @backoff.on_exception(backoff.expo, all_retryable_errors, max_time=max_retry_time)
    def write(self, data: bytes, overwrite: bool = True) -> None:
        """Write the supplied data to the resource."""
        if not overwrite:
            if self.exists():
                raise FileExistsError(f"Remote resource {self} exists and overwrite has been disabled")
        with time_this(log, msg="Write to %s", args=(self,)):
            self.client.put_object(Bucket=self.netloc, Key=self.relativeToPathRoot,
                                   Body=data)

    @backoff.on_exception(backoff.expo, all_retryable_errors, max_time=max_retry_time)
    def mkdir(self) -> None:
        """Write a directory key to S3."""
        if not bucketExists(self.netloc):
            raise ValueError(f"Bucket {self.netloc} does not exist for {self}!")

        if not self.dirLike:
            raise ValueError(f"Can not create a 'directory' for file-like URI {self}")

        # don't create S3 key when root is at the top-level of an Bucket
        if not self.path == "/":
            self.client.put_object(Bucket=self.netloc, Key=self.relativeToPathRoot)

    @backoff.on_exception(backoff.expo, all_retryable_errors, max_time=max_retry_time)
    def _as_local(self) -> Tuple[str, bool]:
        """Download object from S3 and place in temporary directory.

        Returns
        -------
        path : `str`
            Path to local temporary file.
        temporary : `bool`
            Always returns `True`. This is always a temporary file.
        """
        with tempfile.NamedTemporaryFile(suffix=self.getExtension(), delete=False) as tmpFile:
            with time_this(log, msg="Downloading %s to local file", args=(self,)):
                self.client.download_fileobj(self.netloc, self.relativeToPathRoot, tmpFile)
        return tmpFile.name, True

    @backoff.on_exception(backoff.expo, all_retryable_errors, max_time=max_retry_time)
    def transfer_from(self, src: ButlerURI, transfer: str = "copy",
                      overwrite: bool = False,
                      transaction: Optional[Union[DatastoreTransaction, NoTransaction]] = None) -> None:
        """Transfer the current resource to an S3 bucket.

        Parameters
        ----------
        src : `ButlerURI`
            Source URI.
        transfer : `str`
            Mode to use for transferring the resource. Supports the following
            options: copy.
        overwrite : `bool`, optional
            Allow an existing file to be overwritten. Defaults to `False`.
        transaction : `DatastoreTransaction`, optional
            Currently unused.
        """
        # Fail early to prevent delays if remote resources are requested
        if transfer not in self.transferModes:
            raise ValueError(f"Transfer mode '{transfer}' not supported by URI scheme {self.scheme}")

        # Existence checks cost time so do not call this unless we know
        # that debugging is enabled.
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Transferring %s [exists: %s] -> %s [exists: %s] (transfer=%s)",
                      src, src.exists(), self, self.exists(), transfer)

        if not overwrite and self.exists():
            raise FileExistsError(f"Destination path '{self}' already exists.")

        if transfer == "auto":
            transfer = self.transferDefault

        timer_msg = "Transfer from %s to %s"
        timer_args = (src, self)

        if isinstance(src, type(self)):
            # Looks like an S3 remote uri so we can use direct copy
            # note that boto3.resource.meta.copy is cleverer than the low
            # level copy_object
            copy_source = {
                "Bucket": src.netloc,
                "Key": src.relativeToPathRoot,
            }
            with time_this(log, msg=timer_msg, args=timer_args):
                self.client.copy_object(CopySource=copy_source, Bucket=self.netloc,
                                        Key=self.relativeToPathRoot)
        else:
            # Use local file and upload it
            with src.as_local() as local_uri:

                # resource.meta.upload_file seems like the right thing
                # but we have a low level client
                with time_this(log, msg=timer_msg, args=timer_args):
                    with open(local_uri.ospath, "rb") as fh:
                        self.client.put_object(Bucket=self.netloc,
                                               Key=self.relativeToPathRoot, Body=fh)

        # This was an explicit move requested from a remote resource
        # try to remove that resource
        if transfer == "move":
            # Transactions do not work here
            src.remove()

    @backoff.on_exception(backoff.expo, all_retryable_errors, max_time=max_retry_time)
    def walk(self, file_filter: Optional[Union[str, re.Pattern]] = None) -> Iterator[Union[List,
                                                                                           Tuple[ButlerURI,
                                                                                                 List[str],
                                                                                                 List[str]]]]:
        """Walk the directory tree returning matching files and directories.

        Parameters
        ----------
        file_filter : `str` or `re.Pattern`, optional
            Regex to filter out files from the list before it is returned.

        Yields
        ------
        dirpath : `ButlerURI`
            Current directory being examined.
        dirnames : `list` of `str`
            Names of subdirectories within dirpath.
        filenames : `list` of `str`
            Names of all the files within dirpath.
        """
        # We pretend that S3 uses directories and files and not simply keys
        if not (self.isdir() or self.is_root):
            raise ValueError(f"Can not walk a non-directory URI: {self}")

        if isinstance(file_filter, str):
            file_filter = re.compile(file_filter)

        s3_paginator = self.client.get_paginator('list_objects_v2')

        # Limit each query to a single "directory" to match os.walk
        # We could download all keys at once with no delimiter and work
        # it out locally but this could potentially lead to large memory
        # usage for millions of keys. It will also make the initial call
        # to this method potentially very slow. If making this method look
        # like os.walk was not required, we could query all keys with
        # pagination and return them in groups of 1000, but that would
        # be a different interface since we can't guarantee we would get
        # them all grouped properly across the 1000 limit boundary.
        prefix = self.relativeToPathRoot if not self.is_root else ""
        prefix_len = len(prefix)
        dirnames = []
        filenames = []
        files_there = False

        for page in s3_paginator.paginate(Bucket=self.netloc, Prefix=prefix, Delimiter="/"):
            # All results are returned as full key names and we must
            # convert them back to the root form. The prefix is fixed
            # and delimited so that is a simple trim

            # Directories are reported in the CommonPrefixes result
            # which reports the entire key and must be stripped.
            found_dirs = [dir["Prefix"][prefix_len:] for dir in page.get("CommonPrefixes", ())]
            dirnames.extend(found_dirs)

            found_files = [file["Key"][prefix_len:] for file in page.get("Contents", ())]
            if found_files:
                files_there = True
            if file_filter is not None:
                found_files = [f for f in found_files if file_filter.search(f)]

            filenames.extend(found_files)

        # Directories do not exist so we can't test for them. If no files
        # or directories were found though, this means that it effectively
        # does not exist and we should match os.walk() behavior and return
        # [].
        if not dirnames and not files_there:
            yield []
        else:
            yield self, dirnames, filenames

        for dir in dirnames:
            new_uri = self.join(dir)
            yield from new_uri.walk(file_filter)
