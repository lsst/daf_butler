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

__all__ = ("getWebdavClient", "webdavCheckFileExists", "folderExists", "setAwsEnvCredentials",
           "unsetAwsEnvCredentials")

import os
import requests
import urllib3
urllib3.disable_warnings()

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

def getHttpSession() -> requests.Session:
    
    s = requests.Session()
    s.cert = (os.environ.get("WEBDAV_PROXY_CERT"), os.environ.get("WEBDAV_PROXY_CERT"))
    s.verify = False

    return s

def getWebdavClient() -> wc.Client:
    """Create a S3 client with AWS (default) or the specified endpoint

    Returns
    -------
    s3client : `botocore.client.S3`
        A client of the S3 service.

    Notes
    -----
    The endpoint URL is from the environment variable S3_ENDPOINT_URL.
    If none is specified, the default AWS one is used.
    """
    if wc is None:
        raise ModuleNotFoundError("Could not find webdav.client. "
                                  "Are you sure it is installed?")
    
    options = {
        'webdav_hostname': os.environ.get("WEBDAV_ENDPOINT_URL", None),
        'cert_path': os.environ.get("WEBDAV_PROXY_CERT", None),
        'key_path': os.environ.get("WEBDAV_PROXY_CERT", None),
        'verbose'    : False
    }

    try:
        client = wc.Client(options)
    except WebDavException as exception:
        raise ValueError(f"Failure to create webdav client, please check your WEBDAV_ENDPOINT_URL and WEBDAV_PROXY_CERT values") from exception   
        
    client.verify = False
    return client


def webdavCheckFileExists(path: Union[Location, ButlerURI, str],
                      client: Optional[wc.Client] = None) -> Tuple[bool, int]:
    """Returns (True, filesize) if file exists in the bucket and (False, -1) if
    the file is not found.

    Parameters
    ----------
    path : `Location`, `ButlerURI` or `str`
        Location or ButlerURI containing the bucket name and filepath.
    bucket : `str`, optional
        Name of the bucket in which to look. If provided, path will be assumed
        to correspond to be relative to the given bucket.
    client : `boto3.client`, optional
        S3 Client object to query, if not supplied boto3 will try to resolve
        the credentials as in order described in its manual_.

    Returns
    -------
    exists : `bool`
        True if key exists, False otherwise.
    size : `int`
        Size of the key, if key exists, in bytes, otherwise -1

    Notes
    -----
    S3 Paths are sensitive to leading and trailing path separators.

    .. _manual: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/\
    configuration.html#configuring-credentials
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
            raise ValueError(f"Failed to retrieve size of file, maybe check your permissions ?") from exception
        return (True, size)
    
    return (False, -1)


def folderExists(folderName: str, client: Optional[wc.Client] = None) -> bool:
    """Check if the S3 bucket with the given name actually exists.

    Parameters
    ----------
    bucketName : `str`
        Name of the S3 Bucket
    client : `boto3.client`, optional
        S3 Client object to query, if not supplied boto3 will try to resolve
        the credentials as in order described in its manual_.

    Returns
    -------
    exists : `bool`
        True if it exists, False if no Bucket with specified parameters is
        found.

    .. _manual: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/\
    configuration.html#configuring-credentials
    """
    if wc is None:
        raise ModuleNotFoundError("Could not find webdav.client. "
                                  "Are you sure it is installed?")

    if client is None:
        client = getWebdavClient()

    return client.check(folderName)


def setAwsEnvCredentials(accessKeyId: str = 'dummyAccessKeyId',
                         secretAccessKey: str = "dummySecretAccessKey") -> bool:
    """Set AWS credentials environmental variables AWS_ACCESS_KEY_ID and
    AWS_SECRET_ACCESS_KEY.

    Parameters
    ----------
    accessKeyId : `str`
        Value given to AWS_ACCESS_KEY_ID environmental variable. Defaults to
        'dummyAccessKeyId'
    secretAccessKey : `str`
        Value given to AWS_SECRET_ACCESS_KEY environmental variable. Defaults
        to 'dummySecretAccessKey'

    Returns
    -------
    setEnvCredentials : `bool`
        True when environmental variables were set, False otherwise.

    Notes
    -----
    If either AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY are not set, both
    values are overwritten.
    """
    if "AWS_ACCESS_KEY_ID" not in os.environ or "AWS_SECRET_ACCESS_KEY" not in os.environ:
        os.environ["AWS_ACCESS_KEY_ID"] = accessKeyId
        os.environ["AWS_SECRET_ACCESS_KEY"] = secretAccessKey
        return True
    return False


def unsetAwsEnvCredentials() -> None:
    """Unsets AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environmental
    variables.
    """
    if "AWS_ACCESS_KEY_ID" in os.environ:
        del os.environ["AWS_ACCESS_KEY_ID"]
    if "AWS_SECRET_ACCESS_KEY" in os.environ:
        del os.environ["AWS_SECRET_ACCESS_KEY"]
