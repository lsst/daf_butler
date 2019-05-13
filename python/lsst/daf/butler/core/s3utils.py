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

__all__ = ("s3CheckFileExists", "parsePathToUriElements", "parsePathToUriElementsV2",
           "bucketExists", "splitPathRelativeToRoot", "normalizePosixPath",
           "normalizeS3Path")

import os
import sys
import functools
import urllib
from urllib.parse import urlparse

try:
    import boto3
except:
    boto3 = None

from lsst.utils import doImport


def s3CheckFileExists(client, bucket, filepath):
    """Returns (True, filesize) if file exists in the bucket
    and (False, -1) if the file is not found.

    You are getting charged for a Bucket GET request. The request
    returns the list of all files matching the given filepath.
    Multiple matches are considered a non-match.

    Parameters
    ----------
    client : 'boto3.client'
        S3 Client object to query.
    bucket : 'str'
        Name of the bucket in which to look.
    filepath : 'str'
        Path to file.

    Returns
    -------
    (`bool`, `int`) : `tuple`
       Tuple (exists, size). If file exists (True, filesize)
       and (False, -1) when the file is not found.
    """
    # this has maxkeys kwarg, limited to 1000 by default
    # apparently this is the fastest way do look-ups as it avoids having
    # to create and add a new HTTP connection to pool
    # https://github.com/boto/botocore/issues/1248
    # https://github.com/boto/boto3/issues/1128
    response = client.list_objects_v2(
        Bucket=bucket,
        Prefix=filepath
    )
    # Hopefully multiple identical files will never exist?
    matches = [x for x in response.get('Contents', []) if x["Key"] == filepath]
    if len(matches) == 1:
        return (True, matches[0]['Size'])
    else:
        return (False, -1)

def splitPathRelativeToRoot(path, root):
    rootPath = os.path.join(path.split(root)[0], root)
    relPath = path[len(rootPath):].lstrip('/')
    return rootPath, relPath

def normalizePosixPath(path):
    if path.startswith('~'):
        path = os.path.expanduser(path)
    path = os.path.abspath(path)
    return path

def normalizeS3Path(path, root):
    return splitPathRelativeToRoot(path, root)

def parsePathToUriElementsV2(path, root=''):
    path = os.path.join(root, path)
    parsed = urlparse(path)

    if not parsed.scheme:
        scheme = 'file://'
    else:
        scheme = parsed.scheme + '://'

    if scheme == 'file://':
        normPath = normalizePosixPath(parsed.path)
        rootPath, relPath = splitPathRelativeToRoot(normPath, root)
    if scheme == 's3://':
        s3Path = os.path.join(parsed.netloc, parsed.path.lstrip('/'))
        root, relpath = splitPathRelativeToRoot(s3Path, root)


def parsePathToUriElements(path, root=None):
    """
    TODO: THIS FUNCTION MAKES NO SENSE AS IT WON'T BE RELATIVE TO DATASTORE ROOT

    If the path is a local filesystem path constructs elements of a URI.
    If path is an URI returns the URI elements: (schema, root, relpath).

    Parameters
    ----------
    uri : `str`
        URI or a POSIX-like path to parse.
    root : `str` (optional)
        When provided path is relative root to which it is relative can
        be optionally provided such that returned URI elements constitute
        a fully specified location. Otherwise the root will be an empty
        string.

    Returns
    -------
    scheme : 'str'
        Either 'file://' or 's3://'.
    root : 'str'
        S3 Bucket name or Posix absolute path up to the top of
        the relative path
    relpath : 'str'
        Posix-like path relative to root.
    """
    parsed = urlparse(path)

    # urllib assumes only absolute paths exist in URIs
    # It will not parse rel and abs paths the same way.
    # We want handle POSIX like paths too.
    # 'file://' prefixed URIs and POSIX paths
    if parsed.scheme == 'file' or not parsed.scheme:
        scheme = 'file://'
        # Absolute paths in URI and absolute POSIX paths
        if not parsed.netloc and os.path.isabs(parsed.path):
            root = '/'
            relpath = parsed.path.lstrip('/')
        # Relative paths in URI and relative POSIX paths
        else:
            relpath = os.path.join(parsed.netloc, parsed.path.lstrip('/'))
            root = '' if root is None else root
    # S3 URIs are always s3://bucketName/root/subdir/file.ext
    elif parsed.scheme == 's3':
        scheme = 's3://'
        root = parsed.netloc
        relpath = parsed.path.lstrip('/')
    else:
        raise urllib.error.URLError(f'Can not parse path: {path}')

    return scheme, root, relpath


def bucketExists(uri):
    """Check if the S3 bucket at a given URI actually exists.

    Parameters
    ----------
    uri : `str`
        URI of the S3 Bucket

    Returns
    -------
    exists : `bool`
        True if it exists, False if no Bucket with specified parameters is found.
    """
    if boto3 is None:
        raise ModuleNotFoundError(("Could not find boto3. "
                                   "Are you sure it is installed?"))

    session = boto3.Session(profile_name='default')
    client = boto3.client('s3')
    scheme, root, relpath = parsePathToUriElements(uri)

    try:
        client.get_bucket_location(Bucket=root)
        # bucket exists, all is well
        return True
    except client.exceptions.NoSuchBucket:
        return False
