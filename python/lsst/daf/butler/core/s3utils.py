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

__all__ = ("s3CheckFileExists", "parsePathToUriElements", "bucketExists")

import os
import urllib
from urllib.parse import urlparse

try:
    import boto3
except ImportError:
    boto3 = None


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


def parsePathToUriElements(path, root=None):
    """Constructs elements of an URI.

    Parameters
    ----------
    path : `str`
        URI or a POSIX-like path to parse. URI can contain an absolute or an
        relative path.
    root : `str` (optional)
        If provided path is relative, it is taken relative to given root.
        If the provided path is absolute and contains given root, it is split
        on the root.
        If the provided path is absolute and does not contain root, then root
        is ignored

    Returns
    -------
    scheme : 'str'
        Either 'file://' or 's3://'.
    root : 'str'
        Absolute path up to the top of the relative path determined by root. If
        no root is given root is taken to be the entire path to the last dir.
        Practically speaking, this is the bucket name or the given root.
    relpath : 'str'
        Posix-like path relative to root.

    Examples
    --------
    When URIs contain absolute paths to files or directories:

    >>> parsePathToUriElements('s3://bucketname/root/relative/file.ext')
    ('s3://', 'bucketname', 'root/relative/')

    >>> parsePathToUriElements('file:///root/relative/file.ext')
    ('file://', '/root/relative', 'file.ext')

    >>> parsePathToUriElements('file:///root/relative/file.ext', '/root')
    ('file://', '/root', 'relative/file.ext')

    Only `file://` URIs can contain relative paths. The same case for S3 URIs
    would not make any sense as neither the bucket nor root key would be known.

    >>> parsePathToUriElements('file://relative/file.ext', '/root')
    ('file://', '/root', 'relative/file.ext')

    The behaviour is the same in the case POSIX-like paths are given instead of
    an URI.

    >>> parsePathToUriElements('relative/file.ext', '/root')
    ('file://', '/root', 'relative/file.ext')

    >>> parsePathToUriElements('/root/relative/file.ext')
    ('file://', '/root/relative', 'file.ext')

    Root is as an empty string if a relative path without root is given.

    >>> parsePathToUriElements('relative/file.ext')
    ('file://', '', 'relative/file.ext')
    """
    if path.startswith('~'):
        path = os.path.expanduser(path)

    parsed = urlparse(path)

    if parsed.scheme == 'file' or not parsed.scheme:
        scheme = 'file://'
        # Absolute paths in URI and absolute POSIX paths
        if not parsed.netloc and os.path.isabs(parsed.path):
            if root is None or root not in path:
                rootPath, relPath = os.path.split(parsed.path)
            else:
                parts = parsed.path.split(root)
                rootPath = os.path.abspath(os.path.join(parts[0], root))
                relPath = parts[-1].lstrip(os.sep)
        # Relative paths in URI and relative POSIX paths
        else:
            if root is None:
                rootPath = '' if root is None else os.path.abspath(root)
                relPath = os.path.join(parsed.netloc, parsed.path.lstrip('/'))
            else:
                tmpPath = os.path.abspath(
                    os.path.join(
                        root, parsed.netloc, parsed.path.lstrip(os.sep)
                    )
                )
                rootPath = os.path.commonpath((tmpPath, root))
                relPath = tmpPath[len(rootPath):].lstrip(os.sep)
    # S3 URIs are always s3://bucketName/root/subdir/file.ext
    elif parsed.scheme == 's3':
        scheme = 's3://'
        rootPath = parsed.netloc
        relPath = parsed.path.lstrip('/')
    else:
        raise urllib.error.URLError(f'Can not parse path: {path}')

    return scheme, rootPath, relPath


def bucketExists(uri):
    """Check if the S3 bucket at a given URI actually exists.

    Parameters
    ----------
    uri : `str`
        URI of the S3 Bucket

    Returns
    -------
    exists : `bool`
        True if it exists, False if no Bucket with specified parameters is
        found.
    """
    if boto3 is None:
        raise ModuleNotFoundError(("Could not find boto3. "
                                   "Are you sure it is installed?"))
    client = boto3.client('s3')
    scheme, root, relpath = parsePathToUriElements(uri)

    try:
        client.get_bucket_location(Bucket=root)
        # bucket exists, all is well
        return True
    except client.exceptions.NoSuchBucket:
        return False
