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

__all__ = ("s3CheckFileExists", "bucketExists")

try:
    import boto3
except ImportError:
    boto3 = None

from lsst.daf.butler.core.location import ButlerURI, Location


def s3CheckFileExistsGET(client, bucket, filepath):
    """Returns (True, filesize) if file exists in the bucket and (False, -1) if
    the file is not found.

    Parameters
    ----------
    client : `boto3.client`
        S3 Client object to query.
    bucket : `str`
        Name of the bucket in which to look.
    filepath : `str`
        Path to file.

    Returns
    -------
    (`bool`, `int`) : `tuple`
       Tuple (exists, size). If file exists (True, filesize)
       and (False, -1) when the file is not found.

    Notes
    -----
    A Bucket GET request will be charged against your account. This is on
    average 10x cheaper than a LIST request (see `s3CheckFileExistsLIST`
    function) but can be up to 90% slower whenever additional work is done with
    the s3 client. See PR:
        ``https://github.com/boto/botocore/issues/1248``
    for details, in boto3 versions >=1.11.0 the loss of performance should not
    be an issue anymore.
    S3 Paths are sensitive to leading and trailing path separators.
    """
    try:
        obj = client.head_object(Bucket=bucket, Key=filepath)
        return (True, obj['ContentLength'])
    except client.exceptions.ClientError as err:
        if err.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            return (False, -1)
        raise


def s3CheckFileExistsLIST(client, bucket, filepath):
    """Returns (True, filesize) if file exists in the bucket and (False, -1) if
    the file is not found.

    Parameters
    ----------
    client : `boto3.client`
        S3 Client object to query.
    bucket : `str`
        Name of the bucket in which to look.
    filepath : `str`
        Path to file.

    Returns
    -------
    (`bool`, `int`) : `tuple`
       Tuple (exists, size). If file exists (True, filesize)
       and (False, -1) when the file is not found.

    Notes
    -----
    You are getting charged for a Bucket LIST request. This is on average 10x
    more expensive than a GET request (see `s3CheckFileExistsGET` fucntion) but
    can be up to 90% faster whenever additional work is done with the s3
    client. For details, see PR:
        ``https://github.com/boto/botocore/issues/1248``
    Boto3 versions >=1.11.0 the loss of performance should not be an issue
    anymore.
    """
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


def s3CheckFileExists(client, path=None, bucket=None, filepath=None, cheap=True):
    """Returns (True, filesize) if file exists in the bucket and (False, -1) if
    the file is not found.

    Parameters
    ----------
    client : `boto3.client`
        S3 Client object to query.
    bucket : `str`
        Name of the bucket in which to look.
    filepath : `str`
        Path to file.
    cheap : `bool`
        If True, makes a GET request to S3 instead of a LIST request. See
        `s3CheckFileExistsGET` or `s3CheckFileExistsLIST` for more details.

    Returns
    -------
    (`bool`, `int`) : `tuple`
       Tuple (exists, size). If file exists (True, filesize)
       and (False, -1) when the file is not found.
    """
    if isinstance(path, ButlerURI):
        bucket = path.netloc
        filepath = path.path.lstrip('/')
    elif isinstance(path, Location):
        bucket = path.bucketName
        filepath = path.pathInBucket

    if bucket is None and filepath is None:
        raise ValueError(('Expected ButlerURI, Location or (bucket, filepath) pair '
                          f'but got {path}, ({bucket}, {filepath}) instead.'))

    if cheap:
        return s3CheckFileExistsGET(client, bucket=bucket, filepath=filepath)
    return s3CheckFileExistsLIST(client, bucket=bucket, filepath=filepath)


def bucketExists(bucketName):
    """Check if the S3 bucket with the given name actually exists.

    Parameters
    ----------
    bucketName : `str`
        Name of the S3 Bucket

    Returns
    -------
    exists : `bool`
        True if it exists, False if no Bucket with specified parameters is
        found.
    """
    if boto3 is None:
        raise ModuleNotFoundError(("Could not find boto3. "
                                   "Are you sure it is installed?"))

    s3 = boto3.client('s3')
    try:
        s3.get_bucket_location(Bucket=bucketName)
        return True
    except s3.exceptions.NoSuchBucket:
        return False
