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

__all__ = ("Location", "LocationFactory", "S3Location", "S3LocationFactory")

import os
import os.path
import urllib
from .utils import parsePath2Uri


class Location:
    """Identifies a location in the `Datastore`.
    """

    __slots__ = ("_datastoreRoot", "_uri")

    def __init__(self, datastoreRoot, uri):
        self._datastoreRoot = datastoreRoot
        self._uri = urllib.parse.urlparse(uri)

    def __str__(self):
        return self.uri

    @property
    def uri(self):
        """URI corresponding to location.
        """
        return self._uri.geturl()

    @property
    def path(self):
        """Path corresponding to location.
        This path includes the root of the `Datastore`.
        """
        return os.path.join(self._datastoreRoot, self._uri.path.lstrip("/"))

    @property
    def pathInStore(self):
        """Path corresponding to location relative to `Datastore` root.
        """
        return self._uri.path.lstrip("/")

    def updateExtension(self, ext):
        """Update the file extension associated with this `Location`.
        Parameters
        ----------
        ext : `str`
            New extension. If an empty string is given any extension will
            be removed. If `None` is given there will be no change.
        """
        if ext is None:
            return
        path, _ = os.path.splitext(self._uri.path)

        # Ensure that we have a leading "." on file extension (and we do not
        # try to modify the empty string)
        if ext and not ext.startswith("."):
            ext = "." + ext

        parts = list(self._uri)
        parts[2] = path + ext
        self._uri = urllib.parse.urlparse(urllib.parse.urlunparse(parts))


class LocationFactory:
    """Factory for `Location` instances.
    """

    def __init__(self, datastoreRoot):
        """Constructor
        Parameters
        ----------
        datastoreRoot : `str`
            Root location of the `Datastore` in the filesystem.
        """
        self._datastoreRoot = datastoreRoot

    def fromUri(self, uri):
        """Factory function to create a `Location` from a URI.
        Parameters
        ----------
        uri : `str`
            A valid Universal Resource Identifier.
        Returns
        location : `Location`
            The equivalent `Location`.
        """
        if uri is None or not isinstance(uri, str):
            raise ValueError("URI must be a string and not {}".format(uri))
        return Location(self._datastoreRoot, uri)

    def fromPath(self, path):
        """Factory function to create a `Location` from a POSIX path.
        Parameters
        ----------
        path : `str`
            A standard POSIX path, relative to the `Datastore` root.
        Returns
        location : `Location`
            The equivalent `Location`.
        """
        uri = urllib.parse.urljoin("file://", path)
        return self.fromUri(uri)



class S3Location:
    """Identifies a location in the `Datastore`.
    TODO: This will have broken functionality for extensions etc.
    """

    __slots__ = ("_scheme", "_bucket", "_datastoreRoot", "_relpath")

    def __init__(self, scheme, bucket, datastoreRoot, relpath, **kwargs):
        # no risks, maximal sanitation
        self._scheme = scheme+'://' if scheme[-3:]!='://' else scheme
        self._bucket = bucket.strip('/') + '/'
        self._datastoreRoot = datastoreRoot.strip('/') + '/'
        self._relpath = relpath.lstrip('/')

    def __str__(self):
        return self.scheme + os.path.join(self._bucket, self._datastoreRoot, self._relpath)

    @property
    def uri(self):
        """URI corresponding to location.
        """
        # uri.geturl will return only s3:/ not s3://
        return self._scheme + os.path.join(self._bucket, self._datastoreRoot, self._relpath)

    @property
    def bucket(self):
        """Return the bucketname of this location.
        """
        # buckets are special because you only want their name, but path.joining them will
        # not understand its relationship to rootDir without it.
        return self._bucket.strip('/')

    @property
    def path(self):
        """Path corresponding to location.

        This path includes the root of the `Datastore`.
        """
        return os.path.join(self._datastoreRoot, self._relpath)

    @property
    def pathInStore(self):
        """Path corresponding to location relative to `Datastore` root.
        """
        return self._relpath

    def updateExtension(self, ext):
        """Update the file extension associated with this `Location`.

        Parameters
        ----------
        ext : `str`
            New extension. If an empty string is given any extension will
            be removed. If `None` is given there will be no change.
        """
        if ext is None:
            return
        path, _ = os.path.splitext(self._relpath)

        # Ensure that we have a leading "." on file extension (and we do not
        # try to modify the empty string)
        if ext and not ext.startswith("."):
            ext = "." + ext

        self._relpath = path + ext



class S3LocationFactory:
    """Factory for `Location` instances.
    """

    def __init__(self, bucket, datastoreRoot):
        """Constructor
        Parameters
        ----------
        datastoreRoot : `str`
            Root location of the `Datastore` in the filesystem.
        """
        # no chances, maximal sanitation
        self._bucket = bucket.strip('/')
        self._datastoreRoot = datastoreRoot.strip('/')

    def fromUri(self, uri):
        """Factory function to create a `Location` from a URI.
        Parameters
        ----------
        uri : `str`
            A valid Universal Resource Identifier.
        Returns
        location : `Location`
            The equivalent `Location`.
        """
        if uri is None or not isinstance(uri, str):
            raise ValueError("URI must be a string and not {}".format(uri))

        # uri should alwazs contain scheme, bucket, root, and relpath
        parsed = urllib.parse.urlparse(uri)
        scheme = parsed.scheme
        bucketname = parsed.netloc
        relpath = parsed.path.lstrip('/')
        dirs = relpath.split('/')
        root = dirs[0]
        relpath = os.path.join(*dirs[1:])

        return S3Location(scheme, bucketname, root, relpath)

    def fromPath(self, path):
        """Factory function to create a `Location` from a POSIX path.
        Parameters
        ----------
        path : `str`
            A standard POSIX path, relative to the `Datastore` root.
        Returns
        location : `Location`
            The equivalent `Location`.
        """
        if os.path.isabs(path):
            raise ValueError(('A path whose absolute location is in an S3 bucket '
                             'can not have an absolute path: {}').format(path))

        # sometimes people give relative path with the datastoreRoot
        # and sometimes they give the pathInStore type path. Sometimes the bucket
        # is provided as well. This can duplicate the datastoreRoot and bucket
        # which we remove
        dirs = path.lstrip('/').split('/')
        nEqual = 0
        # we shouldn't remove a sub-dir in the bucket that can have the of the same name
        # so we require that bucketname/rootdir order is respected. A nicer way of writing
        # this would be nice
        for d in dirs:
            if (d==self._bucket):
                nEqual += 1
            else:
                break

        for d in dirs[nEqual:]:
            if (d==self._datastoreRoot):
                nEqual += 1
            else:
                break
        path = os.path.join(*dirs[nEqual:])

        if nEqual == 0:
            # path does not contain the rootdir or bucket
            return self.fromUri('s3://' + os.path.join(self._bucket, self._datastoreRoot, path))
        elif nEqual == 1:
            # path contains the root dir or bucket
            return self.fromUri('s3://' + os.path.join(self._bucket, path))
        elif nEqual >1:
            # path probably contains multiple datastoreRoots and/or buckets, or is completely broken
            return self.fromUri('s3://'+ os.path.join(self._bucket, self._datastoreRoot, path))
        else:
            # an act of god
            raise OSError("Don't understand path at all: {}".format(path))
