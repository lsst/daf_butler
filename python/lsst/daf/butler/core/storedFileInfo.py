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

from .formatter import Formatter
from .utils import slotValuesAreEqual
from .storageClass import StorageClass

__all__ = ("StoredFileInfo", )


class StoredFileInfo:
    """Information associated with a stored file in a Datastore.

    Parameters
    ----------
    formatter : `str` or `Formatter`
        Full name of formatter to use to read this Dataset or a `Formatter`
        instance.
    path : `str`
        Path to Dataset, relative to `Datastore` root.
    storageClass : `StorageClass`
        `StorageClass` used when writing the file. This can differ from that
        used to read the file if a component is being requested from
        a concrete composite.

    See Also
    --------
    StorageInfo
    """

    __eq__ = slotValuesAreEqual
    __slots__ = ("_formatter", "_path", "_storageClass", "_checksum", "_size")

    def __init__(self, formatter, path, storageClass, checksum=None, size=None):
        assert isinstance(formatter, str) or isinstance(formatter, Formatter)
        if isinstance(formatter, Formatter):
            formatter = formatter.name()
        self._formatter = formatter
        assert isinstance(path, str)
        self._path = path
        assert isinstance(storageClass, StorageClass)
        self._storageClass = storageClass
        assert checksum is None or isinstance(checksum, str)
        self._checksum = checksum
        assert size is None or isinstance(size, int)
        self._size = size

    @property
    def formatter(self):
        """Full name of formatter (`str`).
        """
        return self._formatter

    @property
    def path(self):
        """Path to Dataset (`str`).
        """
        return self._path

    @property
    def storageClass(self):
        """StorageClass used (`StorageClass`).
        """
        return self._storageClass

    @property
    def checksum(self):
        """Checksum (`str`).
        """
        return self._checksum

    @property
    def size(self):
        """Size of stored object in bytes (`int`).
        """
        return self._size

    def __repr__(self):
        return f'{type(self).__qualname__}(path="{self.path}", formatter="{self.formatter}"' \
            f' size={self.size}, checksum="{self.checksum}", storageClass="{self.storageClass.name}")'
