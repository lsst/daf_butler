#
# LSST Data Management System
#
# Copyright 2008-2018  AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#


class FileDescriptor(object):
    """Describes a particular file.

    Parameters
    ----------
    location : `Location`
        Storage location.
    type : `cls`
        Type the object will have after reading in Python (typically
        `StorageClass.pytype` but can be overridden).
    storageClass : `StorageClass`
        `StorageClass` associated with this file.
    parameters : `dict`
        Additional parameters that can be used for reading and writing.
    """

    __slots__ = ('location', 'pytype', 'storageClass', 'parameters')

    def __init__(self, location, pytype=None, storageClass=None, parameters=None):
        self.location = location
        self.pytype = pytype
        self.storageClass = storageClass
        self.parameters = parameters
