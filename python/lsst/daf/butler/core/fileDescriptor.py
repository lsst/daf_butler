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


class FileDescriptor(object):
    """Describes a particular file.

    Parameters
    ----------
    location : `Location`
        Storage location.
    pytype : `type`, optional
        Type the object will have after reading in Python (typically
        `StorageClass.pytype` but can be overridden).
    storageClass : `StorageClass`, optional
        `StorageClass` associated with this file.
    parameters : `dict`, optional
        Additional parameters that can be used for reading and writing.
    """

    __slots__ = ('location', 'pytype', 'storageClass', 'parameters')

    def __init__(self, location, pytype=None, storageClass=None, parameters=None):
        self.location = location
        self.pytype = pytype
        self.storageClass = storageClass
        self.parameters = parameters
