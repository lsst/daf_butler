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

import lsst.afw.table


class StorageClassMeta(type):

    def __init__(self, name, bases, dct):
        super().__init__(name, bases, dct)
        if hasattr(self, "name"):
            StorageClass.subclasses[self.name] = self


class StorageClass(metaclass=StorageClassMeta):

    subclasses = dict()

    components = dict()

    @classmethod
    def assemble(cls, parent, components):
        return parent


class TablePersistable(StorageClass):
    name = "TablePersistable"


class Image(StorageClass):
    name = "Image"


class Exposure(StorageClass):
    name = "Exposure"
    components = {
        "image": Image,
        "mask": Image,
        "variance": Image,
        "wcs": TablePersistable,
        "psf": TablePersistable,
        "photoCalib": TablePersistable,
        "visitInfo": TablePersistable,
        "apCorr": TablePersistable,
        "coaddInputs": TablePersistable,
    }

    @classmethod
    def assemble(cls, parent, components):
        raise NotImplementedError("TODO")


class Catalog(StorageClass):
    name = "Catalog"
    type = None  # Catalog is abstract (I think)


class SourceCatalog(StorageClass):
    name = "SourceCatalog"
    type = lsst.afw.table.SourceCatalog
