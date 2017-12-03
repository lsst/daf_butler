#
# LSST Data Management System
#
# Copyright 2008-2017  AURA/LSST.
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

import os

import lsst.afw.table

testDir = os.path.dirname(__file__)


def makeExampleCatalog():
    import lsst.afw.table
    catalogPath = os.path.join(testDir, "data", "basic", "source_catalog.fits")
    return lsst.afw.table.SourceCatalog.readFits(catalogPath)


def assertCatalogEqual(testObj, inputCatalog, outputCatalog):
    testObj.assertIsInstance(outputCatalog, lsst.afw.table.SourceCatalog)
