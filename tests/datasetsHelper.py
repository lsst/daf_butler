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

import os
from lsst.daf.butler import DatasetType, DatasetRef


class FitsCatalogDatasetsHelper:

    def makeExampleCatalog(self):
        import lsst.afw.table
        catalogPath = os.path.join(self.testDir, "data", "basic", "source_catalog.fits")
        return lsst.afw.table.SourceCatalog.readFits(catalogPath)

    def assertCatalogEqual(self, inputCatalog, outputCatalog):
        import lsst.afw.table
        self.assertIsInstance(outputCatalog, lsst.afw.table.SourceCatalog)
        inputTable = inputCatalog.getTable()
        inputRecord = inputCatalog[0]
        outputTable = outputCatalog.getTable()
        outputRecord = outputCatalog[0]
        self.assertEqual(inputTable.getPsfFluxDefinition(), outputTable.getPsfFluxDefinition())
        self.assertEqual(inputRecord.getPsfFlux(), outputRecord.getPsfFlux())
        self.assertEqual(inputRecord.getPsfFluxFlag(), outputRecord.getPsfFluxFlag())
        self.assertEqual(inputTable.getCentroidDefinition(), outputTable.getCentroidDefinition())
        self.assertEqual(inputRecord.getCentroid(), outputRecord.getCentroid())
        self.assertFloatsAlmostEqual(
            inputRecord.getCentroidErr()[0, 0],
            outputRecord.getCentroidErr()[0, 0], rtol=1e-6)
        self.assertFloatsAlmostEqual(
            inputRecord.getCentroidErr()[1, 1],
            outputRecord.getCentroidErr()[1, 1], rtol=1e-6)
        self.assertEqual(inputTable.getShapeDefinition(), outputTable.getShapeDefinition())
        self.assertFloatsAlmostEqual(
            inputRecord.getShapeErr()[0, 0],
            outputRecord.getShapeErr()[0, 0], rtol=1e-6)
        self.assertFloatsAlmostEqual(
            inputRecord.getShapeErr()[1, 1],
            outputRecord.getShapeErr()[1, 1], rtol=1e-6)
        self.assertFloatsAlmostEqual(
            inputRecord.getShapeErr()[2, 2],
            outputRecord.getShapeErr()[2, 2], rtol=1e-6)


class DatasetTestHelper:
    """Helper methods for Datasets"""

    def makeDatasetRef(self, datasetTypeName, dataUnits, storageClass, dataId, id=None):
        """Make a DatasetType and wrap it in a DatasetRef for a test"""
        datasetType = DatasetType(datasetTypeName, dataUnits, storageClass)
        if id is None:
            self.id += 1
            id = self.id
        return DatasetRef(datasetType, dataId, id=id)
