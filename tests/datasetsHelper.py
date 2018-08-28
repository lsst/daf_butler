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
from lsst.daf.butler import DatasetType, DatasetRef, Run


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

    def makeDatasetRef(self, datasetTypeName, dataUnits, storageClass, dataId, id=None, run=None):
        """Make a DatasetType and wrap it in a DatasetRef for a test"""
        datasetType = DatasetType(datasetTypeName, dataUnits, storageClass)
        if id is None:
            self.id += 1
            id = self.id
        if run is None:
            run = Run(id=1, collection="dummy")
        return DatasetRef(datasetType, dataId, id=id, run=run)


class DatastoreTestHelper:
    """Helper methods for Datastore tests"""

    def setUpDatastoreTests(self, registryClass, configClass):
        """Shared setUp code for all Datastore tests"""
        self.registry = registryClass()

        # Need to keep ID for each datasetRef since we have no butler
        # for these tests
        self.id = 1

        self.config = configClass(self.configFile)

        # Some subclasses override the working root directory
        if self.root is not None:
            self.datastoreType.setConfigRoot(self.root, self.config, self.config.copy())

    def makeDatastore(self, sub=None):
        """Make a new Datastore instance of the appropriate type.

        Parameters
        ----------
        sub : str, optional
            If not None, the returned Datastore will be distinct from any
            Datastore constructed with a different value of ``sub``.  For
            PosixDatastore, for example, the converse is also true, and ``sub``
            is used as a subdirectory to form the new root.

        Returns
        -------
        datastore : `Datastore`
            Datastore constructed by this routine using the supplied
            optional subdirectory if supported.
        """
        config = self.config.copy()
        if sub is not None and self.root is not None:
            self.datastoreType.setConfigRoot(os.path.join(self.root, sub), config, self.config)
        return self.datastoreType(config=config, registry=self.registry)
