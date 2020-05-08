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

__all__ = ("FitsCatalogDatasetsHelper", "DatasetTestHelper", "DatastoreTestHelper",
           "BadWriteFormatter", "BadNoWriteFormatter", "MultiDetectorFormatter")

import os
from lsst.daf.butler import DatasetType, DatasetRef
from lsst.daf.butler.formatters.yamlFormatter import YamlFormatter


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
        self.assertEqual(inputRecord.getPsfInstFlux(), outputRecord.getPsfInstFlux())
        self.assertEqual(inputRecord.getPsfFluxFlag(), outputRecord.getPsfFluxFlag())
        self.assertEqual(inputTable.getSchema().getAliasMap().get("slot_Centroid"),
                         outputTable.getSchema().getAliasMap().get("slot_Centroid"))
        self.assertEqual(inputRecord.getCentroid(), outputRecord.getCentroid())
        self.assertFloatsAlmostEqual(
            inputRecord.getCentroidErr()[0, 0],
            outputRecord.getCentroidErr()[0, 0], rtol=1e-6)
        self.assertFloatsAlmostEqual(
            inputRecord.getCentroidErr()[1, 1],
            outputRecord.getCentroidErr()[1, 1], rtol=1e-6)
        self.assertEqual(inputTable.getSchema().getAliasMap().get("slot_Shape"),
                         outputTable.getSchema().getAliasMap().get("slot_Shape"))
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

    def makeDatasetRef(self, datasetTypeName, dimensions, storageClass, dataId, *, id=None, run=None,
                       conform=True):
        """Make a DatasetType and wrap it in a DatasetRef for a test"""
        compRefs = {}
        for compName, sc in storageClass.components.items():
            compRefs[compName] = self._makeDatasetRef(DatasetType.nameWithComponent(datasetTypeName,
                                                                                    compName),
                                                      dimensions, sc, dataId, id=None, run=run,
                                                      conform=conform)

        return self._makeDatasetRef(datasetTypeName, dimensions, storageClass, dataId, id=id, run=run,
                                    conform=conform, components=compRefs)

    def _makeDatasetRef(self, datasetTypeName, dimensions, storageClass, dataId, *, id=None, run=None,
                        conform=True, components=None):
        # helper for makeDatasetRef
        datasetType = DatasetType(datasetTypeName, dimensions, storageClass)
        if id is None:
            self.id += 1
            id = self.id
        if run is None:
            run = "dummy"
        return DatasetRef(datasetType, dataId, id=id, run=run, conform=conform, components=components)


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
        if sub is not None:
            # Ensure that each datastore gets its own registry
            registryClass = type(self.registry)
            registry = registryClass()
        else:
            registry = self.registry
        return self.datastoreType(config=config, registry=registry)


class BadWriteFormatter(YamlFormatter):
    """A formatter that never works but does leave a file behind."""

    def _readFile(self, path, pytype=None):
        raise NotImplementedError("This formatter can not read anything")

    def _writeFile(self, inMemoryDataset):
        """Write an empty file and then raise an exception."""
        with open(self.fileDescriptor.location.path, "wb"):
            pass
        raise RuntimeError("Did not succeed in writing file")


class BadNoWriteFormatter(BadWriteFormatter):
    """A formatter that always fails without writing anything."""

    def _writeFile(self, inMemoryDataset):
        raise RuntimeError("Did not writing anything at all")


class MultiDetectorFormatter(YamlFormatter):

    def _writeFile(self, inMemoryDataset):
        raise NotImplementedError("Can not write")

    def _fromBytes(self, serializedDataset, pytype=None):
        data = super()._fromBytes(serializedDataset)
        if self.dataId is None:
            raise RuntimeError("This formatter requires a dataId")
        if "detector" not in self.dataId:
            raise RuntimeError("This formatter requires detector to be present in dataId")
        key = f"detector{self.dataId['detector']}"
        if key in data:
            return pytype(data[key])
        raise RuntimeError(f"Could not find '{key}' in data file")
