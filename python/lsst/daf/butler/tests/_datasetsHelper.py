# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

from __future__ import annotations

__all__ = (
    "DatasetTestHelper",
    "DatastoreTestHelper",
    "BadWriteFormatter",
    "BadNoWriteFormatter",
    "MultiDetectorFormatter",
)

import os
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from lsst.daf.butler import DataCoordinate, DatasetRef, DatasetType, DimensionGroup, StorageClass
from lsst.daf.butler.formatters.yaml import YamlFormatter

if TYPE_CHECKING:
    from lsst.daf.butler import Config, DatasetId, Datastore, Dimension, DimensionGraph


class DatasetTestHelper:
    """Helper methods for Datasets."""

    def makeDatasetRef(
        self,
        datasetTypeName: str,
        dimensions: DimensionGroup | DimensionGraph | Iterable[str | Dimension],
        storageClass: StorageClass | str,
        dataId: DataCoordinate | Mapping[str, Any],
        *,
        id: DatasetId | None = None,
        run: str | None = None,
        conform: bool = True,
    ) -> DatasetRef:
        """Make a DatasetType and wrap it in a DatasetRef for a test."""
        return self._makeDatasetRef(
            datasetTypeName,
            dimensions,
            storageClass,
            dataId,
            id=id,
            run=run,
            conform=conform,
        )

    def _makeDatasetRef(
        self,
        datasetTypeName: str,
        dimensions: DimensionGroup | DimensionGraph | Iterable[str | Dimension],
        storageClass: StorageClass | str,
        dataId: DataCoordinate | Mapping,
        *,
        id: DatasetId | None = None,
        run: str | None = None,
        conform: bool = True,
    ) -> DatasetRef:
        # helper for makeDatasetRef

        # Pretend we have a parent if this looks like a composite
        compositeName, componentName = DatasetType.splitDatasetTypeName(datasetTypeName)
        parentStorageClass = StorageClass("component") if componentName else None

        datasetType = DatasetType(
            datasetTypeName, dimensions, storageClass, parentStorageClass=parentStorageClass
        )

        if run is None:
            run = "dummy"
        if not isinstance(dataId, DataCoordinate):
            dataId = DataCoordinate.standardize(dataId, dimensions=datasetType.dimensions)
        return DatasetRef(datasetType, dataId, id=id, run=run, conform=conform)


class DatastoreTestHelper:
    """Helper methods for Datastore tests."""

    root: str | None
    config: Config
    datastoreType: type[Datastore]
    configFile: str

    def setUpDatastoreTests(self, registryClass: type, configClass: type[Config]) -> None:
        """Shared setUp code for all Datastore tests."""
        self.registry = registryClass()
        self.config = configClass(self.configFile)

        # Some subclasses override the working root directory
        if self.root is not None:
            self.datastoreType.setConfigRoot(self.root, self.config, self.config.copy())

    def makeDatastore(self, sub: str | None = None) -> Datastore:
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
        return self.datastoreType(config=config, bridgeManager=registry.getDatastoreBridgeManager())


class BadWriteFormatter(YamlFormatter):
    """A formatter that never works but does leave a file behind."""

    def _readFile(self, path: str, pytype: type[Any] | None = None) -> Any:
        raise NotImplementedError("This formatter can not read anything")

    def _writeFile(self, inMemoryDataset: Any) -> None:
        """Write an empty file and then raise an exception."""
        with open(self.fileDescriptor.location.path, "wb"):
            pass
        raise RuntimeError("Did not succeed in writing file")


class BadNoWriteFormatter(BadWriteFormatter):
    """A formatter that always fails without writing anything."""

    def _writeFile(self, inMemoryDataset: Any) -> None:
        raise RuntimeError("Did not writing anything at all")


class MultiDetectorFormatter(YamlFormatter):
    """A formatter that requires a detector to be specified in the dataID."""

    def _writeFile(self, inMemoryDataset: Any) -> None:
        raise NotImplementedError("Can not write")

    def _fromBytes(self, serializedDataset: bytes, pytype: type[Any] | None = None) -> Any:
        data = super()._fromBytes(serializedDataset)
        if self.dataId is None:
            raise RuntimeError("This formatter requires a dataId")
        if "detector" not in self.dataId:
            raise RuntimeError("This formatter requires detector to be present in dataId")
        key = f"detector{self.dataId['detector']}"
        assert pytype is not None
        if key in data:
            return pytype(data[key])
        raise RuntimeError(f"Could not find '{key}' in data file")
