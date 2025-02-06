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
    "BadNoWriteFormatter",
    "BadWriteFormatter",
    "DatasetTestHelper",
    "DatastoreTestHelper",
    "MultiDetectorFormatter",
)

import os
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from lsst.daf.butler import DataCoordinate, DatasetRef, DatasetType, DimensionGroup, StorageClass
from lsst.daf.butler.datastore import Datastore
from lsst.daf.butler.formatters.yaml import YamlFormatter
from lsst.resources import ResourcePath

if TYPE_CHECKING:
    from lsst.daf.butler import Config, DatasetId
    from lsst.daf.butler.datastore.cache_manager import AbstractDatastoreCacheManager


class DatasetTestHelper:
    """Helper methods for Datasets."""

    def makeDatasetRef(
        self,
        datasetTypeName: str,
        dimensions: DimensionGroup | Iterable[str],
        storageClass: StorageClass | str,
        dataId: DataCoordinate | Mapping[str, Any],
        *,
        id: DatasetId | None = None,
        run: str | None = None,
        conform: bool = True,
    ) -> DatasetRef:
        """Make a DatasetType and wrap it in a DatasetRef for a test.

        Parameters
        ----------
        datasetTypeName : `str`
            The name of the dataset type.
        dimensions : `DimensionGroup` or `~collections.abc.Iterable` of `str`
            The dimensions to use for this dataset type.
        storageClass : `StorageClass` or `str`
            The relevant storage class.
        dataId : `DataCoordinate` or `~collections.abc.Mapping`
            The data ID of this ref.
        id : `DatasetId` or `None`, optional
            The Id of this ref. Will be assigned automatically.
        run : `str` or `None`, optional
            The run for this ref. Will be assigned a default value if `None`.
        conform : `bool`, optional
            Whther to force the dataID to be checked for conformity with
            the provided dimensions.

        Returns
        -------
        ref : `DatasetRef`
            The new ref.
        """
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
        dimensions: DimensionGroup | Iterable[str],
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
        """Shared setUp code for all Datastore tests.

        Parameters
        ----------
        registryClass : `type`
            Type of registry to use.
        configClass : `type`
            Type of config to use.
        """
        self.registry = registryClass()
        self.config = configClass(self.configFile)

        # Some subclasses override the working root directory
        if self.root is not None:
            self.datastoreType.setConfigRoot(self.root, self.config, self.config.copy())

    def makeDatastore(self, sub: str | None = None) -> Datastore:
        """Make a new Datastore instance of the appropriate type.

        Parameters
        ----------
        sub : `str`, optional
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
        return Datastore.fromConfig(config=config, bridgeManager=registry.getDatastoreBridgeManager())


class BadWriteFormatter(YamlFormatter):
    """A formatter that never works but does leave a file behind."""

    can_read_from_uri = False
    can_read_from_local_file = False
    can_read_from_stream = False

    def read_from_uri(self, uri: ResourcePath, component: str | None = None, expected_size: int = -1) -> Any:
        return NotImplemented

    def write_direct(
        self,
        in_memory_dataset: Any,
        uri: ResourcePath,
        cache_manager: AbstractDatastoreCacheManager | None = None,
    ) -> bool:
        uri.write(b"")
        raise RuntimeError("Did not succeed in writing file.")


class BadNoWriteFormatter(BadWriteFormatter):
    """A formatter that always fails without writing anything."""

    def write_direct(
        self,
        in_memory_dataset: Any,
        uri: ResourcePath,
        cache_manager: AbstractDatastoreCacheManager | None = None,
    ) -> bool:
        raise RuntimeError("Did not writing anything at all")


class MultiDetectorFormatter(YamlFormatter):
    """A formatter that requires a detector to be specified in the dataID."""

    can_read_from_uri = True

    def read_from_uri(self, uri: ResourcePath, component: str | None = None, expected_size: int = -1) -> Any:
        if self.data_id is None:
            raise RuntimeError("This formatter requires a dataId")
        if "detector" not in self.data_id:
            raise RuntimeError("This formatter requires detector to be present in dataId")

        key = f"detector{self.data_id['detector']}"

        data = super().read_from_uri(uri, component)
        if key not in data:
            raise RuntimeError(f"Could not find '{key}' in data file.")

        return data[key]
