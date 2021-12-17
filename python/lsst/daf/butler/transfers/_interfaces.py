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

from __future__ import annotations

__all__ = ["RepoExportBackend", "RepoImportBackend", "RepoTransferFormatConfig"]

from abc import ABC, abstractmethod
from typing import Iterable, Optional, Set

from ..core import (
    ConfigSubset,
    DatasetAssociation,
    DatasetType,
    Datastore,
    DimensionElement,
    DimensionRecord,
    FileDataset,
)
from ..registry import CollectionType
from ..registry.interfaces import CollectionRecord, DatasetIdGenEnum


class RepoTransferFormatConfig(ConfigSubset):
    """The section of butler configuration that associates repo import/export
    backends with file formats.
    """

    component = "repo_transfer_formats"
    defaultConfigFile = "repo_transfer_formats.yaml"


class RepoExportBackend(ABC):
    """An abstract interface for data repository export implementations.

    Methods are guaranteed to be called in ways that reflect foreign key
    dependencies.
    """

    @abstractmethod
    def saveDimensionData(self, element: DimensionElement, *data: DimensionRecord) -> None:
        """Export one or more dimension element records.

        Parameters
        ----------
        element : `DimensionElement`
            The `DimensionElement` whose elements are being exported.
        data : `DimensionRecord` (variadic)
            One or more records to export.
        """
        raise NotImplementedError()

    @abstractmethod
    def saveCollection(self, record: CollectionRecord, doc: Optional[str]) -> None:
        """Export a collection.

        This only exports the collection's own state, not its associations with
        datasets.

        Parameters
        ----------
        record: `CollectionRecord`
            Object representing the collection to export.
        doc : `str` or `None`
            Documentation string for the collection.
        """
        raise NotImplementedError()

    @abstractmethod
    def saveDatasets(self, datasetType: DatasetType, run: str, *datasets: FileDataset) -> None:
        """Export one or more datasets, including their associated DatasetType
        and run information (but not including associated dimension
        information).

        Parameters
        ----------
        datasetType : `DatasetType`
            Type of all datasets being exported with this call.
        run : `str`
            Run associated with all datasets being exported with this call.
        datasets : `FileDataset`, variadic
            Per-dataset information to be exported.  `FileDataset.formatter`
            attributes should be strings, not `Formatter` instances or classes.
        """
        raise NotImplementedError()

    @abstractmethod
    def saveDatasetAssociations(
        self, collection: str, collectionType: CollectionType, associations: Iterable[DatasetAssociation]
    ) -> None:
        """Export the dataset-collection associations for a single collection.

        Parameters
        ----------
        collection : `str`
            The name of the collection.
        collectionType : `CollectionType`
            The type of the collection; either `CollectionType.TAGGED` or
            `CollectionType.CALIBRATION` (as other collection types are
            exported in other ways).
        associations : `Iterable` [ `DatasetAssociation` ]
            Structs representing an association between this collection and
            this dataset.
        """
        raise NotImplementedError()

    @abstractmethod
    def finish(self) -> None:
        """Complete the export process."""
        raise NotImplementedError()


class RepoImportBackend(ABC):
    """An abstract interface for data repository import implementations.

    Import backends are expected to be constructed with a description of
    the objects that need to be imported (from, e.g., a file written by the
    corresponding export backend), along with a `Registry`.
    """

    @abstractmethod
    def register(self) -> None:
        """Register all runs and dataset types associated with the backend with
        the `Registry` the backend was constructed with.

        These operations cannot be performed inside transactions, unlike those
        performed by `load`, and must in general be performed before `load`.
        """

    @abstractmethod
    def load(
        self,
        datastore: Optional[Datastore],
        *,
        directory: Optional[str] = None,
        transfer: Optional[str] = None,
        skip_dimensions: Optional[Set] = None,
        idGenerationMode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE,
        reuseIds: bool = False,
    ) -> None:
        """Import information associated with the backend into the given
        registry and datastore.

        This must be run after `register`, and may be performed inside a
        transaction.

        Parameters
        ----------
        datastore : `Datastore`
            Datastore to import into.  If `None`, datasets will only be
            inserted into the `Registry` (primarily intended for tests).
        directory : `str`, optional
            File all dataset paths are relative to.
        transfer : `str`, optional
            Transfer mode forwarded to `Datastore.ingest`.
        skip_dimensions : `set`, optional
            Dimensions that should be skipped and not imported. This can
            be useful when importing into a registry that already knows
            about a specific instrument.
        idGenerationMode : `DatasetIdGenEnum`, optional
            Specifies option for generating dataset IDs when IDs are not
            provided or their type does not match backend type. By default
            unique IDs are generated for each inserted dataset.
        reuseIds : `bool`, optional
            If `True` then forces re-use of imported dataset IDs for integer
            IDs which are normally generated as auto-incremented. This option
            has no effect on the use of globally-unique IDs which are always
            re-used (or generated if integer IDs are being imported).
        """
        raise NotImplementedError()
