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

import logging
import os
from collections.abc import Iterable
from typing import TYPE_CHECKING

from astropy.table import Table

from .._butler import Butler
from ..registry import CollectionType

if TYPE_CHECKING:
    from lsst.daf.butler import DatasetRef, DatasetType, Registry

log = logging.getLogger(__name__)


def parseCalibrationCollection(
    registry: Registry, collection: str, datasetTypes: Iterable[DatasetType]
) -> tuple[list[str], list[DatasetRef]]:
    """Search a calibration collection for calibration datasets.

    Parameters
    ----------
    registry : `lsst.daf.butler.Registry`
        Butler registry to use.
    collection : `str`
        Collection to search.  This should be a CALIBRATION
        collection.
    datasetTypes : `list` [`lsst.daf.Butler.DatasetType`]
        List of calibration dataset types.

    Returns
    -------
    exportCollections : `list` [`str`]
        List of collections to save on export.
    exportDatasets : `list` [`lsst.daf.butler.DatasetRef`]
        Datasets to save on export.

    Raises
    ------
    RuntimeError
        Raised if the collection to search is not a CALIBRATION collection.
    """
    if registry.getCollectionType(collection) != CollectionType.CALIBRATION:
        raise RuntimeError(f"Collection {collection} is not a CALIBRATION collection.")

    exportCollections = []
    exportDatasets = []
    for calibType in datasetTypes:
        associations = registry.queryDatasetAssociations(
            calibType, collections=collection, collectionTypes=[CollectionType.CALIBRATION]
        )
        for result in associations:
            # Need an expanded dataId in case file templates will be used
            # in the transfer.
            dataId = registry.expandDataId(result.ref.dataId)
            ref = result.ref.expanded(dataId)
            exportDatasets.append(ref)
            assert ref.run is not None, "These refs must all be resolved."
            exportCollections.append(ref.run)
    return exportCollections, exportDatasets


def exportCalibs(
    repo: str, directory: str, collections: Iterable[str], dataset_type: Iterable[str], transfer: str
) -> Table:
    """Certify a set of calibrations with a validity range.

    Parameters
    ----------
    repo : `str`
        URI to the location of the repo or URI to a config file
        describing the repo and its location.
    directory : `str`
        URI string of the directory to write the exported
        calibrations.
    collections : `list` [`str`]
       Data collections to pull calibrations from.  Must be an
       existing `~CollectionType.CHAINED` or
       `~CollectionType.CALIBRATION` collection.
    dataset_type : `tuple` [`str`]
       The dataset types to export. Default is to export all.
    transfer : `str`
        The transfer mode to use for exporting.

    Returns
    -------
    datasetTable : `astropy.table.Table`
        A table containing relevant information about the calibrations
        exported.

    Raises
    ------
    RuntimeError :
        Raised if the output directory already exists.
    """
    butler = Butler(repo, writeable=False)

    dataset_type_query = dataset_type if dataset_type else ...
    collections_query = collections if collections else ...

    calibTypes = [
        datasetType
        for datasetType in butler.registry.queryDatasetTypes(dataset_type_query)
        if datasetType.isCalibration()
    ]

    collectionsToExport = []
    datasetsToExport = []

    for collection in butler.registry.queryCollections(
        collections_query,
        flattenChains=True,
        includeChains=True,
        collectionTypes={CollectionType.CALIBRATION, CollectionType.CHAINED},
    ):
        log.info("Checking collection: %s", collection)

        # Get collection information.
        collectionsToExport.append(collection)
        collectionType = butler.registry.getCollectionType(collection)
        if collectionType == CollectionType.CALIBRATION:
            exportCollections, exportDatasets = parseCalibrationCollection(
                butler.registry, collection, calibTypes
            )
            collectionsToExport.extend(exportCollections)
            datasetsToExport.extend(exportDatasets)

    if os.path.exists(directory):
        raise RuntimeError(f"Export directory exists: {directory}")
    os.makedirs(directory)
    with butler.export(directory=directory, format="yaml", transfer=transfer) as export:
        collectionsToExport = list(set(collectionsToExport))
        datasetsToExport = list(set(datasetsToExport))

        for exportable in collectionsToExport:
            try:
                export.saveCollection(exportable)
            except Exception as e:
                log.warning("Did not save collection %s due to %s.", exportable, e)

        log.info("Saving %d dataset(s)", len(datasetsToExport))
        export.saveDatasets(datasetsToExport)

    sortedDatasets = sorted(datasetsToExport, key=lambda x: x.datasetType.name)

    requiredDimensions: set[str] = set()
    for ref in sortedDatasets:
        requiredDimensions.update(ref.dimensions.names)
    dimensionColumns = {
        dimensionName: [ref.dataId.get(dimensionName, "") for ref in sortedDatasets]
        for dimensionName in requiredDimensions
    }

    return Table(
        {
            "calibrationType": [ref.datasetType.name for ref in sortedDatasets],
            "run": [ref.run for ref in sortedDatasets],
            **dimensionColumns,
        }
    )
