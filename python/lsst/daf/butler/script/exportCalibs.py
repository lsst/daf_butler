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

import logging
import os

from astropy.table import Table

from .._butler import Butler
from ..registry import CollectionType

log = logging.getLogger(__name__)


def parseCalibrationCollection(registry, collection, datasetTypes):
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
    exportDatasets : `list` [`lsst.daf.butler.queries.DatasetQueryResults`]
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
            exportCollections.append(ref.run)
    return exportCollections, exportDatasets


def exportCalibs(repo, directory, collections, dataset_type, transfer):
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

    if not dataset_type:
        dataset_type = ...
    if not collections:
        collections = ...

    calibTypes = [
        datasetType
        for datasetType in butler.registry.queryDatasetTypes(dataset_type)
        if datasetType.isCalibration()
    ]

    collectionsToExport = []
    datasetsToExport = []

    for collection in butler.registry.queryCollections(
        collections,
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

    requiredDimensions = set()
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
