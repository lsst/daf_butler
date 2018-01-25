#!/usr/bin/env python
"""Dump Gen3 Registry tables as NumPy save files, using a Gen2 Butler
pointed at an repository that includes HSC calexp, bias, dark, and flat files.

The calexp datasets will not be included, but the corresponding raw datasets
will be; we need calexp.wcs to determine spatial regions.

This script populates the Dataset, Visit, Snap, and ObservedSensor tables as all
 *DatasetJoin tables for the added Datasets.
"""

import os

import numpy as np

from lsst.utils import getPackageDir
from lsst.daf.base import DateTime
from lsst.sphgeom import ConvexPolygon
from lsst.daf.persistence import Butler
from lsst.daf.butler.regions import makeBoxWcsRegion


DATA_ROOT = os.path.split(__file__)[0]

dtypes = {
    "Dataset": np.dtype([('dataset_id', '<i8'), ('registry_id', '<i8'), ('dataset_type_name', '<U64'),
                         ('uri', '<U512'), ('run_id', '<i8')]),
    "Snap": np.dtype([('camera_name', '<U16'), ('visit_number', '<i8'), ('obs_begin', '<U128'),
                      ('exposure_time', '<f8'), ('snap_index', '<i8')]),
    "ObservedSensor": np.dtype([('camera_name', '<U16'), ('visit_number', '<i8'),
                                ('physical_sensor_number', '<i2'), ('region', 'S128')]),
    "Visit": np.dtype([('camera_name', '<U16'), ('visit_number', '<i8'), ('obs_begin', '<U128'),
                       ('exposure_time', '<f8'), ('physical_filter_name', '<U16'), ('region', 'S256')]),
    "VisitRange": np.dtype([('camera_name', '<U16'), ('visit_begin', '<i8'), ('visit_end', '<i8')]),
    "SnapDatasetJoin": np.dtype([('camera_name', '<U16'), ('visit_number', '<i8'), ('snap_index', '<i8'),
                                 ('dataset_id', '<i8'), ('registry_id', '<i8')]),
    "PhysicalSensorDatasetJoin": np.dtype([('camera_name', '<U16'), ('physical_sensor_number', '<i8'),
                                           ('dataset_id', '<i8'), ('registry_id', '<i8')]),
    "VisitRangeDatasetJoin": np.dtype([('camera_name', '<U16'), ('visit_begin', '<i8'), ('visit_end', '<i8'),
                                       ('dataset_id', '<i8'), ('registry_id', '<i8')]),
    "PhysicalFilterDatasetJoin": np.dtype([('camera_name', '<U16'), ('physical_filter_name', '<U16'),
                                           ('dataset_id', '<i8'), ('registry_id', '<i8')]),

}

tables = {
    "VisitRange": np.array(
        [("HSC", 204, 909138),
         ("HSC", 902420, 905024)],
        dtype=dtypes["VisitRange"]
    ),
}


def selectCalibUnits(datasetType, sensor, filter=None):
    """Select the VisitRange and possibly filter appropriate for a
    calibration DatasetType and PhysicalFilter name.

    This logic is specific to ci_hsc, as the VisitRange table was constructed
    by hand by comparing the begin/end dates in its calibRegistry.sqlite3 to
    the visits in a more complete (Gen2) registry.sqlite3 database. A more
    general script for populating a Gen3 Registry from Gen2 would have to
    include creating the VisitRange table by doing that translation at the
    same time, and it would probably require direct access to the sqlite Gen2
    registry database instead of just a butler subset.
    """
    if datasetType == 'flat' and filter == 'HSC-R':
        record = tables["VisitRange"][1]
    else:
        record = tables["VisitRange"][0]
    if datasetType == 'flat':
        return (record["visit_begin"], record["visit_end"], sensor, filter)
    else:
        return (record["visit_begin"], record["visit_end"], sensor)


def main(subset, margin=10):
    visits = {}
    regions = {}
    vertices = {}
    raw = {}
    bias = {}
    dark = {}
    flat = {}
    for ref in subset:
        visit = ref.dataId["visit"]
        sensor = ref.dataId["ccd"]
        filter = ref.dataId["filter"]
        if visit not in visits:
            info = ref.get("calexp_visitInfo")
            obsMid = info.getDate()
            expTime = info.getExposureTime()
            # convert from middle of exposure date/time to beginning,
            # by subtracting half of the exposure time in nanoseconds
            obsBegin = DateTime(obsMid.nsecs() - int(expTime)*500000000)
            visits[visit] = (filter, obsBegin, expTime)
        raw[visit, sensor] = ref.get("raw_filename")[0]
        biasUnits = selectCalibUnits("bias", sensor)
        assert biasUnits[0] <= visit and biasUnits[1] > visit
        bias[biasUnits] = ref.get("bias_filename")[0]
        darkUnits = selectCalibUnits("dark", sensor)
        assert darkUnits[0] <= visit and darkUnits[1] > visit
        dark[darkUnits] = ref.get("dark_filename")[0]
        flatUnits = selectCalibUnits("flat", sensor, filter)
        assert flatUnits[0] <= visit and flatUnits[1] > visit
        flat[flatUnits] = ref.get("flat_filename")[0]
        bbox = ref.get("calexp_bbox")
        wcs = ref.get("calexp_wcs")
        region = makeBoxWcsRegion(box=bbox, wcs=wcs, margin=margin)
        regions[visit, sensor] = region
        vertices.setdefault(visit, []).extend(region.getVertices())

    tables["Dataset"] = np.zeros(len(raw) + len(bias) + len(dark) + len(flat),
                                 dtype=dtypes["Dataset"])
    tables["Visit"] = np.zeros(len(visits), dtype=dtypes["Visit"])
    tables["Snap"] = np.zeros(len(visits), dtype=dtypes["Snap"])
    tables["ObservedSensor"] = np.zeros(len(raw), dtype=dtypes["ObservedSensor"])
    tables["ObservedSensor"] = np.zeros(len(raw), dtype=dtypes["ObservedSensor"])
    tables["SnapDatasetJoin"] = np.zeros(len(raw), dtype=dtypes["SnapDatasetJoin"])
    tables["PhysicalSensorDatasetJoin"] = np.zeros(len(raw) + len(bias) + len(dark) + len(flat),
                                                   dtype=dtypes["PhysicalSensorDatasetJoin"])
    tables["PhysicalFilterDatasetJoin"] = np.zeros(len(flat), dtype=dtypes["PhysicalFilterDatasetJoin"])
    tables["VisitRangeDatasetJoin"] = np.zeros(len(flat) + len(bias) + len(dark),
                                               dtype=dtypes["VisitRangeDatasetJoin"])

    snapIndex = 1
    cameraName = "HSC"
    for n, (visit, (filter, obsBegin, expTime)) in enumerate(visits.items()):
        visitRecord = tables["Visit"][n]
        visitRecord["visit_number"] = visit
        visitRecord["physical_filter_name"] = filter
        visitRecord["obs_begin"] = obsBegin
        visitRecord["exposure_time"] = expTime
        visitRecord["region"] = ConvexPolygon.convexHull(vertices[visit]).encode()
        visitRecord["camera_name"] = cameraName
        snapRecord = tables["Snap"][n]
        snapRecord["visit_number"] = visit
        snapRecord["snap_index"] = snapIndex
        snapRecord["obs_begin"] = obsBegin
        snapRecord["exposure_time"] = expTime
        snapRecord["camera_name"] = cameraName

    datasetId = 1
    registryId = 1
    runId = 0
    for n, ((visit, sensor), uri) in enumerate(raw.items()):
        datasetRecord = tables["Dataset"][n]
        datasetRecord["dataset_id"] = datasetId
        datasetRecord["registry_id"] = registryId
        datasetRecord["uri"] = uri
        datasetRecord["dataset_type_name"] = "raw"
        datasetRecord["run_id"] = runId
        osRecord = tables["ObservedSensor"][n]
        osRecord["visit_number"] = visit
        osRecord["physical_sensor_number"] = sensor
        osRecord["camera_name"] = cameraName
        osRecord["region"] = regions[visit, sensor].encode()
        snapJoinRecord = tables["SnapDatasetJoin"][n]
        snapJoinRecord["dataset_id"] = datasetId
        snapJoinRecord["registry_id"] = registryId
        snapJoinRecord["visit_number"] = visit
        snapJoinRecord["camera_name"] = cameraName
        snapJoinRecord["snap_index"] = snapIndex
        psJoinRecord = tables["PhysicalSensorDatasetJoin"][n]
        psJoinRecord["dataset_id"] = datasetId
        psJoinRecord["registry_id"] = registryId
        psJoinRecord["physical_sensor_number"] = sensor
        psJoinRecord["camera_name"] = cameraName
        datasetId += 1

    for n1, ((visitBegin, visitEnd, sensor), uri) in enumerate(bias.items()):
        n2 = n1 + len(raw)
        n3 = n1
        datasetRecord = tables["Dataset"][n2]
        datasetRecord["dataset_id"] = datasetId
        datasetRecord["registry_id"] = registryId
        datasetRecord["uri"] = uri
        datasetRecord["dataset_type_name"] = "bias"
        datasetRecord["run_id"] = runId
        psJoinRecord = tables["PhysicalSensorDatasetJoin"][n2]
        psJoinRecord["dataset_id"] = datasetId
        psJoinRecord["registry_id"] = registryId
        psJoinRecord["physical_sensor_number"] = sensor
        psJoinRecord["camera_name"] = cameraName
        vrJoinRecord = tables["VisitRangeDatasetJoin"][n3]
        vrJoinRecord["dataset_id"] = datasetId
        vrJoinRecord["registry_id"] = registryId
        vrJoinRecord["visit_begin"] = visitBegin
        vrJoinRecord["visit_end"] = visitEnd
        vrJoinRecord["camera_name"] = cameraName
        datasetId += 1

    for n1, ((visitBegin, visitEnd, sensor), uri) in enumerate(dark.items()):
        n2 = n1 + len(raw) + len(bias)
        n3 = n1 + len(bias)
        datasetRecord = tables["Dataset"][n2]
        datasetRecord["dataset_id"] = datasetId
        datasetRecord["registry_id"] = registryId
        datasetRecord["uri"] = uri
        datasetRecord["dataset_type_name"] = "dark"
        datasetRecord["run_id"] = runId
        psJoinRecord = tables["PhysicalSensorDatasetJoin"][n2]
        psJoinRecord["dataset_id"] = datasetId
        psJoinRecord["registry_id"] = registryId
        psJoinRecord["physical_sensor_number"] = sensor
        psJoinRecord["camera_name"] = cameraName
        vrJoinRecord = tables["VisitRangeDatasetJoin"][n3]
        vrJoinRecord["dataset_id"] = datasetId
        vrJoinRecord["registry_id"] = registryId
        vrJoinRecord["visit_begin"] = visitBegin
        vrJoinRecord["visit_end"] = visitEnd
        vrJoinRecord["camera_name"] = cameraName
        datasetId += 1

    for n1, ((visitBegin, visitEnd, sensor, filter), uri) in enumerate(flat.items()):
        n2 = n1 + len(raw) + len(bias) + len(dark)
        n3 = n1 + len(bias) + len(dark)
        datasetRecord = tables["Dataset"][n2]
        datasetRecord["dataset_id"] = datasetId
        datasetRecord["registry_id"] = registryId
        datasetRecord["uri"] = uri
        datasetRecord["dataset_type_name"] = "flat"
        datasetRecord["run_id"] = runId
        psJoinRecord = tables["PhysicalSensorDatasetJoin"][n2]
        psJoinRecord["dataset_id"] = datasetId
        psJoinRecord["registry_id"] = registryId
        psJoinRecord["physical_sensor_number"] = sensor
        psJoinRecord["camera_name"] = cameraName
        vrJoinRecord = tables["VisitRangeDatasetJoin"][n3]
        vrJoinRecord["dataset_id"] = datasetId
        vrJoinRecord["registry_id"] = registryId
        vrJoinRecord["visit_begin"] = visitBegin
        vrJoinRecord["visit_end"] = visitEnd
        vrJoinRecord["camera_name"] = cameraName
        pfJoinRecord = tables["PhysicalFilterDatasetJoin"][n1]
        pfJoinRecord["dataset_id"] = datasetId
        pfJoinRecord["registry_id"] = registryId
        pfJoinRecord["physical_filter_name"] = filter
        pfJoinRecord["camera_name"] = cameraName
        datasetId += 1

    for name, table in tables.items():
        np.save(os.path.join(DATA_ROOT, "{}.npy".format(name)), table)


if __name__ == "__main__":
    butler = Butler(os.path.join(getPackageDir("ci_hsc"), "DATA", "rerun", "ci_hsc"))
    main(butler.subset("raw"))
