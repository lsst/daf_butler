#!/usr/bin/env python
import argparse

from lsst.afw.image import bboxFromMetadata
from lsst.geom import Box2D
from lsst.sphgeom import ConvexPolygon
from lsst.log import Log

from lsst.daf.butler.gen2convert import repoConvert
from lsst.obs.subaru.gen3.hsc import HyperSuprimeCam

def InsertObservationRegions(confArray, registry, datastore, allowUpdate=False):
    """Add spatial regions for Visit-Detector combinations.
    """
    sql = ("SELECT Wcs.instrument AS instrument, Wcs.visit AS visit, Wcs.detector AS detector, "
           "        Wcs.dataset_id AS wcs, Metadata.dataset_id AS metadata "
           "    FROM Dataset Wcs "
           "        INNER JOIN DatasetCollection WcsCollection "
           "            ON (Wcs.dataset_id = WcsCollection.dataset_id) "
           "        INNER JOIN Dataset Metadata "
           "            ON (Wcs.instrument = Metadata.instrument "
           "                AND Wcs.visit = Metadata.visit "
           "                AND Wcs.detector = Metadata.detector) "
           "        INNER JOIN DatasetCollection MetadataCollection "
           "            ON (Metadata.dataset_id = MetadataCollection.dataset_id) "
           "    WHERE WcsCollection.collection = :collection "
           "          AND MetadataCollection.collection = :collection "
           "          AND Wcs.dataset_type_name = :wcsName"
           "          AND Metadata.dataset_type_name = :metadataName")
    log = Log.getLogger("lsst.daf.butler.gen2convert")
    import pdb
    pdb.set_trace()
    for config in confArray:
        log.info("Adding observation regions using %s from %s.",
                 config["DatasetType"], config["collection"])
        visits = {}
        with registry.transaction():
            for row in registry.query(sql, collection=config["collection"],
                                      wcsName="{}.wcs".format(config["DatasetType"]),
                                      metadataName="{}.metadata".format(config["DatasetType"])):
                wcsRef = registry.getDataset(row["wcs"])
                metadataRef = registry.getDataset(row["metadata"])
                wcs = datastore.get(wcsRef)
                metadata = datastore.get(metadataRef)
                bbox = Box2D(bboxFromMetadata(metadata))
                bbox.grow(config["padding"])
                region = ConvexPolygon([sp.getVector() for sp in wcs.pixelToSky(bbox.getCorners())])
                registry.setDimensionRegion({k: row[k] for k in ("instrument", "visit", "detector")},
                                            region=region, update=allowUpdate)
                visits.setdefault((row["instrument"], row["visit"]), []).extend(region.getVertices())
                print("VDI: %s %s %s" % (row["instrument"], row["visit"], row["detector"]))
        with registry.transaction():
            for (instrument, visit), vertices in visits.items():
                region = ConvexPolygon(vertices)
                registry.setDimensionRegion(instrument=instrument, visit=visit, region=region)
                print("Visit: %s %s" % (row["instrument"], row["visit"]))


def main(kwargs):

    confArray = []

    rawConfig = dict()
    rawConfig['DatasetType'] = 'raw'
    rawConfig['collection'] = 'raw'
    rawConfig['padding'] = 50
    confArray.append(rawConfig)

    calExpConfig = dict()
    calExpConfig['DatasetType'] = 'calexp'
    calExpConfig['collection'] = 'tmpColl'
    calExpConfig['padding'] = 5
    confArray.append(calExpConfig)

    # Use this to instantiate things
    g3g = repoConvert.Gen3Generic(kwargs)
    registry = g3g.getRegistry()
    datastore = g3g.getDatastore(registry)

    InsertObservationRegions(confArray, registry, datastore, allowUpdate=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert ci_hsc data repos to Butler Gen 3.")
    parser.add_argument("-v", "--verbose", action="store_const", dest="logLevel",
                        default=Log.INFO, const=Log.DEBUG,
                        help="Set the log level to DEBUG.")
    parser.add_argument("-R", "--root",
                        help="Destination of the repository to write (needs butler.yaml/gen3.sqlite3).")
    parser.add_argument("-N", "--name", help="Run Name of the data to ingest.")
    parser.add_argument("-S", "--source", help="Root of the repository to scan (needs _mapper).")
    parser.add_argument("-K", "--scan", help="Repo to scan (can be subdirectory of source).")
    parser.add_argument("-M", "--mapper", help="Mapper location.")
    parser.add_argument("-O", "--only", help="dataset to only include.")

    args = parser.parse_args()
    log = Log.getLogger("czw.1convert")
    log.setLevel(args.logLevel)
    main(vars(args))
