#!/usr/bin/env python
import argparse

from lsst.log import Log

from lsst.daf.butler.gen2convert import repoConvert
from lsst.obs.subaru.gen3.hsc import HyperSuprimeCam


def main(kwargs):
    g3g = repoConvert.Gen3Generic(kwargs)

    registry = g3g.getRegistry()
    datastore = g3g.getDatastore(registry)

    walker = g3g.walk()
    walker.readObsInfo()
    g3g.write(walker, registry, datastore)

    if isinstance(g3g.instrument, HyperSuprimeCam) and kwargs["curated_calibs"] is True:
        HyperSuprimeCam().writeCuratedCalibrations(g3g.getButler("calib"))


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
    parser.add_argument("-P", "--parent", action="store_true",
                        help="include as if from the parent directory.")
    parser.add_argument("-C", "--curated_calibs", action="store_true",
                        help="Add instrument curated calibs.")

    args = parser.parse_args()
    log = Log.getLogger("czw.1convert")
    log.setLevel(args.logLevel)
    main(vars(args))
