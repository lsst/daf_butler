#!/usr/bin/env python
import argparse

from lsst.log import Log

# from lsst.obs.subaru.gen3.hsc import HyperSuprimeCam

from lsst.daf.butler.gen2convert import repoConvert


def main(kwargs):
    g3g = repoConvert.gen3generic(kwargs)

    registry = g3g.getRegistry()
    datastore = g3g.getDatastore(registry)

    walker = g3g.walk()
    walker.readObsInfo()
    g3g.write(walker, registry, datastore)
    #    HyperSuprimeCam().writeCuratedCalibrations(gen3.getButler("calib"))


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
