#!/usr/bin/env python

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

import argparse
import sys

from lsst.daf.butler import Butler, ValidationError


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate the configuration files for a "
                                     "Gen3 Butler repository.")
    parser.add_argument("root",
                        help="Filesystem path for an existing Butler repository.")
    parser.add_argument("--collection", "-c", default="validate", type=str,
                        help="Collection to refer to in this repository.")
    parser.add_argument("--quiet", "-q", action="store_true",
                        help="Do not report individual failures.")
    parser.add_argument("--datasettype", "-d", action="append", type=str,
                        help="Specific DatasetType to validate")
    parser.add_argument("--ignore", "-i", action="append", type=str,
                        help="DatasetType(s) to ignore for validation (e.g., 'raw')")

    args = parser.parse_args()
    # The collection does not matter for validation but if a run is specified
    # in the configuration then it must be consistent with this collection
    butler = Butler(config=args.root, collection=args.collection)
    try:
        logFailures = True
        if args.quiet:
            logFailures = False
        butler.validateConfiguration(logFailures=logFailures, datasetTypeNames=args.datasettype,
                                     ignore=args.ignore)
    except ValidationError:
        sys.exit(1)
    else:
        print("No problems encountered with configuration.")
