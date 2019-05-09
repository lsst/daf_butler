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
import logging

from lsst.daf.butler import Butler, Config


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create an empty Gen3 Butler repository.")
    parser.add_argument("root",
                        help=("Filesystem path for the new repository.  "
                              "Will be created if it does not exist."))
    parser.add_argument("-c", "--config",
                        help=("Path to an existing YAML config file to apply (on top of defaults)."))
    parser.add_argument("--standalone", action="store_true", default=False,
                        help=("Include all defaults in the config file in the repo, insulating "
                              "the repo from changes in package defaults."))
    parser.add_argument("--outfile", "-f", default=None, type=str,
                        help="Name of output file to receive repository configuration."
                             " Default is to write butler.yaml into the specified root.")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Turn on debug reporting.")
    parser.add_argument("--override", "-o", action="store_true",
                        help="Allow values in the supplied config to override any root settings.")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    forceConfigRoot = not args.override

    config = Config(args.config) if args.config is not None else None
    Butler.makeRepo(args.root, config=config, standalone=args.standalone, forceConfigRoot=forceConfigRoot,
                    outfile=args.outfile)
