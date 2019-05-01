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
import logging

from lsst.daf.butler import ButlerConfig

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dump a Butler configuration to standard output "
                                     "or a subset of one.")
    parser.add_argument("root",
                        help=("Filesystem path for an existing Butler repository or path to config file."))
    parser.add_argument("--subset", "-s", default=None, type=str,
                        help="Subset of a configuration to report. This can be any key in the"
                             " hierarchy such as '.datastore.root' where the leading '.' specified"
                             " the delimiter for the hierarchy.")
    parser.add_argument("--searchpath", "-p", action="append", type=str,
                        help="Additional search paths to use for configuration overrides")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Turn on debug reporting.")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    config = ButlerConfig(args.root, searchPaths=args.searchpath)

    if args.subset is not None:
        config = config[args.subset]

    try:
        config.dump(sys.stdout)
    except AttributeError:
        print(config)
