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

__all__ = ("main",)

import argparse
import sys
import logging

from lsst.daf.butler import ButlerConfig


def build_argparser():
    """Construct an argument parser for the ``dumpButlerConfig`` script.

    Returns
    -------
    argparser : `argparse.ArgumentParser`
        The argument parser that defines the script
        command-line interface.
    """

    parser = argparse.ArgumentParser(description="Dump either a subset or full Butler configuration to "
                                     "standard output.")
    parser.add_argument("root",
                        help="Filesystem path for an existing Butler repository or path to config file.")
    parser.add_argument("--subset", "-s", default=None, type=str,
                        help="Subset of a configuration to report. This can be any key in the"
                             " hierarchy such as '.datastore.root' where the leading '.' specified"
                             " the delimiter for the hierarchy.")
    parser.add_argument("--searchpath", "-p", action="append", type=str,
                        help="Additional search paths to use for configuration overrides")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Turn on debug reporting.")

    return parser


def dumpButlerConfig(root, searchPath=None, subset=None, outfh=sys.stdout):
    """Dump a config or subset to the specified file handle.

    Parameters
    ----------
    root : `str`
        Location of butler configuration.  Can be a path to a YAML
        file or a directory containing the configuration file.
    searchPath : `list` of `str`
        Directory paths for resolving the locations of configuration
        file lookups.
    subset : `str` or `tuple` of `str`
        Key into a subset of the configuration. If a string it should use
        the standard notation of supplying the relevant delimiter in the
        first character.
    outfh
        File descriptor to use to write the configuration content.
    """

    config = ButlerConfig(root, searchPaths=searchPath)

    if subset is not None:
        config = config[subset]

    try:
        config.dump(outfh)
    except AttributeError:
        print(config, file=outfh)


def main():
    args = build_argparser().parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    try:
        dumpButlerConfig(args.root, args.searchpath, args.subset, sys.stdout)
    except Exception as e:
        print(f"{e}", file=sys.stderr)
        return 1
    return 0
