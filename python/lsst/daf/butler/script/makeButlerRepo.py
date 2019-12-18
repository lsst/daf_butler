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

import sys
import argparse
import logging

from lsst.daf.butler import Butler, Config


def build_argparser():
    """Construct an argument parser for the ``makeButlerRepo`` script.

    Returns
    -------
    argparser : `argparse.ArgumentParser`
        The argument parser that defines the script
        command-line interface.
    """
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

    return parser


def makeButlerRepo(root, config=None, standalone=False, override=False, outfile=None):
    """Make a new Butler repository.

    Parameters
    ----------
    root : `str`
        Location to seed a butler repository.
    config : `str`, optional
        Configuration to seed the repository.
    standalone : `bool`, optional
        If `True` a fully expanded configuration will be written.
    override : `bool`, optional
        If `True` a root provided in the supplied config will not be
        overwritten.
    outfile : `str`, optional
        Path to output configuration. This can be left `None`
        if the configuration is to be written to ``root``.
    """
    forceConfigRoot = not override
    config = Config(config) if config is not None else None
    Butler.makeRepo(root, config=config, standalone=standalone, forceConfigRoot=forceConfigRoot,
                    outfile=outfile)


def main():
    args = build_argparser().parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    try:
        makeButlerRepo(args.root, args.config, args.standalone, args.override, args.outfile)
    except Exception as e:
        print(f"{e}", file=sys.stderr)
        return 1

    return 0
