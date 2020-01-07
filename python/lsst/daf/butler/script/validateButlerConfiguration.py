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

from lsst.daf.butler import Butler, ValidationError


def processCommas(arg):
    """Given a list that might contain strings with commas, return expanded
    list.

    Parameters
    ----------
    arg : iterable of `str`
        Values read from command line.

    Returns
    -------
    expanded : `list` of `str`
        List where any items with commas are expanded into multiple entries.
    """
    expanded = []
    if arg is None:
        return expanded
    for item in arg:
        expanded.extend(item.split(","))
    return expanded


def build_argparser():
    """Construct an argument parser for the ``validateButlerConfiguration``
    script.

    Returns
    -------
    argparser : `argparse.ArgumentParser`
        The argument parser that defines the script
        command-line interface.
    """
    parser = argparse.ArgumentParser(description="Validate the configuration files for a "
                                     "Gen3 Butler repository.")
    parser.add_argument("root",
                        help="Filesystem path for an existing Butler repository.")
    parser.add_argument("--collection", "-c", default="validate", type=str,
                        help="Collection to refer to in this repository.")
    parser.add_argument("--quiet", "-q", action="store_true",
                        help="Do not report individual failures.")
    parser.add_argument("--datasettype", "-d", action="append", type=str,
                        help="Specific DatasetType(s) to validate (can be comma-separated)")
    parser.add_argument("--ignore", "-i", action="append", type=str,
                        help="DatasetType(s) to ignore for validation (can be comma-separated)")

    return parser


def validateButlerConfiguration(root, datasetTypes=None, ignore=None, quiet=False, collection="validate"):
    """Validate a bulter configuration.

    Parameters
    ----------
    root : `str`
        Butler root to validate.
    datasetTypes : `list` of `str`
        Dataset types to specifically check.
    ignore : `list` of `str`
        Dataset types to ignore.
    quiet : `bool`, optional
        If `True` report pass/fail but not details.
    collection : `str`, optional
        Collection to use. Sometimes this is needed to ensure that butler
        can be instantiated properly.

    Returns
    -------
    validates : `bool`
        `True` if everything looks okay, `False` if there is a problem.
    """
    logFailures = not quiet

    # The collection does not matter for validation but if a run is specified
    # in the configuration then it must be consistent with this collection
    butler = Butler(config=root, collection=collection)
    try:
        butler.validateConfiguration(logFailures=logFailures, datasetTypeNames=datasetTypes,
                                     ignore=ignore)
    except ValidationError:
        return False

    return True


def main():
    args = build_argparser().parse_args()

    # Process any commas in dataset type or ignore list
    ignore = processCommas(args.ignore)
    datasetTypes = processCommas(args.datasettype)

    valid = validateButlerConfiguration(args.root, datasetTypes, ignore, args.quiet, args.collection)

    if valid:
        print("No problems encountered with configuration.")
        return 0
    else:
        return 1
