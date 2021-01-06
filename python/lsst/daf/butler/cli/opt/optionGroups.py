# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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

import click

from . import (
    collections_option,
    datasets_option,
    dimensions_argument,
    glob_argument,
    repo_argument,
    where_option,
)
from ..utils import OptionGroup, unwrap, where_help


class query_datasets_options(OptionGroup):  # noqa: N801

    def __init__(self, repo=True, showUri=True):
        self.decorators = []
        if repo:
            self.decorators.append(repo_argument(required=True))
        self.decorators.extend([
            glob_argument(help=unwrap("""GLOB is one or more glob-style expressions that fully or partially
                                      identify the dataset types to be queried.""")),
            collections_option(),
            where_option(help=where_help),
            click.option("--find-first",
                         is_flag=True,
                         help=unwrap("""For each result data ID, only yield one DatasetRef of each
                                     DatasetType, from the first collection in which a dataset of that dataset
                                     type appears (according to the order of 'collections' passed in).  If
                                     used, 'collections' must specify at least one expression and must not
                                     contain wildcards."""))])
        if showUri:
            self.decorators.append(click.option("--show-uri",
                                                is_flag=True,
                                                help="Show the dataset URI in results."))



class query_data_ids_options(OptionGroup):  # noqa: N801
    """Decorator to add options for the query-data-ids command.

    Parameters
    ----------
    repo : `bool`
        If True the repository argument will be included in the group of
        options.
    """

    def __init__(self, repo=True):
        self.decorators = []
        if repo:
            self.decorators.append(repo_argument(required=True))
        self.decorators.extend([
            dimensions_argument(help=unwrap("""DIMENSIONS are the keys of the data IDs to yield, such as
                                            exposure, instrument, or tract. Will be expanded to include any
                                            dependencies.""")),
            collections_option(),
            datasets_option(help=unwrap("""An expression that fully or partially identifies dataset types that
                                        should constrain the yielded data IDs.  For example, including "raw"
                                        here would constrain the yielded "instrument", "exposure", "detector",
                                        and "physical_filter" values to only those for which at least one
                                        "raw" dataset exists in "collections".""")),
            where_option(help=where_help),
        ])
