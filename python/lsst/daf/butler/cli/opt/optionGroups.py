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


__all__ = ("query_datasets_options",)


import click

from ..utils import OptionGroup, unwrap, where_help
from .arguments import glob_argument, repo_argument
from .options import collections_option, dataset_type_option, where_option


class query_datasets_options(OptionGroup):  # noqa: N801
    def __init__(self, repo: bool = True, showUri: bool = True, useArguments: bool = True) -> None:
        self.decorators = []
        if repo:
            if not useArguments:
                raise RuntimeError("repo as an option is not currently supported.")
            self.decorators.append(repo_argument(required=True))
        if useArguments:
            self.decorators.append(
                glob_argument(
                    help=unwrap(
                        """GLOB is one or more glob-style expressions that fully or partially identify the
                            dataset type names to be queried."""
                    )
                )
            )
        else:
            self.decorators.append(
                dataset_type_option(
                    help=unwrap(
                        """One or more glob-style expressions that fully or partially identify the dataset
                            type names to be queried."""
                    )
                )
            )
        self.decorators.extend(
            [
                collections_option(),
                where_option(help=where_help),
                click.option(
                    "--find-first",
                    is_flag=True,
                    help=unwrap(
                        """For each result data ID, only yield one DatasetRef of each
                                     DatasetType, from the first collection in which a dataset of that dataset
                                     type appears (according to the order of 'collections' passed in).  If
                                     used, 'collections' must specify at least one expression and must not
                                     contain wildcards."""
                    ),
                ),
            ]
        )
        if showUri:
            self.decorators.append(
                click.option("--show-uri", is_flag=True, help="Show the dataset URI in results.")
            )
