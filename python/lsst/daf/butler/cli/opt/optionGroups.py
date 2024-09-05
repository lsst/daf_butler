# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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
from .options import collections_option, dataset_type_option, limit_option, order_by_option, where_option


class query_datasets_options(OptionGroup):  # noqa: N801
    """A collection of options common to querying datasets.

    Parameters
    ----------
    repo : `bool`
        The Butler repository URI.
    showUri : `bool`
        Whether to include the dataset URI.
    useArguments : `bool`
        Whether this is an argument or an option.
    default_limit : `int`
        The default value to use for the limit parameter.
    use_order_by : `bool`
        Whether to include an order_by option.
    """

    def __init__(
        self,
        repo: bool = True,
        showUri: bool = True,
        useArguments: bool = True,
        default_limit: int = -20_000,
        use_order_by: bool = True,
    ) -> None:
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
                    "--find-first/--no-find-first",
                    is_flag=True,
                    default=False,
                    help=unwrap(
                        """For each result data ID, only yield one DatasetRef of each
                                     DatasetType, from the first collection in which a dataset of that dataset
                                     type appears (according to the order of 'collections' passed in).  If
                                     used, 'collections' must specify at least one expression and must not
                                     contain wildcards."""
                    ),
                ),
                limit_option(
                    help="Limit the number of results that are processed. 0 means no limit. A negative "
                    "value specifies a cap where a warning will be issued if the cap is hit. "
                    f"Default value is {default_limit}.",
                    default=default_limit,
                ),
            ]
        )
        if use_order_by:
            self.decorators.append(order_by_option())
        if showUri:
            self.decorators.append(
                click.option("--show-uri", is_flag=True, help="Show the dataset URI in results.")
            )
