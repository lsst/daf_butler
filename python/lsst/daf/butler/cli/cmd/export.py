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


import click
from functools import partial, update_wrapper

from ..opt import (
    directory_option,
    format_option,
    query_data_ids_options,
    repo_argument
)
from ..utils import ButlerCommand, ButlerGroup, unwrap
from ... import Butler
from ... import script
from ...script import export as exportScript


def processor(f):
    """Helper decorator to rewrite a function so that it returns another
    function from it.
    """

    def new_func(*args, **kwargs):
        def processor(stream):
            return f(stream, *args, **kwargs)

        return processor

    return update_wrapper(new_func, f)


@click.group(cls=ButlerGroup, chain=True)
@repo_argument(required=True)
@directory_option(help="Directory dataset files should be written to if --transfer is used.")
@format_option(help=unwrap("""File format for the database information file. If not provided, the extension of
                           --filename will be used."""))
def export(*args, **kwargs):
    """Export data from a repository."""
    pass  # all implementation is in process_commands


@export.resultcallback()
@click.pass_context
def process_commands(ctx, processors, repo, directory, format_):
    # import pdb; pdb.set_trace()
    butler = Butler(repo)

# TODO NEXT: export is probably going to need some of its options filled in.
# then, work on seeing if query_data_ids works inside of exportDataIds, and
# pass the results into context.saveDataIds

    with butler.export(directory=directory, format=format_) as repoExportContext:
        for processor in processors:
            processor(butler, repoExportContext)


@export.command(short_help="Export one or more datasets.",
                cls=ButlerCommand)
@processor
def datasets(repoExportContext, **kwargs):
    """Export any DatasetType, RUN collections, and dimension records
    associated with the datasets.
    """
    print("export datasets " + kwargs["repo"])


@export.command(cls=ButlerCommand)
@query_data_ids_options(repo=False)
def data_ids(**kwargs):
    """Export dimension records associated with data IDs."""
    return partial(exportScript.exportDataIds, **kwargs)


@export.command(cls=ButlerCommand)
@repo_argument(required=True)
def dimension_data(repoExportContext, **kwargs):
    """Export dimension records associated with data IDs.
    """
    print("export dimension-data " + kwargs["repo"])


@export.command(cls=ButlerCommand)
@repo_argument(required=True)
def collection(repoExportContext, **kwargs):
    """Export a collection.
    """
    print("export collection " + kwargs["repo"])
