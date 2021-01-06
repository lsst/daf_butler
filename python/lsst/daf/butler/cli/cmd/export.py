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
    query_datasets_options,
    repo_argument,
    transfer_option,
)
from ..utils import (
    butler_epilog,
    MWCommand,
    ButlerGroup,
    MWOptionDecorator,
    split_commas,
    unwrap,
)
from ... import Butler
from ... import script
from ...script import export as exportScript


class ExportGroup(ButlerGroup):

    extra_epilog = "See 'butler export <command> --help' or 'butler --help' for more information."


@click.group(cls=ExportGroup, chain=True)
@repo_argument()
@directory_option(help="Directory dataset files should be written to if --transfer is used.")
@format_option(help=unwrap("""File format for the database information file. If not provided, the extension of
                           --filename will be used."""))
@transfer_option()
def export(*args, **kwargs):
    """Export data from a repository."""
    pass  # all implementation is in process_commands


@export.resultcallback()
@click.pass_context
def process_commands(ctx, processors, repo, directory, format_, transfer):
    butler = Butler(repo)
    with butler.export(directory=directory, format=format_, transfer="copy") as repoExportContext:
        for processor in processors:
            processor(butler, repoExportContext)


class ExportCommand(MWCommand):

    extra_epilog = f"See 'butler export --help' or 'butler --help' for more options."


@export.command(short_help="Export one or more datasets.",
                cls=ExportCommand)
@query_datasets_options(repo=False, showUri=False, useArguments=False)
def datasets(**kwargs):
    """Export any DatasetType, RUN collections, and dimension records
    associated with the datasets.
    """
    return partial(exportScript.exportDatasets, **kwargs)


@export.command(cls=ExportCommand)
@query_data_ids_options(repo=False)
def data_ids(**kwargs):
    """Export dimension records associated with data IDs."""
    return partial(exportScript.exportDataIds, **kwargs)


@export.command(cls=ExportCommand)
@repo_argument(required=True)
def dimension_data(repoExportContext, **kwargs):
    """Export dimension records associated with data IDs.
    """
    # I haven't yet found any uses of this in scripts (in the lsst github org)
    raise NotImplementedError()


names_option = MWOptionDecorator("--name", "names",
                                 help="The name of one or more collections to export.",
                                 callback=split_commas,
                                 multiple=True,
                                 required=True)


@export.command(cls=ExportCommand)
@names_option()
def collection(**kwargs):
    """Export a collection.
    """
    return partial(exportScript.exportCollection, **kwargs)
