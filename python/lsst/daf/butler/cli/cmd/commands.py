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

from astropy.table import Table
import click
from collections import OrderedDict
import yaml

from ..opt import (collection_type_option, dataset_type_option, directory_argument, options_file_option,
                   glob_argument, repo_argument, transfer_option, verbose_option)
from ..utils import cli_handle_exception, split_commas, to_upper, typeStrAcceptsMultiple, unwrap
from ... import script


willCreateRepoHelp = "REPO is the URI or path to the new repository. Will be created if it does not exist."
existingRepoHelp = "REPO is the URI or path to an existing data repository root or configuration file."


# The conversion from the import command name to the butler_import function
# name for subcommand lookup is implemented in the cli/butler.py, in
# funcNameToCmdName and cmdNameToFuncName. If name changes are made here they
# must be reflected in that location. If this becomes a common pattern a better
# mechanism should be implemented.
@click.command("import")
@repo_argument(required=True, help=willCreateRepoHelp)
@directory_argument(required=True)
@transfer_option()
@click.option("--export-file",
              help="Name for the file that contains database information associated with the exported "
                   "datasets.  If this is not an absolute path, does not exist in the current working "
                   "directory, and --dir is provided, it is assumed to be in that directory.  Defaults "
                   "to \"export.yaml\".",
              type=click.File("r"))
@click.option("--skip-dimensions", "-s", type=str, multiple=True, callback=split_commas,
              metavar=typeStrAcceptsMultiple,
              help="Dimensions that should be skipped during import")
@options_file_option()
def butler_import(*args, **kwargs):
    """Import data into a butler repository."""
    cli_handle_exception(script.butlerImport, *args, **kwargs)


@click.command()
@repo_argument(required=True, help=willCreateRepoHelp)
@click.option("--seed-config", help="Path to an existing YAML config file to apply (on top of defaults).")
@click.option("--standalone", is_flag=True, help="Include all defaults in the config file in the repo, "
              "insulating the repo from changes in package defaults.")
@click.option("--override", is_flag=True, help="Allow values in the supplied config to override all "
              "repo settings.")
@click.option("--outfile", "-f", default=None, type=str, help="Name of output file to receive repository "
              "configuration. Default is to write butler.yaml into the specified repo.")
@options_file_option()
def create(*args, **kwargs):
    """Create an empty Gen3 Butler repository."""
    cli_handle_exception(script.createRepo, *args, **kwargs)


@click.command(short_help="Dump butler config to stdout.")
@repo_argument(required=True, help=existingRepoHelp)
@click.option("--subset", "-s", type=str,
              help="Subset of a configuration to report. This can be any key in the hierarchy such as "
              "'.datastore.root' where the leading '.' specified the delimiter for the hierarchy.")
@click.option("--searchpath", "-p", type=str, multiple=True, callback=split_commas,
              metavar=typeStrAcceptsMultiple,
              help="Additional search paths to use for configuration overrides")
@click.option("--file", "outfile", type=click.File("w"), default="-",
              help="Print the (possibly-expanded) configuration for a repository to a file, or to stdout "
              "by default.")
@options_file_option()
def config_dump(*args, **kwargs):
    """Dump either a subset or full Butler configuration to standard output."""
    cli_handle_exception(script.configDump, *args, **kwargs)


@click.command(short_help="Validate the configuration files.")
@repo_argument(required=True, help=existingRepoHelp)
@click.option("--quiet", "-q", is_flag=True, help="Do not report individual failures.")
@dataset_type_option(help="Specific DatasetType(s) to validate.", multiple=True)
@click.option("--ignore", "-i", type=str, multiple=True, callback=split_commas,
              metavar=typeStrAcceptsMultiple,
              help="DatasetType(s) to ignore for validation.")
@options_file_option()
def config_validate(*args, **kwargs):
    """Validate the configuration files for a Gen3 Butler repository."""
    is_good = cli_handle_exception(script.configValidate, *args, **kwargs)
    if not is_good:
        raise click.exceptions.Exit(1)


@click.command()
@repo_argument(required=True)
@click.option("--collection",
              help=unwrap("""Name of the collection to remove. If this is a TAGGED or CHAINED collection,
                          datasets within the collection are not modified unless --unstore is passed. If this
                          is a RUN collection, --purge and --unstore must be passed, and all datasets in it
                          are fully removed from the data repository. """))
@click.option("--purge",
              help=unwrap("""Permit RUN collections to be removed, fully removing datasets within them.
                          Requires --unstore as an added precaution against accidental deletion. Must not be
                          passed if the collection is not a RUN."""),
              is_flag=True)
@click.option("--unstore",
              help=("""Remove all datasets in the collection from all datastores in which they appear."""),
              is_flag=True)
@options_file_option()
def prune_collection(**kwargs):
    """Remove a collection and possibly prune datasets within it."""
    cli_handle_exception(script.pruneCollection, **kwargs)


@click.command(short_help="Search for collections.")
@repo_argument(required=True)
@glob_argument(help="GLOB is one or more glob-style expressions that fully or partially identify the "
                    "collections to return.")
@collection_type_option()
@click.option("--flatten-chains/--no-flatten-chains",
              help="Recursively get the child collections of matching CHAINED collections. Default is "
                   "--no-flatten-chains.")
@click.option("--include-chains/--no-include-chains",
              default=None,
              help="For --include-chains, return records for matching CHAINED collections. For "
                   "--no-include-chains do not return records for CHAINED collections. Default is the "
                   "opposite of --flatten-chains: include either CHAINED collections or their children, but "
                   "not both.")
@options_file_option()
def query_collections(*args, **kwargs):
    """Get the collections whose names match an expression."""
    print(yaml.dump(cli_handle_exception(script.queryCollections, *args, **kwargs)))


@click.command()
@repo_argument(required=True)
@glob_argument(help="GLOB is one or more glob-style expressions that fully or partially identify the "
                    "dataset types to return.")
@verbose_option(help="Include dataset type name, dimensions, and storage class in output.")
@click.option("--components/--no-components",
              default=None,
              help="For --components, apply all expression patterns to component dataset type names as well. "
                   "For --no-components, never apply patterns to components. Default (where neither is "
                   "specified) is to apply patterns to components only if their parent datasets were not "
                   "matched by the expression. Fully-specified component datasets (`str` or `DatasetType` "
                   "instances) are always included.")
@options_file_option()
def query_dataset_types(*args, **kwargs):
    """Get the dataset types in a repository."""
    print(yaml.dump(cli_handle_exception(script.queryDatasetTypes, *args, **kwargs), sort_keys=False))


@click.command()
@repo_argument(required=True)
@click.argument('dataset-type-name', nargs=1)
def remove_dataset_type(*args, **kwargs):
    """Remove a dataset type definition from a repository."""
    cli_handle_exception(script.removeDatasetType, *args, **kwargs)


@click.command()
@repo_argument(required=True)
@glob_argument(help="GLOB is one or more glob-style expressions that fully or partially identify the "
                    "dataset types to be queried.")
@click.option("--collections",
              help=unwrap("""One or more expressions that fully or partially identify the collections to
                          search for datasets.If not provided all datasets are returned."""),
              multiple=True,
              metavar=typeStrAcceptsMultiple,
              callback=split_commas)
@click.option("--where",
              help=unwrap("""A string expression similar to a SQL WHERE clause. May involve any column of a
                          dimension table or a dimension name as a shortcut for the primary key column of a
                          dimension table."""))
@click.option("--find-first",
              is_flag=True,
              help=unwrap("""For each result data ID, only yield one DatasetRef of each DatasetType, from the
                          first collection in which a dataset of that dataset type appears (according to the
                          order of `collections` passed in).  If used, `collections` must specify at least one
                          expression and must not contain wildcards."""))
@click.option("--components",
              type=click.Choice(["ALL", "NONE", "UNMATCHED"], case_sensitive=False),
              default="UNMATCHED",
              show_default=True,
              metavar="[ALL|NONE|UNMATCHED]",
              callback=to_upper,
              help=unwrap("""If UNMATCHED: apply dataset expression patterns to component dataset type names
                          only if their parent datasets were not matched by the expression. If ALL: apply all
                          dataset expression patterns to components. If NONE: never apply patterns to
                          components. Fully-specified component datasets are always included."""))
@options_file_option()
def query_datasets(**kwargs):
    """List the datasets in a repository."""
    datasets = cli_handle_exception(script.queryDatasets, **kwargs)

    tables = {}
    for datasetRef in datasets:
        rows = tables.get(datasetRef.datasetType.name, [])
        row = OrderedDict(type=datasetRef.datasetType.name, run=datasetRef.run, id=datasetRef.id)
        row.update(datasetRef.dataId.items())
        rows.append(row)
        tables[datasetRef.datasetType.name] = rows

    for datasetName, rows in tables.items():
        print("")
        Table(rows).pprint_all()
    print("")
