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

from ..opt import (
    collection_type_option,
    collection_argument,
    collections_argument,
    collections_option,
    components_option,
    dataset_type_option,
    datasets_option,
    dimensions_argument,
    directory_argument,
    element_argument,
    glob_argument,
    options_file_option,
    query_datasets_options,
    repo_argument,
    transfer_option,
    verbose_option,
    where_option,
)

from ..utils import (
    ButlerCommand,
    MWOptionDecorator,
    option_section,
    printAstropyTables,
    split_commas,
    to_upper,
    typeStrAcceptsMultiple,
    unwrap,
    where_help,
)

from ... import script


willCreateRepoHelp = "REPO is the URI or path to the new repository. Will be created if it does not exist."
existingRepoHelp = "REPO is the URI or path to an existing data repository root or configuration file."


@click.command(cls=ButlerCommand, short_help="Add existing datasets to a tagged collection.")
@repo_argument(required=True)
@collection_argument(help="COLLECTION is the collection the datasets should be associated with.")
@query_datasets_options(repo=False, showUri=False, useArguments=False)
@options_file_option()
def associate(**kwargs):
    """Add existing datasets to a tagged collection; searches for datasets with
    the options and adds them to the named COLLECTION.
    """
    script.associate(**kwargs)


# The conversion from the import command name to the butler_import function
# name for subcommand lookup is implemented in the cli/butler.py, in
# funcNameToCmdName and cmdNameToFuncName. If name changes are made here they
# must be reflected in that location. If this becomes a common pattern a better
# mechanism should be implemented.
@click.command("import", cls=ButlerCommand)
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
    script.butlerImport(*args, **kwargs)


@click.command(cls=ButlerCommand)
@repo_argument(required=True, help=willCreateRepoHelp)
@click.option("--seed-config", help="Path to an existing YAML config file to apply (on top of defaults).")
@click.option("--dimension-config", help="Path to an existing YAML config file with dimension configuration.")
@click.option("--standalone", is_flag=True, help="Include all defaults in the config file in the repo, "
              "insulating the repo from changes in package defaults.")
@click.option("--override", is_flag=True, help="Allow values in the supplied config to override all "
              "repo settings.")
@click.option("--outfile", "-f", default=None, type=str, help="Name of output file to receive repository "
              "configuration. Default is to write butler.yaml into the specified repo.")
@options_file_option()
def create(*args, **kwargs):
    """Create an empty Gen3 Butler repository."""
    script.createRepo(*args, **kwargs)


@click.command(short_help="Dump butler config to stdout.", cls=ButlerCommand)
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
    script.configDump(*args, **kwargs)


@click.command(short_help="Validate the configuration files.", cls=ButlerCommand)
@repo_argument(required=True, help=existingRepoHelp)
@click.option("--quiet", "-q", is_flag=True, help="Do not report individual failures.")
@dataset_type_option(help="Specific DatasetType(s) to validate.", multiple=True)
@click.option("--ignore", "-i", type=str, multiple=True, callback=split_commas,
              metavar=typeStrAcceptsMultiple,
              help="DatasetType(s) to ignore for validation.")
@options_file_option()
def config_validate(*args, **kwargs):
    """Validate the configuration files for a Gen3 Butler repository."""
    is_good = script.configValidate(*args, **kwargs)
    if not is_good:
        raise click.exceptions.Exit(1)


@click.command(cls=ButlerCommand)
@repo_argument(required=True)
@collection_argument(help=unwrap("""COLLECTION is the Name of the collection to remove. If this is a tagged or
                          chained collection, datasets within the collection are not modified unless --unstore
                          is passed. If this is a run collection, --purge and --unstore must be passed, and
                          all datasets in it are fully removed from the data repository."""))
@click.option("--purge",
              help=unwrap("""Permit RUN collections to be removed, fully removing datasets within them.
                          Requires --unstore as an added precaution against accidental deletion. Must not be
                          passed if the collection is not a RUN."""),
              is_flag=True)
@click.option("--unstore",
              help=("""Remove all datasets in the collection from all datastores in which they appear."""),
              is_flag=True)
@click.option("--unlink",
              help="Before removing the given `collection` unlink it from from this parent collection.",
              multiple=True,
              callback=split_commas)
@options_file_option()
def prune_collection(**kwargs):
    """Remove a collection and possibly prune datasets within it."""
    script.pruneCollection(**kwargs)


pruneDatasets_wouldRemoveMsg = unwrap("""The following datasets will be removed from any datastores in which
                                      they are present:""")
pruneDatasets_wouldDisassociateMsg = unwrap("""The following datasets will be disassociated from {collections}
                                            if they are currently present in it (which is not checked):""")
pruneDatasets_wouldDisassociateAndRemoveMsg = unwrap("""The following datasets will be disassociated from
                                                  {collections} if they are currently present in it (which is
                                                  not checked), and removed from any datastores in which they
                                                  are present.""")
pruneDatasets_willRemoveMsg = "The following datasets will be removed:"
pruneDatasets_askContinueMsg = "Continue?"
pruneDatasets_didRemoveAforementioned = "The datasets were removed."
pruneDatasets_didNotRemoveAforementioned = "Did not remove the datasets."
pruneDatasets_didRemoveMsg = "Removed the following datasets:"
pruneDatasets_noDatasetsFound = "Did not find any datasets."
pruneDatasets_errPurgeAndDisassociate = unwrap(
    """"--disassociate and --purge may not be used together: --disassociate purges from just the passed TAGged
    collections, but --purge forces disassociation from all of them. """
)
pruneDatasets_errQuietWithDryRun = "Can not use --quiet and --dry-run together."
pruneDatasets_errNoCollectionRestriction = unwrap(
    """Must indicate collections from which to prune datasets by passing COLLETION arguments (select all
    collections by passing '*', or consider using 'butler prune-collections'), by using --purge to pass a run
    collection, or by using --disassociate to select a tagged collection.""")
pruneDatasets_errPruneOnNotRun = "Can not prune a collection that is not a RUN collection: {collection}"
pruneDatasets_errNoOp = "No operation: one of --purge, --unstore, or --disassociate must be provided."

disassociate_option = MWOptionDecorator(
    "--disassociate", "disassociate_tags",
    help=unwrap("""Disassociate pruned datasets from the given tagged collections. May not be used with
                --purge."""),
    multiple=True,
    callback=split_commas,
    metavar="TAG"
)


purge_option = MWOptionDecorator(
    "--purge", "purge_run",
    help=unwrap("""Completely remove the dataset from the given RUN in the Registry. May not be used with
                --disassociate. Note, this may remove provenance information from datasets other than those
                provided, and should be used with extreme care."""),
    metavar="RUN"
)


find_all_option = MWOptionDecorator(
    "--find-all", is_flag=True,
    help=unwrap("""Purge the dataset results from all of the collections in which a dataset of that dataset
                type + data id combination appear. (By default only the first found dataset type + data id is
                purged, according to the order of COLLECTIONS passed in).""")
)


unstore_option = MWOptionDecorator(
    "--unstore",
    is_flag=True,
    help=unwrap("""Remove these datasets from all datastores configured with this data repository. If
                --disassociate and --purge are not used then --unstore will be used by default. Note that
                --unstore will make it impossible to retrieve these datasets even via other collections.
                Datasets that are already not stored are ignored by this option.""")
)


dry_run_option = MWOptionDecorator(
    "--dry-run",
    is_flag=True,
    help=unwrap("""Display the datasets that would be removed but do not remove them.

                Note that a dataset can be in collections other than its RUN-type collection, and removing it
                will remove it from all of them, even though the only one this will show is its RUN
                collection.""")
)


confirm_option = MWOptionDecorator(
    "--confirm/--no-confirm",
    default=True,
    help="Print expected action and a confirmation prompt before executing. Default is --confirm."
)


quiet_option = MWOptionDecorator(
    "--quiet",
    is_flag=True,
    help=unwrap("""Makes output quiet. Implies --no-confirm. Requires --dry-run not be passed.""")
)


@click.command(cls=ButlerCommand, short_help="Remove datasets.")
@repo_argument(required=True)
@collections_argument(help=unwrap("""COLLECTIONS is or more expressions that identify the collections to
                                  search for datasets. Glob-style expressions may be used but only if the
                                  --find-all flag is also passed."""))
@option_section("Query Datasets Options:")
@datasets_option(help="One or more glob-style expressions that identify the dataset types to be pruned.",
                 multiple=True,
                 callback=split_commas)
@find_all_option()
@where_option(help=where_help)
@option_section("Prune Options:")
@disassociate_option()
@purge_option()
@unstore_option()
@option_section("Execution Options:")
@dry_run_option()
@confirm_option()
@quiet_option()
@option_section("Other Options:")
@options_file_option()
def prune_datasets(**kwargs):
    """Query for and remove one or more datasets from a collection and/or
    storage.
    """
    quiet = kwargs.pop("quiet", False)
    if quiet:
        if kwargs["dry_run"]:
            raise click.ClickException(pruneDatasets_errQuietWithDryRun)
        kwargs["confirm"] = False

    result = script.pruneDatasets(**kwargs)

    if result.errPurgeAndDisassociate:
        raise click.ClickException(pruneDatasets_errPurgeAndDisassociate)
        return
    if result.errNoCollectionRestriction:
        raise click.ClickException(pruneDatasets_errNoCollectionRestriction)
    if result.errPruneOnNotRun:
        raise click.ClickException(pruneDatasets_errPruneOnNotRun.format(**result.errDict))
    if result.errNoOp:
        raise click.ClickException(pruneDatasets_errNoOp)
    if result.dryRun:
        if result.action["disassociate"] and result.action["unstore"]:
            msg = pruneDatasets_wouldDisassociateAndRemoveMsg
        elif result.action["disassociate"]:
            msg = pruneDatasets_wouldDisassociateMsg
        else:
            msg = pruneDatasets_wouldRemoveMsg
        print(msg.format(**result.action))
        printAstropyTables(result.tables)
        return
    if result.confirm:
        if not result.tables:
            print(pruneDatasets_noDatasetsFound)
            return
        print(pruneDatasets_willRemoveMsg)
        printAstropyTables(result.tables)
        doContinue = click.confirm(pruneDatasets_askContinueMsg, default=False)
        if doContinue:
            result.onConfirmation()
            print(pruneDatasets_didRemoveAforementioned)
        else:
            print(pruneDatasets_didNotRemoveAforementioned)
        return
    if result.finished:
        if not quiet:
            print(pruneDatasets_didRemoveMsg)
            printAstropyTables(result.tables)
        return


@click.command(short_help="Search for collections.", cls=ButlerCommand)
@repo_argument(required=True)
@glob_argument(help="GLOB is one or more glob-style expressions that fully or partially identify the "
                    "collections to return.")
@collection_type_option()
@click.option("--chains",
              default="table",
              help=unwrap("""Affects how results are presented. TABLE lists each dataset in a row with
                          chained datasets' children listed in a Definition column. TREE lists children below
                          their parent in tree form. FLATTEN lists all datasets, including child datasets in
                          one list.Defaults to TABLE. """),
              callback=to_upper,
              type=click.Choice(("TABLE", "TREE", "FLATTEN"), case_sensitive=False))
@options_file_option()
def query_collections(*args, **kwargs):
    """Get the collections whose names match an expression."""
    table = script.queryCollections(*args, **kwargs)
    # The unit test that mocks script.queryCollections does not return a table
    # so we need the following `if`.
    if table:
        # When chains==TREE, the children of chained datasets are indented
        # relative to their parents. For this to work properly the table must
        # be left-aligned.
        table.pprint_all(align="<")


@click.command(cls=ButlerCommand)
@repo_argument(required=True)
@glob_argument(help="GLOB is one or more glob-style expressions that fully or partially identify the "
                    "dataset types to return.")
@verbose_option(help="Include dataset type name, dimensions, and storage class in output.")
@components_option()
@options_file_option()
def query_dataset_types(*args, **kwargs):
    """Get the dataset types in a repository."""
    table = script.queryDatasetTypes(*args, **kwargs)
    if table:
        table.pprint_all()
    else:
        print("No results. Try --help for more information.")


@click.command(cls=ButlerCommand)
@repo_argument(required=True)
@click.argument('dataset-type-name', nargs=1)
def remove_dataset_type(*args, **kwargs):
    """Remove a dataset type definition from a repository."""
    script.removeDatasetType(*args, **kwargs)


@click.command(cls=ButlerCommand)
@query_datasets_options()
@options_file_option()
def query_datasets(**kwargs):
    """List the datasets in a repository."""
    for table in script.QueryDatasets(**kwargs).getTables():
        print("")
        table.pprint_all()
    print("")


@click.command(cls=ButlerCommand)
@repo_argument(required=True)
@click.argument('input-collection')
@click.argument('output-collection')
@click.argument('dataset-type-name')
@click.option("--begin-date", type=str, default=None,
              help=unwrap("""ISO-8601 datetime (TAI) of the beginning of the validity range for the
                          certified calibrations."""))
@click.option("--end-date", type=str, default=None,
              help=unwrap("""ISO-8601 datetime (TAI) of the end of the validity range for the
                          certified calibrations."""))
@click.option("--search-all-inputs", is_flag=True, default=False,
              help=unwrap("""Search all children of the inputCollection if it is a CHAINED collection,
                          instead of just the most recent one."""))
@options_file_option()
def certify_calibrations(*args, **kwargs):
    """Certify calibrations in a repository.
    """
    script.certifyCalibrations(*args, **kwargs)


@click.command(cls=ButlerCommand)
@repo_argument(required=True)
@dimensions_argument(help=unwrap("""DIMENSIONS are the keys of the data IDs to yield, such as exposure,
                                 instrument, or tract. Will be expanded to include any dependencies."""))
@collections_option()
@datasets_option(help=unwrap("""An expression that fully or partially identifies dataset types that should
                             constrain the yielded data IDs.  For example, including "raw" here would
                             constrain the yielded "instrument", "exposure", "detector", and
                             "physical_filter" values to only those for which at least one "raw" dataset
                             exists in "collections"."""))
@where_option(help=where_help)
@options_file_option()
def query_data_ids(**kwargs):
    """List the data IDs in a repository.
    """
    table = script.queryDataIds(**kwargs)
    if table:
        table.pprint_all()
    else:
        if not kwargs.get("dimensions") and not kwargs.get("datasets"):
            print("No results. Try requesting some dimensions or datasets, see --help for more information.")
        else:
            print("No results. Try --help for more information.")


@click.command(cls=ButlerCommand)
@repo_argument(required=True)
@element_argument(required=True)
@datasets_option(help=unwrap("""An expression that fully or partially identifies dataset types that should
                             constrain the yielded records. Only affects results when used with
                             --collections."""))
@collections_option(help=collections_option.help + " Only affects results when used with --datasets.")
@where_option(help=where_help)
@click.option("--no-check", is_flag=True,
              help=unwrap("""Don't check the query before execution. By default the query is checked before it
                          executed, this may reject some valid queries that resemble common mistakes."""))
@options_file_option()
def query_dimension_records(**kwargs):
    """Query for dimension information."""
    table = script.queryDimensionRecords(**kwargs)
    if table:
        table.pprint_all()
    else:
        print("No results. Try --help for more information.")
