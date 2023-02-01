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
from __future__ import annotations

__all__ = ()

from collections.abc import Callable
from typing import Any, cast

import click
from deprecated.sphinx import deprecated

from ... import script
from .. import utils as cmd_utils
from ..opt import (
    collection_argument,
    collection_type_option,
    collections_argument,
    collections_option,
    components_option,
    confirm_option,
    dataset_type_option,
    datasets_option,
    destination_argument,
    dimensions_argument,
    directory_argument,
    element_argument,
    glob_argument,
    limit_option,
    offset_option,
    options_file_option,
    order_by_option,
    query_datasets_options,
    register_dataset_types_option,
    repo_argument,
    transfer_dimensions_option,
    transfer_option,
    verbose_option,
    where_option,
)
from ..utils import (
    ButlerCommand,
    MWOptionDecorator,
    option_section,
    printAstropyTables,
    typeStrAcceptsMultiple,
    unwrap,
    where_help,
)

# Cast the callback signatures to appease mypy since mypy thinks they
# are too constrained.
split_commas = cast(
    Callable[[click.Context, click.Option | click.Parameter, Any], Any], cmd_utils.split_commas
)
to_upper = cast(Callable[[click.Context, click.Option | click.Parameter, Any], Any], cmd_utils.to_upper)

willCreateRepoHelp = "REPO is the URI or path to the new repository. Will be created if it does not exist."
existingRepoHelp = "REPO is the URI or path to an existing data repository root or configuration file."


@click.command(cls=ButlerCommand, short_help="Add existing datasets to a tagged collection.")
@repo_argument(required=True)
@collection_argument(help="COLLECTION is the collection the datasets should be associated with.")
@query_datasets_options(repo=False, showUri=False, useArguments=False)
@options_file_option()
def associate(**kwargs: Any) -> None:
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
@click.option(
    "--export-file",
    help="Name for the file that contains database information associated with the exported "
    "datasets.  If this is not an absolute path, does not exist in the current working "
    "directory, and --dir is provided, it is assumed to be in that directory.  Defaults "
    'to "export.yaml".',
    type=click.File(mode="r"),
)
@click.option(
    "--skip-dimensions",
    "-s",
    type=str,
    multiple=True,
    callback=split_commas,
    metavar=typeStrAcceptsMultiple,
    help="Dimensions that should be skipped during import",
)
@click.option("--reuse-ids", is_flag=True, help="Force re-use of imported dataset IDs for integer IDs.")
@options_file_option()
def butler_import(*args: Any, **kwargs: Any) -> None:
    """Import data into a butler repository."""
    script.butlerImport(*args, **kwargs)


@click.command(cls=ButlerCommand)
@repo_argument(required=True, help=willCreateRepoHelp)
@click.option("--seed-config", help="Path to an existing YAML config file to apply (on top of defaults).")
@click.option("--dimension-config", help="Path to an existing YAML config file with dimension configuration.")
@click.option(
    "--standalone",
    is_flag=True,
    help="Include all defaults in the config file in the repo, "
    "insulating the repo from changes in package defaults.",
)
@click.option(
    "--override", is_flag=True, help="Allow values in the supplied config to override all repo settings."
)
@click.option(
    "--outfile",
    "-f",
    default=None,
    type=str,
    help="Name of output file to receive repository "
    "configuration. Default is to write butler.yaml into the specified repo.",
)
@options_file_option()
def create(*args: Any, **kwargs: Any) -> None:
    """Create an empty Gen3 Butler repository."""
    script.createRepo(*args, **kwargs)


@click.command(short_help="Dump butler config to stdout.", cls=ButlerCommand)
@repo_argument(required=True, help=existingRepoHelp)
@click.option(
    "--subset",
    "-s",
    type=str,
    help="Subset of a configuration to report. This can be any key in the hierarchy such as "
    "'.datastore.root' where the leading '.' specified the delimiter for the hierarchy.",
)
@click.option(
    "--searchpath",
    "-p",
    type=str,
    multiple=True,
    callback=split_commas,
    metavar=typeStrAcceptsMultiple,
    help="Additional search paths to use for configuration overrides",
)
@click.option(
    "--file",
    "outfile",
    type=click.File(mode="w"),
    default="-",
    help="Print the (possibly-expanded) configuration for a repository to a file, or to stdout by default.",
)
@options_file_option()
def config_dump(*args: Any, **kwargs: Any) -> None:
    """Dump either a subset or full Butler configuration to standard output."""
    script.configDump(*args, **kwargs)


@click.command(short_help="Validate the configuration files.", cls=ButlerCommand)
@repo_argument(required=True, help=existingRepoHelp)
@click.option("--quiet", "-q", is_flag=True, help="Do not report individual failures.")
@dataset_type_option(help="Specific DatasetType(s) to validate.", multiple=True)
@click.option(
    "--ignore",
    "-i",
    type=str,
    multiple=True,
    callback=split_commas,
    metavar=typeStrAcceptsMultiple,
    help="DatasetType(s) to ignore for validation.",
)
@options_file_option()
def config_validate(*args: Any, **kwargs: Any) -> None:
    """Validate the configuration files for a Gen3 Butler repository."""
    is_good = script.configValidate(*args, **kwargs)
    if not is_good:
        raise click.exceptions.Exit(1)


@click.command(cls=ButlerCommand)
@repo_argument(required=True)
@collection_argument(
    help=unwrap(
        """COLLECTION is the Name of the collection to remove. If this is a tagged or
                          chained collection, datasets within the collection are not modified unless --unstore
                          is passed. If this is a run collection, --purge and --unstore must be passed, and
                          all datasets in it are fully removed from the data repository."""
    )
)
@click.option(
    "--purge",
    help=unwrap(
        """Permit RUN collections to be removed, fully removing datasets within them.
                          Requires --unstore as an added precaution against accidental deletion. Must not be
                          passed if the collection is not a RUN."""
    ),
    is_flag=True,
)
@click.option(
    "--unstore",
    help="Remove all datasets in the collection from all datastores in which they appear.",
    is_flag=True,
)
@click.option(
    "--unlink",
    help="Before removing the given `collection` unlink it from from this parent collection.",
    multiple=True,
    callback=split_commas,
)
@confirm_option()
@options_file_option()
@deprecated(
    reason="Please consider using remove-collections or remove-runs instead. Will be removed after v24.",
    version="v24.0",
    category=FutureWarning,
)
def prune_collection(**kwargs: Any) -> None:
    """Remove a collection and possibly prune datasets within it."""
    result = script.pruneCollection(**kwargs)
    if result.confirm:
        print("The following collections will be removed:")
        result.removeTable.pprint_all(align="<")
        doContinue = click.confirm(text="Continue?", default=False)
    else:
        doContinue = True
    if doContinue:
        result.onConfirmation()
        print("Removed collections.")
    else:
        print("Aborted.")


pruneDatasets_wouldRemoveMsg = unwrap(
    """The following datasets will be removed from any datastores in which
                                      they are present:"""
)
pruneDatasets_wouldDisassociateMsg = unwrap(
    """The following datasets will be disassociated from {collections}
                                            if they are currently present in it (which is not checked):"""
)
pruneDatasets_wouldDisassociateAndRemoveMsg = unwrap(
    """The following datasets will be disassociated from
                                                  {collections} if they are currently present in it (which is
                                                  not checked), and removed from any datastores in which they
                                                  are present."""
)
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
    collection, or by using --disassociate to select a tagged collection."""
)
pruneDatasets_errPruneOnNotRun = "Can not prune a collection that is not a RUN collection: {collection}"
pruneDatasets_errNoOp = "No operation: one of --purge, --unstore, or --disassociate must be provided."

disassociate_option = MWOptionDecorator(
    "--disassociate",
    "disassociate_tags",
    help=unwrap(
        """Disassociate pruned datasets from the given tagged collections. May not be used with
                --purge."""
    ),
    multiple=True,
    callback=split_commas,
    metavar="TAG",
)


purge_option = MWOptionDecorator(
    "--purge",
    "purge_run",
    help=unwrap(
        """Completely remove the dataset from the given RUN in the Registry. May not be used with
                --disassociate. Note, this may remove provenance information from datasets other than those
                provided, and should be used with extreme care. RUN has to provided for backward
                compatibility, but datasets will be removed from any RUN-type collections."""
    ),
    metavar="RUN",
)


find_all_option = MWOptionDecorator(
    "--find-all",
    is_flag=True,
    help=unwrap(
        """Purge the dataset results from all of the collections in which a dataset of that dataset
                type + data id combination appear. (By default only the first found dataset type + data id is
                purged, according to the order of COLLECTIONS passed in)."""
    ),
)


unstore_option = MWOptionDecorator(
    "--unstore",
    is_flag=True,
    help=unwrap(
        """Remove these datasets from all datastores configured with this data repository. If
                --disassociate and --purge are not used then --unstore will be used by default. Note that
                --unstore will make it impossible to retrieve these datasets even via other collections.
                Datasets that are already not stored are ignored by this option."""
    ),
)


dry_run_option = MWOptionDecorator(
    "--dry-run",
    is_flag=True,
    help=unwrap(
        """Display the datasets that would be removed but do not remove them.

                Note that a dataset can be in collections other than its RUN-type collection, and removing it
                will remove it from all of them, even though the only one this will show is its RUN
                collection."""
    ),
)


quiet_option = MWOptionDecorator(
    "--quiet",
    is_flag=True,
    help=unwrap("""Makes output quiet. Implies --no-confirm. Requires --dry-run not be passed."""),
)


@click.command(cls=ButlerCommand, short_help="Remove datasets.")
@repo_argument(required=True)
@collections_argument(
    help=unwrap(
        """COLLECTIONS is or more expressions that identify the collections to
                                  search for datasets. Glob-style expressions may be used but only if the
                                  --find-all flag is also passed."""
    )
)
@option_section("Query Datasets Options:")
@datasets_option(
    help="One or more glob-style expressions that identify the dataset types to be pruned.",
    multiple=True,
    callback=split_commas,
)
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
def prune_datasets(**kwargs: Any) -> None:
    """Query for and remove one or more datasets from a collection and/or
    storage.
    """
    quiet = kwargs.pop("quiet", False)
    if quiet:
        if kwargs["dry_run"]:
            raise click.ClickException(message=pruneDatasets_errQuietWithDryRun)
        kwargs["confirm"] = False

    result = script.pruneDatasets(**kwargs)

    if result.errPurgeAndDisassociate:
        raise click.ClickException(message=pruneDatasets_errPurgeAndDisassociate)
    if result.errNoCollectionRestriction:
        raise click.ClickException(message=pruneDatasets_errNoCollectionRestriction)
    if result.errPruneOnNotRun:
        raise click.ClickException(message=pruneDatasets_errPruneOnNotRun.format(**result.errDict))
    if result.errNoOp:
        raise click.ClickException(message=pruneDatasets_errNoOp)
    if result.dryRun:
        assert result.action is not None, "Dry run results have not been set up properly."
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
        doContinue = click.confirm(text=pruneDatasets_askContinueMsg, default=False)
        if doContinue:
            if result.onConfirmation:
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
@glob_argument(
    help="GLOB is one or more glob-style expressions that fully or partially identify the "
    "collections to return."
)
@collection_type_option()
@click.option(
    "--chains",
    default="TREE",
    help="""Affects how results are presented:

        TABLE lists each dataset in table form, with columns for dataset name
        and type, and a column that lists children of CHAINED datasets (if any
        CHAINED datasets are found).

        INVERSE-TABLE is like TABLE but instead of a column listing CHAINED
        dataset children, it lists the parents of the dataset if it is contained
        in any CHAINED collections.

        TREE recursively lists children below each CHAINED dataset in tree form.

        INVERSE-TREE recursively lists parent datasets below each dataset in
        tree form.

        FLATTEN lists all datasets, including child datasets, in one list.

        [default: TREE]""",
    # above, the default value is included, instead of using show_default, so
    # that the default is printed on its own line instead of coming right after
    # the FLATTEN text.
    callback=to_upper,
    type=click.Choice(
        choices=("TABLE", "INVERSE-TABLE", "TREE", "INVERSE-TREE", "FLATTEN"),
        case_sensitive=False,
    ),
)
@options_file_option()
def query_collections(*args: Any, **kwargs: Any) -> None:
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
@glob_argument(
    help="GLOB is one or more glob-style expressions that fully or partially identify the "
    "dataset types to return."
)
@verbose_option(help="Include dataset type name, dimensions, and storage class in output.")
@components_option()
@options_file_option()
def query_dataset_types(*args: Any, **kwargs: Any) -> None:
    """Get the dataset types in a repository."""
    table = script.queryDatasetTypes(*args, **kwargs)
    if table:
        table.pprint_all()
    else:
        print("No results. Try --help for more information.")


@click.command(cls=ButlerCommand)
@repo_argument(required=True)
@click.argument("dataset-type-name", nargs=1)
def remove_dataset_type(*args: Any, **kwargs: Any) -> None:
    """Remove a dataset type definition from a repository."""
    script.removeDatasetType(*args, **kwargs)


@click.command(cls=ButlerCommand)
@query_datasets_options()
@options_file_option()
def query_datasets(**kwargs: Any) -> None:
    """List the datasets in a repository."""
    for table in script.QueryDatasets(**kwargs).getTables():
        print("")
        table.pprint_all()
    print("")


@click.command(cls=ButlerCommand)
@repo_argument(required=True)
@click.argument("input-collection")
@click.argument("output-collection")
@click.argument("dataset-type-name")
@click.option(
    "--begin-date",
    type=str,
    default=None,
    help=unwrap(
        """ISO-8601 datetime (TAI) of the beginning of the validity range for the
                          certified calibrations."""
    ),
)
@click.option(
    "--end-date",
    type=str,
    default=None,
    help=unwrap(
        """ISO-8601 datetime (TAI) of the end of the validity range for the
                          certified calibrations."""
    ),
)
@click.option(
    "--search-all-inputs",
    is_flag=True,
    default=False,
    help=unwrap(
        """Search all children of the inputCollection if it is a CHAINED collection,
                          instead of just the most recent one."""
    ),
)
@options_file_option()
def certify_calibrations(*args: Any, **kwargs: Any) -> None:
    """Certify calibrations in a repository."""
    script.certifyCalibrations(*args, **kwargs)


@click.command(cls=ButlerCommand)
@repo_argument(required=True)
@dimensions_argument(
    help=unwrap(
        """DIMENSIONS are the keys of the data IDs to yield, such as exposure,
                                 instrument, or tract. Will be expanded to include any dependencies."""
    )
)
@collections_option(help=collections_option.help + " May only be used with --datasets.")
@datasets_option(
    help=unwrap(
        """An expression that fully or partially identifies dataset types that should
                             constrain the yielded data IDs.  For example, including "raw" here would
                             constrain the yielded "instrument", "exposure", "detector", and
                             "physical_filter" values to only those for which at least one "raw" dataset
                             exists in "collections".  Requires --collections."""
    )
)
@where_option(help=where_help)
@order_by_option()
@limit_option()
@offset_option()
@options_file_option()
def query_data_ids(**kwargs: Any) -> None:
    """List the data IDs in a repository."""
    table, reason = script.queryDataIds(**kwargs)
    if table:
        table.pprint_all()
    else:
        if reason:
            print(reason)
        if not kwargs.get("dimensions") and not kwargs.get("datasets"):
            print("No results. Try requesting some dimensions or datasets, see --help for more information.")
        else:
            print("No results. Try --help for more information.")


@click.command(cls=ButlerCommand)
@repo_argument(required=True)
@element_argument(required=True)
@datasets_option(
    help=unwrap(
        """An expression that fully or partially identifies dataset types that should
                             constrain the yielded records. May only be used with
                             --collections."""
    )
)
@collections_option(help=collections_option.help + " May only be used with --datasets.")
@where_option(help=where_help)
@order_by_option()
@limit_option()
@offset_option()
@click.option(
    "--no-check",
    is_flag=True,
    help=unwrap(
        """Don't check the query before execution. By default the query is checked before it
                          executed, this may reject some valid queries that resemble common mistakes."""
    ),
)
@options_file_option()
def query_dimension_records(**kwargs: Any) -> None:
    """Query for dimension information."""
    table = script.queryDimensionRecords(**kwargs)
    if table:
        table.pprint_all()
    else:
        print("No results. Try --help for more information.")


@click.command(cls=ButlerCommand)
@repo_argument(required=True)
@query_datasets_options(showUri=False, useArguments=False, repo=False)
@destination_argument(help="Destination URI of folder to receive file artifacts.")
@transfer_option()
@verbose_option(help="Report destination location of all transferred artifacts.")
@click.option(
    "--preserve-path/--no-preserve-path",
    is_flag=True,
    default=True,
    help="Preserve the datastore path to the artifact at the destination.",
)
@click.option(
    "--clobber/--no-clobber",
    is_flag=True,
    default=False,
    help="If clobber, overwrite files if they exist locally.",
)
@options_file_option()
def retrieve_artifacts(**kwargs: Any) -> None:
    """Retrieve file artifacts associated with datasets in a repository."""
    verbose = kwargs.pop("verbose")
    transferred = script.retrieveArtifacts(**kwargs)
    if verbose and transferred:
        print(f"Transferred the following to {kwargs['destination']}:")
        for uri in transferred:
            print(uri)
        print()
    print(f"Number of artifacts retrieved into destination {kwargs['destination']}: {len(transferred)}")


@click.command(cls=ButlerCommand)
@click.argument("source", required=True)
@click.argument("dest", required=True)
@query_datasets_options(showUri=False, useArguments=False, repo=False)
@transfer_option()
@register_dataset_types_option()
@transfer_dimensions_option()
@options_file_option()
def transfer_datasets(**kwargs: Any) -> None:
    """Transfer datasets from a source butler to a destination butler.

    SOURCE is a URI to the Butler repository containing the RUN dataset.

    DEST is a URI to the Butler repository that will receive copies of the
    datasets.
    """
    number = script.transferDatasets(**kwargs)
    print(f"Number of datasets transferred: {number}")


@click.command(cls=ButlerCommand)
@repo_argument(required=True)
@click.argument("parent", required=True, nargs=1)
@click.argument("children", required=False, nargs=-1, callback=split_commas)
@click.option(
    "--doc",
    default="",
    help="Documentation string associated with this collection. "
    "Only relevant if the collection is newly created.",
)
@click.option(
    "--flatten/--no-flatten",
    default=False,
    help="If `True` recursively flatten out any nested chained collections in children first.",
)
@click.option(
    "--mode",
    type=click.Choice(["redefine", "extend", "remove", "prepend", "pop"]),
    default="redefine",
    help="Update mode: "
    "'redefine': Create new chain or redefine existing chain with the supplied CHILDREN. "
    "'remove': Modify existing chain to remove the supplied CHILDREN. "
    "'pop': Pop a numbered element off the chain. Defaults to popping "
    "the first element (0). ``children`` must be integers if given. "
    "'prepend': Modify existing chain to prepend the supplied CHILDREN to the front. "
    "'extend': Modify existing chain to extend it with the supplied CHILDREN.",
)
def collection_chain(**kwargs: Any) -> None:
    """Define a collection chain.

    PARENT is the name of the chained collection to create or modify. If the
    collection already exists the chain associated with it will be updated.

    CHILDREN are the collections to be used to modify the chain. The supplied
    values will be split on comma. The exact usage depends on the MODE option.
    For example,

    $ butler collection-chain REPO PARENT child1,child2 child3

    will result in three children being included in the chain.

    When the MODE is 'pop' the CHILDREN should be integer indices indicating
    collections to be removed from the current chain.
    MODE 'pop' can take negative integers to indicate removal relative to the
    end of the chain, but when doing that '--' must be given to indicate the
    end of the options specification.

    $ butler collection-chain REPO --mode=pop PARENT -- -1

    Will remove the final collection from the chain.
    """
    chain = script.collectionChain(**kwargs)
    print(f"[{', '.join(chain)}]")


@click.command(cls=ButlerCommand)
@repo_argument(required=True)
@click.argument("dataset_type", required=True)
@click.argument("run", required=True)
@click.argument("table_file", required=True)
@click.option(
    "--formatter",
    type=str,
    help="Fully-qualified python class to use as the Formatter. If not specified the formatter"
    " will be determined from the dataset type and datastore configuration.",
)
@click.option(
    "--id-generation-mode",
    default="UNIQUE",
    help="Mode to use for generating dataset IDs. The default creates a unique ID. Other options"
    " are: 'DATAID_TYPE' for creating a reproducible ID from the dataID and dataset type;"
    " 'DATAID_TYPE_RUN' for creating a reproducible ID from the dataID, dataset type and run."
    " The latter is usually used for 'raw'-type data that will be ingested in multiple."
    " repositories.",
    callback=to_upper,
    type=click.Choice(("UNIQUE", "DATAID_TYPE", "DATAID_TYPE_RUN"), case_sensitive=False),
)
@click.option(
    "--data-id",
    type=str,
    multiple=True,
    callback=split_commas,
    help="Keyword=value string with an additional dataId value that is fixed for all ingested"
    " files. This can be used to simplify the table file by removing repeated entries that are"
    " fixed for all files to be ingested.  Multiple key/values can be given either by using"
    " comma separation or multiple command line options.",
)
@click.option(
    "--prefix",
    type=str,
    help="For relative paths in the table file, specify a prefix to use. The default is to"
    " use the current working directory.",
)
@transfer_option()
def ingest_files(**kwargs: Any) -> None:
    """Ingest files from table file.

    DATASET_TYPE is the name of the dataset type to be associated with these
    files. This dataset type must already exist and will not be created by
    this command. There can only be one dataset type per invocation of this
    command.

    RUN is the run to use for the file ingest.

    TABLE_FILE refers to a file that can be read by astropy.table with
    columns of:

    file URI, dimension1, dimension2, ..., dimensionN

    where the first column is the URI to the file to be ingested and the
    remaining columns define the dataId to associate with that file.
    The column names should match the dimensions for the specified dataset
    type. Relative file URI by default is assumed to be relative to the
    current working directory but can be overridden using the ``--prefix``
    option.

    This command does not create dimension records and so any records must
    be created by other means. This command should not be used to ingest
    raw camera exposures.
    """
    script.ingest_files(**kwargs)


@click.command(cls=ButlerCommand)
@repo_argument(required=True)
@click.argument("dataset_type", required=True)
@click.argument("storage_class", required=True)
@click.argument("dimensions", required=False, nargs=-1)
@click.option(
    "--is-calibration/--no-is-calibration",
    is_flag=True,
    default=False,
    help="Indicate that this dataset type can be part of a calibration collection.",
)
def register_dataset_type(**kwargs: Any) -> None:
    """Register a new dataset type with this butler repository.

    DATASET_TYPE is the name of the dataset type.

    STORAGE_CLASS is the name of the StorageClass to be associated with
    this dataset type.

    DIMENSIONS is a list of all the dimensions relevant to this
    dataset type. It can be an empty list.

    A component dataset type (such as "something.component") is not a
    real dataset type and so can not be defined by this command. They are
    automatically derived from the composite dataset type when a composite
    storage class is specified.
    """
    inserted = script.register_dataset_type(**kwargs)
    if inserted:
        print("Dataset type successfully registered.")
    else:
        print("Dataset type already existed in identical form.")


@click.command(cls=ButlerCommand)
@repo_argument(required=True)
@directory_argument(required=True, help="DIRECTORY is the folder to receive the exported calibrations.")
@collections_argument(help="COLLECTIONS are the collection to export calibrations from.")
@dataset_type_option(help="Specific DatasetType(s) to export.", multiple=True)
@transfer_option()
def export_calibs(*args: Any, **kwargs: Any) -> None:
    """Export calibrations from the butler for import elsewhere."""
    table = script.exportCalibs(*args, **kwargs)
    if table:
        table.pprint_all(align="<")
