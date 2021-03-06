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
from functools import partial

from ..utils import MWOptionDecorator, split_commas, split_kv, unwrap, yaml_presets
from lsst.daf.butler.registry import CollectionType


class CollectionTypeCallback:

    collectionTypes = CollectionType.__members__.keys()

    @staticmethod
    def makeCollectionTypes(context, param, value):
        if not value:
            # Click seems to demand that the default be an empty tuple, rather
            # than a sentinal like None.  The behavior that we want is that
            # not passing this option at all passes all collection types, while
            # passing it uses only the passed collection types.  That works
            # fine for now, since there's no command-line option to subtract
            # collection types, and hence the only way to get an empty tuple
            # is as the default.
            return tuple(CollectionType.all())
        result = []
        for item in split_commas(context, param, value):
            item = item.upper()
            try:
                result.append(CollectionType.__members__[item])
            except KeyError:
                raise KeyError(f"{item} is not a valid CollectionType.") from None
        return tuple(result)


collection_type_option = MWOptionDecorator("--collection-type",
                                           callback=CollectionTypeCallback.makeCollectionTypes,
                                           multiple=True,
                                           help="If provided, only list collections of this type.",
                                           type=click.Choice(CollectionTypeCallback.collectionTypes,
                                                             case_sensitive=False))


collections_option = MWOptionDecorator("--collections",
                                       help=unwrap("""One or more expressions that fully or partially identify
                                                   the collections to search for datasets. If not provided all
                                                   datasets are returned."""),
                                       multiple=True,
                                       callback=split_commas)


components_option = MWOptionDecorator("--components/--no-components",
                                      default=None,
                                      help=unwrap("""For --components, apply all expression patterns to
                                                  component dataset type names as well. For --no-components,
                                                  never apply patterns to components. Default (where neither
                                                  is specified) is to apply patterns to components only if
                                                  their parent datasets were not matched by the expression.
                                                  Fully-specified component datasets (`str` or `DatasetType`
                                                  instances) are always included."""))


config_option = MWOptionDecorator("-c", "--config",
                                  callback=split_kv,
                                  help="Config override, as a key-value pair.",
                                  metavar="TEXT=TEXT",
                                  multiple=True)


config_file_option = MWOptionDecorator("-C", "--config-file",
                                       help=unwrap("""Path to a pex config override to be included after the
                                       Instrument config overrides are applied."""))


dataset_type_option = MWOptionDecorator("-d", "--dataset-type",
                                        callback=split_commas,
                                        help="Specific DatasetType(s) to validate.",
                                        multiple=True)


datasets_option = MWOptionDecorator("--datasets")


logLevelChoices = ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]
log_level_option = MWOptionDecorator("--log-level",
                                     callback=partial(split_kv,
                                                      choice=click.Choice(logLevelChoices,
                                                                          case_sensitive=False),
                                                      normalize=True,
                                                      unseparated_okay=True,
                                                      add_to_default=True),
                                     help="The logging level. "
                                          f"Supported levels are [{'|'.join(logLevelChoices)}]",
                                     is_eager=True,
                                     metavar="LEVEL|COMPONENT=LEVEL",
                                     multiple=True)


long_log_option = MWOptionDecorator("--long-log",
                                    help="Make log messages appear in long format.",
                                    is_flag=True)


options_file_option = MWOptionDecorator("--options-file", "-@",
                                        expose_value=False,  # This option should not be forwarded
                                        help=unwrap("""URI to YAML file containing overrides
                                                    of command line options. The YAML should be organized
                                                    as a hierarchy with subcommand names at the top
                                                    level options for that subcommand below."""),
                                        callback=yaml_presets)


processes_option = MWOptionDecorator("-j", "--processes",
                                     default=1,
                                     help="Number of processes to use.",
                                     type=click.IntRange(min=1))


regex_option = MWOptionDecorator("--regex")


run_option = MWOptionDecorator("--output-run",
                               help="The name of the run datasets should be output to.")


transfer_option = MWOptionDecorator("-t", "--transfer",
                                    default="auto",  # set to `None` if using `required=True`
                                    help="The external data transfer mode.",
                                    type=click.Choice(["auto", "link", "symlink", "hardlink", "copy", "move",
                                                       "relsymlink", "direct"],
                                                      case_sensitive=False))


verbose_option = MWOptionDecorator("-v", "--verbose",
                                   help="Increase verbosity.",
                                   is_flag=True)


where_option = MWOptionDecorator("--where",
                                 help="A string expression similar to a SQL WHERE clause.")
