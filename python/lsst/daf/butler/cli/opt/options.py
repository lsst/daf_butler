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

from ..utils import MWOptionDecorator, split_commas, split_kv, unwrap
from lsst.daf.butler.core.utils import iterable
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

config_option = MWOptionDecorator("-c", "--config",
                                  callback=split_kv,
                                  help="Config override, as a key-value pair.",
                                  multiple=True)


config_file_option = MWOptionDecorator("-C", "--config-file",
                                       help=unwrap("""Path to a pex config override to be included after the
                                       Instrument config overrides are applied."""))


dataset_type_option = MWOptionDecorator("-d", "--dataset-type",
                                        callback=split_commas,
                                        help="Specific DatasetType(s) to validate.",
                                        multiple=True)


log_level_option = MWOptionDecorator("--log-level",
                                     callback=partial(split_kv,
                                                      choice=click.Choice(["CRITICAL", "ERROR", "WARNING",
                                                                          "INFO", "DEBUG"],
                                                                          case_sensitive=False),
                                                      normalize=True,
                                                      unseparated_okay=True),
                                     default=iterable("WARNING"),
                                     help="The Python log level to use.",
                                     is_eager=True,
                                     multiple=True)


long_log_option = MWOptionDecorator("--long-log",
                                    help="Make log messages appear in long format.",
                                    is_flag=True)


regex_option = MWOptionDecorator("--regex")


run_option = MWOptionDecorator("--output-run",
                               help="The name of the run datasets should be output to.")


transfer_option = MWOptionDecorator("-t", "--transfer",
                                    default="auto",  # set to `None` if using `required=True`
                                    help="The external data transfer mode.",
                                    type=click.Choice(["auto", "link", "symlink", "hardlink", "copy", "move",
                                                       "relsymlink"],
                                                      case_sensitive=False))


verbose_option = MWOptionDecorator("-v", "--verbose",
                                   help="Increase verbosity.",
                                   is_flag=True)
