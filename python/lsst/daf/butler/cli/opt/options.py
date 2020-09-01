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

    collectionTypes = dict(CHAINED=CollectionType.CHAINED,
                           RUN=CollectionType.RUN,
                           TAGGED=CollectionType.TAGGED)

    @staticmethod
    def makeCollectionType(context, param, value):
        if value is None:
            return value
        try:
            return CollectionTypeCallback.collectionTypes[value.upper()]
        except KeyError:
            raise ValueError(f"Invalid collection type: {value}")


collection_type_option = MWOptionDecorator("--collection-type",
                                           callback=CollectionTypeCallback.makeCollectionType,
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
