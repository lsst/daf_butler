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

"""Unit tests for the daf_butler dataset-type CLI option.
"""

import unittest

from lsst.daf.butler.registry import CollectionType
from lsst.daf.butler.tests import (OptFlagTest,
                                   OptCaseInsensitiveTest,
                                   OptChoiceTest,
                                   OptHelpTest,
                                   OptMultipleTest,
                                   OptRequiredTest,
                                   OptSplitKeyValueTest)
from lsst.daf.butler.cli.opt import (collection_type_option, config_file_option, config_option,
                                     dataset_type_option, directory_argument, glob_parameter,
                                     log_level_option, long_log_option, repo_argument, transfer_option,
                                     verbose_option)


class CollectionTypeTestCase(OptHelpTest,
                             OptCaseInsensitiveTest,
                             OptChoiceTest,
                             OptRequiredTest,
                             unittest.TestCase):
    optionClass = collection_type_option
    optionName = "collection-type"
    choices = ["CHAINED", "RUN", "TAGGED"]
    expectedChoiceValues = [CollectionType.CHAINED, CollectionType.RUN, CollectionType.TAGGED]


class ConfigTestCase(OptHelpTest,
                     OptMultipleTest,
                     OptRequiredTest,
                     OptSplitKeyValueTest,
                     unittest.TestCase):
    metavar = "test metavar"
    optionName = "config"
    shortOptionName = "c"
    optionClass = config_option
    optionMultipleKeyValues = ["one=two,three=four", "five=six"]


class ConfigFileTestCase(OptHelpTest,
                         OptMultipleTest,
                         OptRequiredTest,
                         unittest.TestCase):
    optionName = "config-file"
    shortOptionName = "C"
    optionClass = config_file_option


class DatasetTypeTestCase(OptHelpTest,
                          OptMultipleTest,
                          OptRequiredTest,
                          unittest.TestCase):

    optionClass = dataset_type_option
    optionName = "dataset-type"
    shortOptionName = "d"


class DirectoryArgumentTestCase(OptHelpTest,
                                OptRequiredTest,
                                unittest.TestCase):
    optionClass = directory_argument
    optionName = "directory"
    isArgument = True


class GlobArgumentTestCase(OptHelpTest,
                           OptRequiredTest,
                           OptMultipleTest,
                           unittest.TestCase):
    optionClass = glob_parameter
    optionName = "glob"
    isArgument = True
    isParameter = True


class GlobOptionTestCase(OptHelpTest,
                         OptRequiredTest,
                         OptMultipleTest,
                         unittest.TestCase):
    optionClass = glob_parameter
    optionName = "glob"
    isParameter = True


class LogLevelTestCase(OptChoiceTest,
                       OptHelpTest,
                       OptCaseInsensitiveTest,
                       OptRequiredTest,
                       unittest.TestCase):

    expectedValDefault = {"": log_level_option.defaultValue}
    optionClass = log_level_option
    optionName = "log-level"
    choices = ["DEBUG", "lsst.daf.butler=DEBUG"]
    expectedChoiceValues = [{'': "DEBUG"}, {"lsst.daf.butler": "DEBUG"}]


class LongLogOption(OptFlagTest,
                    OptHelpTest,
                    unittest.TestCase):
    optionClass = long_log_option
    optionName = "long-log"


class RepoArgumentTestCase(OptHelpTest,
                           OptRequiredTest,
                           unittest.TestCase):
    isArgument = True
    optionClass = repo_argument
    optionName = "repo"


class TransferTestCase(OptChoiceTest,
                       OptHelpTest,
                       OptRequiredTest,
                       unittest.TestCase):
    expectedValDefault = "auto"
    optionClass = transfer_option
    optionName = "transfer"
    shortOptionName = "t"


# Doesn't test for required; this is nonsensical for a flag (where
# present=True and not-present=False)
class VerboseTestCase(OptFlagTest,
                      OptHelpTest,
                      unittest.TestCase):
    optionClass = verbose_option
    optionName = "verbose"
    isBool = True
    expectedValDefault = "False"


if __name__ == "__main__":
    unittest.main()
