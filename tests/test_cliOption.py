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

import abc
import click
import click.testing
import unittest
import yaml

from lsst.daf.butler.registry import CollectionType
from lsst.daf.butler.cli.opt import (collection_type_option, config_file_option, config_option,
                                     dataset_type_option, directory_argument)
from lsst.daf.butler.cli.utils import clickResultMsg


class OptionTestBase(unittest.TestCase, abc.ABC):

    def setUp(self):
        self.runner = click.testing.CliRunner()

    def run_command(self, cmd, args):
        """

        Parameters
        ----------
        cmd : click.Command
            The command function to call
        args : [`str`]
            The arguments to pass to the function call.

        Returns
        -------
        [type]
            [description]
        """
        return self.runner.invoke(cmd, args)

    def run_test(self, cmd, cmdArgs, verifyFunc, verifyArgs):
        result = self.run_command(cmd, cmdArgs)
        verifyFunc(result, verifyArgs)

    def run_help_test(self, cmd, expcectedHelpText):
        result = self.runner.invoke(cmd, ["--help"])
        # remove all whitespace to work around line-wrap differences.
        self.assertIn("".join(expcectedHelpText.split()), "".join(result.output.split()))

    @property
    @abc.abstractmethod
    def optionClass(self):
        pass

    def help_test(self):
        @click.command()
        @self.optionClass()
        def cli():
            pass

        self.run_help_test(cli, self.optionClass.defaultHelp)

    def custom_help_test(self):
        @click.command()
        @self.optionClass(help="foobarbaz")
        def cli(collection_type):
            pass

        self.run_help_test(cli, "foobarbaz")


class CollectionTypeTestCase(OptionTestBase):

    optionClass = collection_type_option

    def setUp(self):
        super().setUp()
        CollectionTypeTestCase.collectionType = None

    @staticmethod
    @click.command()
    @collection_type_option()
    def cli(collection_type):
        CollectionTypeTestCase.collectionType = collection_type

    def verify(self, result, verifyArgs):
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertEqual(CollectionTypeTestCase.collectionType, verifyArgs)

    def test_run(self):
        self.run_test(CollectionTypeTestCase.cli, ["--collection-type", "RUN"],
                      self.verify, CollectionType.RUN)

    def test_chained(self):
        self.run_test(CollectionTypeTestCase.cli, ["--collection-type", "CHAINED"],
                      self.verify, CollectionType.CHAINED)

    def test_tagged(self):
        self.run_test(CollectionTypeTestCase.cli, ["--collection-type", "TAGGED"],
                      self.verify, CollectionType.TAGGED)

    def test_default(self):
        self.run_test(CollectionTypeTestCase.cli, [],
                      self.verify, None)

    def test_caseInsensitive(self):
        self.run_test(CollectionTypeTestCase.cli, ["--collection-type", "TaGGeD"],
                      self.verify, CollectionType.TAGGED)

    def test_help(self):
        self.help_test()
        self.custom_help_test()


class ConfigTestCase(OptionTestBase):

    optionClass = config_option

    @staticmethod
    @click.command()
    @config_option()
    def cli(config):
        click.echo(yaml.dump(config), nl=False)

    def test_basic(self):
        """test arguments"""
        result = self.runner.invoke(ConfigTestCase.cli, ["--config", "a=1", "-c", "b=2,c=3"])
        self.assertEqual(result.exit_code, 0, f"output: {result.output} exception: {result.exception}")
        self.assertEqual(yaml.safe_load(result.stdout), dict(a="1", b="2", c="3"))

    def test_missing(self):
        @click.command()
        @config_option(required=True)
        def cli(config):
            pass

        result = self.runner.invoke(cli, [])
        self.assertNotEqual(result.exit_code, 0, f"output: {result.output} exception: {result.exception}")
        self.assertIn('Missing option "-c" / "--config"', result.output)

    def test_help(self):
        self.help_test()
        self.custom_help_test()


class ConfigFileTestCase(OptionTestBase):

    optionClass = config_file_option

    @staticmethod
    @click.command()
    @config_file_option()
    def cli(config_file):
        click.echo(config_file, nl=False)

    def test_basic(self):
        """test arguments"""
        result = self.runner.invoke(ConfigFileTestCase.cli, ["--config-file", "path/to/file"])
        self.assertEqual(result.exit_code, 0, f"output: {result.output} exception: {result.exception}")
        self.assertEqual("path/to/file", result.stdout)

    def test_missing(self):
        @click.command()
        @config_file_option(required=True)
        def cli(config):
            pass
        result = self.runner.invoke(cli, [])
        self.assertNotEqual(result.exit_code, 0, f"output: {result.output} exception: {result.exception}")
        self.assertIn('Missing option "-C" / "--config-file"', result.output)

    def test_help(self):
        self.help_test()
        self.custom_help_test()


class DatasetTypeTestCase(OptionTestBase):

    optionClass = dataset_type_option

    @staticmethod
    @click.command()
    @dataset_type_option(help="the dataset type")
    def cli(dataset_type):
        click.echo(dataset_type, nl=False)

    def verify(self, result, verifyArgs):
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertEqual(result.stdout, verifyArgs)

    def test_single(self):
        """test a single argument"""
        self.run_test(DatasetTypeTestCase.cli, ["--dataset-type", "one"], self.verify, "['one']")

    def test_multiple(self):
        """test multiple arguments, using the long and short option names"""
        self.run_test(DatasetTypeTestCase.cli, ["--dataset-type", "one", "-d", "two"],
                      self.verify, "['one', 'two']")

    def test_singlePair(self):
        """test a single comma-separated value pair"""
        self.run_test(DatasetTypeTestCase.cli, ["--dataset-type", "one,two"],
                      self.verify, "['one', 'two']")

    def test_multiplePair(self):
        """test multiple comma-separated value pairs"""
        self.run_test(DatasetTypeTestCase.cli, ["--dataset-type", "one,two", "-d", "three,four"],
                      self.verify, "['one', 'two', 'three', 'four']")

    def test_help(self):
        # dataset_type_option does not have default help
        self.custom_help_test()


class DirectoryArgumentTestCase(OptionTestBase):

    optionClass = directory_argument

    def test_required(self):
        """test arguments"""
        @click.command()
        @directory_argument(required=True)
        def cli(directory):
            click.echo(directory, nl=False)
        result = self.runner.invoke(cli, ["this_dir"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertEqual("this_dir", result.stdout)
        result = self.runner.invoke(cli, [])
        self.assertNotEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertIn('Missing argument "DIRECTORY"', result.stdout)

    def test_notRequired(self):
        """test arguments"""
        @click.command()
        @directory_argument()
        def cli(directory):
            click.echo(directory, nl=False)
        result = self.runner.invoke(cli, ["this_dir"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertEqual("this_dir", result.stdout)
        result = self.runner.invoke(cli, [])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertEqual("", result.stdout)

    def test_help(self):
        # directory_argument does not have default help.
        self.custom_help_test()


if __name__ == "__main__":
    unittest.main()
