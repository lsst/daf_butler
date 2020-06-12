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

"""Unit tests for the daf_butler CLI plugin loader.
"""

import click
import click.testing
from collections import defaultdict
from contextlib import contextmanager
import os
import unittest
from unittest.mock import patch
import yaml

from lsst.daf.butler.cli import butler, cmd


@click.command()
def command_test():
    click.echo("test command")


@contextmanager
def command_test_env(runner, commandName):
    """A context manager that creates (and then cleans up) an environment that
    provides a plugin command named 'command-test'.

    Parameters
    ----------
    runner : click.testing.CliRunner
        The test runner to use to create the isolated filesystem.
    """
    with runner.isolated_filesystem():
        with open("resources.yaml", "w") as f:
            f.write(yaml.dump({"cmd": {"import": "test_cliPluginLoader", "commands": [commandName]}}))
        # Add a colon to the end of the path on the next line, this tests the
        # case where the lookup in LoaderCLI._getPluginList generates an empty
        # string in one of the list entries and verifies that the empty string
        # is properly stripped out.
        with patch.dict("os.environ", {"DAF_BUTLER_PLUGINS": f"{os.path.realpath(f.name)}:"}):
            yield


@contextmanager
def duplicate_command_test_env(runner):
    """A context manager that creates (and then cleans up) an environment that
    declares a plugin command named 'create', which will conflict with the
    daf_butler 'create' command.

    Parameters
    ----------
    runner : click.testing.CliRunner
        The test runner to use to create the isolated filesystem.
    """
    with runner.isolated_filesystem():
        with open("resources.yaml", "w") as f:
            f.write(yaml.dump({"cmd": {"import": "test_cliPluginLoader", "commands": ["create"]}}))
        with patch.dict("os.environ", {"DAF_BUTLER_PLUGINS": os.path.realpath(f.name)}):
            yield


class FailedLoadTest(unittest.TestCase):

    def test_unimportablePlugin(self):
        runner = click.testing.CliRunner()
        with command_test_env(runner, "non-existant-command-function"):
            with self.assertLogs() as cm:
                result = runner.invoke(butler.cli, "--help")
            self.assertEqual(result.exit_code, 0, f"output: {result.output} exception: {result.exception}")
            expectedErrMsg = "Could not import plugin from " \
                             "test_cliPluginLoader.non_existant_command_function, skipping."
            self.assertIn(expectedErrMsg, cm.output[0])


class PluginLoaderTest(unittest.TestCase):

    def test_loadAndExecutePluginCommand(self):
        """Test that a plugin command can be loaded and executed."""
        runner = click.testing.CliRunner()
        with command_test_env(runner, "command-test"):
            result = runner.invoke(butler.cli, "command-test")
            self.assertEqual(result.exit_code, 0, f"output: {result.output} exception: {result.exception}")
            self.assertEqual(result.stdout, "test command\n")

    def test_loadAndExecuteLocalCommand(self):
        """Test that a command in daf_butler can be loaded and executed."""
        runner = click.testing.CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(butler.cli, ["create", "test_repo"])
            self.assertEqual(result.exit_code, 0, f"output: {result.output} exception: {result.exception}")
            self.assertTrue(os.path.exists("test_repo"))

    def test_loadTopHelp(self):
        """Test that an expected command is produced by 'butler --help'"""
        runner = click.testing.CliRunner()
        with command_test_env(runner, "command-test"):
            result = runner.invoke(butler.cli, "--help")
            self.assertEqual(result.exit_code, 0, f"output: {result.output} exception: {result.exception}")
            self.assertIn("command-test", result.stdout)

    def test_getLocalCommands(self):
        """Test getting the daf_butler CLI commands."""
        localCommands = butler.LoaderCLI._getLocalCommands()
        # the number of local commands should equal the number of functions
        # in cmd.__all__
        self.assertEqual(len(localCommands), len(cmd.__all__))
        for command, pkg in localCommands.items():
            self.assertEqual(pkg, ["lsst.daf.butler.cli.cmd"])

    def test_mergeCommandLists(self):
        """Verify dicts of command to list-of-source-package get merged
        properly."""
        first = defaultdict(list, {"a": [1]})
        second = defaultdict(list, {"b": [2]})
        self.assertEqual(butler.LoaderCLI._mergeCommandLists(first, second), {"a": [1], "b": [2]})
        first = defaultdict(list, {"a": [1]})
        second = defaultdict(list, {"a": [2]})
        self.assertEqual(butler.LoaderCLI._mergeCommandLists(first, second), {"a": [1, 2]})

    def test_listCommands_duplicate(self):
        """Test executing a command in a situation where duplicate commands are
        present and verify it fails to run.
        """
        self.maxDiff = None
        runner = click.testing.CliRunner()
        with duplicate_command_test_env(runner):
            result = runner.invoke(butler.cli, ["create", "test_repo"])
            self.assertEqual(result.exit_code, 1, f"output: {result.output} exception: {result.exception}")
            self.assertEqual(result.output, "Error: Command 'create' "
                             "exists in packages lsst.daf.butler.cli.cmd, test_cliPluginLoader. "
                             "Duplicate commands are not supported, aborting.\n")


if __name__ == "__main__":
    unittest.main()
