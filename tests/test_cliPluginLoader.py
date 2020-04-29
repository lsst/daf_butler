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
def command_test_env(runner):
    """A context manager that creates (and then cleans up) an environment that
    provides a plugin command named 'command-test'.

    Parameters
    ----------
    runner : click.testing.CliRunner
        The test runner to use to create the isolated filesystem.
    """
    with runner.isolated_filesystem():
        with open("resources.yaml", "w") as f:
            f.write(yaml.dump({"cmd": {"import": "test_cliPluginLoader", "commands": ["command-test"]}}))
        with patch.dict("os.environ", {"DAF_BUTLER_PLUGINS": os.path.realpath(f.name)}):
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


class Suite(unittest.TestCase):

    def setUp(self):
        butler.cli.commands = None

    def test_loadAndExecutePluginCommand(self):
        """Test that a plugin command can be loaded and executed."""
        runner = click.testing.CliRunner()
        with command_test_env(runner):
            result = runner.invoke(butler.cli, "command-test")
            self.assertEqual(result.exit_code, 0, result.output)
            self.assertEqual(result.stdout, "test command\n")

    def test_loadAndExecuteLocalCommand(self):
        """Test that a command in daf_butler can be loaded and executed."""
        runner = click.testing.CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(butler.cli, ["create", "--repo", "test_repo"])
            self.assertEqual(result.exit_code, 0, result.output)
            self.assertTrue(os.path.exists("test_repo"))

    def test_loadTopHelp(self):
        """Test that an expected command is produced by 'butler --help'"""
        runner = click.testing.CliRunner()
        with command_test_env(runner):
            result = runner.invoke(butler.cli, "--help")
            self.assertEqual(result.exit_code, 0, result.stdout)
            self.assertIn("command-test", result.stdout)

    def test_getLocalCommands(self):
        """Test getting the daf_butler CLI commands."""
        localCommands = butler.LoaderCLI._getLocalCommands()
        for command in cmd.__all__:
            command = command.replace("_", "-")
            self.assertEqual(localCommands[command], ["lsst.daf.butler.cli.cmd"])

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
            result = runner.invoke(butler.cli, ["create", "--repo", "test_repo"])
            self.assertEqual(result.exit_code, 1, result.output)
            self.assertEqual(result.output, "Error: Command 'create' "
                             "exists in packages lsst.daf.butler.cli.cmd, test_cliPluginLoader. "
                             "Duplicate commands are not supported, aborting.\n")


if __name__ == "__main__":
    unittest.main()
