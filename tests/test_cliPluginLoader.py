# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

"""Unit tests for the daf_butler CLI plugin loader."""

import os
import unittest
from collections import defaultdict
from contextlib import contextmanager
from unittest.mock import patch

import click
import yaml

from lsst.daf.butler.cli import butler, cmd
from lsst.daf.butler.cli.butler import UncachedButlerCLI
from lsst.daf.butler.cli.utils import LogCliRunner, command_test_env


@click.command()
def command_test():
    """Run command test."""
    click.echo(message="test command")


@contextmanager
def duplicate_command_test_env(runner):
    """Context manager that creates (and then cleans up) an environment that
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


@click.command(cls=UncachedButlerCLI)
def uncached_cli():
    """ButlerCLI that does not cache the commands."""
    pass


class FailedLoadTest(unittest.TestCase):
    """Test failed plugin loading."""

    def setUp(self):
        self.runner = LogCliRunner()

    def test_unimportablePlugin(self):
        with command_test_env(self.runner, "test_cliPluginLoader", "non-existant-command-function"):
            with self.assertLogs() as cm:
                result = self.runner.invoke(uncached_cli, "--help")
            self.assertEqual(result.exit_code, 0, f"output: {result.output!r} exception: {result.exception}")
            expectedErrMsg = (
                "Could not import plugin from test_cliPluginLoader.non_existant_command_function, skipping."
            )
            self.assertIn(expectedErrMsg, " ".join(cm.output))

    def test_unimportableLocalPackage(self):
        class FailCLI(butler.LoaderCLI):
            localCmdPkg = "lsst.daf.butler.cli.cmds"  # should not be an importable location

        @click.command(cls=FailCLI)
        def cli():
            pass

        with self.assertLogs() as cm:
            result = self.runner.invoke(cli)
        self.assertEqual(result.exit_code, 0, f"output: {result.output} exception: {result.exception}")
        expectedErrMsg = f"Could not import plugin from {FailCLI.localCmdPkg}, skipping."
        self.assertIn(expectedErrMsg, " ".join(cm.output))


class PluginLoaderTest(unittest.TestCase):
    """Test the command-line plugin loader."""

    def setUp(self):
        self.runner = LogCliRunner()

    def test_loadAndExecutePluginCommand(self):
        """Test that a plugin command can be loaded and executed."""
        with command_test_env(self.runner, "test_cliPluginLoader", "command-test"):
            result = self.runner.invoke(uncached_cli, "command-test")
            self.assertEqual(result.exit_code, 0, f"output: {result.output} exception: {result.exception}")
            self.assertEqual(result.stdout, "test command\n")

    def test_loadAndExecuteLocalCommand(self):
        """Test that a command in daf_butler can be loaded and executed."""
        with self.runner.isolated_filesystem():
            result = self.runner.invoke(butler.cli, ["create", "test_repo"])
            self.assertEqual(result.exit_code, 0, f"output: {result.output} exception: {result.exception}")
            self.assertTrue(os.path.exists("test_repo"))

    def test_loadTopHelp(self):
        """Test that an expected command is produced by 'butler --help'"""
        with command_test_env(self.runner, "test_cliPluginLoader", "command-test"):
            result = self.runner.invoke(uncached_cli, "--help")
            self.assertEqual(result.exit_code, 0, f"output: {result.output} exception: {result.exception}")
            self.assertIn("command-test", result.stdout)

    def test_getLocalCommands(self):
        """Test getting the daf_butler CLI commands."""
        localCommands = butler.ButlerCLI().getLocalCommands()
        # the number of local commands should equal the number of functions
        # in cmd.__all__
        self.assertEqual(len(localCommands), len(cmd.__all__))

    def test_mergeCommandLists(self):
        """Verify dicts of command to list-of-source-package get merged
        properly.
        """
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
        with duplicate_command_test_env(self.runner):
            result = self.runner.invoke(uncached_cli, ["create", "test_repo"])
            self.assertEqual(result.exit_code, 1, f"output: {result.output} exception: {result.exception}")
            self.assertEqual(
                result.output,
                "Error: Command 'create' "
                "exists in packages lsst.daf.butler.cli.cmd, test_cliPluginLoader. "
                "Duplicate commands are not supported, aborting.\n",
            )


if __name__ == "__main__":
    unittest.main()
