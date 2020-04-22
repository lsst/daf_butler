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
import unittest

from lsst.daf.butler.cli import butler


__all__ = ["command_test"]


@click.command()
def command_test():
    click.echo("test command")


class LoaderTestCLI(butler.LoaderCLI):
    """Overrides LodaerCLI._getPluginList to return a local command instead of
    a list of plugin modules from an environment variable."""

    @staticmethod
    def _getPluginList():
        # See Lodaer.CLI._getPluginList for the standard implementation that
        # gets the plugin modules from the environment.
        # In normal use the plugin module is a file with a command that is
        # named in the that file's __all__ parameter, and can be imported.
        # For this test we use the name of this file as the module, and this
        # file's __all__ indicates the name of the test command that will be
        # executed.
        name = ["test_cliPluginLoader"]
        return name


@click.command(cls=LoaderTestCLI)
def cli():
    pass


class Suite(unittest.TestCase):

    def test_loadAndExecuteCommand(self):
        runner = click.testing.CliRunner()
        result = runner.invoke(cli, "command-test")
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(result.stdout, "test command\n")

    def test_loadTopHelp(self):
        runner = click.testing.CliRunner()
        result = runner.invoke(cli, "--help")
        self.assertEqual(result.exit_code, 0, result.stdout)
        self.assertIn("Commands:\n  command-test", result.stdout)


if __name__ == "__main__":
    unittest.main()
