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

import abc
import click
import click.testing
import unittest


class CliOptionTestBase(unittest.TestCase, abc.ABC):
    """A test case base that is used to verify click Options.
    """

    def setUp(self):
        self.runner = click.testing.CliRunner()

    def run_command(self, cmd, args):
        """Run a command.

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

    def run_test(self, cmd, cmdArgs, verifyFunc, verifyArgs=None):
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
