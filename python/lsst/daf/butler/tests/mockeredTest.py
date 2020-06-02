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
import copy
import unittest

from ..cli.utils import clickResultMsg, mockEnvVar, Mocker
from ..cli import butler


class MockeredTestBase(unittest.TestCase, abc.ABC):
    """A test case base that is used to verify click command functions import
    and call their respective script fucntions correctly.
    """

    @classmethod
    @property
    @abc.abstractmethod
    def defaultExpected(cls):
        pass

    def setUp(self):
        self.runner = click.testing.CliRunner(env=mockEnvVar)

    def makeExpected(self, **kwargs):
        expected = copy.copy(self.defaultExpected)
        expected.update(kwargs)
        return expected

    def run_command(self, inputs):
        """Use the CliRunner with the mock environment variable set to execute
        a butler subcommand and parameters specified in inputs.

        Parameters
        ----------
        inputs : [`str`]
            A list of strings that begins with the subcommand name and is
            followed by arguments, option keys and option values.

        Returns
        -------
        result : `click.testing.Result`
            The Result object contains the results from calling
            self.runner.invoke.
        """
        return self.runner.invoke(butler.cli, inputs)

    def run_test(self, inputs, expectedKwargs):
        """Run the subcommand specified in inputs and verify a successful
        outcome where exit code = 0 and the mock object has been called with
        the expected arguments.

        Parameters
        ----------
        inputs : [`str`]
            A list of strings that begins with the subcommand name and is
            followed by arguments, option keys and option values.
        expectedKwargs : `dict` [`str`, `str`]
            The arguments that the subcommand function is expected to have been
            called with. Keys are the argument name and values are the argument
            value.
        """
        result = self.run_command(inputs)
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        Mocker.mock.assert_called_with(**expectedKwargs)

    def run_missing(self, inputs, expectedMsg):
        """Run the subcommand specified in inputs and verify a failed outcome
        where exit code != 0 and an expected message has been written to
        stdout.

        Parameters
        ----------
        inputs : [`str`]
            A list of strings that begins with the subcommand name and is
            followed by arguments, option keys and option values.
        expectedMsg : `str`
            An error message that should be present in stdout after running the
            subcommand.
        """
        result = self.run_command(inputs)
        self.assertNotEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertIn(expectedMsg, result.stdout)
