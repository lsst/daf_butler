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
import copy
import unittest

from .utils import clickResultMsg, mockEnvVar, Mocker
from . import butler


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

    def makeExpected(cls, **kwargs):
        expected = copy.copy(cls.defaultExpected)
        expected.update(kwargs)
        return expected

    def run_test(self, inputs, expectedKwargs):
        """Test command line interaction with import command function.

        Parameters
        ----------
        inputs : [`str`]
            A list of the arguments to the butler command, starting with
            `import`
        expectedKwargs : `dict` [`str`, `str`]
            The expected arguments to the import command function, keys are
            the argument name and values are the argument value.
        """
        result = self.runner.invoke(butler.cli, inputs)
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        Mocker.mock.assert_called_with(**expectedKwargs)
