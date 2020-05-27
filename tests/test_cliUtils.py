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

"""Unit tests for the daf_butler shared CLI options.
"""

import click
import click.testing
import unittest

from lsst.daf.butler.cli import butler
from lsst.daf.butler.cli.utils import Mocker, mockEnvVar


class MockerTestCase(unittest.TestCase):

    def test_callMock(self):
        """Test that a mocked subcommand calls the Mocker and can be verified.
        """
        runner = click.testing.CliRunner(env=mockEnvVar)
        result = runner.invoke(butler.cli, ["create", "repo"])
        self.assertEqual(result.exit_code, 0, f"output: {result.output} exception: {result.exception}")
        Mocker.mock.assert_called_with(repo="repo", config_file=None, standalone=False, override=False,
                                       outfile=None)


if __name__ == "__main__":
    unittest.main()
