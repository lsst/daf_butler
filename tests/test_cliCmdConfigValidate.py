
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

"""Unit tests for daf_butler CLI config-validate command.
"""

import click
import click.testing
import os
import unittest

from lsst.daf.butler.cli import butler


TESTDIR = os.path.abspath(os.path.dirname(__file__))


class Suite(unittest.TestCase):

    def testConfigValidate(self):
        """Test validating a valid config."""
        runner = click.testing.CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(butler.cli, ["create", "--repo", "here"])
            self.assertEqual(result.exit_code, 0, result.stdout)
            # verify the just-created repo validates without error
            result = runner.invoke(butler.cli, ["config-validate", "--repo", "here"])
            self.assertEqual(result.exit_code, 0, result.stdout)
            self.assertEqual(result.stdout, "No problems encountered with configuration.\n")

    def testConfigValidate_ignore(self):
        """Test the ignore flag"""
        runner = click.testing.CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(butler.cli, ["create", "--repo", "here"])
            self.assertEqual(result.exit_code, 0, result.stdout)
            # verify the just-created repo validates without error
            result = runner.invoke(butler.cli, ["config-validate", "--repo", "here",
                                   "--ignore", "storageClasses,repoTransferFormats", "-i", "dimensions"])
            self.assertEqual(result.exit_code, 0, result.stdout)
            self.assertEqual(result.stdout, "No problems encountered with configuration.\n")


if __name__ == "__main__":
    unittest.main()
