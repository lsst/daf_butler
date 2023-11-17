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

"""Unit tests for daf_butler CLI config-validate command.
"""

import unittest

from lsst.daf.butler.cli import butler
from lsst.daf.butler.cli.cmd import config_validate
from lsst.daf.butler.cli.utils import LogCliRunner
from lsst.daf.butler.tests import CliCmdTestBase


class ValidateTest(CliCmdTestBase, unittest.TestCase):
    """Test the config validation command-line."""

    mockFuncName = "lsst.daf.butler.cli.cmd.commands.script.configValidate"

    @staticmethod
    def defaultExpected():
        return {}

    @staticmethod
    def command():
        return config_validate


class ConfigValidateUseTest(unittest.TestCase):
    """Test executing the command."""

    def setUp(self):
        self.runner = LogCliRunner()

    def testConfigValidate(self):
        """Test validating a valid config."""
        with self.runner.isolated_filesystem():
            result = self.runner.invoke(butler.cli, ["create", "here"])
            self.assertEqual(result.exit_code, 0, result.stdout)
            # verify the just-created repo validates without error
            result = self.runner.invoke(butler.cli, ["config-validate", "here"])
            self.assertEqual(result.exit_code, 0, result.stdout)
            self.assertIn("No problems encountered with configuration.", result.stdout)

    def testConfigValidate_ignore(self):
        """Test the ignore flag"""
        with self.runner.isolated_filesystem():
            result = self.runner.invoke(butler.cli, ["create", "here"])
            self.assertEqual(result.exit_code, 0, result.stdout)
            # verify the just-created repo validates without error
            result = self.runner.invoke(
                butler.cli,
                [
                    "config-validate",
                    "here",
                    "--ignore",
                    "storageClasses,repoTransferFormats",
                    "-i",
                    "dimensions",
                ],
            )
            self.assertEqual(result.exit_code, 0, result.stdout)
            self.assertIn("No problems encountered with configuration.", result.stdout)


if __name__ == "__main__":
    unittest.main()
