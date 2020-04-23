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

"""Unit tests for daf_butler command line interface commands.
"""

import click
import click.testing
import os
import unittest
import yaml

from lsst.daf.butler.cli import butler


TESTDIR = os.path.abspath(os.path.dirname(__file__))


class Suite(unittest.TestCase):

    def testCreate(self):
        """Test creating a repostiry."""
        runner = click.testing.CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(butler.cli, ["create", "--repo", "here"])
            self.assertEqual(result.exit_code, 0)

    def testCreate_outfile(self):
        """Test creating a repository and specify an outfile location."""
        runner = click.testing.CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(butler.cli, ["create", "--repo", "here", "--outfile=there"])
            self.assertEqual(result.exit_code, 0)
            self.assertTrue(os.path.exists("there"))  # verify the separate config file was made
            with open("there", "r") as f:
                cfg = yaml.safe_load(f)
                self.assertIn("datastore", cfg)
                self.assertIn("PosixDatastore", cfg["datastore"]["cls"])
                self.assertIn("root", cfg)


if __name__ == "__main__":
    unittest.main()
