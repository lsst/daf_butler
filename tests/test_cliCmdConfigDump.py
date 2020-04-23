
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

"""Unit tests for daf_butler CLI config-dump command.
"""

import click
import click.testing
import os
import unittest
import yaml

from lsst.daf.butler.cli import butler


TESTDIR = os.path.abspath(os.path.dirname(__file__))


class Suite(unittest.TestCase):

    def test_stdout(self):
        """Test dumping the config to stdout."""
        runner = click.testing.CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(butler.cli, ["create", "--repo", "here"])
            self.assertEqual(result.exit_code, 0, result.stdout)

            # test dumping to stdout:
            result = runner.invoke(butler.cli, ["config-dump", "--repo", "here"])
            self.assertEqual(result.exit_code, 0, result.stdout)
            # check for some expected keywords:
            cfg = yaml.safe_load(result.stdout)
            self.assertIn("composites", cfg)
            self.assertIn("datastore", cfg)
            self.assertIn("storageClasses", cfg)

    def test_file(self):
        """test dumping the config to a file."""
        runner = click.testing.CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(butler.cli, ["create", "--repo", "here"])
            self.assertEqual(result.exit_code, 0, result.stdout)
            result = runner.invoke(butler.cli, ["config-dump", "--repo", "here", "--file=there"])
            self.assertEqual(result.exit_code, 0, result.stdout)
            # check for some expected keywords:
            with open("there", "r") as f:
                cfg = yaml.safe_load(f)
                self.assertIn("composites", cfg)
                self.assertIn("datastore", cfg)
                self.assertIn("storageClasses", cfg)

    def test_subset(self):
        """Test selecting a subset of the config."""
        runner = click.testing.CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(butler.cli, ["create", "--repo", "here"])
            self.assertEqual(result.exit_code, 0, result.stdout)
            result = runner.invoke(butler.cli, ["config-dump", "--repo", "here", "--subset", "datastore"])
            self.assertEqual(result.exit_code, 0, result.stdout)
            cfg = yaml.safe_load(result.stdout)
            # the datastore cfg is expected to have exactly six keys:
            self.assertIs(len(cfg.keys()), 6)
            self.assertIn("cls", cfg)
            self.assertIn("create", cfg)
            self.assertIn("formatters", cfg)
            self.assertIn("records", cfg)
            self.assertIn("root", cfg)
            self.assertIn("templates", cfg)

    def test_invalidSubset(self):
        """Test selecting a subset key that does not exist in the config."""
        runner = click.testing.CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(butler.cli, ["create", "--repo", "here"])
            self.assertEqual(result.exit_code, 0, result.stdout)
            # test dumping to stdout:
            result = runner.invoke(butler.cli, ["config-dump", "--repo", "here", "--subset", "foo"])
            self.assertEqual(result.exit_code, 1)
            self.assertEqual(result.output, "Error: foo not found in config at here\n")


if __name__ == "__main__":
    unittest.main()
