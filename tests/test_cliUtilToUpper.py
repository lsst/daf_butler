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

import unittest

import click
from lsst.daf.butler.cli.utils import LogCliRunner, to_upper


@click.command()
@click.option("--value", callback=to_upper)
def cli(value):
    click.echo(value)


class ToUpperTestCase(unittest.TestCase):
    def setUp(self):
        self.runner = LogCliRunner()

    def test_isolated(self):
        """test the to_upper callback by itself"""
        ctx = "unused"
        param = "unused"
        self.assertEqual(to_upper(ctx, param, "debug"), "DEBUG")

    def test_lowerToUpper(self):
        """test the to_upper callback in an option with a lowercase value"""
        result = self.runner.invoke(cli, ["--value", "debug"])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.stdout, "DEBUG\n")

    def test_upperToUpper(self):
        """test the to_upper callback in an option with a uppercase value"""
        result = self.runner.invoke(cli, ["--value", "DEBUG"])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.stdout, "DEBUG\n")

    def test_mixedToUpper(self):
        """test the to_upper callback in an option with a mixed-case value"""
        result = self.runner.invoke(cli, ["--value", "DeBuG"])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.stdout, "DEBUG\n")


if __name__ == "__main__":
    unittest.main()
