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

"""Unit tests for the daf_butler dataset-type CLI option.
"""

import click
import click.testing
import unittest

from lsst.daf.butler.cli.opt import dataset_type_option


@click.command()
@click.pass_context
@dataset_type_option(help="the dataset type")
def cli(ctx, dataset_type):
    click.echo(dataset_type, nl=False)


class Suite(unittest.TestCase):

    def test_single(self):
        runner = click.testing.CliRunner()
        result = runner.invoke(cli, ["--dataset-type", "one"])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.stdout, "['one']")

    def test_multiple(self):
        runner = click.testing.CliRunner()
        result = runner.invoke(cli, ["--dataset-type", "one,two"])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.stdout, "['one', 'two']")

    def test_singlePair(self):
        runner = click.testing.CliRunner()
        result = runner.invoke(cli, ["--dataset-type", "one,two"])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.stdout, "['one', 'two']")

    def test_multiplePair(self):
        runner = click.testing.CliRunner()
        result = runner.invoke(cli, ["--dataset-type", "one,two", "-d", "three,four"])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.stdout, "['one', 'two', 'three', 'four']")


if __name__ == "__main__":
    unittest.main()
