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
from unittest.mock import MagicMock

from lsst.daf.butler.cli.utils import clickResultMsg, split_commas

mock = MagicMock()


@click.command()
@click.option("--list-of-values", "-l", multiple=True, callback=split_commas)
def cli(list_of_values):
    mock(list_of_values)


class SplitCommasTestCase(unittest.TestCase):

    def test_separate(self):
        """test the split_commas callback by itself"""
        ctx = "unused"
        param = "unused"
        self.assertEqual(split_commas(ctx, param, ("one,two", "three,four")), # noqa E231
                         ("one", "two", "three", "four"))
        self.assertEqual(split_commas(ctx, param, None), None)

    def test_single(self):
        """test the split_commas callback in an option with one value"""
        runner = click.testing.CliRunner()
        result = runner.invoke(cli, ["-l", "one"])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with(("one", ))

    def test_multiple(self):
        """test the split_commas callback in an option with two single
        values"""
        runner = click.testing.CliRunner()
        result = runner.invoke(cli, ["-l", "one", "-l", "two"])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with(("one", "two"))

    def test_singlePair(self):
        """test the split_commas callback in an option with one pair of
        values"""
        runner = click.testing.CliRunner()
        result = runner.invoke(cli, ["-l", "one,two"])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with(("one", "two"))

    def test_multiplePair(self):
        """test the split_commas callback in an option with two pairs of
        values"""
        runner = click.testing.CliRunner()
        result = runner.invoke(cli, ["-l", "one,two", "-l", "three,four"])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with(("one", "two", "three", "four"))

    def test_none(self):
        """test that passing None does not fail and returns None, producing an
        empty tuple in the command function call."""
        runner = click.testing.CliRunner()
        result = runner.invoke(cli, [])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with(())


if __name__ == "__main__":
    unittest.main()
