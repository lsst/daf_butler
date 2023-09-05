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

"""Unit tests for the daf_butler shared CLI options.
"""

import unittest
from unittest.mock import MagicMock

import click
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg, split_commas

mock = MagicMock()


@click.command()
@click.option("--list-of-values", "-l", multiple=True, callback=split_commas)
def cli(list_of_values):
    """Run mocked command line."""
    mock(list_of_values)


class SplitCommasTestCase(unittest.TestCase):
    """Test the split commas utility."""

    def setUp(self):
        self.runner = LogCliRunner()

    def test_separate(self):
        """Test the split_commas callback by itself."""
        ctx = "unused"
        param = "unused"
        self.assertEqual(split_commas(ctx, param, ("one,two", "three,four")), ("one", "two", "three", "four"))
        self.assertEqual(split_commas(ctx, param, None), ())

    def test_single(self):
        """Test the split_commas callback in an option with one value."""
        result = self.runner.invoke(cli, ["-l", "one"])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with(("one",))

    def test_multiple(self):
        """Test the split_commas callback in an option with two single
        values.
        """
        result = self.runner.invoke(cli, ["-l", "one", "-l", "two"])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with(("one", "two"))

    def test_singlePair(self):
        """Test the split_commas callback in an option with one pair of
        values.
        """
        result = self.runner.invoke(cli, ["-l", "one,two"])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with(("one", "two"))

    def test_multiplePair(self):
        """Test the split_commas callback in an option with two pairs of
        values.
        """
        result = self.runner.invoke(cli, ["-l", "one,two", "-l", "three,four"])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with(("one", "two", "three", "four"))

    def test_none(self):
        """Test that passing None does not fail and returns None, producing an
        empty tuple in the command function call.
        """
        result = self.runner.invoke(cli, [])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with(())

    def test_parens(self):
        """Test that split commas understands ``[a, b]``."""
        for test, expected in (
            ("single", ("single",)),
            ("a,b", ("a", "b")),
            ("[a,b]", ("[a,b]",)),
            ("a[1,2],b", ("a[1,2]", "b")),
        ):
            result = split_commas(None, None, test)
            self.assertEqual(result, expected)

        # These should warn because it's likely a typo.
        for test, expected in (
            ("a[1,b[2,3],c", ("a[1,b[2,3]", "c")),
            ("a[1,b,c", ("a[1,b,c",)),
            ("a[1,b", ("a[1,b",)),
            ("a1,b]", ("a1", "b]")),
        ):
            with self.assertWarns(UserWarning, msg=f"Testing {test!r}"):
                result = split_commas(None, None, test)
            self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
