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

from lsst.daf.butler.cli.utils import split_kv


class Suite(unittest.TestCase):

    def test_single(self):
        self.assertEqual(split_kv("context", "param", "first=1"), {"first": "1"})

    def test_multiple(self):
        self.assertEqual(split_kv("context", "param", "first=1,second=2"), {"first": "1", "second": "2"})

    def test_wrongSeparator(self):
        with self.assertRaises(click.ClickException):
            split_kv("context", "param", "first-1")

    def test_missingSeparator(self):
        with self.assertRaises(click.ClickException):
            split_kv("context", "param", "first 1")

    def test_duplicateKeys(self):
        with self.assertRaises(click.ClickException):
            split_kv("context", "param", "first=1,first=2")

    # todo I'm not sure click will allow a space separator.
    def test_spaceSeparator(self):
        self.assertEqual(split_kv("context", "param", "first 1,second 2", " "), {"first": "1", "second": "2"})

    def test_dashSeparator(self):
        self.assertEqual(split_kv("context", "param", "first-1,second-2", "-"), {"first": "1", "second": "2"})

    def test_cli(self):
        @click.command()
        @click.option("--value", callback=split_kv, multiple=True)
        def cli(value):
            click.echo(value)
        runner = click.testing.CliRunner()
        result = runner.invoke(cli, ["--value", "first=1"])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.stdout, "{'first': '1'}\n")
        result = runner.invoke(cli, ["--value", "first=1,second=2"])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(eval(result.stdout), {'first': '1', 'second': '2'})
        result = runner.invoke(cli, ["--value", "first=1", "--value", "second=2"])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(eval(result.stdout), {'first': '1', 'second': '2'})


if __name__ == "__main__":
    unittest.main()
