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

"""Unit tests for daf_butler CLI query-collections command.
"""

import click
import unittest
import yaml

from lsst.daf.butler import Butler
from lsst.daf.butler.cli.butler import cli
from lsst.daf.butler.cli.cmd import query_collections
from lsst.daf.butler.tests.mockeredTest import MockeredTestBase


class QueryCollectionsCmdTest(MockeredTestBase):

    defaultExpected = dict(repo=None,
                           collection_type=None,
                           flatten_chains=False,
                           include_chains=None)

    command = query_collections

    def test_minimal(self):
        """Test only the required parameters, and omit the optional parameters.
        """
        self.run_test(["query-collections", "here"],
                      self.makeExpected(repo="here"))

    def test_all(self):
        """Test all parameters"""
        self.run_test(["query-collections", "here",
                       "--flatten-chains",
                       "--include-chains"],
                      self.makeExpected(repo="here",
                                        flatten_chains=True,
                                        include_chains=True))

    def test_help(self):
        self.help_test()


class QueryCollectionsScriptTest(unittest.TestCase):

    def testGetCollections(self):
        run = "ingest/run"
        tag = "ingest"
        expected = {"collections": [run, tag]}
        runner = click.testing.CliRunner()
        with runner.isolated_filesystem():
            butlerCfg = Butler.makeRepo("here")
            # the purpose of this call is to create some collections
            _ = Butler(butlerCfg, run=run, tags=[tag], collections=[tag])
            result = runner.invoke(cli, ["query-collections", "here"])
            self.assertEqual(expected, yaml.safe_load(result.output))


if __name__ == "__main__":
    unittest.main()
