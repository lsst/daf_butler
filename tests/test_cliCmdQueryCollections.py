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

from astropy.table import Table
from numpy import array
import os
import unittest

from lsst.daf.butler import (
    Butler,
    CollectionType,
)
from lsst.daf.butler.cli.butler import cli
from lsst.daf.butler.cli.cmd import query_collections
from lsst.daf.butler.cli.utils import clickResultMsg, LogCliRunner
from lsst.daf.butler.script import queryCollections
from lsst.daf.butler.tests import CliCmdTestBase, DatastoreMock
from lsst.daf.butler.tests.utils import ButlerTestHelper, readTable


TESTDIR = os.path.abspath(os.path.dirname(__file__))


class QueryCollectionsCmdTest(CliCmdTestBase, unittest.TestCase):

    mockFuncName = "lsst.daf.butler.cli.cmd.commands.script.queryCollections"

    @staticmethod
    def defaultExpected():
        return dict(repo=None,
                    collection_type=tuple(CollectionType.__members__.values()),
                    chains="TABLE",
                    glob=())

    @staticmethod
    def command():
        return query_collections

    def test_minimal(self):
        """Test only the required parameters, and omit the optional parameters.
        """
        self.run_test(["query-collections", "here"],
                      self.makeExpected(repo="here"))

    def test_all(self):
        """Test all parameters"""
        self.run_test(["query-collections", "here", "foo*",
                       "--collection-type", "TAGGED",
                       "--collection-type", "RUN"],
                      self.makeExpected(repo="here",
                                        glob=("foo*",),
                                        collection_type=(CollectionType.TAGGED, CollectionType.RUN),
                                        chains="TABLE"))


class QueryCollectionsScriptTest(ButlerTestHelper, unittest.TestCase):

    def setUp(self):
        self.runner = LogCliRunner()

    def testGetCollections(self):
        run = "ingest/run"
        tag = "tag"
        with self.runner.isolated_filesystem():
            butlerCfg = Butler.makeRepo("here")
            # the purpose of this call is to create some collections
            butler = Butler(butlerCfg, run=run, collections=[tag], writeable=True)
            butler.registry.registerCollection(tag, CollectionType.TAGGED)

            # Verify collections that were created are found by
            # query-collections.
            result = self.runner.invoke(cli, ["query-collections", "here"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expected = Table((("ingest/run", "tag"), ("RUN", "TAGGED")),
                             names=("Name", "Type"))
            self.assertAstropyTablesEqual(readTable(result.output), expected)

            # Verify that with a glob argument, that only collections whose
            # name matches with the specified pattern are returned.
            result = self.runner.invoke(cli, ["query-collections", "here", "t*"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expected = Table((("tag",), ("TAGGED",)),
                             names=("Name", "Type"))
            self.assertAstropyTablesEqual(readTable(result.output), expected)

            # Verify that with a collection type argument, only collections of
            # that type are returned.
            result = self.runner.invoke(cli, ["query-collections", "here", "--collection-type", "RUN"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expected = Table((("ingest/run",), ("RUN",)),
                             names=("Name", "Type"))
            self.assertAstropyTablesEqual(readTable(result.output), expected)


class ChainedCollectionsTest(ButlerTestHelper, unittest.TestCase):

    def setUp(self):
        self.runner = LogCliRunner()

    def testChained(self):
        with self.runner.isolated_filesystem():

            # Create a butler and add some chained collections:
            butlerCfg = Butler.makeRepo("here")

            butler1 = Butler(butlerCfg, writeable=True)

            # Replace datastore functions with mocks:
            DatastoreMock.apply(butler1)

            butler1.import_(filename=os.path.join(TESTDIR, "data", "registry", "base.yaml"))
            butler1.import_(filename=os.path.join(TESTDIR, "data", "registry", "datasets.yaml"))
            registry1 = butler1.registry
            registry1.registerRun("run1")
            registry1.registerCollection("tag1", CollectionType.TAGGED)
            registry1.registerCollection("calibration1", CollectionType.CALIBRATION)

            # Create the collection chain
            result = self.runner.invoke(cli, ["collection-chain", "here", "chain2", "calibration1", "run1"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            result = self.runner.invoke(cli, ["collection-chain", "here", "chain1", "tag1", "run1", "chain2"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))

            # Use the script function to test the query-collections TREE
            # option, because the astropy.table.Table.read method, which we are
            # using for verification elsewhere in this file, seems to strip
            # leading whitespace from columns. This makes it impossible to test
            # the nested TREE output of the query-collections subcommand from
            # the command line interface.
            table = queryCollections("here", glob=(), collection_type=CollectionType.all(), chains="TREE")

            # self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expected = Table(array((("imported_g", "RUN"),
                                    ("imported_r", "RUN"),
                                    ("run1", "RUN"),
                                    ("tag1", "TAGGED"),
                                    ("calibration1", "CALIBRATION"),
                                    ("chain2", "CHAINED"),
                                    ("  calibration1", "CALIBRATION"),
                                    ("  run1", "RUN"),
                                    ("chain1", "CHAINED"),
                                    ("  tag1", "TAGGED"),
                                    ("  run1", "RUN"),
                                    ("  chain2", "CHAINED"),
                                    ("    calibration1", "CALIBRATION"),
                                    ("    run1", "RUN"))),
                             names=("Name", "Type"))
            self.assertAstropyTablesEqual(table, expected)

            result = self.runner.invoke(cli, ["query-collections", "here"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expected = Table(array((
                ("imported_g", "RUN", ""),
                ("imported_r", "RUN", ""),
                ("run1", "RUN", ""),
                ("tag1", "TAGGED", ""),
                ("calibration1", "CALIBRATION", ""),
                ("chain2", "CHAINED", "[calibration1, run1]"),
                ("chain1", "CHAINED", "[tag1, run1, chain2]"))),
                names=("Name", "Type", "Definition"))
            table = readTable(result.output)
            self.assertAstropyTablesEqual(readTable(result.output), expected)

            result = self.runner.invoke(cli, ["query-collections", "here", "--chains", "FLATTEN"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expected = Table(array((
                ("imported_g", "RUN"),
                ("imported_r", "RUN"),
                ("run1", "RUN"),
                ("tag1", "TAGGED"),
                ("calibration1", "CALIBRATION"),
                ("calibration1", "CALIBRATION"),
                ("run1", "RUN"),
                ("tag1", "TAGGED"),
                ("run1", "RUN"),
                ("calibration1", "CALIBRATION"))),
                names=("Name", "Type"))
            self.assertAstropyTablesEqual(readTable(result.output), expected)


if __name__ == "__main__":
    unittest.main()
