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

"""Unit tests for daf_butler CLI query-collections command.
"""

import os
import unittest

from astropy.table import Table
from lsst.daf.butler import Butler, CollectionType
from lsst.daf.butler.cli.butler import cli
from lsst.daf.butler.cli.cmd import query_collections
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg
from lsst.daf.butler.script import queryCollections
from lsst.daf.butler.tests import CliCmdTestBase, DatastoreMock
from lsst.daf.butler.tests.utils import ButlerTestHelper, readTable
from numpy import array

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class QueryCollectionsCmdTest(CliCmdTestBase, unittest.TestCase):
    """Test the query-collections command-line."""

    mockFuncName = "lsst.daf.butler.cli.cmd.commands.script.queryCollections"

    @staticmethod
    def defaultExpected():
        return dict(
            repo=None, collection_type=tuple(CollectionType.__members__.values()), chains="TABLE", glob=()
        )

    @staticmethod
    def command():
        return query_collections

    def test_minimal(self):
        """Test only required parameters, and omit optional parameters."""
        self.run_test(["query-collections", "here", "--chains", "TABLE"], self.makeExpected(repo="here"))

    def test_all(self):
        """Test all parameters"""
        self.run_test(
            [
                "query-collections",
                "here",
                "foo*",
                "--collection-type",
                "TAGGED",
                "--collection-type",
                "RUN",
                "--chains",
                "TABLE",
            ],
            self.makeExpected(
                repo="here",
                glob=("foo*",),
                collection_type=(CollectionType.TAGGED, CollectionType.RUN),
                chains="TABLE",
            ),
        )


class QueryCollectionsScriptTest(ButlerTestHelper, unittest.TestCase):
    """Test the query-collections script interface."""

    def setUp(self):
        self.runner = LogCliRunner()

    def testGetCollections(self):
        run = "ingest/run"
        tag = "tag"
        with self.runner.isolated_filesystem():
            butlerCfg = Butler.makeRepo("here")
            # the purpose of this call is to create some collections
            butler = Butler.from_config(butlerCfg, run=run, collections=[tag], writeable=True)
            butler.registry.registerCollection(tag, CollectionType.TAGGED)

            # Verify collections that were created are found by
            # query-collections.
            result = self.runner.invoke(cli, ["query-collections", "here"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expected = Table((("ingest/run", "tag"), ("RUN", "TAGGED")), names=("Name", "Type"))
            self.assertAstropyTablesEqual(readTable(result.output), expected)

            # Verify that with a glob argument, that only collections whose
            # name matches with the specified pattern are returned.
            result = self.runner.invoke(cli, ["query-collections", "here", "t*"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expected = Table((("tag",), ("TAGGED",)), names=("Name", "Type"))
            self.assertAstropyTablesEqual(readTable(result.output), expected)

            # Verify that with a collection type argument, only collections of
            # that type are returned.
            result = self.runner.invoke(cli, ["query-collections", "here", "--collection-type", "RUN"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expected = Table((("ingest/run",), ("RUN",)), names=("Name", "Type"))
            self.assertAstropyTablesEqual(readTable(result.output), expected)


class ChainedCollectionsTest(ButlerTestHelper, unittest.TestCase):
    """Test the collection-chain command-line interface."""

    def setUp(self):
        self.runner = LogCliRunner()

    def assertChain(self, args: list[str], expected: str):
        """Run collection-chain and check the expected result"""
        result = self.runner.invoke(cli, ["collection-chain", "here", *args])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertEqual(result.output.strip(), expected, clickResultMsg(result))

    def testChained(self):
        with self.runner.isolated_filesystem():
            # Create a butler and add some chained collections:
            butlerCfg = Butler.makeRepo("here")

            butler1 = Butler.from_config(butlerCfg, writeable=True)

            # Replace datastore functions with mocks:
            DatastoreMock.apply(butler1)

            butler1.import_(filename=os.path.join(TESTDIR, "data", "registry", "base.yaml"))
            butler1.import_(filename=os.path.join(TESTDIR, "data", "registry", "datasets.yaml"))
            registry1 = butler1.registry
            registry1.registerRun("run1")
            registry1.registerCollection("tag1", CollectionType.TAGGED)
            registry1.registerCollection("calibration1", CollectionType.CALIBRATION)

            # Create the collection chain
            self.assertChain(["chain2", "calibration1", "run1"], "[calibration1, run1]")
            self.assertChain(
                ["--mode", "redefine", "chain1", "tag1", "run1", "chain2"], "[tag1, run1, chain2]"
            )

            # Use the script function to test the query-collections TREE
            # option, because the astropy.table.Table.read method, which we are
            # using for verification elsewhere in this file, seems to strip
            # leading whitespace from columns. This makes it impossible to test
            # the nested TREE output of the query-collections subcommand from
            # the command line interface.
            table = queryCollections("here", glob=(), collection_type=CollectionType.all(), chains="TREE")

            expected = Table(
                array(
                    (
                        ("calibration1", "CALIBRATION"),
                        ("chain1", "CHAINED"),
                        ("  tag1", "TAGGED"),
                        ("  run1", "RUN"),
                        ("  chain2", "CHAINED"),
                        ("    calibration1", "CALIBRATION"),
                        ("    run1", "RUN"),
                        ("chain2", "CHAINED"),
                        ("  calibration1", "CALIBRATION"),
                        ("  run1", "RUN"),
                        ("imported_g", "RUN"),
                        ("imported_r", "RUN"),
                        ("run1", "RUN"),
                        ("tag1", "TAGGED"),
                    )
                ),
                names=("Name", "Type"),
            )
            self.assertAstropyTablesEqual(table, expected)

            # Test table with inverse == True
            table = queryCollections(
                "here",
                glob=(),
                collection_type=CollectionType.all(),
                chains="INVERSE-TREE",
            )
            expected = Table(
                array(
                    (
                        ("calibration1", "CALIBRATION"),
                        ("  chain2", "CHAINED"),
                        ("    chain1", "CHAINED"),
                        ("chain1", "CHAINED"),
                        ("chain2", "CHAINED"),
                        ("  chain1", "CHAINED"),
                        ("imported_g", "RUN"),
                        ("imported_r", "RUN"),
                        ("run1", "RUN"),
                        ("  chain1", "CHAINED"),
                        ("  chain2", "CHAINED"),
                        ("    chain1", "CHAINED"),
                        ("tag1", "TAGGED"),
                        ("  chain1", "CHAINED"),
                    )
                ),
                names=("Name", "Type"),
            )
            self.assertAstropyTablesEqual(table, expected)

            result = self.runner.invoke(cli, ["query-collections", "here", "--chains", "TABLE"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expected = Table(
                array(
                    (
                        ("calibration1", "CALIBRATION", ""),
                        ("chain1", "CHAINED", "tag1"),
                        ("", "", "run1"),
                        ("", "", "chain2"),
                        ("chain2", "CHAINED", "calibration1"),
                        ("", "", "run1"),
                        ("imported_g", "RUN", ""),
                        ("imported_r", "RUN", ""),
                        ("run1", "RUN", ""),
                        ("tag1", "TAGGED", ""),
                    )
                ),
                names=("Name", "Type", "Children"),
            )
            table = readTable(result.output)
            self.assertAstropyTablesEqual(readTable(result.output), expected)

            result = self.runner.invoke(cli, ["query-collections", "here", "--chains", "INVERSE-TABLE"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expected = Table(
                array(
                    (
                        ("calibration1", "CALIBRATION", "chain2"),
                        ("chain1", "CHAINED", ""),
                        ("chain2", "CHAINED", "chain1"),
                        ("imported_g", "RUN", ""),
                        ("imported_r", "RUN", ""),
                        ("run1", "RUN", "chain1"),
                        ("", "", "chain2"),
                        ("tag1", "TAGGED", "chain1"),
                    )
                ),
                names=("Name", "Type", "Parents"),
            )
            table = readTable(result.output)
            self.assertAstropyTablesEqual(readTable(result.output), expected)

            result = self.runner.invoke(cli, ["query-collections", "here", "--chains", "FLATTEN"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expected = Table(
                array(
                    (
                        ("calibration1", "CALIBRATION"),
                        ("imported_g", "RUN"),
                        ("imported_r", "RUN"),
                        ("run1", "RUN"),
                        ("tag1", "TAGGED"),
                    )
                ),
                names=("Name", "Type"),
            )
            self.assertAstropyTablesEqual(readTable(result.output), expected, unorderedRows=True)

            # Add a couple more run collections for chain testing
            registry1.registerRun("run2")
            registry1.registerRun("run3")
            registry1.registerRun("run4")

            self.assertChain(["--mode", "pop", "chain1"], "[run1, chain2]")

            self.assertChain(["--mode", "extend", "chain1", "run2", "run3"], "[run1, chain2, run2, run3]")

            self.assertChain(["--mode", "remove", "chain1", "chain2", "run2"], "[run1, run3]")

            self.assertChain(["--mode", "prepend", "chain1", "chain2", "run2"], "[chain2, run2, run1, run3]")

            self.assertChain(["--mode", "pop", "chain1", "1", "3"], "[chain2, run1]")

            self.assertChain(
                ["--mode", "redefine", "chain1", "chain2", "run2", "run3,run4", "--flatten"],
                "[calibration1, run1, run2, run3, run4]",
            )

            self.assertChain(["--mode", "pop", "chain1", "--", "-1", "-3"], "[calibration1, run1, run3]")

            # Out-of-bounds index
            result = self.runner.invoke(cli, ["collection-chain", "here", "--mode", "pop", "chain1", "10"])
            self.assertEqual(result.exit_code, 1)


if __name__ == "__main__":
    unittest.main()
