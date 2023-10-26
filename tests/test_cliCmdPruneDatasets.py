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

"""Unit tests for daf_butler CLI prune-datasets subcommand.
"""

import unittest
from unittest.mock import patch

# Tests require the SqlRegistry
import lsst.daf.butler.registry.sql_registry
import lsst.daf.butler.script
from astropy.table import Table
from lsst.daf.butler.cli.butler import cli as butlerCli
from lsst.daf.butler.cli.cmd.commands import (
    pruneDatasets_askContinueMsg,
    pruneDatasets_didNotRemoveAforementioned,
    pruneDatasets_didRemoveAforementioned,
    pruneDatasets_didRemoveMsg,
    pruneDatasets_errNoCollectionRestriction,
    pruneDatasets_errNoOp,
    pruneDatasets_errPruneOnNotRun,
    pruneDatasets_errPurgeAndDisassociate,
    pruneDatasets_errQuietWithDryRun,
    pruneDatasets_noDatasetsFound,
    pruneDatasets_willRemoveMsg,
    pruneDatasets_wouldDisassociateAndRemoveMsg,
    pruneDatasets_wouldDisassociateMsg,
    pruneDatasets_wouldRemoveMsg,
)
from lsst.daf.butler.cli.utils import LogCliRunner, astropyTablesToStr, clickResultMsg
from lsst.daf.butler.direct_butler import DirectButler
from lsst.daf.butler.registry import CollectionType
from lsst.daf.butler.script import QueryDatasets

doFindTables = True


def getTables():
    """Return test table."""
    if doFindTables:
        return (Table(((1, 2, 3),), names=("foo",)),)
    return ()


def getDatasets():
    """Return the datasets string."""
    return "datasets"


def makeQueryDatasets(*args, **kwargs):
    """Return a query datasets object."""
    return QueryDatasets(*args, **kwargs)


class PruneDatasetsTestCase(unittest.TestCase):
    """Tests the ``prune_datasets`` "command" function (in
    ``cli/cmd/commands.py``) and the ``pruneDatasets`` "script" function (in
    ``scripts/_pruneDatasets.py``).

    ``Butler.pruneDatasets`` and a few other functions that get called before
    it are mocked, and tests check for expected arguments to those mocks.
    """

    def setUp(self):
        self.repo = "here"

    @staticmethod
    def makeQueryDatasetsArgs(*, repo, **kwargs):
        expectedArgs = dict(
            repo=repo, collections=(), where="", find_first=True, show_uri=False, glob=tuple()
        )
        expectedArgs.update(kwargs)
        return expectedArgs

    @staticmethod
    def makePruneDatasetsArgs(**kwargs):
        expectedArgs = dict(refs=tuple(), disassociate=False, tags=(), purge=False, unstore=False)
        expectedArgs.update(kwargs)
        return expectedArgs

    # Mock the QueryDatasets.getTables function to return a set of Astropy
    # tables, similar to what would be returned by a call to
    # QueryDatasets.getTables on a repo with real data.
    @patch.object(lsst.daf.butler.script._pruneDatasets.QueryDatasets, "getTables", side_effect=getTables)
    # Mock the QueryDatasets.getDatasets function. Normally it would return a
    # list of queries.DatasetQueryResults, but all we need to do is verify that
    # the output of this function is passed into our pruneDatasets magicMock,
    # so we can return something arbitrary that we can test is equal."""
    @patch.object(lsst.daf.butler.script._pruneDatasets.QueryDatasets, "getDatasets", side_effect=getDatasets)
    # Mock the actual QueryDatasets class, so we can inspect calls to its init
    # function. Note that the side_effect returns an instance of QueryDatasets,
    # so this mock records and then is a pass-through.
    @patch.object(lsst.daf.butler.script._pruneDatasets, "QueryDatasets", side_effect=makeQueryDatasets)
    # Mock the pruneDatasets butler command so we can test for expected calls
    # to it, without dealing with setting up a full repo with data for it.
    @patch.object(DirectButler, "pruneDatasets")
    def run_test(
        self,
        mockPruneDatasets,
        mockQueryDatasets_init,
        mockQueryDatasets_getDatasets,
        mockQueryDatasets_getTables,
        cliArgs,
        exMsgs,
        exPruneDatasetsCallArgs,
        exGetTablesCalled,
        exQueryDatasetsCallArgs,
        invokeInput=None,
        exPruneDatasetsExitCode=0,
    ):
        """Execute the test.

        Makes a temporary repo, invokes ``prune-datasets``. Verifies expected
        output, exit codes, and mock calls.

        Parameters
        ----------
        mockPruneDatasets : `MagicMock`
            The MagicMock for the ``Butler.pruneDatasets`` function.
        mockQueryDatasets_init : `MagicMock`
            The MagicMock for the ``QueryDatasets.__init__`` function.
        mockQueryDatasets_getDatasets : `MagicMock`
            The MagicMock for the ``QueryDatasets.getDatasets`` function.
        mockQueryDatasets_getTables : `MagicMock`
            The MagicMock for the ``QueryDatasets.getTables`` function.
        cliArgs : `list` [`str`]
            The arguments to pass to the command line. Do not include the
            subcommand name or the repo.
        exMsgs : `list` [`str`] or None
            A list of text fragments that should appear in the text output
            after calling the CLI command, or None if no output should be
            produced.
        exPruneDatasetsCallArgs : `dict` [`str`, `Any`]
            The arguments that ``Butler.pruneDatasets`` should have been called
            with, or None if that function should not have been called.
        exGetTablesCalled : bool
            `True` if ``QueryDatasets.getTables`` should have been called, else
            `False`.
        exQueryDatasetsCallArgs : `dict` [`str`, `Any`]
            The arguments that ``QueryDatasets.__init__`` should have bene
            called with, or `None` if the function should not have been called.
        invokeInput : `str`, optional.
            As string to pass to the ``CliRunner.invoke`` `input` argument. By
            default None.
        exPruneDatasetsExitCode : `int`
            The expected exit code returned from invoking ``prune-datasets``.
        """
        runner = LogCliRunner()
        with runner.isolated_filesystem():
            # Make a repo so a butler can be created
            result = runner.invoke(butlerCli, ["create", self.repo])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))

            # Run the prune-datasets CLI command, this will call all of our
            # mocks:
            cliArgs = ["prune-datasets", self.repo] + cliArgs
            result = runner.invoke(butlerCli, cliArgs, input=invokeInput)
            self.assertEqual(result.exit_code, exPruneDatasetsExitCode, clickResultMsg(result))

            # Verify the Butler.pruneDatasets was called exactly once with
            # expected arguments. The datasets argument is the value returned
            # by QueryDatasets, which we've mocked with side effect
            # ``getDatasets()``.
            if exPruneDatasetsCallArgs:
                mockPruneDatasets.assert_called_once_with(**exPruneDatasetsCallArgs)
            else:
                mockPruneDatasets.assert_not_called()

            # Less critical, but do a quick verification that the QueryDataset
            # member function mocks were called, in this case we expect one
            # time each.
            if exQueryDatasetsCallArgs:
                mockQueryDatasets_init.assert_called_once_with(**exQueryDatasetsCallArgs)
            else:
                mockQueryDatasets_init.assert_not_called()
            # If Butler.pruneDatasets was not called, then
            # QueryDatasets.getDatasets also does not get called.
            if exPruneDatasetsCallArgs:
                mockQueryDatasets_getDatasets.assert_called_once()
            else:
                mockQueryDatasets_getDatasets.assert_not_called()
            if exGetTablesCalled:
                mockQueryDatasets_getTables.assert_called_once()
            else:
                mockQueryDatasets_getTables.assert_not_called()

            if exMsgs is None:
                self.assertEqual("", result.output)
            else:
                for expectedMsg in exMsgs:
                    self.assertIn(expectedMsg, result.output)

    def test_defaults_doContinue(self):
        """Test running with the default values.

        Verify that with the default flags that the subcommand says what it
        will do, prompts for input, and says that it's done.
        """
        self.run_test(
            cliArgs=["myCollection", "--unstore"],
            exPruneDatasetsCallArgs=self.makePruneDatasetsArgs(refs=getDatasets(), unstore=True),
            exQueryDatasetsCallArgs=self.makeQueryDatasetsArgs(repo=self.repo, collections=("myCollection",)),
            exGetTablesCalled=True,
            exMsgs=(
                pruneDatasets_willRemoveMsg,
                pruneDatasets_askContinueMsg,
                astropyTablesToStr(getTables()),
                pruneDatasets_didRemoveAforementioned,
            ),
            invokeInput="yes",
        )

    def test_defaults_doNotContinue(self):
        """Test running with the default values but not continuing.

        Verify that with the default flags that the subcommand says what it
        will do, prompts for input, and aborts when told not to continue.
        """
        self.run_test(
            cliArgs=["myCollection", "--unstore"],
            exPruneDatasetsCallArgs=None,
            exQueryDatasetsCallArgs=self.makeQueryDatasetsArgs(repo=self.repo, collections=("myCollection",)),
            exGetTablesCalled=True,
            exMsgs=(
                pruneDatasets_willRemoveMsg,
                pruneDatasets_askContinueMsg,
                pruneDatasets_didNotRemoveAforementioned,
            ),
            invokeInput="no",
        )

    def test_dryRun_unstore(self):
        """Test the --dry-run flag with --unstore.

        Verify that with the dry-run flag the subcommand says what it would
        remove, but does not remove the datasets.
        """
        self.run_test(
            cliArgs=["myCollection", "--dry-run", "--unstore"],
            exPruneDatasetsCallArgs=None,
            exQueryDatasetsCallArgs=self.makeQueryDatasetsArgs(repo=self.repo, collections=("myCollection",)),
            exGetTablesCalled=True,
            exMsgs=(pruneDatasets_wouldRemoveMsg, astropyTablesToStr(getTables())),
        )

    def test_dryRun_disassociate(self):
        """Test the --dry-run flag with --disassociate.

        Verify that with the dry-run flag the subcommand says what it would
        remove, but does not remove the datasets.
        """
        collection = "myCollection"
        self.run_test(
            cliArgs=[collection, "--dry-run", "--disassociate", "tag1"],
            exPruneDatasetsCallArgs=None,
            exQueryDatasetsCallArgs=self.makeQueryDatasetsArgs(repo=self.repo, collections=(collection,)),
            exGetTablesCalled=True,
            exMsgs=(
                pruneDatasets_wouldDisassociateMsg.format(collections=(collection,)),
                astropyTablesToStr(getTables()),
            ),
        )

    def test_dryRun_unstoreAndDisassociate(self):
        """Test the --dry-run flag with --unstore and --disassociate.

        Verify that with the dry-run flag the subcommand says what it would
        remove, but does not remove the datasets.
        """
        collection = "myCollection"
        self.run_test(
            cliArgs=[collection, "--dry-run", "--unstore", "--disassociate", "tag1"],
            exPruneDatasetsCallArgs=None,
            exQueryDatasetsCallArgs=self.makeQueryDatasetsArgs(repo=self.repo, collections=(collection,)),
            exGetTablesCalled=True,
            exMsgs=(
                pruneDatasets_wouldDisassociateAndRemoveMsg.format(collections=(collection,)),
                astropyTablesToStr(getTables()),
            ),
        )

    def test_noConfirm(self):
        """Test the --no-confirm flag.

        Verify that with the no-confirm flag the subcommand does not ask for
        a confirmation, prints the did remove message and the tables that were
        passed for removal.
        """
        self.run_test(
            cliArgs=["myCollection", "--no-confirm", "--unstore"],
            exPruneDatasetsCallArgs=self.makePruneDatasetsArgs(refs=getDatasets(), unstore=True),
            exQueryDatasetsCallArgs=self.makeQueryDatasetsArgs(repo=self.repo, collections=("myCollection",)),
            exGetTablesCalled=True,
            exMsgs=(pruneDatasets_didRemoveMsg, astropyTablesToStr(getTables())),
        )

    def test_quiet(self):
        """Test the --quiet flag.

        Verify that with the quiet flag and the no-confirm flags set that no
        output is produced by the subcommand.
        """
        self.run_test(
            cliArgs=["myCollection", "--quiet", "--unstore"],
            exPruneDatasetsCallArgs=self.makePruneDatasetsArgs(refs=getDatasets(), unstore=True),
            exQueryDatasetsCallArgs=self.makeQueryDatasetsArgs(repo=self.repo, collections=("myCollection",)),
            exGetTablesCalled=True,
            exMsgs=None,
        )

    def test_quietWithDryRun(self):
        """Test for an error using the --quiet flag with --dry-run."""
        self.run_test(
            cliArgs=["--quiet", "--dry-run", "--unstore"],
            exPruneDatasetsCallArgs=None,
            exQueryDatasetsCallArgs=None,
            exGetTablesCalled=False,
            exMsgs=(pruneDatasets_errQuietWithDryRun,),
            exPruneDatasetsExitCode=1,
        )

    def test_noCollections(self):
        """Test for an error if no collections are indicated."""
        self.run_test(
            cliArgs=["--find-all", "--unstore"],
            exPruneDatasetsCallArgs=None,
            exQueryDatasetsCallArgs=None,
            exGetTablesCalled=False,
            exMsgs=(pruneDatasets_errNoCollectionRestriction,),
            exPruneDatasetsExitCode=1,
        )

    def test_noDatasets(self):
        """Test for expected outputs when no datasets are found."""
        global doFindTables
        reset = doFindTables
        try:
            doFindTables = False
            self.run_test(
                cliArgs=["myCollection", "--unstore"],
                exPruneDatasetsCallArgs=None,
                exQueryDatasetsCallArgs=self.makeQueryDatasetsArgs(
                    repo=self.repo, collections=("myCollection",)
                ),
                exGetTablesCalled=True,
                exMsgs=(pruneDatasets_noDatasetsFound,),
            )
        finally:
            doFindTables = reset

    def test_purgeWithDisassociate(self):
        """Verify there is an error when --purge and --disassociate are both
        passed in.
        """
        self.run_test(
            cliArgs=["--purge", "run", "--disassociate", "tag1", "tag2"],
            exPruneDatasetsCallArgs=None,
            exQueryDatasetsCallArgs=None,  # should not make it far enough to call this.
            exGetTablesCalled=False,  # ...or this.
            exMsgs=(pruneDatasets_errPurgeAndDisassociate,),
            exPruneDatasetsExitCode=1,
        )

    def test_purgeNoOp(self):
        """Verify there is an error when none of --purge, --unstore, or
        --disassociate are passed.
        """
        self.run_test(
            cliArgs=[],
            exPruneDatasetsCallArgs=None,
            exQueryDatasetsCallArgs=None,  # should not make it far enough to call this.
            exGetTablesCalled=False,  # ...or this.
            exMsgs=(pruneDatasets_errNoOp,),
            exPruneDatasetsExitCode=1,
        )

    @patch.object(
        lsst.daf.butler.registry.sql_registry.SqlRegistry,
        "getCollectionType",
        side_effect=lambda x: CollectionType.RUN,
    )
    def test_purgeImpliedArgs(self, mockGetCollectionType):
        """Verify the arguments implied by --purge.

        --purge <run> implies the following arguments to butler.pruneDatasets:
        purge=True, disassociate=True, unstore=True
        And for QueryDatasets, if COLLECTIONS is not passed then <run> gets
        used as the value of COLLECTIONS (and when there is a COLLECTIONS
        value then find_first gets set to True)
        """
        self.run_test(
            cliArgs=["--purge", "run"],
            invokeInput="yes",
            exPruneDatasetsCallArgs=self.makePruneDatasetsArgs(
                purge=True, refs=getDatasets(), disassociate=True, unstore=True
            ),
            exQueryDatasetsCallArgs=self.makeQueryDatasetsArgs(
                repo=self.repo, collections=("run",), find_first=True
            ),
            exGetTablesCalled=True,
            exMsgs=(
                pruneDatasets_willRemoveMsg,
                pruneDatasets_askContinueMsg,
                astropyTablesToStr(getTables()),
                pruneDatasets_didRemoveAforementioned,
            ),
        )

    @patch.object(
        lsst.daf.butler.registry.sql_registry.SqlRegistry,
        "getCollectionType",
        side_effect=lambda x: CollectionType.RUN,
    )
    def test_purgeImpliedArgsWithCollections(self, mockGetCollectionType):
        """Verify the arguments implied by --purge, with a COLLECTIONS."""
        self.run_test(
            cliArgs=["myCollection", "--purge", "run"],
            invokeInput="yes",
            exPruneDatasetsCallArgs=self.makePruneDatasetsArgs(
                purge=True, disassociate=True, unstore=True, refs=getDatasets()
            ),
            exQueryDatasetsCallArgs=self.makeQueryDatasetsArgs(
                repo=self.repo, collections=("myCollection",), find_first=True
            ),
            exGetTablesCalled=True,
            exMsgs=(
                pruneDatasets_willRemoveMsg,
                pruneDatasets_askContinueMsg,
                astropyTablesToStr(getTables()),
                pruneDatasets_didRemoveAforementioned,
            ),
        )

    @patch.object(
        lsst.daf.butler.registry.sql_registry.SqlRegistry,
        "getCollectionType",
        side_effect=lambda x: CollectionType.TAGGED,
    )
    def test_purgeOnNonRunCollection(self, mockGetCollectionType):
        """Verify calling run on a non-run collection fails with expected
        error message.
        """
        collectionName = "myTaggedCollection"
        self.run_test(
            cliArgs=["--purge", collectionName],
            invokeInput="yes",
            exPruneDatasetsCallArgs=None,
            exQueryDatasetsCallArgs=None,
            exGetTablesCalled=False,
            exMsgs=(pruneDatasets_errPruneOnNotRun.format(collection=collectionName)),
            exPruneDatasetsExitCode=1,
        )

    def test_disassociateImpliedArgs(self):
        """Verify the arguments implied by --disassociate.

        --disassociate <tags> implies the following arguments to
        butler.pruneDatasets:
        disassociate=True, tags=<tags>
        and if COLLECTIONS is not passed then <tags> gets used as the value
        of COLLECTIONS.

        Use the --no-confirm flag instead of invokeInput="yes", and check for
        the associated output.
        """
        self.run_test(
            cliArgs=["--disassociate", "tag1", "--disassociate", "tag2", "--no-confirm"],
            exPruneDatasetsCallArgs=self.makePruneDatasetsArgs(
                tags=("tag1", "tag2"), disassociate=True, refs=getDatasets()
            ),
            exQueryDatasetsCallArgs=self.makeQueryDatasetsArgs(
                repo=self.repo, collections=("tag1", "tag2"), find_first=True
            ),
            exGetTablesCalled=True,
            exMsgs=(pruneDatasets_didRemoveMsg, astropyTablesToStr(getTables())),
        )

    def test_disassociateImpliedArgsWithCollections(self):
        """Verify the arguments implied by --disassociate, with a --collection
        flag.
        """
        self.run_test(
            cliArgs=["myCollection", "--disassociate", "tag1", "--disassociate", "tag2", "--no-confirm"],
            exPruneDatasetsCallArgs=self.makePruneDatasetsArgs(
                tags=("tag1", "tag2"), disassociate=True, refs=getDatasets()
            ),
            exQueryDatasetsCallArgs=self.makeQueryDatasetsArgs(
                repo=self.repo, collections=("myCollection",), find_first=True
            ),
            exGetTablesCalled=True,
            exMsgs=(pruneDatasets_didRemoveMsg, astropyTablesToStr(getTables())),
        )


if __name__ == "__main__":
    unittest.main()
