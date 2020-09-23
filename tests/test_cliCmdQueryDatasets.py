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

import os
import unittest

from lsst.daf.butler import Butler, Datastore, FileDataset
from lsst.daf.butler.cli.butler import cli
from lsst.daf.butler.cli.utils import clickResultMsg, LogCliRunner


TESTDIR = os.path.abspath(os.path.dirname(__file__))


def _mock_export(refs, *,
                 directory=None,
                 transfer=None):
    """A mock of `Datastore.export` that satisifies the requirement that the
    refs passed in are included in the `FileDataset` objects returned.

    This can be used to construct a `Datastore` mock that can be used in
    repository export via::

        datastore = unittest.mock.Mock(spec=Datastore)
        datastore.export = _mock_export

    """
    for ref in refs:
        yield FileDataset(refs=[ref],
                          path="mock/path",
                          formatter="lsst.daf.butler.formatters.json.JsonFormatter")


def _mock_get(ref, parameters=None):
    """A mock of `Datastore.get` that just returns the integer dataset ID value
    and parameters it was given.
    """
    return (ref.id, parameters)


def _splitRows(text):
    """Transform a text table into a list of rows.

    Removes empty rows. Does not attempt to remove header or divider
    rows.

    Parameters
    ----------
    text : `str`
        Text that represents a text-based table.

    Returns
    -------
    `list` [`str`]
        The non-empty rows of the table.
    """
    return [line.strip() for line in text.splitlines() if line]


class QueryDatasetsScriptTest(unittest.TestCase):

    def setUp(self):
        self.repoName = "repo"
        self.runner = LogCliRunner()

    def makeTestRepo(self):
        # create the repo:
        result = self.runner.invoke(cli, ["create", self.repoName])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        # add datasets to the registry, mock the datastore:
        with unittest.mock.patch.object(Datastore, "fromConfig", spec=Datastore.fromConfig):
            butler = Butler(self.repoName, writeable=True)
            butler.datastore.export = _mock_export
            butler.datastore.get = _mock_get
        butler.import_(filename=os.path.join(TESTDIR, "data", "registry", "base.yaml"))
        butler.import_(filename=os.path.join(TESTDIR, "data", "registry", "datasets.yaml"))

    def testQueryDatasets(self):
        with self.runner.isolated_filesystem():
            self.makeTestRepo()

            # verify that query-datasets now returns expected output:
            result = self.runner.invoke(cli, ["query-datasets", self.repoName])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expected = """type    run      id instrument detector physical_filter
                        ---- ---------- --- ---------- -------- ---------------
                        flat imported_g   4       Cam1        2          Cam1-G
                        flat imported_g   5       Cam1        3          Cam1-G
                        flat imported_g   6       Cam1        4          Cam1-G
                        flat imported_r  10       Cam1        1         Cam1-R1
                        flat imported_r  11       Cam1        2         Cam1-R1
                        flat imported_r  12       Cam1        3         Cam1-R2
                        flat imported_r  13       Cam1        4         Cam1-R2

                        type    run      id instrument detector
                        ---- ---------- --- ---------- --------
                        bias imported_g   1       Cam1        1
                        bias imported_g   2       Cam1        2
                        bias imported_g   3       Cam1        3
                        bias imported_r   7       Cam1        2
                        bias imported_r   8       Cam1        3
                        bias imported_r   9       Cam1        4"""
            self.assertEqual(_splitRows(expected), _splitRows(result.output))

            # verify the --where option
            result = self.runner.invoke(cli, ["query-datasets", self.repoName, "flat",
                                              "--where", "detector=2"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expected = """type    run      id instrument detector physical_filter
                          ---- ---------- --- ---------- -------- ---------------
                          flat imported_g   4       Cam1        2          Cam1-G
                          flat imported_r  11       Cam1        2         Cam1-R1"""
            self.assertEqual(_splitRows(expected), _splitRows(result.output))

            # verify the --collections option
            result = self.runner.invoke(cli, ["query-datasets", self.repoName, "bias",
                                              "--collections", "imported_g,imported_r"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expected = """type    run      id instrument detector
                          ---- ---------- --- ---------- --------
                          bias imported_g   1       Cam1        1
                          bias imported_g   2       Cam1        2
                          bias imported_g   3       Cam1        3
                          bias imported_r   7       Cam1        2
                          bias imported_r   8       Cam1        3
                          bias imported_r   9       Cam1        4"""
            self.assertEqual(_splitRows(expected), _splitRows(result.output))

            # verify the --deduplicate option
            result = self.runner.invoke(cli, ["query-datasets", self.repoName, "bias",
                                              "--collections", "imported_g,imported_r",
                                              "--deduplicate"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expected = """type    run      id instrument detector
                          ---- ---------- --- ---------- --------
                          bias imported_g   1       Cam1        1
                          bias imported_g   2       Cam1        2
                          bias imported_g   3       Cam1        3
                          bias imported_r   9       Cam1        4"""
            self.assertEqual(_splitRows(expected), _splitRows(result.output))

            # verify the --components option
            result = self.runner.invoke(cli, ["query-datasets", self.repoName,
                                              "--components", "ALL"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            # The output for ALL is pretty long, instead of testing for the
            # exact contents, just look for the expected number of rows
            # (including header and separator rows).
            self.assertEqual(len(_splitRows(result.output)), 306)


if __name__ == "__main__":
    unittest.main()
