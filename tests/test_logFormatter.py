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

"""Tests for ButlerLogRecordsFormatter."""

import logging
import os
import tempfile
import unittest
from logging import FileHandler

from lsst.daf.butler import Butler, DatasetRef, DatasetType, FileDataset
from lsst.daf.butler.logging import ButlerLogRecordHandler, JsonLogFormatter
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class ButlerLogRecordsFormatterTestCase(unittest.TestCase):
    """Test for ButlerLogRecords put/get."""

    def setUp(self):
        self.root = makeTestTempDir(TESTDIR)
        Butler.makeRepo(self.root)

        self.run = "testrun"
        self.butler = Butler.from_config(self.root, run=self.run)
        self.datasetType = DatasetType("test_logs", [], "ButlerLogRecords", universe=self.butler.dimensions)

        self.butler.registry.registerDatasetType(self.datasetType)

    def tearDown(self):
        removeTestTempDir(self.root)

    def testButlerLogRecordsFormatter(self):
        handler = ButlerLogRecordHandler()

        log = logging.getLogger(self.id())
        log.setLevel(logging.INFO)
        log.addHandler(handler)

        log.info("An INFO message")
        log.debug("A DEBUG message")
        log.warning("A WARNING message")

        ref = self.butler.put(handler.records, self.datasetType)
        records = self.butler.get(ref)

        self.assertEqual(records, handler.records)
        self.assertEqual(len(records), 2)

    def testJsonLogRecordsFormatter(self):
        """Test that externally created JSON format stream files work."""
        log = logging.getLogger(self.id())
        log.setLevel(logging.INFO)

        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", prefix="butler-log-", delete=False)

        handler = FileHandler(tmp.name)
        handler.setFormatter(JsonLogFormatter())
        log.addHandler(handler)

        log.info("An INFO message")
        log.debug("A DEBUG message")
        log.warning("A WARNING message")

        handler.close()

        # Now ingest the file.
        ref = DatasetRef(self.datasetType, dataId={}, run=self.run)
        dataset = FileDataset(path=tmp.name, refs=ref)
        self.butler.ingest(dataset, transfer="move")

        records = self.butler.get(ref)
        self.assertEqual(len(records), 2)


if __name__ == "__main__":
    unittest.main()
