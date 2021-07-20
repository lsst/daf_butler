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

"""Tests for ButlerLogRecordsFormatter.
"""

import unittest
import os
import logging

from lsst.daf.butler import Butler, DatasetType, ButlerLogRecordHandler
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class ButlerLogRecordsFormatterTestCase(unittest.TestCase):
    """Test for ButlerLogRecords put/get."""

    def setUp(self):
        self.root = makeTestTempDir(TESTDIR)
        Butler.makeRepo(self.root)

    def tearDown(self):
        removeTestTempDir(self.root)

    def testButlerLogRecordsFormatter(self):
        butler = Butler(self.root, run="testrun")
        datasetType = DatasetType("test_logs", [], "ButlerLogRecords",
                                  universe=butler.registry.dimensions)

        butler.registry.registerDatasetType(datasetType)

        handler = ButlerLogRecordHandler()

        log = logging.getLogger(self.id())
        log.setLevel(logging.INFO)
        log.addHandler(handler)

        log.info("An INFO message")
        log.debug("A DEBUG message")
        log.warning("A WARNING message")

        ref = butler.put(handler.records, datasetType)
        records = butler.getDirect(ref)

        self.assertEqual(records, handler.records)
        self.assertEqual(len(records), 2)


if __name__ == "__main__":
    unittest.main()
