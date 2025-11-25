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

import json
import logging
import os
import sys
import tempfile
import unittest
from logging import FileHandler

from lsst.daf.butler import Butler, DatasetRef, DatasetType, FileDataset
from lsst.daf.butler.logging import (
    ButlerLogRecordHandler,
    ButlerLogRecords,
    JsonLogFormatter,
    _ButlerLogRecordsModelV1,
)
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class ButlerLogRecordsFormatterTestCase(unittest.TestCase):
    """Test for ButlerLogRecords put/get."""

    def setUp(self):
        self.root = makeTestTempDir(TESTDIR)
        Butler.makeRepo(self.root)

        self.run = "testrun"
        self.butler = Butler.from_config(self.root, run=self.run)
        self.enterContext(self.butler)
        self.datasetType = DatasetType("test_logs", [], "ButlerLogRecords", universe=self.butler.dimensions)

        self.butler.registry.registerDatasetType(self.datasetType)

    def tearDown(self):
        removeTestTempDir(self.root)

    def testButlerLogRecordsFormatter(self):
        """Test that we can get and put records handled by
        ButlerLogRecordHandler.
        """
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

    def testButlerLogRecordsFormatterExtra(self):
        """Test that we can get and put records handled by
        ButlerLogRecordHandler with extra JSON data attached.
        """
        handler = ButlerLogRecordHandler()

        log = logging.getLogger(self.id())
        log.setLevel(logging.INFO)
        log.addHandler(handler)

        log.info("An INFO message")
        log.debug("A DEBUG message")
        log.warning("A WARNING message")

        extra_data = {"extra1": 1, "extra2": 2}
        handler.records.extra = extra_data
        ref = self.butler.put(handler.records, self.datasetType)
        records = self.butler.get(ref)

        self.assertEqual(records, handler.records)
        self.assertEqual(len(records), 2)

    def testButlerLogRecordsFormatterEmptyExtra(self):
        """Test that we can store extra information even with no log
        records written.
        """
        handler = ButlerLogRecordHandler()

        log = logging.getLogger(self.id())
        log.setLevel(logging.INFO)
        log.addHandler(handler)

        extra_data = {"extra1": 1, "extra2": 2}
        handler.records.extra = extra_data
        ref = self.butler.put(handler.records, self.datasetType)
        records = self.butler.get(ref)

        self.assertEqual(records, handler.records)
        self.assertEqual(len(records), 0)

    @unittest.skipIf(
        sys.version_info < (3, 12, 0),
        "This test requires NamedTemporaryFile behavior not available in older Python versions.",
    )
    def testButlerLogRecordsFormatterV1(self):
        """Test that we can read log records stored via the old V1 format
        (just a JSON list).
        """
        handler = ButlerLogRecordHandler()

        log = logging.getLogger(self.id())
        log.setLevel(logging.INFO)
        log.addHandler(handler)

        log.info("An INFO message")
        log.debug("A DEBUG message")
        log.warning("A WARNING message")

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", prefix="butler-log-", delete_on_close=False
        ) as tmp:
            # We can't use butler.put since that always writes the new format,
            # so we write manually and ingest.
            tmp.write(
                _ButlerLogRecordsModelV1(root=handler.records._records).model_dump_json(
                    exclude_unset=True, exclude_defaults=True
                )
            )
            tmp.close()
            ref = DatasetRef(self.datasetType, dataId={}, run=self.run)
            dataset = FileDataset(path=os.path.abspath(tmp.name), refs=ref)
            self.butler.ingest(dataset, transfer="move")

        records = self.butler.get(ref)
        self.assertEqual(records, handler.records)
        self.assertEqual(len(records), 2)

    @unittest.skipIf(
        sys.version_info < (3, 12, 0),
        "This test requires NamedTemporaryFile behavior not available in older Python versions.",
    )
    def testJsonLogRecordsFormatter(self):
        """Test that externally created JSON format stream files work."""
        log = logging.getLogger(self.id())
        log.setLevel(logging.INFO)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", prefix="butler-log-") as tmp:
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

    @unittest.skipIf(
        sys.version_info < (3, 12, 0),
        "This test requires NamedTemporaryFile behavior not available in older Python versions.",
    )
    def testJsonLogRecordsFormatterExtra(self):
        """Test that externally created JSON format stream files work with
        extra JSON data appended.
        """
        log = logging.getLogger(self.id())
        log.setLevel(logging.INFO)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", prefix="butler-log-") as tmp:
            handler = FileHandler(tmp.name)
            handler.setFormatter(JsonLogFormatter())
            log.addHandler(handler)

            log.info("An INFO message")
            log.debug("A DEBUG message")
            log.warning("A WARNING message")

            handler.close()
            extra_data = {"extra1": 1, "extra2": 2}
            with open(tmp.name, "a") as stream:
                ButlerLogRecords.write_streaming_extra(stream, json.dumps(extra_data))

            # Now ingest the file.
            ref = DatasetRef(self.datasetType, dataId={}, run=self.run)
            dataset = FileDataset(path=tmp.name, refs=ref)
            self.butler.ingest(dataset, transfer="move")

            records: ButlerLogRecords = self.butler.get(ref)
            self.assertEqual(len(records), 2)
            self.assertEqual(records.extra, extra_data)

    @unittest.skipIf(
        sys.version_info < (3, 12, 0),
        "This test requires NamedTemporaryFile behavior not available in older Python versions.",
    )
    def testJsonLogRecordsFormatterExtraEmpty(self):
        """Test that externally created JSON format stream files work with
        extra JSON data appended even if no log messages written.
        """
        log = logging.getLogger(self.id())
        log.setLevel(logging.INFO)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", prefix="butler-log-") as tmp:
            handler = FileHandler(tmp.name)
            handler.setFormatter(JsonLogFormatter())
            log.addHandler(handler)

            handler.close()
            extra_data = {"extra1": 1, "extra2": 2}
            with open(tmp.name, "a") as stream:
                ButlerLogRecords.write_streaming_extra(stream, json.dumps(extra_data))

            # Now ingest the file.
            ref = DatasetRef(self.datasetType, dataId={}, run=self.run)
            dataset = FileDataset(path=tmp.name, refs=ref)
            self.butler.ingest(dataset, transfer="move")

            records: ButlerLogRecords = self.butler.get(ref)
            self.assertEqual(len(records), 0)
            self.assertEqual(records.extra, extra_data)


if __name__ == "__main__":
    unittest.main()
