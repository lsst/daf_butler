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

import unittest
from datetime import datetime

from lsst.daf.butler.core.run import Run

"""Tests for Run.
"""


class RunTestCase(unittest.TestCase):
    """Test for Run.
    """

    def testConstructor(self):
        """Test of constructor.
        """
        collection = "ingest"
        environment = None
        pipeline = None
        # Base class arguments
        startTime = datetime(2018, 1, 1)
        endTime = datetime(2018, 1, 2)
        host = "localhost"
        run = Run(collection, environment, pipeline, startTime, endTime, host)
        self.assertEqual(run.collection, collection)
        self.assertEqual(run.environment, environment)
        self.assertEqual(run.pipeline, pipeline)
        self.assertIsNone(run.id)
        self.assertEqual(run.startTime, startTime)
        self.assertEqual(run.endTime, endTime)
        self.assertEqual(run.host, host)


if __name__ == "__main__":
    unittest.main()
