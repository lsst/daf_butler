# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Tests for DirectButler._query with SQLite.
"""

from __future__ import annotations

import os
import unittest

from lsst.daf.butler import Butler
from lsst.daf.butler.tests.butler_queries import ButlerQueryTests

try:
    from lsst.daf.butler.tests.server import create_test_server

    reason_text = ""
except ImportError as e:
    create_test_server = None
    reason_text = str(e)

TESTDIR = os.path.abspath(os.path.dirname(__file__))


@unittest.skipIf(create_test_server is None, f"Server dependencies not installed: {reason_text}")
class RemoteButlerQueryTests(ButlerQueryTests, unittest.TestCase):
    """Test query system using client/server butler."""

    data_dir = os.path.join(TESTDIR, "data/registry")

    def make_butler(self, *args: str) -> Butler:
        server_instance = self.enterContext(create_test_server(TESTDIR))
        butler = server_instance.hybrid_butler

        for arg in args:
            self.load_data(butler, arg)

        return butler


if __name__ == "__main__":
    unittest.main()
