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

import unittest
import unittest.mock
from uuid import RFC_4122

from lsst.daf.butler._uuid import generate_uuidv7


class UUIDv7TestCase(unittest.TestCase):
    """Test generation of v7 UUIDs."""

    def test_generate_uuidv7(self) -> None:
        mock_timestamp_milliseconds = 1759355298139
        mock_timestamp_nanoseconds = (mock_timestamp_milliseconds * 1_000_000) + 123456
        with unittest.mock.patch("time.time_ns") as mock:
            mock.return_value = mock_timestamp_nanoseconds
            id = generate_uuidv7()
            self.assertEqual(id.version, 7)
            self.assertEqual(id.variant, RFC_4122)
            self.assertEqual(int.from_bytes(id.bytes[0:6], byteorder="big"), mock_timestamp_milliseconds)
            # The rest of the bytes in the ID are random, so just make sure
            # that two consecutive IDs are generating different bytes.
            second_id = generate_uuidv7()
            self.assertNotEqual(second_id.bytes[6:], id.bytes[6:])
            # But the top six bytes should be the same, because we mocked
            # the same timestamp tick for both ID generations.
            self.assertEqual(second_id.bytes[0:6], id.bytes[0:6])
