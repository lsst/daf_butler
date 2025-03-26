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

from __future__ import annotations

import unittest
import uuid

from astropy.time import Time

from lsst.daf.butler import Timespan
from lsst.daf.butler import column_spec as cs
from lsst.sphgeom import Mq3cPixelization, Region


class ColumnSpecTestCase(unittest.TestCase):
    """Tests for the ColumnSpec classes."""

    def setUp(self) -> None:
        self.specs = {
            int: cs.IntColumnSpec(name="i"),
            float: cs.FloatColumnSpec(name="f"),
            str: cs.StringColumnSpec(name="s", length=16),
            bytes: cs.HashColumnSpec(name="h", nbytes=16),
            bool: cs.BoolColumnSpec(name="b"),
            uuid.UUID: cs.UUIDColumnSpec(name="u"),
            Region: cs.RegionColumnSpec(name="r"),
            Timespan: cs.TimespanColumnSpec(name="t"),
            Time: cs.DateTimeColumnSpec(name="d"),
        }
        pix = Mq3cPixelization(10)
        self.data = [
            {
                "i": 1,
                "f": 0.5,
                "s": "foo",
                "h": uuid.uuid4().bytes,
                "b": True,
                "u": uuid.uuid4(),
                "r": pix.pixel(12058870),
                "t": Timespan(
                    Time("2021-09-09T03:00:00", format="isot", scale="tai"),
                    Time("2021-09-09T03:01:00", format="isot", scale="tai"),
                ),
                "d": Time("2021-09-09T03:00:30", format="isot", scale="tai"),
            },
            {
                "i": 2,
                "f": 0.25,
                "s": "bar",
                "h": uuid.uuid4().bytes,
                "b": False,
                "u": uuid.uuid4(),
                "r": pix.pixel(12058871),
                "t": Timespan(
                    Time("2021-09-09T03:02:00", format="isot", scale="tai"),
                    Time("2021-09-09T03:03:00", format="isot", scale="tai"),
                ),
                "d": Time("2021-09-09T03:02:30", format="isot", scale="tai"),
            },
        ]

    def test_serialize_tuple(self) -> None:
        """Test that we can use ColumnSpec to create validators and serializers
        for tuples with a specific sequence of types.
        """
        type_adapter = cs.make_tuple_type_adapter(self.specs.values())
        py0 = type_adapter.dump_python(tuple(self.data[0].values()))
        json0 = type_adapter.dump_json(tuple(self.data[0].values()))
        self.assertEqual(type_adapter.validate_python(py0), tuple(self.data[0].values()))
        self.assertEqual(type_adapter.validate_json(json0), tuple(self.data[0].values()))
