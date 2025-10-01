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

from __future__ import annotations

__all__ = ("generate_uuidv7",)

import time
from uuid import UUID, uuid4


def generate_uuidv7() -> UUID:
    """Generate a v7 UUID compliant with IETF RFC-9562.

    Returns
    -------
    uuid : `uuid.UUID`
        Version 7 UUID.
    """
    # Python 3.14 will include an implementation of UUIDv7 that can replace
    # this, but at the time of writing 3.14 hasn't been released and we're
    # still supporting 3.12. There are a few libraries on PyPI with an
    # implementation of v7 UUIDs, but none of them look well-maintained.
    #
    # This is the format of a v7 UUID:
    #  0                   1                   2                   3
    #  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    # |                           unix_ts_ms                          |
    # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    # |          unix_ts_ms           |  ver  |       rand_a          |
    # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    # |var|                        rand_b                             |
    # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    # |                            rand_b                             |
    # +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    #
    # It's basically identical to a UUID v4, but with the top 6 bytes
    # replaced with a millisecond UNIX timestamp.

    # Generate a UUIDv4 for the random portion of the data.
    # A little wasteful, but means we inherit the best practices from
    # the standard library for generating this random data.
    byte_data = bytearray(uuid4().bytes)

    # Replace high 6 bytes with millisecond UNIX timestamp.
    timestamp = time.time_ns() // 1_000_000
    timestamp_bytes = timestamp.to_bytes(length=6, byteorder="big")
    byte_data[0:6] = timestamp_bytes

    # Set 4-bit version field to 7.
    byte_data[6] &= 0x0F
    byte_data[6] |= 0x70

    return UUID(bytes=bytes(byte_data))
