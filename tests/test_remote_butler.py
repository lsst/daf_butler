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

from lsst.daf.butler import Butler
from pydantic import ValidationError

try:
    from lsst.daf.butler.remote_butler import RemoteButler
except ImportError:
    # httpx is not available in rubin-env yet, so skip these tests if it's not
    # available
    RemoteButler = None


@unittest.skipIf(RemoteButler is None, "httpx is not installed")
class RemoteButlerConfigTests(unittest.TestCase):
    """Test construction of RemoteButler via Butler()"""

    def test_instantiate_via_butler(self):
        butler = Butler(
            {
                "cls": "lsst.daf.butler.remote_butler.RemoteButler",
                "remote_butler": {"url": "https://validurl.example"},
            },
            collections=["collection1", "collection2"],
            run="collection2",
        )
        assert isinstance(butler, RemoteButler)
        self.assertEqual(butler.collections, ("collection1", "collection2"))
        self.assertEqual(butler.run, "collection2")

    def test_bad_config(self):
        with self.assertRaises(ValidationError):
            Butler({"cls": "lsst.daf.butler.remote_butler.RemoteButler", "remote_butler": {"url": "!"}})


if __name__ == "__main__":
    unittest.main()
