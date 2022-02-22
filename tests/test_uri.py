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

from lsst.daf.butler import ButlerURI


class ButlerURITestCase(unittest.TestCase):
    """Test ButlerURI compatibility.

    Basic tests to show that `lsst.daf.butler.ButlerURI` compatibility
    import still works. Can be removed when deprecation period ends.
    """

    def test_uri(self):
        for scheme in ("file", "s3", "https", "resource"):
            uri_str = f"{scheme}://netloc/a/b/c.txt"
            with self.assertWarns(FutureWarning):  # Capture the deprecation warning
                uri = ButlerURI(uri_str)
            self.assertIsInstance(uri, ButlerURI)
            self.assertEqual(uri.scheme, scheme)

        with self.assertWarns(FutureWarning):  # Capture the deprecation warning
            schemeless = ButlerURI("b/c.txt", forceAbsolute=False)
        self.assertIsInstance(schemeless, ButlerURI)
        self.assertFalse(schemeless.scheme)


if __name__ == "__main__":
    unittest.main()
