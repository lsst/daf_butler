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
from unittest.mock import MagicMock

try:
    import webdav3.client as wc
except ImportError:
    wc = None

from lsst.daf.butler.core.webdavutils import (folderExists, webdavCheckFileExists)
from lsst.daf.butler.core.location import Location, ButlerURI


@unittest.skipIf(not wc, "Warning: WebDav client module not found!")
class WebdavUtilsTestCase(unittest.TestCase):
    """Test for the Webdav related utilities.
    """
    serverURI = "www.lsst.org"
    existingfolderName = "testFolder"
    existingfileName = "testFileName"

    def folderDoesExist(*args, **kwargs):
        if args[1] == "testFolder":
            return True
        return False

    def fileDoesExist(*args, **kwargs):
        if args[1] == "testFolder/testFileName":
            return True
        return False

    def setUp(self):

        self.client = MagicMock()
        self.client.getWebdavClient.return_value = self.client

    def testFolderExists(self):

        self.client.check.side_effect = self.folderDoesExist
        self.assertTrue(folderExists(f"{self.existingfolderName}", self.client))
        self.assertFalse(folderExists(f"{self.existingfolderName}_no_exist", self.client))

    def testWebdavCheckFileExists(self):

        self.client.check.side_effect = self.fileDoesExist
        self.assertTrue(webdavCheckFileExists(f"{self.existingfolderName}/{self.existingfileName}",
                                              self.client)[0])
        self.assertFalse(webdavCheckFileExists(self.existingfolderName + "/"
                                               + self.existingfileName + "_NO_EXIST", self.client)[0])

        datastoreRootUri = f"https://{self.serverURI}/"
        uri = f"{self.existingfolderName}/{self.existingfileName}"

        buri = ButlerURI(f"http://{self.serverURI}/{uri}")
        location = Location(datastoreRootUri, f"{self.existingfolderName}/{self.existingfileName}")

        self.assertTrue(webdavCheckFileExists(client=self.client, path=buri)[0])
        # just to make sure the overloaded keyword works correctly
        self.assertTrue(webdavCheckFileExists(buri, self.client)[0])
        self.assertTrue(webdavCheckFileExists(location, self.client)[0])

        # make sure supplying strings resolves correctly too
        self.assertTrue(webdavCheckFileExists(uri, client=self.client)[0])


if __name__ == "__main__":
    unittest.main()
