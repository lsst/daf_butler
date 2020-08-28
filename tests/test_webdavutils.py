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
import requests
import responses


from lsst.daf.butler.core.webdavutils import (folderExists, webdavCheckFileExists,
                                              _getFileURL, webdavDeleteFile, isWebdavEndpoint)
from lsst.daf.butler import Location, ButlerURI


class WebdavUtilsTestCase(unittest.TestCase):
    """Test for the Webdav related utilities.
    """
    session = requests.Session()
    serverRoot = "www.lsstwithwebdav.orgx"
    wrongRoot = "www.lsstwithoutwebdav.org"
    existingfolderName = "testFolder"
    notExistingfolderName = "testFolder_not_exist"
    existingfileName = "testFileName"
    notExistingfileName = "testFileName_not_exist"

    def setUp(self):
        # Used by folderExists()
        responses.add(responses.HEAD, f"https://{self.serverRoot}/{self.existingfolderName}", status=200)
        responses.add(responses.HEAD, f"https://{self.serverRoot}/{self.notExistingfolderName}", status=404)

        # Used by webdavCheckFileExists()
        responses.add(responses.HEAD,
                      f"https://{self.serverRoot}/{self.existingfolderName}/{self.existingfileName}",
                      status=200, headers={"Content-Length": "1024"})
        responses.add(responses.HEAD,
                      f"https://{self.serverRoot}/{self.existingfolderName}/{self.notExistingfileName}",
                      status=404)

        # Used by webdavDeleteFile()
        responses.add(responses.DELETE,
                      f"https://{self.serverRoot}/{self.existingfolderName}/{self.existingfileName}",
                      status=200)
        responses.add(responses.DELETE,
                      f"https://{self.serverRoot}/{self.existingfolderName}/{self.notExistingfileName}",
                      status=404)

        # Used by isWebdavEndpoint()
        responses.add(responses.OPTIONS, f"https://{self.serverRoot}", status=200, headers={"DAV": "1,2,3"})
        responses.add(responses.OPTIONS, f"https://{self.wrongRoot}", status=200)

    @responses.activate
    def testFolderExists(self):

        self.assertTrue(folderExists(f"https://{self.serverRoot}/{self.existingfolderName}",
                        session=self.session))
        self.assertFalse(folderExists(f"https://{self.serverRoot}/{self.notExistingfolderName}",
                         session=self.session))

    @responses.activate
    def testWebdavCheckFileExists(self):

        self.assertTrue(webdavCheckFileExists(
                        f"https://{self.serverRoot}/{self.existingfolderName}/{self.existingfileName}",
                        session=self.session)[0])
        self.assertFalse(webdavCheckFileExists(
                         f"https://{self.serverRoot}/{self.existingfolderName}/{self.notExistingfileName}",
                         session=self.session)[0])

    @responses.activate
    def testWebdavDeleteFile(self):

        self.assertIsNone(webdavDeleteFile(
                          f"https://{self.serverRoot}/{self.existingfolderName}/{self.existingfileName}",
                          session=self.session))
        with self.assertRaises(FileNotFoundError):
            webdavDeleteFile(
                f"https://{self.serverRoot}/{self.existingfolderName}/{self.notExistingfileName}",
                session=self.session)

    @responses.activate
    def testIsWebdavEndpoint(self):

        self.assertTrue(isWebdavEndpoint(f"https://{self.serverRoot}"))
        self.assertFalse(isWebdavEndpoint(f"https://{self.wrongRoot}"))

    @responses.activate
    def testGetFileURL(self):

        s = f"https://{self.serverRoot}/{self.existingfolderName}/{self.existingfileName}"
        buri = ButlerURI(f"https://{self.serverRoot}/{self.existingfolderName}/{self.existingfileName}")
        loc = Location(f"https://{self.serverRoot}/", f"{self.existingfolderName}/{self.existingfileName}")

        self.assertEqual(_getFileURL(s), s)
        self.assertEqual(_getFileURL(buri), s)
        self.assertEqual(_getFileURL(loc), s)


if __name__ == "__main__":
    unittest.main()
