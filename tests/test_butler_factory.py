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

import tempfile
import unittest

from lsst.daf.butler import LabeledButlerFactory
from lsst.daf.butler.direct_butler import DirectButler
from lsst.daf.butler.tests import makeTestRepo
from lsst.daf.butler.tests.utils import mock_env


class ButlerFactoryTestCase(unittest.TestCase):
    """Test LabeledButlerFactory."""

    # RemoteButler cases aren't tested in this file because RemoteButler's
    # dependencies aren't available in rubin-env.  RemoteButler gets covered in
    # test_server.py.

    @classmethod
    def setUpClass(cls):
        repo_dir = cls.enterClassContext(tempfile.TemporaryDirectory())
        makeTestRepo(repo_dir)
        cls.config_file_uri = f"file://{repo_dir}"

    def test_factory_via_global_repository_index(self):
        with tempfile.NamedTemporaryFile(suffix=".yaml") as index_file:
            index_file.write(f"test_repo: {self.config_file_uri}\n".encode())
            index_file.flush()
            with mock_env({"DAF_BUTLER_REPOSITORY_INDEX": index_file.name}):
                factory = LabeledButlerFactory()
                self._test_factory(factory)

    def test_factory_via_custom_index(self):
        factory = LabeledButlerFactory({"test_repo": self.config_file_uri})
        self._test_factory(factory)

    def _test_factory(self, factory: LabeledButlerFactory) -> None:
        butler = factory.create_butler(label="test_repo", access_token=None)
        self.assertIsInstance(butler, DirectButler)
        # This identical second call covers a read from the cache.
        butler2 = factory.create_butler(label="test_repo", access_token=None)
        self.assertIsInstance(butler2, DirectButler)
        with self.assertRaises(KeyError):
            factory.create_butler(label="unknown", access_token=None)


if __name__ == "__main__":
    unittest.main()
