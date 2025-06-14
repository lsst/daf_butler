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

"""Tests for ConnectionStringBuilder."""

import glob
import os
import os.path
import shutil
import tempfile
import unittest

from lsst.daf.butler.registry import RegistryConfig
from lsst.daf.butler.registry.connectionString import ConnectionStringFactory
from lsst.utils.db_auth import DbAuthError

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class ConnectionStringBuilderTestCase(unittest.TestCase):
    """Tests for ConnectionStringBuilder."""

    configDir = os.path.join(TESTDIR, "config", "dbAuth")
    configFiles = glob.glob(os.path.join(configDir, "registryConf*"))
    credentialsFile = os.path.join(configDir, "db-auth.yaml")

    def setUp(self):
        temporaryCredentialsFile = self.enterContext(tempfile.NamedTemporaryFile()).name
        shutil.copyfile(self.credentialsFile, temporaryCredentialsFile)
        os.chmod(temporaryCredentialsFile, 0o600)
        self.db_auth_path = temporaryCredentialsFile

    def testBuilder(self):
        """Tests ConnectionStringFactory returns correct connection strings."""
        regConfigs = [RegistryConfig(os.path.join(self.configDir, name)) for name in self.configFiles]

        for regConf, fileName in zip(regConfigs, self.configFiles, strict=True):
            conStr = ConnectionStringFactory.fromConfig(regConf, db_auth_path=self.db_auth_path)
            with self.subTest(confFile=repr(fileName)):
                self.assertEqual(
                    conStr.render_as_string(hide_password=False),
                    regConf["expected"],
                    "test connection string built from config",
                )

    def testRelVsAbsPath(self):
        """Tests that relative and absolute paths are preserved."""
        regConf = RegistryConfig(os.path.join(self.configDir, "registryConf1.yaml"))
        regConf["db"] = "sqlite:///relative/path/conf1.sqlite3"
        conStr = ConnectionStringFactory.fromConfig(regConf, db_auth_path=self.db_auth_path)
        self.assertEqual(str(conStr), "sqlite:///relative/path/conf1.sqlite3")

    def testRaises(self):
        """Test that DbAuthError propagates through the class."""
        # Create a bad yaml file that triggers a parsing error.
        # It will be created with the correct 600 permissions.
        with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w") as temp_config:
            print("[", file=temp_config, flush=True)
            regConf = RegistryConfig(os.path.join(self.configDir, "registryConf2.yaml"))
            with self.assertRaises(DbAuthError):
                ConnectionStringFactory.fromConfig(regConf, db_auth_path=temp_config.name)


if __name__ == "__main__":
    unittest.main()
