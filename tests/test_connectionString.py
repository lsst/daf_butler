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

"""Tests for ConnectionStringBuilder.
"""

import unittest
import os
import os.path
import stat

from lsst.daf.butler.core.registry import RegistryConfig
from lsst.daf.butler.core.connectionString import ConnectionStringFactory

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class ConnectionStringBuilderTestCase(unittest.TestCase):
    """Tests for ConnectionStringBuilder."""
    configDir = os.path.join(TESTDIR, "config/basic/connectionStringConfs/")
    configFiles = os.listdir(configDir)
    credentialsFile = os.path.join(TESTDIR, "testDbAuth.paf")

    def setUp(self):
        os.chmod(self.credentialsFile, ~stat.S_IRWXG & ~stat.S_IRWXO)

    def testBuilder(self):
        """Tests ConnectionStringBuilder builds correct connection strings.
        """
        regConfigs = [RegistryConfig(os.path.join(self.configDir, name)) for name in self.configFiles]

        conStrFactory = ConnectionStringFactory(self.credentialsFile)
        for regConf, fileName in zip(regConfigs, self.configFiles):
            conStr = conStrFactory.fromConfig(regConf)
            with self.subTest(confFile=fileName):
                self.assertEqual(str(conStr), regConf['expected'],
                                 "test connection string built from config")

    def testRelVsAbsPath(self):
        """Tests that relative and absolute paths are preserved."""
        regConf = RegistryConfig(os.path.join(self.configDir, 'conf1.yaml'))
        regConf['db'] = 'sqlite:///relative/path/conf1.sqlite3'

        conStrFactory = ConnectionStringFactory(self.credentialsFile)
        conStr = conStrFactory.fromConfig(regConf)
        self.assertEqual(str(conStr), 'sqlite:///relative/path/conf1.sqlite3')
