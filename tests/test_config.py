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
import os
import contextlib

from lsst.daf.butler import ConfigSubset, Config


@contextlib.contextmanager
def modified_environment(**environ):
    """
    Temporarily set environment variables.

    >>> with modified_environment(DAF_BUTLER_DIR="/somewhere"):
    ...    os.environ["DAF_BUTLER_DIR"] == "/somewhere"
    True

    >>> "DAF_BUTLER_DIR" != "/somewhere"
    True

    Parameters
    ----------
    environ : `dict`
        Key value pairs of environment variables to temporarily set.
    """
    old_environ = dict(os.environ)
    os.environ.update(environ)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


class ExampleWithConfigFileReference:
    defaultConfigFile = "viacls.yaml"


class ExampleWithConfigFileReference2:
    defaultConfigFile = "viacls2.yaml"


class ConfigTest(ConfigSubset):
    component = "comp"
    requiredKeys = ("item1", "item2")
    defaultConfigFile = "testconfig.yaml"


class ConfigTestEmpty(ConfigTest):
    defaultConfigFile = "testconfig_empty.yaml"
    requiredKeys = ()


class ConfigTestButlerDir(ConfigTest):
    defaultConfigFile = "testConfigs/testconfig.yaml"


class ConfigTestNoDefaults(ConfigTest):
    defaultConfigFile = None
    requiredKeys = ()


class ConfigTestAbsPath(ConfigTest):
    defaultConfigFile = None
    requiredKeys = ()


class ConfigTestCls(ConfigTest):
    defaultConfigFile = "withcls.yaml"


class ConfigSubsetTestCase(unittest.TestCase):
    """Tests for ConfigSubset
    """

    def setUp(self):
        self.testDir = os.path.abspath(os.path.dirname(__file__))
        self.configDir = os.path.join(self.testDir, "config", "testConfigs")
        self.configDir2 = os.path.join(self.testDir, "config", "testConfigs", "test2")

    def testEmpty(self):
        """Ensure that we can read an empty file."""
        c = ConfigTestEmpty(searchPaths=(self.configDir,))
        self.assertIsInstance(c, ConfigSubset)

    def testDefaults(self):
        """Read of defaults"""

        # Supply the search path explicitly
        c = ConfigTest(searchPaths=(self.configDir,))
        self.assertIsInstance(c, ConfigSubset)
        self.assertIn("item3", c)
        self.assertEqual(c["item3"], 3)

        # Use environment
        with modified_environment(DAF_BUTLER_CONFIG_PATH=self.configDir):
            c = ConfigTest()
            self.assertIsInstance(c, ConfigSubset)
            self.assertEqual(c["item3"], 3)

        # No default so this should fail
        with self.assertRaises(KeyError):
            c = ConfigTest()

    def testButlerDir(self):
        """Test that DAF_BUTLER_DIR is used to locate files."""
        # with modified_environment(DAF_BUTLER_DIR=self.testDir):
        #     c = ConfigTestButlerDir()
        #     self.assertIn("item3", c)

        # Again with a search path
        with modified_environment(DAF_BUTLER_DIR=self.testDir,
                                  DAF_BUTLER_CONFIG_PATH=self.configDir2):
            c = ConfigTestButlerDir()
            self.assertIn("item3", c)
            self.assertEqual(c["item3"], "override")
            self.assertEqual(c["item4"], "new")

    def testExternalOverride(self):
        """Ensure that external values win"""
        c = ConfigTest({"item3": "newval"}, searchPaths=(self.configDir,))
        self.assertIn("item3", c)
        self.assertEqual(c["item3"], "newval")

    def testSearchPaths(self):
        """Two search paths"""
        c = ConfigTest(searchPaths=(self.configDir2, self.configDir))
        self.assertIsInstance(c, ConfigSubset)
        self.assertIn("item3", c)
        self.assertEqual(c["item3"], "override")
        self.assertEqual(c["item4"], "new")

        c = ConfigTest(searchPaths=(self.configDir, self.configDir2))
        self.assertIsInstance(c, ConfigSubset)
        self.assertIn("item3", c)
        self.assertEqual(c["item3"], 3)
        self.assertEqual(c["item4"], "new")

    def testExternalHierarchy(self):
        """Test that we can provide external config parameters in hierarchy"""
        c = ConfigTest({"comp": {"item1": 6, "item2": "a", "a": "b",
                                 "item3": 7}, "item4": 8})
        self.assertIn("a", c)
        self.assertEqual(c["a"], "b")
        self.assertNotIn("item4", c)

    def testNoDefaults(self):
        """Ensure that defaults can be turned off."""

        # Mandatory keys but no defaults
        c = ConfigTest({"item1": "a", "item2": "b", "item6": 6})
        self.assertEqual(len(c.filesRead), 0)
        self.assertIn("item1", c)
        self.assertEqual(c["item6"], 6)

        c = ConfigTestNoDefaults()
        self.assertEqual(len(c.filesRead), 0)

    def testAbsPath(self):
        """Read default config from an absolute path"""
        # Force the path to be absolute in the class
        ConfigTestAbsPath.defaultConfigFile = os.path.join(self.configDir, "abspath.yaml")
        c = ConfigTestAbsPath()
        self.assertEqual(c["item11"], "eleventh")
        self.assertEqual(len(c.filesRead), 1)

        # Now specify the normal config file with an absolute path
        ConfigTestAbsPath.defaultConfigFile = os.path.join(self.configDir, ConfigTest.defaultConfigFile)
        c = ConfigTestAbsPath()
        self.assertEqual(c["item11"], 11)
        self.assertEqual(len(c.filesRead), 1)

        # and a search path that will also include the file
        c = ConfigTestAbsPath(searchPaths=(self.configDir, self.configDir2,))
        self.assertEqual(c["item11"], 11)
        self.assertEqual(len(c.filesRead), 1)

        # Same as above but this time with relative path and two search paths
        # to ensure the count changes
        ConfigTestAbsPath.defaultConfigFile = ConfigTest.defaultConfigFile
        c = ConfigTestAbsPath(searchPaths=(self.configDir, self.configDir2,))
        self.assertEqual(len(c.filesRead), 2)

        # Reset the class
        ConfigTestAbsPath.defaultConfigFile = None

    def testClassDerived(self):
        """Read config specified in class determined from config"""
        c = ConfigTestCls(searchPaths=(self.configDir,))
        self.assertEqual(c["item50"], 50)
        self.assertEqual(c["help"], "derived")

        # Same thing but additional search path
        c = ConfigTestCls(searchPaths=(self.configDir, self.configDir2))
        self.assertEqual(c["item50"], 50)
        self.assertEqual(c["help"], "derived")
        self.assertEqual(c["help2"], "second")

        # Same thing but reverse the two paths
        c = ConfigTestCls(searchPaths=(self.configDir2, self.configDir))
        self.assertEqual(c["item50"], 500)
        self.assertEqual(c["help"], "class")
        self.assertEqual(c["help2"], "second")
        self.assertEqual(c["help3"], "third")

    def testInclude(self):
        """Read a config that has an include directive"""
        c = Config(os.path.join(self.configDir, "testinclude.yaml"))
        print(c.ppprint())
        self.assertEqual(c["comp1.item1"], 58)
        self.assertEqual(c["comp2.comp.item1"], 1)
        self.assertEqual(c["comp3.1.comp.item1"], "posix")


if __name__ == "__main__":
    unittest.main()
