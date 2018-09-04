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
import collections
import itertools

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


class ConfigTestCase(unittest.TestCase):
    """Tests of simple Config"""

    def testBadConfig(self):
        for badArg in ([], "file.fits"):
            with self.assertRaises(RuntimeError):
                Config(badArg)

    def testBasics(self):
        c = Config({1: 2, 3: 4, "key3": 6})
        pretty = c.ppprint()
        self.assertIn("key3", pretty)
        r = repr(c)
        self.assertIn("key3", r)
        regex = "^Config\(\{.*\}\)$"
        self.assertRegex(r, regex)
        s = str(c)
        self.assertIn("\n", s)
        self.assertNotRegex(s, regex)

    def testEscape(self):
        c = Config({"a": {"foo.bar": 1}, "b😂c": {"bar_baz": 2}})
        self.assertEqual(c[r".a.foo\.bar"], 1)
        self.assertEqual(c[":a:foo.bar"], 1)
        self.assertEqual(c[".b😂c.bar_baz"], 2)
        self.assertEqual(c["😂b\😂c😂bar_baz"], 2)
        self.assertEqual(c[r"\a\foo.bar"], 1)
        self.assertEqual(c["\ra\rfoo.bar"], 1)
        with self.assertRaises(ValueError):
            c[".a.foo\.bar\r"]

    def testOperators(self):
        c1 = Config({"a": {"b": 1}, "c": 2})
        c2 = c1.copy()
        self.assertEqual(c1, c2)
        c2[".a.b"] = 5
        self.assertNotEqual(c1, c2)

    def testUpdate(self):
        c = Config({"a": {"b": 1}})
        c.update({"a": {"c": 2}})
        self.assertEqual(c[".a.b"], 1)
        self.assertEqual(c[".a.c"], 2)
        c.update({"a": {"d": [3, 4]}})
        self.assertEqual(c[".a.d.0"], 3)
        c.update({"z": [5, 6, {"g": 2, "h": 3}]})
        self.assertEqual(c[".z.1"], 6)

        # This is detached from parent
        c2 = c[".z.2"]
        self.assertEqual(c2["g"], 2)
        c2.update({"h": 4, "j": 5})
        self.assertEqual(c2["h"], 4)
        self.assertNotIn(".z.2.j", c)
        self.assertNotEqual(c[".z.2.h"], 4)

        with self.assertRaises(RuntimeError):
            c.update([1, 2, 3])

    def testHierarchy(self):
        c = Config()

        # Simple dict
        c["a"] = {"z": 52, "x": "string"}
        self.assertIn(".a.z", c)
        self.assertEqual(c[".a.x"], "string")

        # Try different delimiters
        self.assertEqual(c["⇛a⇛z"], 52)
        self.assertEqual(c[("a", "z")], 52)
        self.assertEqual(c["a", "z"], 52)

        c[".b.new.thing1"] = "thing1"
        c[".b.new.thing2"] = "thing2"
        c[".b.new.thing3.supp"] = "supplemental"
        self.assertEqual(c[".b.new.thing1"], "thing1")
        tmp = c[".b.new"]
        self.assertEqual(tmp["thing2"], "thing2")
        self.assertEqual(c[".b.new.thing3.supp"], "supplemental")

        # Test that we can index into lists
        c[".a.b.c"] = [1, "7", 3, {"1": 4, "5": "Five"}, "hello"]
        self.assertIn(".a.b.c.3.5", c)
        self.assertNotIn(".a.b.c.10", c)
        self.assertNotIn(".a.b.c.10.d", c)
        self.assertEqual(c[".a.b.c.3.5"], "Five")
        # Is the value in the list?
        self.assertIn(".a.b.c.hello", c)
        self.assertNotIn(".a.b.c.hello.not", c)

        # And assign to an element in the list
        self.assertEqual(c[".a.b.c.1"], "7")
        c[".a.b.c.1"] = 8
        self.assertEqual(c[".a.b.c.1"], 8)
        self.assertIsInstance(c[".a.b.c"], collections.Sequence)

        # Test we do get lists back from asArray
        a = c.asArray(".a.b.c")
        self.assertIsInstance(a, list)

        # Is it the *same* list as in the config
        a.append("Sentinel")
        self.assertIn("Sentinel", c[".a.b.c"])
        self.assertIn(".a.b.c.Sentinel", c)

        # Test we always get a list
        for k in c.names():
            a = c.asArray(k)
            self.assertIsInstance(a, list)

        # Check we get the same top level keys
        self.assertEqual(set(c.names(topLevelOnly=True)), set(c.data.keys()))

        # Check that we can iterate through items
        for k, v in c.items():
            self.assertEqual(c[k], v)

        # Check that lists still work even if assigned a dict
        c = Config({"cls": "lsst.daf.butler",
                    "formatters": {"calexp.wcs": "{component}",
                                   "calexp": "{datasetType}"},
                    "datastores": [{"datastore": {"cls": "datastore1"}},
                                   {"datastore": {"cls": "datastore2"}}]})
        c[".datastores.1.datastore"] = {"cls": "datastore2modified"}
        self.assertEqual(c[".datastores.0.datastore.cls"], "datastore1")
        self.assertEqual(c[".datastores.1.datastore.cls"], "datastore2modified")
        self.assertIsInstance(c["datastores"], collections.Sequence)

        # Test that we can get all the listed names.
        # and also that they are marked as "in" the Config
        # Try delimited names and tuples
        for n in itertools.chain(c.names(), c.nameTuples()):
            val = c[n]
            self.assertIsNotNone(val)
            self.assertIn(n, c)

        names = c.names()
        nameTuples = c.nameTuples()
        self.assertEqual(len(names), len(nameTuples))
        self.assertGreater(len(names), 5)
        self.assertGreater(len(nameTuples), 5)

        with self.assertRaises(ValueError):
            names = c.names(delimiter=".")

        top = c.nameTuples(topLevelOnly=True)
        self.assertIsInstance(top[0], tuple)

        # Investigate a possible delimeter in a key
        c = Config({"formatters": {"calexp.wcs": 2, "calexp": 3}})
        self.assertEqual(c[":formatters:calexp.wcs"], 2)
        self.assertEqual(c[":formatters:calexp"], 3)
        for k, v in c["formatters"].items():
            self.assertEqual(c["formatters", k], v)

        # and now try again by forcing a "." delimiter
        c2 = c["formatters"]
        c2._D = "."
        for k, v in c2.items():
            self.assertEqual(c["formatters", k], v)


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
        self.assertEqual(c[".comp1.item1"], 58)
        self.assertEqual(c[".comp2.comp.item1"], 1)
        self.assertEqual(c[".comp3.1.comp.item1"], "posix")
        self.assertEqual(c[".comp4.0.comp.item1"], "posix")
        self.assertEqual(c[".comp4.1.comp.item1"], 1)
        self.assertEqual(c[".comp5.comp6.comp.item1"], "posix")

        # Test a specific name and then test that all
        # returned names are "in" the config.
        names = c.names()
        self.assertIn(c._D.join(("", "comp3", "1", "comp", "item1")), names)
        for n in names:
            self.assertIn(n, c)

        # Test that override delimiter works
        delimiter = "-"
        names = c.names(delimiter=delimiter)
        self.assertIn(delimiter.join(("", "comp3", "1", "comp", "item1")), names)


if __name__ == "__main__":
    unittest.main()
