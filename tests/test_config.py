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

import collections
import contextlib
import itertools
import os
import unittest
from pathlib import Path

from lsst.daf.butler import Config, ConfigSubset
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


@contextlib.contextmanager
def modified_environment(**environ):
    """
    Temporarily set environment variables.

    >>> with modified_environment(DAF_BUTLER_CONFIG_PATHS="/somewhere"):
    ...    os.environ["DAF_BUTLER_CONFIG_PATHS"] == "/somewhere"
    True

    >>> "DAF_BUTLER_CONFIG_PATHS" != "/somewhere"
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


class ConfigTestPathlib(ConfigTest):
    defaultConfigFile = Path("testconfig.yaml")


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
        for badArg in (
            [],  # Bad argument
            __file__,  # Bad file extension for existing file
        ):
            with self.assertRaises(RuntimeError):
                Config(badArg)
        for badArg in (
            "file.fits",  # File that does not exist with bad extension
            "b/c/d/",  # Directory that does not exist
            "file.yaml",  # Good extension for missing file
        ):
            with self.assertRaises(FileNotFoundError):
                Config(badArg)

    def testBasics(self):
        c = Config({"1": 2, "3": 4, "key3": 6, "dict": {"a": 1, "b": 2}})
        pretty = c.ppprint()
        self.assertIn("key3", pretty)
        r = repr(c)
        self.assertIn("key3", r)
        regex = r"^Config\(\{.*\}\)$"
        self.assertRegex(r, regex)
        c2 = eval(r)
        self.assertIn("1", c)
        for n in c.names():
            self.assertEqual(c2[n], c[n])
        self.assertEqual(c, c2)
        s = str(c)
        self.assertIn("\n", s)
        self.assertNotRegex(s, regex)

        self.assertCountEqual(c.keys(), ["1", "3", "key3", "dict"])
        self.assertEqual(list(c), list(c.keys()))
        self.assertEqual(list(c.values()), [c[k] for k in c.keys()])
        self.assertEqual(list(c.items()), [(k, c[k]) for k in c.keys()])

        newKeys = ("key4", ".dict.q", ("dict", "r"), "5")
        oldKeys = ("key3", ".dict.a", ("dict", "b"), "3")
        remainingKey = "1"

        # Check get with existing key
        for k in oldKeys:
            self.assertEqual(c.get(k, "missing"), c[k])

        # Check get, pop with nonexistent key
        for k in newKeys:
            self.assertEqual(c.get(k, "missing"), "missing")
            self.assertEqual(c.pop(k, "missing"), "missing")

        # Check setdefault with existing key
        for k in oldKeys:
            c.setdefault(k, 8)
            self.assertNotEqual(c[k], 8)

        # Check setdefault with nonexistent key (mutates c, adding newKeys)
        for k in newKeys:
            c.setdefault(k, 8)
            self.assertEqual(c[k], 8)

        # Check pop with existing key (mutates c, removing newKeys)
        for k in newKeys:
            v = c[k]
            self.assertEqual(c.pop(k, "missing"), v)

        # Check deletion (mutates c, removing oldKeys)
        for k in ("key3", ".dict.a", ("dict", "b"), "3"):
            self.assertIn(k, c)
            del c[k]
            self.assertNotIn(k, c)

        # Check that `dict` still exists, but is now empty (then remove
        # it, mutatic c)
        self.assertIn("dict", c)
        del c["dict"]

        # Check popitem (mutates c, removing remainingKey)
        v = c[remainingKey]
        self.assertEqual(c.popitem(), (remainingKey, v))

        # Check that c is now empty
        self.assertFalse(c)

    def testDict(self):
        """Test toDict()"""
        c1 = Config({"a": {"b": 1}, "c": 2})
        self.assertIsInstance(c1["a"], Config)
        d1 = c1.toDict()
        self.assertIsInstance(d1["a"], dict)
        self.assertEqual(d1["a"], c1["a"])

        # Modifying one does not change the other
        d1["a"]["c"] = 2
        self.assertNotEqual(d1["a"], c1["a"])

    def assertSplit(self, answer, *args):
        """Helper function to compare string splitting"""
        for s in (answer, *args):
            split = Config._splitIntoKeys(s)
            self.assertEqual(split, answer)

    def testSplitting(self):
        """Test of the internal splitting API."""
        # Try lots of keys that will return the same answer
        answer = ["a", "b", "c", "d"]
        self.assertSplit(answer, ".a.b.c.d", ":a:b:c:d", "\ta\tb\tc\td", "\ra\rb\rc\rd")

        answer = ["a", "calexp.wcs", "b"]
        self.assertSplit(answer, r".a.calexp\.wcs.b", ":a:calexp.wcs:b")

        self.assertSplit(["a.b.c"])
        self.assertSplit(["a", r"b\.c"], r"_a_b\.c")

        # Escaping a backslash before a delimiter currently fails
        with self.assertRaises(ValueError):
            Config._splitIntoKeys(r".a.calexp\\.wcs.b")

        # The next two fail because internally \r is magic when escaping
        # a delimiter.
        with self.assertRaises(ValueError):
            Config._splitIntoKeys("\ra\rcalexp\\\rwcs\rb")

        with self.assertRaises(ValueError):
            Config._splitIntoKeys(".a.cal\rexp\\.wcs.b")

    def testEscape(self):
        c = Config({"a": {"foo.bar": 1}, "b😂c": {"bar_baz": 2}})
        self.assertEqual(c[r".a.foo\.bar"], 1)
        self.assertEqual(c[":a:foo.bar"], 1)
        self.assertEqual(c[".b😂c.bar_baz"], 2)
        self.assertEqual(c[r"😂b\😂c😂bar_baz"], 2)
        self.assertEqual(c[r"\a\foo.bar"], 1)
        self.assertEqual(c["\ra\rfoo.bar"], 1)
        with self.assertRaises(ValueError):
            c[".a.foo\\.bar\r"]

    def testOperators(self):
        c1 = Config({"a": {"b": 1}, "c": 2})
        c2 = c1.copy()
        self.assertEqual(c1, c2)
        self.assertIsInstance(c2, Config)
        c2[".a.b"] = 5
        self.assertNotEqual(c1, c2)

    def testMerge(self):
        c1 = Config({"a": 1, "c": 3})
        c2 = Config({"a": 4, "b": 2})
        c1.merge(c2)
        self.assertEqual(c1, {"a": 1, "b": 2, "c": 3})

        # Check that c2 was not changed
        self.assertEqual(c2, {"a": 4, "b": 2})

        # Repeat with a simple dict
        c1.merge({"b": 5, "d": 42})
        self.assertEqual(c1, {"a": 1, "b": 2, "c": 3, "d": 42})

        with self.assertRaises(TypeError):
            c1.merge([1, 2, 3])

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
        self.assertIsInstance(c[".a.b.c"], collections.abc.Sequence)

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
        self.assertEqual(set(c.names(topLevelOnly=True)), set(c._data.keys()))

        # Check that we can iterate through items
        for k, v in c.items():
            self.assertEqual(c[k], v)

        # Check that lists still work even if assigned a dict
        c = Config(
            {
                "cls": "lsst.daf.butler",
                "formatters": {"calexp.wcs": "{component}", "calexp": "{datasetType}"},
                "datastores": [{"datastore": {"cls": "datastore1"}}, {"datastore": {"cls": "datastore2"}}],
            }
        )
        c[".datastores.1.datastore"] = {"cls": "datastore2modified"}
        self.assertEqual(c[".datastores.0.datastore.cls"], "datastore1")
        self.assertEqual(c[".datastores.1.datastore.cls"], "datastore2modified")
        self.assertIsInstance(c["datastores"], collections.abc.Sequence)

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
        self.assertEqual(len(names), 11)
        self.assertEqual(len(nameTuples), 11)

        # Test that delimiter escaping works
        names = c.names(delimiter=".")
        for n in names:
            self.assertIn(n, c)
        self.assertIn(".formatters.calexp\\.wcs", names)

        # Use a name that includes the internal default delimiter
        # to test automatic adjustment of delimiter
        strangeKey = f"calexp{c._D}wcs"
        c["formatters", strangeKey] = "dynamic"
        names = c.names()
        self.assertIn(strangeKey, "-".join(names))
        self.assertFalse(names[0].startswith(c._D))
        for n in names:
            self.assertIn(n, c)

        top = c.nameTuples(topLevelOnly=True)
        self.assertIsInstance(top[0], tuple)

        # Investigate a possible delimeter in a key
        c = Config({"formatters": {"calexp.wcs": 2, "calexp": 3}})
        self.assertEqual(c[":formatters:calexp.wcs"], 2)
        self.assertEqual(c[":formatters:calexp"], 3)
        for k, v in c["formatters"].items():
            self.assertEqual(c["formatters", k], v)

        # Check internal delimiter inheritance
        c._D = "."
        c2 = c["formatters"]
        self.assertEqual(c._D, c2._D)  # Check that the child inherits
        self.assertNotEqual(c2._D, Config._D)

    def testSerializedString(self):
        """Test that we can create configs from strings"""

        serialized = {
            "yaml": """
testing: hello
formatters:
  calexp: 3""",
            "json": '{"testing": "hello", "formatters": {"calexp": 3}}',
        }

        for format, string in serialized.items():
            c = Config.fromString(string, format=format)
            self.assertEqual(c["formatters", "calexp"], 3)
            self.assertEqual(c["testing"], "hello")

        with self.assertRaises(ValueError):
            Config.fromString("", format="unknown")

        with self.assertRaises(ValueError):
            Config.fromString(serialized["yaml"], format="json")

        # This JSON can be parsed by YAML parser
        j = Config.fromString(serialized["json"])
        y = Config.fromString(serialized["yaml"])
        self.assertEqual(j["formatters", "calexp"], 3)
        self.assertEqual(j.toDict(), y.toDict())

        # Round trip JSON -> Config -> YAML -> Config -> JSON -> Config
        c1 = Config.fromString(serialized["json"], format="json")
        yaml = c1.dump(format="yaml")
        c2 = Config.fromString(yaml, format="yaml")
        json = c2.dump(format="json")
        c3 = Config.fromString(json, format="json")
        self.assertEqual(c3.toDict(), c1.toDict())


class ConfigSubsetTestCase(unittest.TestCase):
    """Tests for ConfigSubset"""

    def setUp(self):
        self.testDir = os.path.abspath(os.path.dirname(__file__))
        self.configDir = os.path.join(self.testDir, "config", "testConfigs")
        self.configDir2 = os.path.join(self.testDir, "config", "testConfigs", "test2")
        self.configDir3 = os.path.join(self.testDir, "config", "testConfigs", "test3")

    def testEmpty(self):
        """Ensure that we can read an empty file."""
        c = ConfigTestEmpty(searchPaths=(self.configDir,))
        self.assertIsInstance(c, ConfigSubset)

    def testPathlib(self):
        """Ensure that we can read an empty file."""
        c = ConfigTestPathlib(searchPaths=(self.configDir,))
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
        c = ConfigTest({"comp": {"item1": 6, "item2": "a", "a": "b", "item3": 7}, "item4": 8})
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
        c = ConfigTestAbsPath(
            searchPaths=(
                self.configDir,
                self.configDir2,
            )
        )
        self.assertEqual(c["item11"], 11)
        self.assertEqual(len(c.filesRead), 1)

        # Same as above but this time with relative path and two search paths
        # to ensure the count changes
        ConfigTestAbsPath.defaultConfigFile = ConfigTest.defaultConfigFile
        c = ConfigTestAbsPath(
            searchPaths=(
                self.configDir,
                self.configDir2,
            )
        )
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

    def testStringInclude(self):
        """Using include directives in strings"""

        # See if include works for absolute path
        c = Config.fromYaml(f"something: !include {os.path.join(self.configDir, 'testconfig.yaml')}")
        self.assertEqual(c["something", "comp", "item3"], 3)

        with self.assertRaises(FileNotFoundError) as cm:
            Config.fromYaml("something: !include /not/here.yaml")
        # Test that it really was trying to open the absolute path
        self.assertIn("'/not/here.yaml'", str(cm.exception))

    def testIncludeConfigs(self):
        """Test the special includeConfigs key for pulling in additional
        files."""
        c = Config(os.path.join(self.configDir, "configIncludes.yaml"))
        self.assertEqual(c["comp", "item2"], "hello")
        self.assertEqual(c["comp", "item50"], 5000)
        self.assertEqual(c["comp", "item1"], "first")
        self.assertEqual(c["comp", "item10"], "tenth")
        self.assertEqual(c["comp", "item11"], "eleventh")
        self.assertEqual(c["unrelated"], 1)
        self.assertEqual(c["addon", "comp", "item1"], "posix")
        self.assertEqual(c["addon", "comp", "item11"], -1)
        self.assertEqual(c["addon", "comp", "item50"], 500)

        c = Config(os.path.join(self.configDir, "configIncludes.json"))
        self.assertEqual(c["comp", "item2"], "hello")
        self.assertEqual(c["comp", "item50"], 5000)
        self.assertEqual(c["comp", "item1"], "first")
        self.assertEqual(c["comp", "item10"], "tenth")
        self.assertEqual(c["comp", "item11"], "eleventh")
        self.assertEqual(c["unrelated"], 1)
        self.assertEqual(c["addon", "comp", "item1"], "posix")
        self.assertEqual(c["addon", "comp", "item11"], -1)
        self.assertEqual(c["addon", "comp", "item50"], 500)

        # Now test with an environment variable in includeConfigs
        with modified_environment(SPECIAL_BUTLER_DIR=self.configDir3):
            c = Config(os.path.join(self.configDir, "configIncludesEnv.yaml"))
            self.assertEqual(c["comp", "item2"], "hello")
            self.assertEqual(c["comp", "item50"], 5000)
            self.assertEqual(c["comp", "item1"], "first")
            self.assertEqual(c["comp", "item10"], "tenth")
            self.assertEqual(c["comp", "item11"], "eleventh")
            self.assertEqual(c["unrelated"], 1)
            self.assertEqual(c["addon", "comp", "item1"], "envvar")
            self.assertEqual(c["addon", "comp", "item11"], -1)
            self.assertEqual(c["addon", "comp", "item50"], 501)

        # This will fail
        with modified_environment(SPECIAL_BUTLER_DIR=self.configDir2):
            with self.assertRaises(FileNotFoundError):
                Config(os.path.join(self.configDir, "configIncludesEnv.yaml"))

    def testResource(self):
        c = Config("resource://lsst.daf.butler/configs/datastore.yaml")
        self.assertIn("datastore", c)

        # Test that we can include a resource URI
        yaml = """
toplevel: true
resource: !include resource://lsst.daf.butler/configs/datastore.yaml
"""
        c = Config.fromYaml(yaml)
        self.assertIn(("resource", "datastore", "cls"), c)

        # Test that we can include a resource URI with includeConfigs
        yaml = """
toplevel: true
resource:
  includeConfigs: resource://lsst.daf.butler/configs/datastore.yaml
"""
        c = Config.fromYaml(yaml)
        self.assertIn(("resource", "datastore", "cls"), c)


class FileWriteConfigTestCase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = makeTestTempDir(TESTDIR)

    def tearDown(self):
        removeTestTempDir(self.tmpdir)

    def testDump(self):
        """Test that we can write and read a configuration."""

        c = Config({"1": 2, "3": 4, "key3": 6, "dict": {"a": 1, "b": 2}})

        for format in ("yaml", "json"):
            outpath = os.path.join(self.tmpdir, f"test.{format}")
            c.dumpToUri(outpath)

            c2 = Config(outpath)
            self.assertEqual(c2, c)

            c.dumpToUri(outpath, overwrite=True)
            with self.assertRaises(FileExistsError):
                c.dumpToUri(outpath, overwrite=False)


if __name__ == "__main__":
    unittest.main()
