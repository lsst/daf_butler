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

"""Unit tests for daf_butler CLI config-dump command.
"""

import os
import os.path
import unittest

import click
import yaml
from lsst.daf.butler.cli import butler
from lsst.daf.butler.cli.cmd import config_dump
from lsst.daf.butler.cli.opt import options_file_option
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg
from lsst.daf.butler.tests import CliCmdTestBase

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class ConfigDumpTest(CliCmdTestBase, unittest.TestCase):
    """Test the butler config-dump command line."""

    mockFuncName = "lsst.daf.butler.cli.cmd.commands.script.configDump"

    @staticmethod
    def defaultExpected():
        return {}

    @staticmethod
    def command():
        return config_dump


class ConfigDumpUseTest(unittest.TestCase):
    """Test executing the command."""

    def setUp(self):
        self.runner = LogCliRunner()

    def test_stdout(self):
        """Test dumping the config to stdout."""
        with self.runner.isolated_filesystem():
            result = self.runner.invoke(butler.cli, ["create", "here"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))

            # test dumping to stdout:
            result = self.runner.invoke(butler.cli, ["config-dump", "here"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            # check for some expected keywords:
            cfg = yaml.safe_load(result.stdout)
            self.assertIn("datastore", cfg)
            self.assertIn("composites", cfg["datastore"])
            self.assertIn("storageClasses", cfg)

    def test_file(self):
        """Test dumping the config to a file."""
        with self.runner.isolated_filesystem():
            result = self.runner.invoke(butler.cli, ["create", "here"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            result = self.runner.invoke(butler.cli, ["config-dump", "here", "--file=there"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            # check for some expected keywords:
            with open("there") as f:
                cfg = yaml.safe_load(f)
                self.assertIn("datastore", cfg)
                self.assertIn("composites", cfg["datastore"])
                self.assertIn("storageClasses", cfg)

    def test_subset(self):
        """Test selecting a subset of the config."""
        with self.runner.isolated_filesystem():
            result = self.runner.invoke(butler.cli, ["create", "here"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            result = self.runner.invoke(butler.cli, ["config-dump", "here", "--subset", "datastore"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            cfg = yaml.safe_load(result.stdout)
            # count the keys in the datastore config
            self.assertEqual(len(cfg), 8)
            self.assertIn("cls", cfg)
            self.assertIn("create", cfg)
            self.assertIn("formatters", cfg)
            self.assertIn("records", cfg)
            self.assertIn("root", cfg)
            self.assertIn("templates", cfg)

            # Test that a subset that returns a scalar quantity does work.
            result = self.runner.invoke(butler.cli, ["config-dump", "here", "--subset", ".datastore.root"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertEqual(result.stdout.strip(), ".datastore.root: <butlerRoot>")

    def test_invalidSubset(self):
        """Test selecting a subset key that does not exist in the config."""
        with self.runner.isolated_filesystem():
            result = self.runner.invoke(butler.cli, ["create", "here"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            # test dumping to stdout:
            result = self.runner.invoke(butler.cli, ["config-dump", "here", "--subset", "foo"])
            self.assertEqual(result.exit_code, 1)
            # exception type is click.Exit, and its argument is a return code
            self.assertEqual(result.exception.args, (1,))

    def test_presets(self):
        """Test that file overrides can set command line options in bulk."""
        with self.runner.isolated_filesystem():
            result = self.runner.invoke(butler.cli, ["create", "here"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            overrides_path = os.path.join(TESTDIR, "data", "config-overrides.yaml")

            # Run with a presets file
            result = self.runner.invoke(butler.cli, ["config-dump", "here", "--options-file", overrides_path])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            cfg = yaml.safe_load(result.stdout)
            # Look for datastore information
            self.assertIn("formatters", cfg)
            self.assertIn("root", cfg)

            # Now run with an explicit subset and presets
            result = self.runner.invoke(
                butler.cli, ["config-dump", "here", f"-@{overrides_path}", "--subset", ".registry"]
            )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            cfg = yaml.safe_load(result.stdout)
            # Look for datastore information
            self.assertNotIn("formatters", cfg)
            self.assertIn("managers", cfg)

            # Now with subset before presets -- explicit always trumps
            # presets.
            result = self.runner.invoke(
                butler.cli, ["config-dump", "here", "--subset", ".registry", "--options-file", overrides_path]
            )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            cfg = yaml.safe_load(result.stdout)
            # Look for datastore information
            self.assertNotIn("formatters", cfg)
            self.assertIn("managers", cfg)

            configfile = "overrides.yaml"
            outfile = "repodef.yaml"
            # Check that a misspelled command option causes an error:
            with open(configfile, "w") as f:
                f.write(yaml.dump({"config-dump": {"fil": outfile}}))
            result = self.runner.invoke(butler.cli, ["config-dump", "here", f"-@{configfile}"])
            self.assertNotEqual(result.exit_code, 0, clickResultMsg(result))

            # Check that an option that declares a different command argument
            # name is mapped correctly.
            # Note that the option `config-dump --file`
            # becomes the `outfile` argument in `def config_dump(..., outfile)`
            # and we use the option name "file" in the presets file.
            with open(configfile, "w") as f:
                f.write(yaml.dump({"config-dump": {"file": outfile}}))
            result = self.runner.invoke(butler.cli, ["config-dump", "here", f"-@{configfile}"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertTrue(os.path.exists(outfile))

    def test_presetsDashedName(self):
        """Test file overrides when the option has a dash in its name."""

        # Instead of using `butler config-dump` as we do in other tests,
        # create a small command for testing, because config-dump does
        # not have any options with dashes in the name.
        @click.command()
        @click.option("--test-option")
        @options_file_option()
        def cmd(test_option):
            print(test_option)

        configfile = "overrides.yaml"
        val = "foo"
        with self.runner.isolated_filesystem():
            with open(configfile, "w") as f:
                f.write(yaml.dump({"cmd": {"test-option": val}}))
            result = self.runner.invoke(cmd, ["-@", configfile])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertTrue(val in result.output)


if __name__ == "__main__":
    unittest.main()
