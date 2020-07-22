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

"""Unit tests for the daf_butler shared CLI options.
"""

import click
import unittest

from lsst.daf.butler.cli import butler
from lsst.daf.butler.cli.utils import (clickResultMsg, LogCliRunner, Mocker, mockEnvVar, MWArgument, MWOption,
                                       option_section, unwrap)
from lsst.daf.butler.cli.opt import directory_argument, repo_argument


class MockerTestCase(unittest.TestCase):

    def test_callMock(self):
        """Test that a mocked subcommand calls the Mocker and can be verified.
        """
        runner = LogCliRunner(env=mockEnvVar)
        result = runner.invoke(butler.cli, ["create", "repo"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        Mocker.mock.assert_called_with(repo="repo", seed_config=None, standalone=False, override=False,
                                       outfile=None)


class ArgumentHelpGeneratorTestCase(unittest.TestCase):

    @staticmethod
    @click.command()
    # Use custom help in the arguments so that any changes to default help text
    # do not break this test unnecessarily.
    @repo_argument(help="repo help text")
    @directory_argument(help="directory help text")
    def cli():
        pass

    def test_help(self):
        """Tests `utils.addArgumentHelp` and its use in repo_argument and
        directory_argument; verifies that the argument help gets added to the
        command fucntion help, and that it's added in the correct order. See
        addArgumentHelp for more details."""
        runner = LogCliRunner()
        result = runner.invoke(ArgumentHelpGeneratorTestCase.cli, ["--help"])
        expected = """Usage: cli [OPTIONS] [REPO] [DIRECTORY]

  directory help text

  repo help text

Options:
  --help  Show this message and exit.
"""
        self.assertIn(expected, result.output)


class UnwrapStringTestCase(unittest.TestCase):

    def test_leadingNewline(self):
        testStr = """
            foo bar
            baz """
        self.assertEqual(unwrap(testStr), "foo bar baz")

    def test_leadingContent(self):
        testStr = """foo bar
            baz """
        self.assertEqual(unwrap(testStr), "foo bar baz")

    def test_trailingNewline(self):
        testStr = """
            foo bar
            baz
            """
        self.assertEqual(unwrap(testStr), "foo bar baz")

    def test_oneLine(self):
        testStr = """foo bar baz"""
        self.assertEqual(unwrap(testStr), "foo bar baz")

    def test_oneLineWithLeading(self):
        testStr = """
            foo bar baz"""
        self.assertEqual(unwrap(testStr), "foo bar baz")

    def test_oneLineWithTrailing(self):
        testStr = """foo bar baz
            """
        self.assertEqual(unwrap(testStr), "foo bar baz")


class MWOptionTest(unittest.TestCase):

    def setUp(self):
        self.runner = LogCliRunner()

    def test_addElipsisToMultiple(self):
        """Verify that MWOption adds elipsis to the option metavar when
        `multiple=True`

        The default behavior of click is to not add elipsis to options that
        have `multiple=True`."""
        @click.command()
        @click.option("--things", cls=MWOption, multiple=True)
        def cmd(things):
            pass
        result = self.runner.invoke(cmd, ["--help"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        expectedOutut = """Options:
  --things TEXT ..."""
        self.assertIn(expectedOutut, result.output)

    def test_addElipsisToNargs(self):
        """Verify that MWOption adds " ..." after the option metavar when
        `nargs` is set to more than 1 and less than 1.

        The default behavior of click is to add elipsis when nargs does not
        equal 1, but it does not put a space before the elipsis and we prefer
        a space between the metavar and the elipsis."""
        for numberOfArgs in (0, 1, 2):  # nargs must be >= 0 for an option
            @click.command()
            @click.option("--things", cls=MWOption, nargs=numberOfArgs)
            def cmd(things):
                pass
            result = self.runner.invoke(cmd, ["--help"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expectedOutut = f"""Options:
  --things TEXT{' ...' if numberOfArgs != 1 else ''}"""
            self.assertIn(expectedOutut, result.output)


class MWArgumentTest(unittest.TestCase):

    def setUp(self):
        self.runner = LogCliRunner()

    def test_addElipsisToNargs(self):
        """Verify that MWOption adds " ..." after the option metavar when
        `nargs` != 1.

        The default behavior of click is to add elipsis when nargs does not
        equal 1, but it does not put a space before the elipsis and we prefer
        a space between the metavar and the elipsis."""
        # nargs can be -1 for any number of args, or >= 1 for a specified
        # number of arguments.
        for numberOfArgs in (-1, 1, 2):
            for required in (True, False):
                @click.command()
                @click.argument("things", cls=MWArgument, required=required, nargs=numberOfArgs)
                def cmd(things):
                    pass
                result = self.runner.invoke(cmd, ["--help"])
                self.assertEqual(result.exit_code, 0, clickResultMsg(result))
                expectedOutut = (f"Usage: cmd [OPTIONS] {'THINGS' if required else '[THINGS]'}"
                                 f"{' ...' if numberOfArgs != 1 else ''}")
                self.assertIn(expectedOutut, result.output)


class SectionOptionTest(unittest.TestCase):

    @staticmethod
    @click.command()
    @click.option("--foo")
    @option_section("Section break between metasyntactic variables.")
    @click.option("--bar")
    def cli(foo, bar):
        pass

    def setUp(self):
        self.runner = click.testing.CliRunner()

    def test_section_help(self):
        """Verify that the section break is printed in the help output in the
        expected location and with expected formatting."""
        result = self.runner.invoke(self.cli, ["--help"])
        # \x20 is a space, added explicity below to prevent the
        # normally-helpful editor setting "remove trailing whitespace" from
        # stripping it out in this case. (The blank line with 2 spaces is an
        # artifact of how click and our code generate help text.)
        expected = """Options:
  --foo TEXT
\x20\x20
Section break between metasyntactic variables.
  --bar TEXT"""
        self.assertIn(expected, result.output)

    def test_section_function(self):
        """Verify that the section does not cause any arguments to be passed to
        the command function.

        The command function `cli` implementation inputs `foo` and `bar`, but
        does accept an argument for the section. When the command is invoked
        and the function called it should result in exit_code=0 (not 1 with a
        missing argument error).
        """
        result = self.runner.invoke(self.cli, [])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))


if __name__ == "__main__":
    unittest.main()
