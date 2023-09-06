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

"""Unit tests for the daf_butler shared CLI options.
"""

import unittest
from unittest.mock import MagicMock

import click
from lsst.daf.butler.cli.opt import directory_argument, repo_argument
from lsst.daf.butler.cli.utils import (
    LogCliRunner,
    MWArgumentDecorator,
    MWCommand,
    MWCtxObj,
    MWOption,
    MWOptionDecorator,
    MWPath,
    clickResultMsg,
    option_section,
    unwrap,
)


class ArgumentHelpGeneratorTestCase(unittest.TestCase):
    """Test the help system."""

    def testHelp(self):
        @click.command()
        # Use custom help in the arguments so that any changes to default help
        # text do not break this test unnecessarily.
        @repo_argument(help="repo help text")
        @directory_argument(help="directory help text")
        def cli():
            """The cli help message."""  # noqa: D401
            pass

        self.runTest(cli)

    def testHelpWrapped(self):
        @click.command()
        # Use custom help in the arguments so that any changes to default help
        # text do not break this test unnecessarily.
        @repo_argument(help="repo help text")
        @directory_argument(help="directory help text")
        def cli():
            """The cli help message."""  # noqa: D401
            pass

        self.runTest(cli)

    def runTest(self, cli):
        """Test `utils.addArgumentHelp` and its use in repo_argument and
        directory_argument; verifies that the argument help gets added to the
        command function help, and that it's added in the correct order. See
        addArgumentHelp for more details.
        """
        expected = """Usage: cli [OPTIONS] REPO DIRECTORY

  The cli help message.

  repo help text

  directory help text

Options:
  --help  Show this message and exit.
"""
        runner = LogCliRunner()
        result = runner.invoke(cli, ["--help"])
        self.assertIn(expected, result.output)


class UnwrapStringTestCase(unittest.TestCase):
    """Test string unwrapping."""

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

    def test_lineBreaks(self):
        testStr = """foo bar
                  baz

                  boz

                  qux"""
        self.assertEqual(unwrap(testStr), "foo bar baz\n\nboz\n\nqux")


class MWOptionTest(unittest.TestCase):
    """Test MWOption."""

    def setUp(self):
        self.runner = LogCliRunner()

    def test_addEllipsisToMultiple(self):
        """Verify that MWOption adds ellipsis to the option metavar when
        `multiple=True`

        The default behavior of click is to not add ellipsis to options that
        have `multiple=True`.
        """

        @click.command()
        @click.option("--things", cls=MWOption, multiple=True)
        def cmd(things):
            pass

        result = self.runner.invoke(cmd, ["--help"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        expectedOutput = """Options:
  --things TEXT ..."""
        self.assertIn(expectedOutput, result.output)

    def test_addEllipsisToNargs(self):
        """Verify that MWOption adds " ..." after the option metavar when
        `nargs` is set to more than 1 and less than 1.

        The default behavior of click is to add ellipsis when nargs does not
        equal 1, but it does not put a space before the ellipsis and we prefer
        a space between the metavar and the ellipsis.
        """
        for numberOfArgs in (0, 1, 2):  # nargs must be >= 0 for an option

            @click.command()
            @click.option("--things", cls=MWOption, nargs=numberOfArgs)
            def cmd(things):
                pass

            result = self.runner.invoke(cmd, ["--help"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expectedOutput = f"""Options:
  --things TEXT{' ...' if numberOfArgs != 1 else ''}"""
            self.assertIn(expectedOutput, result.output)


class MWArgumentDecoratorTest(unittest.TestCase):
    """Tests for the MWArgumentDecorator class."""

    things_argument = MWArgumentDecorator("things")
    otherHelpText = "Help text for OTHER."
    other_argument = MWArgumentDecorator("other", help=otherHelpText)

    def setUp(self):
        self.runner = LogCliRunner()

    def test_help(self):
        """Verify expected help text output.

        Verify argument help gets inserted after the usage, in the order
        arguments are declared.

        Verify that MWArgument adds " ..." after the option metavar when
        `nargs` != 1. The default behavior of click is to add ellipsis when
        nargs does not equal 1, but it does not put a space before the ellipsis
        and we prefer a space between the metavar and the ellipsis.
        """
        # nargs can be -1 for any number of args, or >= 1 for a specified
        # number of arguments.

        helpText = "Things help text."
        for numberOfArgs in (-1, 1, 2):
            for required in (True, False):

                @click.command()
                @self.things_argument(required=required, nargs=numberOfArgs, help=helpText)
                @self.other_argument()
                def cmd(things, other):
                    """Cmd help text."""
                    pass

                result = self.runner.invoke(cmd, ["--help"])
                self.assertEqual(result.exit_code, 0, clickResultMsg(result))
                things = "THINGS" if required else "[THINGS]"
                additional = "... " if numberOfArgs != 1 else ""
                expectedOutput = f"""Usage: cmd [OPTIONS] {things} {additional}OTHER

  Cmd help text.

  {helpText}

  {self.otherHelpText}
"""
                self.assertIn(expectedOutput, result.output)

    def testUse(self):
        """Test using the MWArgumentDecorator with a command."""
        mock = MagicMock()

        @click.command()
        @self.things_argument()
        def cli(things):
            mock(things)

        self.runner = click.testing.CliRunner()
        result = self.runner.invoke(cli, "foo")
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        mock.assert_called_with("foo")


class MWOptionDecoratorTest(unittest.TestCase):
    """Tests for the MWOptionDecorator class."""

    _test_option = MWOptionDecorator("-t", "--test", multiple=True)

    def testGetName(self):
        """Test getting the option name from the MWOptionDecorator."""
        self.assertEqual(self._test_option.name(), "test")

    def testGetOpts(self):
        """Test getting the option flags from the MWOptionDecorator."""
        self.assertEqual(self._test_option.opts(), ["-t", "--test"])

    def testUse(self):
        """Test using the MWOptionDecorator with a command."""
        mock = MagicMock()

        @click.command()
        @self._test_option()
        def cli(test):
            mock(test)

        self.runner = click.testing.CliRunner()
        result = self.runner.invoke(cli, ("-t", "foo"))
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        mock.assert_called_with(("foo",))

    def testOverride(self):
        """Test using the MWOptionDecorator with a command and overriding one
        of the default values.
        """
        mock = MagicMock()

        @click.command()
        @self._test_option(multiple=False)
        def cli(test):
            mock(test)

        self.runner = click.testing.CliRunner()
        result = self.runner.invoke(cli, ("-t", "foo"))
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        mock.assert_called_with("foo")


class SectionOptionTest(unittest.TestCase):
    """Tests for the option_section decorator that inserts section break
    headings between options in the --help output of a command.
    """

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
        expected location and with expected formatting.
        """
        result = self.runner.invoke(self.cli, ["--help"])
        # \x20 is a space, added explicitly below to prevent the
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


class MWPathTest(unittest.TestCase):
    """Test MWPath."""

    def getCmd(self, exists):
        @click.command()
        @click.option("--name", type=MWPath(exists=exists))
        def cmd(name):
            pass

        return cmd

    def setUp(self):
        self.runner = click.testing.CliRunner()

    def test_exist(self):
        """Test the exist argument, verify that True means the file must exist,
        False means the file must not exist, and None means that the file may
        or may not exist.
        """
        with self.runner.isolated_filesystem():
            mustExistCmd = self.getCmd(exists=True)
            mayExistCmd = self.getCmd(exists=None)
            mustNotExistCmd = self.getCmd(exists=False)
            args = ["--name", "foo.txt"]

            result = self.runner.invoke(mustExistCmd, args)
            self.assertNotEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertRegex(result.output, """['"]foo.txt['"] does not exist.""")

            result = self.runner.invoke(mayExistCmd, args)
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))

            result = self.runner.invoke(mustNotExistCmd, args)
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))

            # isolated_filesystem runs in a temporary directory, when it is
            # removed everything inside will be removed.
            with open("foo.txt", "w") as _:
                result = self.runner.invoke(mustExistCmd, args)
                self.assertEqual(result.exit_code, 0, clickResultMsg(result))

                result = self.runner.invoke(mayExistCmd, args)
                self.assertEqual(result.exit_code, 0, clickResultMsg(result))

                result = self.runner.invoke(mustNotExistCmd, args)
                self.assertNotEqual(result.exit_code, 0, clickResultMsg(result))
                self.assertIn('"foo.txt" should not exist.', result.output)


class MWCommandTest(unittest.TestCase):
    """Test MWCommand."""

    def setUp(self):
        self.runner = click.testing.CliRunner()
        self.ctx = None

    def testCaptureOptions(self):
        """Test that command options are captured in order."""

        @click.command(cls=MWCommand)
        @click.argument("ARG_A", required=False)
        @click.argument("ARG_B", required=False)
        @click.option("-o", "--an-option")
        @click.option("-s", "--second-option")
        @click.option("-f", is_flag=True)
        @click.option("-d/-n", "--do/--do-not", "do_or_do_not")
        @click.option("--multi", multiple=True)
        @click.pass_context
        def cmd(ctx, arg_a, arg_b, an_option, second_option, f, do_or_do_not, multi):
            self.assertIsNotNone(ctx)
            self.ctx = ctx

        # When `expected` is `None`, the expected args are exactly the same as
        # as the result of `args.split()`. If `expected` is not `None` then
        # the expected args are the same as `expected.split()`.
        for args, expected in (
            ("--an-option foo --second-option bar", None),
            ("--second-option bar --an-option foo", None),
            ("--an-option foo", None),
            ("--second-option bar", None),
            ("--an-option foo -f --second-option bar", None),
            ("-o foo -s bar", "--an-option foo --second-option bar"),
            ("--an-option foo -f -s bar", "--an-option foo -f --second-option bar"),
            ("--an-option=foo", "--an-option foo"),
            # NB when using a short flag everything that follows, including an
            # equals sign, is part of the value!
            ("-o=foo", "--an-option =foo"),
            ("--do", None),
            ("--do-not", None),
            ("-d", "--do"),
            ("-n", "--do-not"),
            # Arguments always come last, but the order of Arguments is still
            # preserved:
            ("myarg --an-option foo", "--an-option foo myarg"),
            ("argA --an-option foo", "--an-option foo argA"),
            ("argA argB --an-option foo", "--an-option foo argA argB"),
            ("argA --an-option foo argB", "--an-option foo argA argB"),
            ("--an-option foo argA argB", "--an-option foo argA argB"),
            ("--multi one --multi two", None),
        ):
            split_args = args.split()
            expected_args = split_args if expected is None else expected.split()
            result = self.runner.invoke(cmd, split_args)
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIsNotNone(self.ctx)
            ctx_obj = MWCtxObj.getFrom(self.ctx)
            self.assertEqual(ctx_obj.args, expected_args)


if __name__ == "__main__":
    unittest.main()
