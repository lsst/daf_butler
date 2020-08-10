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
from unittest.mock import call, MagicMock


from lsst.daf.butler.cli import butler
from lsst.daf.butler.cli.utils import (clickResultMsg, ForwardOptions, LogCliRunner, Mocker, mockEnvVar,
                                       MWArgument, MWCommand, MWCtxObj, MWOption, MWOptionDecorator, MWPath,
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

    def test_lineBreaks(self):
        testStr = """foo bar
                  baz

                  boz

                  qux"""
        self.assertEqual(unwrap(testStr), "foo bar baz\n\nboz\n\nqux")


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


class MWOptionDecoratorTest(unittest.TestCase):

    def makeTestOption(self, flags, expectedFlag, expectedKey):

        class test_option(MWOptionDecorator):  # noqa: N801

            @staticmethod
            def defaultHelp():
                return "default help"

            @staticmethod
            def optionFlags():
                return flags

        class Expected:

            def __init__(self):
                self.flag = expectedFlag
                self.flags = flags
                self.key = expectedKey

        return test_option, Expected()

    def test(self):
        """Verify the helper funcs in MWOptionDecorator return the correct
        value. Also verifies values can be extracted from click.Option as
        expected."""
        tests = (self.makeTestOption(("-t",), "-t", "t"),  # <- one flag, short:
                 # one flag, long:
                 self.makeTestOption(("--test-val",), "--test-val", "test_val"),
                 # one short, one long:
                 self.makeTestOption(("-t", "--test-val"), "--test-val", "test_val"),
                 # same, opposite order:
                 self.makeTestOption(("--test-val", "-t"), "--test-val", "test_val"),
                 # two longer option flags, reverse alphabetical order (first
                 # flag is still picked):
                 self.makeTestOption(("--test-val", "--alternate-opt"), "--test-val", "test_val"),
                 # multiple short and long flags:
                 self.makeTestOption(("-t", "--test-val", "-a", "--alternate-opt"), "--test-val", "test_val"),
                 # declare alternate kwarg name:
                 self.makeTestOption(("--test-val", "alternate_name"), "--test-val", "alternate_name"))

        for test_option, expected in tests:
            self.assertEqual(test_option.defaultHelp(), "default help")
            # key is extracted from click.Option
            self.assertEqual(test_option.optionKey(), expected.key)
            # flag is generated from the flags extracted from click.Option
            self.assertEqual(test_option.flag(), expected.flag)
            # optionFlags is set in the MWOptionDecorator class.
            self.assertEqual(test_option.optionFlags(), expected.flags)


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


class MWPathTest(unittest.TestCase):

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
        or may not exist."""
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


class ForwardObjectsTest(unittest.TestCase):

    mock = MagicMock()

    class test_option(MWOptionDecorator):  # noqa: N801
        """Decorator that adds a a test option to a command."""

        @staticmethod
        def defaultHelp():
            return "default help"

        @staticmethod
        def optionFlags():
            return ("-t", "--test", "--atest")

        def __call__(self, f):
            return click.option(*self.optionFlags(), cls=MWOption, forward=self.forward,
                                help=self.defaultHelp())(f)

    @click.group(chain=True)
    def cli():
        pass

    @staticmethod
    @cli.command(cls=MWCommand)
    @click.pass_context
    @test_option(forward=True)
    def forwards(ctx, **kwargs):
        """A subcommand that forwards its test_option value to future
        subcommands."""
        def processor(objs):
            newKwargs = objs.update(ctx.command.params, MWCtxObj.getFrom(ctx).args, **kwargs)
            ForwardObjectsTest.mock("forwards", **newKwargs)
            return objs
        return processor

    @staticmethod
    @cli.command(cls=MWCommand)
    @click.pass_context
    @test_option()  # default value of "foward" arg is False
    def no_forward(ctx, **kwargs):
        """A subcommand that accepts test_option but does not forward the value
        to future subcommands."""
        def processor(objs):
            newKwargs = objs.update(ctx.command.params, MWCtxObj.getFrom(ctx).args, **kwargs)
            ForwardObjectsTest.mock("no_forward", **newKwargs)
            return objs
        return processor

    @staticmethod
    @cli.command(cls=MWCommand)
    @click.pass_context
    def no_test_option(ctx, **kwargs):
        """A subcommand that does not accept test_option."""
        def processor(objs):
            newKwargs = objs.update(ctx.command.params, MWCtxObj.getFrom(ctx).args, **kwargs)
            ForwardObjectsTest.mock("no_test_option", **newKwargs)
            return objs
        return processor

    @staticmethod
    @cli.resultcallback()
    def processCli(processors):
        """Executes the subcommand 'processor' functions for all the
        subcommands in the chained command group."""
        objs = ForwardOptions()
        for processor in processors:
            objs = processor(objs)

    def setUp(self):
        self.runner = click.testing.CliRunner()
        self.mock.reset_mock()

    def testForward(self):
        """Test that an option can be forward from one option to another."""
        result = self.runner.invoke(self.cli, ["forwards", "-t", "foo", "forwards"])
        print(result.output)
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.mock.assert_has_calls((call("forwards", test="foo"),
                                    call("forwards", test="foo")))

    def testNoForward(self):
        """Test that when a subcommand that forwards an option value is called
        before an option that does not use that option, that the stored option
        does not get passed to the option that does not use it."""
        result = self.runner.invoke(self.cli, ["forwards", "-t", "foo", "no-forward"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.mock.assert_has_calls((call("forwards", test="foo"),
                                    call("no_forward", test="foo")))

    def testForwardThrough(self):
        """Test that forwarded option values persist when a subcommand that
        does not use that value is called between subcommands that do use the
        value."""
        result = self.runner.invoke(self.cli, ["forwards", "-t", "foo",
                                               "no-test-option",
                                               "forwards"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.mock.assert_has_calls((call("forwards", test="foo"),
                                    call("no_test_option"),
                                    call("forwards", test="foo")))


if __name__ == "__main__":
    unittest.main()
