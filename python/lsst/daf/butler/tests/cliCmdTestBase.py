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
from __future__ import annotations

import abc
import copy
import os
from collections.abc import Callable
from typing import TYPE_CHECKING, Any
from unittest.mock import DEFAULT, call, patch

from ..cli import butler
from ..cli.utils import LogCliRunner, clickResultMsg

if TYPE_CHECKING:
    import unittest

    import click


class CliCmdTestBase(abc.ABC):
    """A test case base that is used to verify click command functions import
    and call their respective script functions correctly.
    """

    if TYPE_CHECKING:
        assertNotEqual: Callable
        assertRegex: Callable
        assertFalse: Callable
        assertEqual: Callable

    @staticmethod
    @abc.abstractmethod
    def defaultExpected() -> dict[str, Any]:
        pass

    @staticmethod
    @abc.abstractmethod
    def command() -> click.Command:
        """Get the click.Command being tested."""
        pass

    @property
    def cli(self) -> click.core.Command:
        """Get the command line interface function under test, can be
        overridden to test CLIs other than butler.
        """
        return butler.cli

    @property
    def mock(self) -> unittest.mock.Mock:
        """Get the mock object to use in place of `mockFuncName`. If not
        provided will use the default provided by `unittest.mock.patch`, this
        is usually a `unittest.mock.MagicMock`.
        """
        return DEFAULT

    @property
    @abc.abstractmethod
    def mockFuncName(self) -> str:
        """The qualified name of the function to mock, will be passed to
        unittest.mock.patch, see python docs for details.
        """
        pass

    def setUp(self) -> None:
        self.runner = LogCliRunner()

    @classmethod
    def makeExpected(cls, **kwargs: Any) -> dict[str, Any]:
        expected = copy.copy(cls.defaultExpected())
        expected.update(kwargs)
        return expected

    def run_command(self, inputs: list[str]) -> click.testing.Result:
        """Use the LogCliRunner with the mock environment variable set to
        execute a butler subcommand and parameters specified in inputs.

        Parameters
        ----------
        inputs : [`str`]
            A list of strings that begins with the subcommand name and is
            followed by arguments, option keys and option values.

        Returns
        -------
        result : `click.testing.Result`
            The Result object contains the results from calling
            self.runner.invoke.
        """
        return self.runner.invoke(self.cli, inputs)

    def run_test(
        self, inputs: list[str], expectedKwargs: dict[str, str], withTempFile: str | None = None
    ) -> click.testing.Result:
        """Run the subcommand specified in inputs and verify a successful
        outcome where exit code = 0 and the mock object has been called with
        the expected arguments.

        Returns the result object for inspection, e.g. sometimes it's useful to
        be able to inspect or print `result.output`.

        Parameters
        ----------
        inputs : [`str`]
            A list of strings that begins with the subcommand name and is
            followed by arguments, option keys and option values.
        expectedKwargs : `dict` [`str`, `str`]
            The arguments that the subcommand function is expected to have been
            called with. Keys are the argument name and values are the argument
            value.
        withTempFile : `str`, optional
            If not None, will run in a temporary directory and create a file
            with the given name, can be used with commands with parameters that
            require a file to exist.

        Returns
        -------
        result : `click.testing.Result`
            The result object produced by invocation of the command under test.
        """
        with self.runner.isolated_filesystem():
            if withTempFile is not None:
                directory, filename = os.path.split(withTempFile)
                if directory:
                    os.makedirs(os.path.dirname(withTempFile), exist_ok=True)
                with open(withTempFile, "w") as _:
                    # just need to make the file, don't need to keep it open.
                    pass
            with patch(self.mockFuncName, self.mock) as mock:
                result = self.run_command(inputs)
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            calls = (call(**expectedKwargs),)
            mock.assert_has_calls(list(calls))
        return result

    def run_missing(self, inputs: list[str], expectedMsg: str) -> None:
        """Run the subcommand specified in inputs and verify a failed outcome
        where exit code != 0 and an expected message has been written to
        stdout.

        Parameters
        ----------
        inputs : [`str`]
            A list of strings that begins with the subcommand name and is
            followed by arguments, option keys and option values.
        expectedMsg : `str`
            An error message that should be present in stdout after running the
            subcommand. Can be a regular expression string.
        """
        result = self.run_command(inputs)
        self.assertNotEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertRegex(result.stdout, expectedMsg)

    def test_help(self) -> None:
        self.assertFalse(
            self.command().get_short_help_str().endswith("..."),
            msg="The command help message is being truncated to "
            f'"{self.command().get_short_help_str()}". It should be shortened, or define '
            '@command(short_help="something short and helpful")',
        )
