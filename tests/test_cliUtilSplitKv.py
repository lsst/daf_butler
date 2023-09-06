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
from functools import partial
from unittest.mock import MagicMock

import click
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg, split_kv


class SplitKvTestCase(unittest.TestCase):
    """Tests that call split_kv directly."""

    def test_single_dict(self):
        """Test that a single kv pair converts to a dict."""
        self.assertEqual(split_kv("context", "param", "first=1"), {"first": "1"})

    def test_single_tuple(self):
        """Test that a single kv pair converts to a tuple when
        return_type=tuple.
        """
        self.assertEqual(split_kv("context", "param", "first=1", return_type=tuple), (("first", "1"),))

    def test_multiple_dict(self):
        """Test that multiple comma separated kv pairs convert to a dict."""
        self.assertEqual(split_kv("context", "param", "first=1,second=2"), {"first": "1", "second": "2"})

    def test_multiple_tuple(self):
        """Test that multiple comma separated kv pairs convert to a tuple when
        return_type=tuple.
        """
        self.assertEqual(
            split_kv("context", "param", "first=1,second=2", return_type=tuple),
            (("first", "1"), ("second", "2")),
        )

    def test_unseparated(self):
        """Test that a value without a key converts to a kv pair with an empty
        string key.
        """
        self.assertEqual(
            split_kv("context", "param", "first,second=2", unseparated_okay=True),
            {"": "first", "second": "2"},
        )

    def test_notMultiple(self):
        """Test that multiple values are rejected if multiple=False."""
        with self.assertRaisesRegex(
            click.ClickException,
            "Could not parse key-value pair "
            "'first=1,second=2' using separator '=', with multiple values not "
            "allowed.",
        ):
            split_kv("context", "param", "first=1,second=2", multiple=False)

    def test_wrongSeparator(self):
        """Test that an input with the wrong separator raises."""
        with self.assertRaises(click.ClickException):
            split_kv("context", "param", "first-1")

    def test_missingSeparator(self):
        """Test that an input with no separator raises when
        unseparated_okay=False (this is the default value).
        """
        with self.assertRaises(click.ClickException):
            split_kv("context", "param", "first 1")

    def test_unseparatedOkay(self):
        """Test that that the default key is used for values without a
        separator when unseparated_okay=True.
        """
        self.assertEqual(split_kv("context", "param", "foo", unseparated_okay=True), {"": "foo"})

    def test_unseparatedOkay_list(self):
        """Test that that the default key is used for values without a
        separator when unseparated_okay=True and the return_type is tuple.
        """
        self.assertEqual(
            split_kv("context", "param", "foo,bar", unseparated_okay=True, return_type=tuple),
            (("", "foo"), ("", "bar")),
        )

    def test_unseparatedOkay_defaultKey(self):
        """Test that that the default key can be set and is used for values
        without a separator when unseparated_okay=True.
        """
        self.assertEqual(
            split_kv("context", "param", "foo", unseparated_okay=True, default_key=...), {...: "foo"}
        )

    def test_dashSeparator(self):
        """Test that specifying a separator is accepted and converts arguments
        to a dict.
        """
        self.assertEqual(
            split_kv("context", "param", "first-1,second-2", separator="-"), {"first": "1", "second": "2"}
        )

    def test_reverseKv(self):
        self.assertEqual(
            split_kv(
                "context",
                "param",
                "first=1,second",
                unseparated_okay=True,
                default_key="key",
                reverse_kv=True,
            ),
            {"1": "first", "second": "key"},
        )

    def test_invalidResultType(self):
        with self.assertRaises(click.ClickException):
            split_kv(
                "context",
                "param",
                "first=1,second=2",
                return_type=set,
            )


class SplitKvCmdTestCase(unittest.TestCase):
    """Tests using split_kv with a command."""

    def setUp(self):
        self.runner = LogCliRunner()

    def test_cli(self):
        mock = MagicMock()

        @click.command()
        @click.option("--value", callback=split_kv, multiple=True)
        def cli(value):
            mock(value)

        result = self.runner.invoke(cli, ["--value", "first=1"])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with({"first": "1"})

        result = self.runner.invoke(cli, ["--value", "first=1,second=2"])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with({"first": "1", "second": "2"})

        result = self.runner.invoke(cli, ["--value", "first=1", "--value", "second=2"])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with({"first": "1", "second": "2"})

        # double separator "==" should fail:
        result = self.runner.invoke(cli, ["--value", "first==1"])
        self.assertEqual(result.exit_code, 1)
        self.assertEqual(
            result.output,
            "Error: Could not parse key-value pair 'first==1' using separator '=', with "
            "multiple values allowed: too many values to unpack (expected 2)\n",
        )

    def test_choice(self):
        choices = ["FOO", "BAR", "BAZ"]
        mock = MagicMock()

        @click.command()
        @click.option(
            "--metasyntactic-var",
            callback=partial(
                split_kv,
                unseparated_okay=True,
                choice=click.Choice(choices=choices, case_sensitive=False),
                normalize=True,
            ),
        )
        def cli(metasyntactic_var):
            mock(metasyntactic_var)

        # check a valid choice without a kv separator
        result = self.runner.invoke(cli, ["--metasyntactic-var", "FOO"])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with({"": "FOO"})

        # check a valid choice with a kv separator
        result = self.runner.invoke(cli, ["--metasyntactic-var", "lsst.daf.butler=BAR"])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with({"lsst.daf.butler": "BAR"})

        # check that invalid choices with and without kv separators fail &
        # return a non-zero exit code.
        for val in ("BOZ", "lsst.daf.butler=BOZ"):
            result = self.runner.invoke(cli, ["--metasyntactic-var", val])
            self.assertNotEqual(result.exit_code, 0, msg=clickResultMsg(result))

        # check value normalization (lower case "foo" should become "FOO")
        result = self.runner.invoke(cli, ["--metasyntactic-var", "lsst.daf.butler=foo"])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with({"lsst.daf.butler": "FOO"})

    def test_separatorDash(self):
        def split_kv_dash(context, param, values):
            return split_kv(context, param, values, separator="-")

        mock = MagicMock()

        @click.command()
        @click.option("--value", callback=split_kv_dash, multiple=True)
        def cli(value):
            mock(value)

        result = self.runner.invoke(cli, ["--value", "first-1"])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with({"first": "1"})

    def test_separatorFunctoolsDash(self):
        mock = MagicMock()

        @click.command()
        @click.option("--value", callback=partial(split_kv, separator="-"), multiple=True)
        def cli(value):
            mock(value)

        result = self.runner.invoke(cli, ["--value", "first-1", "--value", "second-2"])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with({"first": "1", "second": "2"})

    def test_separatorSpace(self):
        @click.command()
        @click.option("--value", callback=partial(split_kv, separator=" "), multiple=True)
        def cli(value):
            pass

        result = self.runner.invoke(cli, ["--value", "first 1"])
        self.assertEqual(str(result.exception), "' ' is not a supported separator for key-value pairs.")

    def test_separatorComma(self):
        @click.command()
        @click.option("--value", callback=partial(split_kv, separator=","), multiple=True)
        def cli(value):
            pass

        result = self.runner.invoke(cli, ["--value", "first,1"])
        self.assertEqual(str(result.exception), "',' is not a supported separator for key-value pairs.")

    def test_normalizeWithoutChoice(self):
        """Test that normalize=True without Choice fails gracefully.

        Normalize uses values in the provided Choice to create the normalized
        value. Without a provided Choice, it can't normalize. Verify that this
        does not cause a crash or other bad behavior, it just doesn't normalize
        anything.
        """
        mock = MagicMock()

        @click.command()
        @click.option("--value", callback=partial(split_kv, normalize=True))
        def cli(value):
            mock(value)

        result = self.runner.invoke(cli, ["--value", "foo=bar"])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with(dict(foo="bar"))

    def test_addToDefaultValue(self):
        """Verify that if add_to_default is True that passed-in values are
        added to the default value set in the option.
        """
        mock = MagicMock()

        @click.command()
        @click.option(
            "--value",
            callback=partial(split_kv, add_to_default=True, unseparated_okay=True),
            default=["INFO"],
            multiple=True,
        )
        def cli(value):
            mock(value)

        result = self.runner.invoke(cli, ["--value", "lsst.daf.butler=DEBUG"])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with({"": "INFO", "lsst.daf.butler": "DEBUG"})

    def test_replaceDefaultValue(self):
        """Verify that if add_to_default is False (this is the default value),
        that passed-in values replace any default value, even if keys are
        different.
        """
        mock = MagicMock()

        @click.command()
        @click.option(
            "--value", callback=partial(split_kv, unseparated_okay=True), default=["INFO"], multiple=True
        )
        def cli(value):
            mock(value)

        result = self.runner.invoke(cli, ["--value", "lsst.daf.butler=DEBUG"])
        self.assertEqual(result.exit_code, 0, msg=clickResultMsg(result))
        mock.assert_called_with({"lsst.daf.butler": "DEBUG"})


if __name__ == "__main__":
    unittest.main()
