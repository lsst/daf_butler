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

import click
import enum
import io
import os
import textwrap
import traceback
from unittest.mock import MagicMock

from ..core.utils import iterable


# CLI_MOCK_ENV is set by some tests as an environment variable, it
# indicates to the cli_handle_exception function that instead of executing the
# command implementation function it should use the Mocker class for unit test
# verification.
mockEnvVarKey = "CLI_MOCK_ENV"
mockEnvVar = {mockEnvVarKey: "1"}

# This is used as the metavar argument to Options that accept multiple string
# inputs, which may be comma-separarated. For example:
# --my-opt foo,bar --my-opt baz.
# Other arguments to the Option should include multiple=true and
# callback=split_kv.
typeStrAcceptsMultiple = "TEXT ..."
typeStrAcceptsSingle = "TEXT"


def textTypeStr(multiple):
    """Get the text type string for CLI help documentation.

    Parameters
    ----------
    multiple : `bool`
        True if multiple text values are allowed, False if only one value is
        allowed.

    Returns
    -------
    textTypeStr : `str`
        The type string to use.
    """
    return typeStrAcceptsMultiple if multiple else typeStrAcceptsSingle


# For parameters that support key-value inputs, this defines the separator
# for those inputs.
split_kv_separator = "="


# The ParameterType enum is used to indicate a click Argument or Option (both
# of which are subclasses of click.Parameter).
class ParameterType(enum.Enum):
    ARGUMENT = 0
    OPTION = 1


class Mocker:

    mock = MagicMock()

    def __init__(self, *args, **kwargs):
        """Mocker is a helper class for unit tests. It can be imported and
        called and later imported again and call can be verified.

        For convenience, constructor arguments are forwarded to the call
        function.
        """
        self.__call__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        """Creates a MagicMock and stores it in a static variable that can
        later be verified.
        """
        Mocker.mock(*args, **kwargs)


def clickResultMsg(result):
    """Get a standard assert message from a click result

    Parameters
    ----------
    result : click.Result
        The result object returned from click.testing.CliRunner.invoke

    Returns
    -------
    msg : `str`
        The message string.
    """
    msg = io.StringIO()
    if result.exception:
        traceback.print_tb(result.exception.__traceback__, file=msg)
        msg.seek(0)
    return f"\noutput: {result.output}\nexception: {result.exception}\ntraceback: {msg.read()}"


def addArgumentHelp(doc, helpText):
    """Add a Click argument's help message to a function's documentation.

    This is needed because click presents arguments in the order the argument
    decorators are applied to a function, top down. But, the evaluation of the
    decorators happens bottom up, so if arguments just append their help to the
    function's docstring, the argument descriptions appear in reverse order
    from the order they are applied in.

    Parameters
    ----------
    doc : `str`
        The function's docstring.
    helpText : `str`
        The argument's help string to be inserted into the function's
        docstring.

    Returns
    -------
    doc : `str`
        Updated function documentation.
    """
    if doc is None:
        doc = helpText
    else:
        doclines = doc.splitlines()
        doclines.insert(1, helpText)
        doclines.insert(1, "\n")
        doc = "\n".join(doclines)
    return doc


def split_commas(context, param, values):
    """Process a tuple of values, where each value may contain comma-separated
    values, and return a single list of all the passed-in values.

    This function can be passed to the 'callback' argument of a click.option to
    allow it to process comma-separated values (e.g. "--my-opt a,b,c").

    Parameters
    ----------
    context : `click.Context` or `None`
        The current execution context. Unused, but Click always passes it to
        callbacks.
    param : `click.core.Option` or `None`
        The parameter being handled. Unused, but Click always passes it to
        callbacks.
    values : [`str`]
        All the values passed for this option. Strings may contain commas,
        which will be treated as delimiters for separate values.

    Returns
    -------
    list of string
        The passed in values separated by commas and combined into a single
        list.
    """
    if values is None:
        return values
    valueList = []
    for value in iterable(values):
        valueList.extend(value.split(","))
    return tuple(valueList)


def split_kv(context, param, values, choice=None, multiple=True, normalize=False, separator="=",
             unseparated_okay=False):
    """Process a tuple of values that are key-value pairs separated by a given
    separator. Multiple pairs may be comma separated. Return a dictionary of
    all the passed-in values.

    This function can be passed to the 'callback' argument of a click.option to
    allow it to process comma-separated values (e.g. "--my-opt a=1,b=2").

    Parameters
    ----------
    context : `click.Context` or `None`
        The current execution context. Unused, but Click always passes it to
        callbacks.
    param : `click.core.Option` or `None`
        The parameter being handled. Unused, but Click always passes it to
        callbacks.
    values : [`str`]
        All the values passed for this option. Strings may contain commas,
        which will be treated as delimiters for separate values.
    choice : `click.Choice`, optional
        If provided, verify each value is a valid choice using the provided
        `click.Choice` instance. If None, no verification will be done.
    multiple : `bool`, optional
        If true, the value may contain multiple comma-separated values.
    normalize : `bool`, optional
        If True and `choice.case_sensitive == False`, normalize the string the
        user provided to match the choice's case.
    separator : str, optional
        The character that separates key-value pairs. May not be a comma or an
        empty space (for space separators use Click's default implementation
        for tuples; `type=(str, str)`). By default "=".
    unseparated_okay : `bool`, optional
        If True, allow values that do not have a separator. They will be
        returned in the values dict as a tuple of values in the key '', that
        is: `values[''] = (unseparated_values, )`.

    Returns
    -------
    values : `dict` [`str`, `str`]
        The passed-in values in dict form.

    Raises
    ------
    `click.ClickException`
        Raised if the separator is not found in an entry, or if duplicate keys
        are encountered.
    """

    def norm(val):
        """If `normalize` is True and `choice` is not `None`, find the value
        in the available choices and return the value as spelled in the
        choices.

        Assumes that val exists in choices; `split_kv` uses the `choice`
        instance to verify val is a valid choice.
        """
        if normalize and choice is not None:
            v = val.casefold()
            for opt in choice.choices:
                if opt.casefold() == v:
                    return opt
        return val

    if separator in (",", " "):
        raise RuntimeError(f"'{separator}' is not a supported separator for key-value pairs.")
    vals = values  # preserve the original argument for error reporting below.
    if multiple:
        vals = split_commas(context, param, vals)
    ret = {}
    for val in iterable(vals):
        if unseparated_okay and separator not in val:
            if choice is not None:
                choice(val)  # will raise if val is an invalid choice
            ret[""] = norm(val)
        else:
            try:
                k, v = val.split(separator)
                if choice is not None:
                    choice(v)  # will raise if val is an invalid choice
            except ValueError:
                raise click.ClickException(
                    f"Could not parse key-value pair '{val}' using separator '{separator}', "
                    f"with multiple values {'allowed' if multiple else 'not allowed'}.")
            if k in ret:
                raise click.ClickException(f"Duplicate entries for '{k}' in '{values}'")
            ret[k] = norm(v)
    return ret


def to_upper(context, param, value):
    """Convert a value to upper case.

    Parameters
    ----------
    context : click.Context

    values : string
        The value to be converted.

    Returns
    -------
    string
        A copy of the passed-in value, converted to upper case.
    """
    return value.upper()


def unwrap(val):
    """Remove newlines and leading whitespace from a multi-line string with
    a consistent indentation level.

    The first line of the string may be only a newline or may contain text
    followed by a newline, either is ok. After the first line, each line must
    begin with a consistant amount of whitespace. So, content of a
    triple-quoted string may begin immediately after the quotes, or the string
    may start with a newline. Each line after that must be the same amount of
    indentation/whitespace followed by text and a newline. The last line may
    end with a new line but is not required to do so.

    Parameters
    ----------
    val : `str`
        The string to change.

    Returns
    -------
    strippedString : `str`
        The string with newlines, indentation, and leading and trailing
        whitespace removed.
    """
    if not val.startswith("\n"):
        firstLine, _, val = val.partition("\n")
        firstLine += " "
    else:
        firstLine = ""
    return (firstLine + textwrap.dedent(val).replace("\n", " ")).strip()


def cli_handle_exception(func, *args, **kwargs):
    """Wrap a function call in an exception handler that raises a
    ClickException if there is an Exception.

    Also provides support for unit testing by testing for an environment
    variable, and if it is present prints the function name, args, and kwargs
    to stdout so they can be read and verified by the unit test code.

    Parameters
    ----------
    func : function
        A function to be called and exceptions handled. Will pass args & kwargs
        to the function.

    Returns
    -------
    The result of calling func.

    Raises
    ------
    click.ClickException
        An exception to be handled by the Click CLI tool.
    """
    if mockEnvVarKey in os.environ:
        Mocker(*args, **kwargs)
        return
    try:
        return func(*args, **kwargs)
    except Exception:
        msg = io.StringIO()
        msg.write("An error occurred during command execution:\n")
        traceback.print_exc(file=msg)
        raise click.ClickException(msg.getvalue())


class MWOption(click.Option):
    """Overrides click.Option with desired behaviors."""

    def make_metavar(self):
        """Overrides `click.Option.make_metavar`. Makes the metavar for the
        help menu. Adds a space and an elipsis after the metavar name if
        the option accepts multiple inputs, otherwise defers to the base
        implementation.

        By default click does not add an elipsis when multiple is True and
        nargs is 1. And when nargs does not equal 1 click adds an elipsis
        without a space between the metavar and the elipsis, but we prefer a
        space between.
        """
        metavar = super().make_metavar()
        if self.multiple and self.nargs == 1:
            metavar += " ..."
        elif self.nargs != 1:
            metavar = f"{metavar[:-3]} ..."
        return metavar


class MWArgument(click.Argument):
    """Overrides click.Argument with desired behaviors."""

    def make_metavar(self):
        """Overrides `click.Option.make_metavar`. Makes the metavar for the
        help menu. Always adds a space and an elipsis (' ...') after the
        metavar name if the option accepts multiple inputs.

        By default click adds an elipsis without a space between the metavar
        and the elipsis, but we prefer a space between.

        Returns
        -------
        metavar : `str`
            The metavar value.
        """
        metavar = super().make_metavar()
        if self.nargs != 1:
            metavar = f"{metavar[:-3]} ..."
        return metavar
