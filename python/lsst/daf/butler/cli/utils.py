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
import os

from ..core.utils import iterable


# DAF_BUTLER_MOCK is set by some tests as an environment variable and indicates
# to the cli_handle_exception function that instead of executing the command
# implementation function it should print details about the called command to
# stdout. These details are then used to verify the command function was loaded
# and received expected inputs.
DAF_BUTLER_MOCK = {"DAF_BUTLER_MOCK": ""}


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
    valueList = []
    for value in iterable(values):
        valueList.extend(value.split(","))
    return valueList


def split_kv(context, param, values, separator="="):
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
    separator : str, optional
        The character that separates key-value pairs. May not be a comma or an
        empty space (for space separators use Click's default implementation
        for tuples; `type=(str, str)`). By default "=".

    Returns
    -------
    `dict` : [`str`, `str`]
        The passed-in values in dict form.

    Raises
    ------
    `click.ClickException`
        Raised if the separator is not found in an entry, or if duplicate keys
        are encountered.
    """
    if "," == separator or " " == separator:
        raise RuntimeError(f"'{separator}' is not a supported separator for key-value pairs.")
    vals = split_commas(context, param, values)
    ret = {}
    for val in vals:
        try:
            k, v = val.split(separator)
        except ValueError:
            raise click.ClickException(f"Missing or invalid key-value separator in value '{val}'")
        if k in ret:
            raise click.ClickException(f"Duplicate entries for '{k}' in '{values}'")
        ret[k] = v
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


def printFunctionInfo(func, *args, **kwargs):
    """For unit testing butler subcommand call execution, write a dict to
    stdout that formats information about a funciton call into a dict that can
    be evaluated by `verifyFunctionInfo`

    Parameters
    ----------
    func : function
        The function that has been called, whose name should be written.
    args : [`str`]
        The values of the arguments the function was called with.
    kwags : `dict` [`str`, `str`]
        The names and values of the kwargs the function was called with.
    """
    print(dict(function=func.__name__,
               args=args,
               kwargs=kwargs))


def verifyFunctionInfo(testSuite, output, function, expectedArgs, expectedKwargs):
    """For unit testing butler subcommand call execution, compare a dict that
    has been printed to stdout to expected data.

    Parameters
    ----------
    testSuite : `unittest.Testsuite`
        The test suite that is executing a unit test.
    output : `str`
        The dict that has been printed to stdout. It should be formatted to
        re-instantiate by calling `eval`.
    function : `str`
        The name of the function that was was expected to have been called.
    expectedArgs : [`str`]
        The values of the arguments that should have been passed to the
        function.
    expectedKwargs : `dict` [`str`, `str`]
        The names and values of the kwargs that should have been passsed to the
        funciton.
    """
    calledWith = eval(output)
    testSuite.assertEqual(calledWith['function'], function)
    testSuite.assertEqual(calledWith['args'], expectedArgs)
    testSuite.assertEqual(calledWith['kwargs'], expectedKwargs)


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
    # "DAF_BUTLER_MOCK" matches the key in the variable DAF_BUTLER_MOCK,
    # defined in the top of this file.
    if "DAF_BUTLER_MOCK" in os.environ:
        printFunctionInfo(func, *args, **kwargs)
        return
    try:
        return func(*args, **kwargs)
    except Exception as err:
        raise click.ClickException(err)
