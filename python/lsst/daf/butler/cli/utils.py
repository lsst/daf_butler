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
import click.testing
from contextlib import contextmanager
import copy
from functools import partial
import io
import os
import textwrap
import traceback
from unittest.mock import MagicMock, patch
import uuid
import yaml

from .cliLog import CliLog
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

    @classmethod
    def reset(cls):
        cls.mock.reset_mock()


class LogCliRunner(click.testing.CliRunner):
    """A test runner to use when the logging system will be initialized by code
    under test, calls CliLog.resetLog(), which undoes any logging setup that
    was done with the CliLog interface.

    lsst.log modules can not be set back to an uninitialized state (python
    logging modules can be set back to NOTSET), instead they are set to
    `CliLog.defaultLsstLogLevel`."""

    def invoke(self, *args, **kwargs):
        result = super().invoke(*args, **kwargs)
        CliLog.resetLog()
        return result


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
    msg = f"""\noutput: {result.output}\nexception: {result.exception}"""
    if result.exception:
        msg += f"""\ntraceback: {"".join(traceback.format_tb(result.exception.__traceback__))}"""
    return msg


@contextmanager
def command_test_env(runner, commandModule, commandName):
    """A context manager that creates (and then cleans up) an environment that
    provides a CLI plugin command with the given name.

    Parameters
    ----------
    runner : click.testing.CliRunner
        The test runner to use to create the isolated filesystem.
    commandModule : `str`
        The importable module that the command can be imported from.
    commandName : `str`
        The name of the command being published to import.
    """
    with runner.isolated_filesystem():
        with open("resources.yaml", "w") as f:
            f.write(yaml.dump({"cmd": {"import": commandModule, "commands": [commandName]}}))
        # Add a colon to the end of the path on the next line, this tests the
        # case where the lookup in LoaderCLI._getPluginList generates an empty
        # string in one of the list entries and verifies that the empty string
        # is properly stripped out.
        with patch.dict("os.environ", {"DAF_BUTLER_PLUGINS": f"{os.path.realpath(f.name)}:"}):
            yield


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
             unseparated_okay=False, return_type=dict, default_key="", reverse_kv=False):
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
        `click.Choice` instance. If None, no verification will be done. By
        default None
    multiple : `bool`, optional
        If true, the value may contain multiple comma-separated values. By
        default True.
    normalize : `bool`, optional
        If True and `choice.case_sensitive == False`, normalize the string the
        user provided to match the choice's case. By default False.
    separator : str, optional
        The character that separates key-value pairs. May not be a comma or an
        empty space (for space separators use Click's default implementation
        for tuples; `type=(str, str)`). By default "=".
    unseparated_okay : `bool`, optional
        If True, allow values that do not have a separator. They will be
        returned in the values dict as a tuple of values in the key '', that
        is: `values[''] = (unseparated_values, )`. By default False.
    return_type : `type`, must be `dict` or `tuple`
        The type of the value that should be returned.
        If `dict` then the returned object will be a dict, for each item in
        values, the value to the left of the separator will be the key and the
        value to the right of the separator will be the value.
        If `tuple` then the returned object will be a tuple. Each item in the
        tuple will be 2-item tuple, the first item will be the value to the
        left of the separator and the second item will be the value to the
        right. By default `dict`.
    default_key : `Any`
        The key to use if a value is passed that is not a key-value pair.
        (Passing values that are not key-value pairs requires
        ``unseparated_okay`` to be `True`.)
    reverse_kv : bool
        If true then for each item in values, the value to the left of the
        separator is treated as the value and the value to the right of the
        separator is treated as the key. By default False.

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

    class RetDict:

        def __init__(self):
            self.ret = {}

        def add(self, key, val):
            if reverse_kv:
                key, val = val, key
            if key in self.ret:
                raise click.ClickException(f"Duplicate entries for '{k}' in '{values}'")
            self.ret[key] = val

        def get(self):
            return self.ret

    class RetTuple:

        def __init__(self):
            self.ret = []

        def add(self, key, val):
            if reverse_kv:
                key, val = val, key
            self.ret.append((key, val))

        def get(self):
            return tuple(self.ret)

    if separator in (",", " "):
        raise RuntimeError(f"'{separator}' is not a supported separator for key-value pairs.")
    vals = values  # preserve the original argument for error reporting below.
    if return_type is dict:
        ret = RetDict()
    elif return_type is tuple:
        ret = RetTuple()
    else:
        raise click.ClickException(f"Internal error: invalid return type '{return_type}' for split_kv.")
    if multiple:
        vals = split_commas(context, param, vals)
    for val in iterable(vals):
        if unseparated_okay and separator not in val:
            if choice is not None:
                choice(val)  # will raise if val is an invalid choice
            ret.add(default_key, norm(val))
        else:
            try:
                k, v = val.split(separator)
                if choice is not None:
                    choice(v)  # will raise if val is an invalid choice
            except ValueError:
                raise click.ClickException(
                    f"Could not parse key-value pair '{val}' using separator '{separator}', "
                    f"with multiple values {'allowed' if multiple else 'not allowed'}.")
            ret.add(k, norm(v))
    return ret.get()


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
    def splitSection(val):
        if not val.startswith("\n"):
            firstLine, _, val = val.partition("\n")
            firstLine += " "
        else:
            firstLine = ""
        return (firstLine + textwrap.dedent(val).replace("\n", " ")).strip()

    return "\n\n".join([splitSection(s) for s in val.split("\n\n")])


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


class option_section:  # noqa: N801
    """Decorator to add a section label between options in the help text of a
    command.

    Parameters
    ----------
    sectionText : `str`
        The text to print in the section identifier.
    """

    def __init__(self, sectionText):
        self.sectionText = "\n" + sectionText

    def __call__(self, f):
        # Generate a parameter declaration that will be unique for this
        # section.
        return click.option(f"--option-section-{str(uuid.uuid4())}",
                            sectionText=self.sectionText,
                            cls=OptionSection)(f)


class MWPath(click.Path):
    """Overrides click.Path to implement file-does-not-exist checking.

    Changes the definition of ``exists` so that `True` indicates the location
    (file or directory) must exist, `False` indicates the location must *not*
    exist, and `None` indicates that the file may exist or not. The standard
    definition for the `click.Path` ``exists`` parameter is that for `True` a
    location must exist, but `False` means it is not required to exist (not
    that it is required to not exist).

    Parameters
    ----------
    exists : `True`, `False`, or `None`
        If `True`, the location (file or directory) indicated by the caller
        must exist. If `False` the location must not exist. If `None`, the
        location may exist or not.

    For other parameters see `click.Path`.
    """

    def __init__(self, exists=None, file_okay=True, dir_okay=True,
                 writable=False, readable=True, resolve_path=False,
                 allow_dash=False, path_type=None):
        self.mustNotExist = exists is False
        if exists is None:
            exists = False
        super().__init__(exists, file_okay, dir_okay, writable, readable,
                         resolve_path, allow_dash, path_type)

    def convert(self, value, param, ctx):
        """Called by click.ParamType to "convert values through types".
        `click.Path` uses this step to verify Path conditions."""
        if self.mustNotExist and os.path.exists(value):
            self.fail(f'{self.path_type} "{value}" should not exist.')
        return super().convert(value, param, ctx)


class MWOption(click.Option):
    """Overrides click.Option with desired behaviors."""

    def __init__(self, *args, forward=False, **kwargs):
        # `forward` indicates weather a subcommand should forward the value of
        # this option to the next subcommand or not.
        self.forward = forward
        super().__init__(*args, **kwargs)

    def make_metavar(self):
        """Overrides `click.Option.make_metavar`. Makes the metavar for the
        help menu. Adds a space and an elipsis after the metavar name if
        the option accepts multiple inputs, otherwise defers to the base
        implementation.

        By default click does not add an elipsis when multiple is True and
        nargs is 1. And when nargs does not equal 1 click adds an elipsis
        without a space between the metavar and the elipsis, but we prefer a
        space between.

        Does not get called for some option types (e.g. flag) so metavar
        transformation that must apply to all types should be applied in
        get_help_record.
        """
        metavar = super().make_metavar()
        if self.multiple and self.nargs == 1:
            metavar += " ..."
        elif self.nargs != 1:
            metavar = f"{metavar[:-3]} ..."
        return metavar

    def get_help_record(self, ctx):
        """Overrides `click.Option.get_help_record`. Adds "(f)" to this
        option's metavar text if its associated subcommand forwards this option
        value to the next subcommand."""
        rv = super().get_help_record(ctx)
        if self.forward:
            rv = (rv[0] + " (f)",) + rv[1:]
        return rv


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


class OptionSection(MWOption):
    """Implements an Option that prints a section label in the help text and
    does not pass any value to the command function.

    This class does a bit of hackery to add a section label to a click command
    help output: first, `expose_value` is set to `False` so that no value is
    passed to the command function. Second, this class overrides
    `click.Option.get_help_record` to return the section label string without
    any prefix so that it stands out as a section label.

    The intention for this implementation is to do minimally invasive overrides
    of the click classes so as to be robust and easy to fix if the click
    internals change.

    Parameters
    ----------
    sectionName : `str`
        The parameter declaration for this option. It is not shown to the user,
        it must be unique within the command. If using the `section` decorator
        to add a section to a command's options, the section name is
        auto-generated.
    sectionText : `str`
        The text to print in the section identifier.
    """

    def __init__(self, sectionName, sectionText):
        super().__init__(sectionName, expose_value=False)
        self.sectionText = sectionText

    def get_help_record(self, ctx):
        return (self.sectionText, "")


class MWOptionDecorator:
    """Wraps the click.option decorator to enable shared options to be declared
    and allows inspection of the shared option.
    """

    def __init__(self, *param_decls, **kwargs):
        forward = kwargs.pop("forward", False)
        self.partialOpt = partial(click.option, *param_decls, cls=partial(MWOption, forward=forward),
                                  **kwargs)
        opt = click.Option(param_decls, **kwargs)
        self._name = opt.name
        self._opts = opt.opts

    def name(self):
        """Get the name that will be passed to the command function for this
        option."""
        return self._name

    def opts(self):
        """Get the flags that will be used for this option on the command
        line."""
        return self._opts

    def __call__(self, *args, **kwargs):
        return self.partialOpt(*args, **kwargs)


class MWArgumentDecorator:
    """Wraps the click.argument decorator to enable shared arguments to be
    declared. """

    def __init__(self, *param_decls, **kwargs):
        self._helpText = kwargs.pop("help", None)
        self.partialArg = partial(click.argument, *param_decls, cls=MWArgument, **kwargs)

    def __call__(self, *args, help=None, **kwargs):
        def decorator(f):
            if help is not None:
                self._helpText = help
            if self._helpText:
                f.__doc__ = addArgumentHelp(f.__doc__, self._helpText)
            return self.partialArg(*args, **kwargs)(f)
        return decorator


class MWCommand(click.Command):
    """Command subclass that stores a copy of the args list for use by the
    command."""

    def parse_args(self, ctx, args):
        MWCtxObj.getFrom(ctx).args = copy.copy(args)
        super().parse_args(ctx, args)


class MWCtxObj():
    """Helper object for managing the `click.Context.obj` parameter, allows
    obj data to be managed in a consistent way.

    `Context.obj` defaults to None. `MWCtxObj.getFrom(ctx)` can be used to
    initialize the obj if needed and return a new or existing MWCtxObj.

    Attributes
    ----------
    args : `list` [`str`]
        The list of arguments (argument values, option flags, and option
        values), split using whitespace, that were passed in on the command
        line for the subcommand represented by the parent context object.
    """

    def __init__(self):

        self.args = None

    @staticmethod
    def getFrom(ctx):
        """If needed, initialize `ctx.obj` with a new MWCtxObj, and return the
        new or already existing MWCtxObj."""
        if ctx.obj is not None:
            return ctx.obj
        ctx.obj = MWCtxObj()
        return ctx.obj


class ForwardOptions:
    """Captures CLI options to be forwarded from one subcommand to future
    subcommands executed as part of a single CLI command invocation
    (called "chained commands").

    Attributes
    ----------
    functionKwargs : `dict` [`str`, `str`]
        The cached kwargs (argument name and argument value) from subcommands
        that were called with values passed in as options on the command line.
    """

    def __init__(self):
        self.functionKwargs = {}

    @staticmethod
    def _getKwargsToSave(mwOptions, arguments, **kwargs):
        """Get kwargs that should be cached for use by subcommands invoked in
        the future.

        Parameters
        ----------
        mwOptions : `list` or `tuple` [`MWOption`]
            The options supported by the current command. For each option, its
            kwarg will be cached if the option's `forward` parameter is `True`.
        arguments : `list` [`str`]
            The arguments that were passed in on the command line for the
            current subcommand, split on whitespace into a list of arguments,
            option flags, and option values.

        Returns
        -------
        `dict` [`str`, `str`]
            The kwargs that should be cached.
        """
        saveableOptions = [opt for opt in mwOptions if opt.forward]
        argumentSet = set(arguments)
        passedOptions = [opt for opt in saveableOptions if set(opt.opts).intersection(argumentSet)]
        return {opt.name: kwargs[opt.name] for opt in passedOptions}

    def _getKwargsToUse(self, mwOptions, arguments, **kwargs):
        """Get kwargs that should be used by the current subcommand.

        Parameters
        ----------
        mwOptions : `list` or `tuple` [`MWOption`]
            The options supported by the current subcommand.
        arguments : `list` [`str`]
            The arguments that were passed in on the command line for the
            current subcommand, split on whitespace into a list of arguments,
            option flags, and option values.

        Returns
        -------
        `dict` [`str`, `str`]
            kwargs that add the cached kwargs accepted by the current command
            to the kwargs that were provided by CLI arguments. When a kwarg is
            present in both places, the CLI argument kwarg value is used.
        """
        argumentSet = set(arguments)
        # get the cached value for options that were not passed in as CLI
        # arguments:
        cachedValues = {opt.name: self.functionKwargs[opt.name] for opt in mwOptions
                        if not set(opt.opts).intersection(argumentSet) and opt.name in self.functionKwargs}
        updatedKwargs = copy.copy(kwargs)
        updatedKwargs.update(cachedValues)
        return updatedKwargs

    def _save(self, mwOptions, arguments, **kwargs):
        """Save the current forwardable CLI options.

        Parameters
        ----------
        mwOptions : `list` or `tuple` [`MWOption`]
            The options, which are accepted by the current command, that may be
            saved.
        arguments : `list` [`str`]
            The arguments that were passed in on the command line for the
            current subcommand, split on whitespace into a list of arguments,
            option flags, and option values.
        kwargs : `dict` [`str`, `str`]
            Arguments that were passed into a command function. Indicated
            option arguments will be extracted and cached.
        """
        self.functionKwargs.update(self._getKwargsToSave(mwOptions, arguments, **kwargs))

    def update(self, mwOptions, arguments, **kwargs):
        """Update what options are forwarded, drop non-forwarded options, and
        update cached kwargs with new values in kwargs, and returns a new dict
        that adds cached kwargs to the passed-in kwargs.

        Parameters
        ----------
        mwOptions : `list` or `tuple` [`MWOption`]
            The options that will be forwarded.
        arguments : `list` [`str`]
            The arguments that were passed in on the command line for the
            current subcommand, split on whitespace into a list of arguments,
            option flags, and option values.
        kwargs : `dict` [`str`, `str`]
            Arguments that were passed into a command function. A new dict will
            be created and returned that adds cached kwargs to the passed-in
            non-default-value kwargs.

        Returns
        -------
        kwargs : dict [`str`, `str`]
            kwargs that add the cached kwargs accepted by the current command
            to the kwargs that were provided by CLI arguments. When a kwarg is
            present in both places, the CLI argument kwarg value is used.
        """
        kwargsToUse = self._getKwargsToUse(mwOptions, arguments, **kwargs)
        self._save(mwOptions, arguments, **kwargs)
        return kwargsToUse
