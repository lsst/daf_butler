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
from __future__ import annotations

__all__ = (
    "astropyTablesToStr",
    "printAstropyTables",
    "textTypeStr",
    "LogCliRunner",
    "clickResultMsg",
    "command_test_env",
    "addArgumentHelp",
    "split_commas",
    "split_kv",
    "to_upper",
    "unwrap",
    "option_section",
    "MWPath",
    "MWOption",
    "MWArgument",
    "OptionSection",
    "MWOptionDecorator",
    "MWArgumentDecorator",
    "MWCommand",
    "ButlerCommand",
    "OptionGroup",
    "MWCtxObj",
    "yaml_presets",
    "sortAstropyTable",
    "catch_and_exit",
)


import itertools
import logging
import os
import re
import sys
import textwrap
import traceback
import uuid
import warnings
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from functools import partial, wraps
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import patch

import click
import click.exceptions
import click.testing
import yaml
from lsst.utils.iteration import ensure_iterable

from ..core.config import Config
from .cliLog import CliLog

if TYPE_CHECKING:
    from astropy.table import Table
    from lsst.daf.butler import Dimension

log = logging.getLogger(__name__)

# This is used as the metavar argument to Options that accept multiple string
# inputs, which may be comma-separarated. For example:
# --my-opt foo,bar --my-opt baz.
# Other arguments to the Option should include multiple=true and
# callback=split_kv.
typeStrAcceptsMultiple = "TEXT ..."
typeStrAcceptsSingle = "TEXT"

# The standard help string for the --where option when it takes a WHERE clause.
where_help = (
    "A string expression similar to a SQL WHERE clause. May involve any column of a "
    "dimension table or a dimension name as a shortcut for the primary key column of a "
    "dimension table."
)


def astropyTablesToStr(tables: list[Table]) -> str:
    """Render astropy tables to string as they are displayed in the CLI.

    Output formatting matches ``printAstropyTables``.
    """
    ret = ""
    for table in tables:
        ret += "\n"
        table.pformat_all()
    ret += "\n"
    return ret


def printAstropyTables(tables: list[Table]) -> None:
    """Print astropy tables to be displayed in the CLI.

    Output formatting matches ``astropyTablesToStr``.
    """
    for table in tables:
        print("")
        table.pprint_all()
    print("")


def textTypeStr(multiple: bool) -> str:
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


class LogCliRunner(click.testing.CliRunner):
    """A test runner to use when the logging system will be initialized by code
    under test, calls CliLog.resetLog(), which undoes any logging setup that
    was done with the CliLog interface.

    lsst.log modules can not be set back to an uninitialized state (python
    logging modules can be set back to NOTSET), instead they are set to
    `CliLog.defaultLsstLogLevel`."""

    def invoke(self, *args: Any, **kwargs: Any) -> Any:
        result = super().invoke(*args, **kwargs)
        CliLog.resetLog()
        return result


def clickResultMsg(result: click.testing.Result) -> str:
    """Get a standard assert message from a click result

    Parameters
    ----------
    result : click.testing.Result
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
def command_test_env(runner: click.testing.CliRunner, commandModule: str, commandName: str) -> Iterator[None]:
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


def addArgumentHelp(doc: str | None, helpText: str) -> str:
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
        # See click documentation for details:
        # https://click.palletsprojects.com/en/7.x/documentation/#truncating-help-texts
        # In short, text for the click command help can be truncated by putting
        # "\f" in the docstring, everything after it should be removed
        if "\f" in doc:
            doc = doc.split("\f")[0]

        doclines = doc.splitlines()
        # The function's docstring may span multiple lines, so combine the
        # docstring from all the first lines until a blank line is encountered.
        # (Lines after the first blank line will be argument help.)
        while len(doclines) > 1 and doclines[1]:
            doclines[0] = " ".join((doclines[0], doclines.pop(1).strip()))
        # Add standard indent to help text for proper alignment with command
        # function documentation:
        helpText = "    " + helpText
        doclines.insert(1, helpText)
        doclines.insert(1, "\n")
        doc = "\n".join(doclines)
    return doc


def split_commas(
    context: click.Context | None, param: click.core.Option | None, values: str | Iterable[str] | None
) -> tuple[str, ...]:
    """Process a tuple of values, where each value may contain comma-separated
    values, and return a single list of all the passed-in values.

    This function can be passed to the 'callback' argument of a click.option to
    allow it to process comma-separated values (e.g. "--my-opt a,b,c"). If
    the comma is inside ``[]`` there will be no splitting.

    Parameters
    ----------
    context : `click.Context` or `None`
        The current execution context. Unused, but Click always passes it to
        callbacks.
    param : `click.core.Option` or `None`
        The parameter being handled. Unused, but Click always passes it to
        callbacks.
    values : iterable of `str` or `str`
        All the values passed for this option. Strings may contain commas,
        which will be treated as delimiters for separate values unless they
        are within ``[]``.

    Returns
    -------
    results : `tuple` [`str`]
        The passed in values separated by commas where appropriate and
        combined into a single tuple.
    """
    if values is None:
        return tuple()
    valueList = []
    for value in ensure_iterable(values):
        # If we have [, or ,] we do the slow split. If square brackets
        # are not matching then that is likely a typo that should result
        # in a warning.
        opens = "["
        closes = "]"
        if re.search(rf"\{opens}.*,|,.*\{closes}", value):
            in_parens = False
            current = ""
            for c in value:
                if c == opens:
                    if in_parens:
                        warnings.warn(
                            f"Found second opening {opens} without corresponding closing {closes}"
                            f" in {value!r}",
                            stacklevel=2,
                        )
                    in_parens = True
                elif c == closes:
                    if not in_parens:
                        warnings.warn(
                            f"Found second closing {closes} without corresponding open {opens} in {value!r}",
                            stacklevel=2,
                        )
                    in_parens = False
                elif c == ",":
                    if not in_parens:
                        # Split on this comma.
                        valueList.append(current)
                        current = ""
                        continue
                current += c
            if in_parens:
                warnings.warn(
                    f"Found opening {opens} that was never closed in {value!r}",
                    stacklevel=2,
                )
            if current:
                valueList.append(current)
        else:
            # Use efficient split since no parens.
            valueList.extend(value.split(","))
    return tuple(valueList)


def split_kv(
    context: click.Context,
    param: click.core.Option,
    values: list[str],
    *,
    choice: click.Choice | None = None,
    multiple: bool = True,
    normalize: bool = False,
    separator: str = "=",
    unseparated_okay: bool = False,
    return_type: type[dict] | type[tuple] = dict,
    default_key: str = "",
    reverse_kv: bool = False,
    add_to_default: bool = False,
) -> dict[str, str] | tuple[tuple[str, str], ...]:
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
    add_to_default : `bool`, optional
        If True, then passed-in values will not overwrite the default value
        unless the ``return_type`` is `dict` and passed-in value(s) have the
        same key(s) as the default value.

    Returns
    -------
    values : `dict` [`str`, `str`] or `tuple`[`tuple`[`str`, `str`], ...]
        The passed-in values in dict form or tuple form.

    Raises
    ------
    `click.ClickException`
        Raised if the separator is not found in an entry, or if duplicate keys
        are encountered.
    """

    def norm(val: str) -> str:
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
        def __init__(self) -> None:
            self.ret: dict[str, str] = {}

        def add(self, key: str, val: str) -> None:
            if reverse_kv:
                key, val = val, key
            self.ret[key] = val

        def get(self) -> dict[str, str]:
            return self.ret

    class RetTuple:
        def __init__(self) -> None:
            self.ret: list[tuple[str, str]] = []

        def add(self, key: str, val: str) -> None:
            if reverse_kv:
                key, val = val, key
            self.ret.append((key, val))

        def get(self) -> tuple[tuple[str, str], ...]:
            return tuple(self.ret)

    if separator in (",", " "):
        raise RuntimeError(f"'{separator}' is not a supported separator for key-value pairs.")
    vals = tuple(ensure_iterable(values))  # preserve the original argument for error reporting below.

    if add_to_default:
        default = param.get_default(context)
        if default:
            vals = tuple(v for v in itertools.chain(default, vals))  # Convert to tuple for mypy

    ret: RetDict | RetTuple
    if return_type is dict:
        ret = RetDict()
    elif return_type is tuple:
        ret = RetTuple()
    else:
        raise click.ClickException(
            message=f"Internal error: invalid return type '{return_type}' for split_kv."
        )
    if multiple:
        vals = split_commas(context, param, vals)
    for val in ensure_iterable(vals):
        if unseparated_okay and separator not in val:
            if choice is not None:
                choice(val)  # will raise if val is an invalid choice
            ret.add(default_key, norm(val))
        else:
            try:
                k, v = val.split(separator)
                if choice is not None:
                    choice(v)  # will raise if val is an invalid choice
            except ValueError as e:
                raise click.ClickException(
                    message=f"Could not parse key-value pair '{val}' using separator '{separator}', "
                    f"with multiple values {'allowed' if multiple else 'not allowed'}: {e}"
                )
            ret.add(k, norm(v))
    return ret.get()


def to_upper(context: click.Context, param: click.core.Option, value: str) -> str:
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


def unwrap(val: str) -> str:
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

    def splitSection(val: str) -> str:
        if not val.startswith("\n"):
            firstLine, _, val = val.partition("\n")
            firstLine += " "
        else:
            firstLine = ""
        return (firstLine + textwrap.dedent(val).replace("\n", " ")).strip()

    return "\n\n".join([splitSection(s) for s in val.split("\n\n")])


class option_section:  # noqa: N801
    """Decorator to add a section label between options in the help text of a
    command.

    Parameters
    ----------
    sectionText : `str`
        The text to print in the section identifier.
    """

    def __init__(self, sectionText: str) -> None:
        self.sectionText = "\n" + sectionText

    def __call__(self, f: Any) -> click.Option:
        # Generate a parameter declaration that will be unique for this
        # section.
        return click.option(
            f"--option-section-{str(uuid.uuid4())}", sectionText=self.sectionText, cls=OptionSection
        )(f)


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

    def __init__(
        self,
        exists: bool | None = None,
        file_okay: bool = True,
        dir_okay: bool = True,
        writable: bool = False,
        readable: bool = True,
        resolve_path: bool = False,
        allow_dash: bool = False,
        path_type: type | None = None,
    ):
        self.mustNotExist = exists is False
        if exists is None:
            exists = False
        super().__init__(
            exists=exists,
            file_okay=file_okay,
            dir_okay=dir_okay,
            writable=writable,
            readable=readable,
            resolve_path=resolve_path,
            allow_dash=allow_dash,
            path_type=path_type,
        )

    def convert(self, value: str, param: click.Parameter | None, ctx: click.Context | None) -> Any:
        """Called by click.ParamType to "convert values through types".
        `click.Path` uses this step to verify Path conditions."""
        if self.mustNotExist and os.path.exists(value):
            self.fail(f'Path "{value}" should not exist.')
        return super().convert(value, param, ctx)


class MWOption(click.Option):
    """Overrides click.Option with desired behaviors."""

    def make_metavar(self) -> str:
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


class MWArgument(click.Argument):
    """Overrides click.Argument with desired behaviors."""

    def make_metavar(self) -> str:
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

    This class overrides the hidden attribute because our documentation build
    tool, sphinx-click, implements its own `get_help_record` function which
    builds the record from other option values (e.g. `name`, `opts`), which
    breaks the hack we use to make `get_help_record` only return the
    `sectionText`. Fortunately, Click gets the value of `hidden` inside the
    `Option`'s `get_help_record`, and `sphinx-click` calls `opt.hidden` before
    entering its `_get_help_record` function. So, making the hidden property
    return True hides this option from sphinx-click, while allowing the section
    text to be returned by our `get_help_record` method when using Click.

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

    @property
    def hidden(self) -> bool:
        return True

    @hidden.setter
    def hidden(self, val: Any) -> None:
        pass

    def __init__(self, sectionName: str, sectionText: str) -> None:
        super().__init__(sectionName, expose_value=False)
        self.sectionText = sectionText

    def get_help_record(self, ctx: click.Context | None) -> tuple[str, str]:
        return (self.sectionText, "")


class MWOptionDecorator:
    """Wraps the click.option decorator to enable shared options to be declared
    and allows inspection of the shared option.
    """

    def __init__(self, *param_decls: Any, **kwargs: Any) -> None:
        self.partialOpt = partial(click.option, *param_decls, cls=partial(MWOption), **kwargs)
        opt = click.Option(param_decls, **kwargs)
        self._name = opt.name
        self._opts = opt.opts

    def name(self) -> str:
        """Get the name that will be passed to the command function for this
        option."""
        return cast(str, self._name)

    def opts(self) -> list[str]:
        """Get the flags that will be used for this option on the command
        line."""
        return self._opts

    @property
    def help(self) -> str:
        """Get the help text for this option. Returns an empty string if no
        help was defined."""
        return self.partialOpt.keywords.get("help", "")

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.partialOpt(*args, **kwargs)


class MWArgumentDecorator:
    """Wraps the click.argument decorator to enable shared arguments to be
    declared."""

    def __init__(self, *param_decls: Any, **kwargs: Any) -> None:
        self._helpText = kwargs.pop("help", None)
        self.partialArg = partial(click.argument, *param_decls, cls=MWArgument, **kwargs)

    def __call__(self, *args: Any, help: str | None = None, **kwargs: Any) -> Callable:
        def decorator(f: Any) -> Any:
            if help is not None:
                self._helpText = help
            if self._helpText:
                f.__doc__ = addArgumentHelp(f.__doc__, self._helpText)
            return self.partialArg(*args, **kwargs)(f)

        return decorator


class MWCommand(click.Command):
    """Command subclass that stores a copy of the args list for use by the
    command."""

    extra_epilog: str | None = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # wrap callback method with catch_and_exit decorator
        callback = kwargs.get("callback")
        if callback is not None:
            kwargs = kwargs.copy()
            kwargs["callback"] = catch_and_exit(callback)
        super().__init__(*args, **kwargs)

    def _capture_args(self, ctx: click.Context, args: list[str]) -> None:
        """Capture the command line options and arguments.

        See details about what is captured and the order in which it is stored
        in the documentation of `MWCtxObj`.

        Parameters
        ----------
        ctx : `click.Context`
            The current Context.
        args : `list` [`str`]
            The list of arguments from the command line, split at spaces but
            not at separators (like "=").
        """
        parser = self.make_parser(ctx)
        opts, _, param_order = parser.parse_args(args=list(args))
        # `param_order` is a list of click.Option and click.Argument, there is
        # one item for each time the Option or Argument was used on the
        # command line. Options will precede Arguments, within each sublist
        # they are in the order they were used on the command line. Note that
        # click.Option and click.Argument do not contain the value from the
        # command line; values are in `opts`.
        #
        # `opts` is a dict where the key is the argument name to the
        # click.Command function, this name matches the `click.Option.name` or
        # `click.Argument.name`. For Options, an item will only be present if
        # the Option was used on the command line. For Arguments, an item will
        # always be present and if no value was provided on the command line
        # the value will be `None`. If the option accepts multiple values, the
        # value in `opts` is a tuple, otherwise it is a single item.
        next_idx: Counter = Counter()
        captured_args = []
        for param in param_order:
            if isinstance(param, click.Option):
                param_name = cast(str, param.name)
                if param.multiple:
                    val = opts[param_name][next_idx[param_name]]
                    next_idx[param_name] += 1
                else:
                    val = opts[param_name]
                if param.is_flag:
                    # Bool options store their True flags in opts and their
                    # False flags in secondary_opts.
                    if val:
                        flag = max(param.opts, key=len)
                    else:
                        flag = max(param.secondary_opts, key=len)
                    captured_args.append(flag)
                else:
                    captured_args.append(max(param.opts, key=len))
                    captured_args.append(val)
            elif isinstance(param, click.Argument):
                param_name = cast(str, param.name)
                if (opt := opts[param_name]) is not None:
                    captured_args.append(opt)
            else:
                assert False  # All parameters should be an Option or an Argument.
        MWCtxObj.getFrom(ctx).args = captured_args

    def parse_args(self, ctx: click.Context, args: Any) -> list[str]:
        """Given a context and a list of arguments this creates the parser and
        parses the arguments, then modifies the context as necessary. This is
        automatically invoked by make_context().

        This function overrides `click.Command.parse_args`.

        The call to `_capture_args` in this override stores the arguments
        (option names, option value, and argument values) that were used by the
        caller on the command line in the context object. These stored
        arugments can be used by the command function, e.g. to process options
        in the order they appeared on the command line (pipetask uses this
        feature to create pipeline actions in an order from different options).

        Parameters
        ----------
        ctx : `click.core.Context`
            The current Context.ß
        args : `list` [`str`]
            The list of arguments from the command line, split at spaces but
            not at separators (like "=").
        """
        self._capture_args(ctx, args)
        return super().parse_args(ctx, args)

    @property
    def epilog(self) -> str | None:
        """Override the epilog attribute to add extra_epilog (if defined by a
        subclass) to the end of any epilog provided by a subcommand.
        """
        ret = self._epilog if self._epilog else ""
        if self.extra_epilog:
            if ret:
                ret += "\n\n"
            ret += self.extra_epilog
        return ret

    @epilog.setter
    def epilog(self, val: str) -> None:
        self._epilog = val


class ButlerCommand(MWCommand):
    """Command subclass with butler-command specific overrides."""

    extra_epilog = "See 'butler --help' for more options."


class OptionGroup:
    """Base class for an option group decorator. Requires the option group
    subclass to have a property called `decorator`."""

    decorators: list[Any]

    def __call__(self, f: Any) -> Any:
        for decorator in reversed(self.decorators):
            f = decorator(f)
        return f


class MWCtxObj:
    """Helper object for managing the `click.Context.obj` parameter, allows
    obj data to be managed in a consistent way.

    `Context.obj` defaults to None. `MWCtxObj.getFrom(ctx)` can be used to
    initialize the obj if needed and return a new or existing `MWCtxObj`.

    The `args` attribute contains a list of options, option values, and
    argument values that is similar to the list of arguments and options that
    were passed in on the command line, with differences noted below:

    * Option namess and option values are first in the list, and argument
      values come last. The order of options and option values is preserved
      within the options. The order of argument values is preserved.

    * The longest option name is used for the option in the `args` list, e.g.
      if an option accepts both short and long names "-o / --option" and the
      short option name "-o" was used on the command line, the longer name will
      be the one that appears in `args`.

    * A long option name (which begins with two dashes "--") and its value may
      be separated by an equal sign; the name and value are split at the equal
      sign and it is removed. In `args`, the option is in one list item, and
      the option value (without the equal sign) is in the next list item. e.g.
      "--option=foo" and "--option foo" both become `["--opt", "foo"]` in
      `args`.

    * A short option name, (which begins with one dash "-") and its value are
      split immediately after the short option name, and if there is
      whitespace between the short option name and its value it is removed.
      Everything after the short option name (excluding whitespace) is included
      in the value. If the `Option` has a long name, the long name will be used
      in `args` e.g. for the option "-o / --option": "-ofoo" and "-o foo"
      become `["--option", "foo"]`, and (note!) "-o=foo" will become
      `["--option", "=foo"]` (because everything after the short option name,
      except whitespace, is used for the value (as is standard with unix
      command line tools).

    Attributes
    ----------
    args : `list` [`str`]
        A list of options, option values, and arguments simialr to those that
        were passed in on the command line. See comments about captured options
        & arguments above.
    """

    def __init__(self) -> None:
        self.args = None

    @staticmethod
    def getFrom(ctx: click.Context) -> Any:
        """If needed, initialize `ctx.obj` with a new `MWCtxObj`, and return
        the new or already existing `MWCtxObj`."""
        if ctx.obj is not None:
            return ctx.obj
        ctx.obj = MWCtxObj()
        return ctx.obj


def yaml_presets(ctx: click.Context, param: str, value: Any) -> None:
    """Click callback that reads additional values from the supplied
    YAML file.

    Parameters
    ----------
    ctx : `click.context`
        The context for the click operation. Used to extract the subcommand
        name and translate option & argument names.
    param : `str`
        The parameter name.
    value : `object`
        The value of the parameter.
    """

    def _name_for_option(ctx: click.Context, option: str) -> str:
        """Use a CLI option name to find the name of the argument to the
        command function.

        Parameters
        ----------
        ctx : `click.Context`
            The context for the click operation.
        option : `str`
            The option/argument name from the yaml file.

        Returns
        -------
        name : str
            The name of the argument to use when calling the click.command
            function, as it should appear in the `ctx.default_map`.

        Raises
        ------
        RuntimeError
            Raised if the option name from the yaml file does not exist in the
            command parameters. This catches misspellings and incorrect useage
            in the yaml file.
        """
        for param in ctx.command.params:
            # Remove leading dashes: they are not used for option names in the
            # yaml file.
            if option in [opt.lstrip("-") for opt in param.opts]:
                return cast(str, param.name)
        raise RuntimeError(f"'{option}' is not a valid option for {ctx.info_name}")

    ctx.default_map = ctx.default_map or {}
    cmd_name = ctx.info_name
    if value:
        try:
            overrides = _read_yaml_presets(value, cmd_name)
            options = list(overrides.keys())
            for option in options:
                name = _name_for_option(ctx, option)
                if name == option:
                    continue
                overrides[name] = overrides.pop(option)
        except Exception as e:
            raise click.BadOptionUsage(
                option_name=param,
                message=f"Error reading overrides file: {e}",
                ctx=ctx,
            )
        # Override the defaults for this subcommand
        ctx.default_map.update(overrides)
    return


def _read_yaml_presets(file_uri: str, cmd_name: str | None) -> dict[str, Any]:
    """Read file command line overrides from YAML config file.

    Parameters
    ----------
    file_uri : `str`
        URI of override YAML file containing the command line overrides.
        They should be grouped by command name.
    cmd_name : `str`
        The subcommand name that is being modified.

    Returns
    -------
    overrides : `dict` of [`str`, Any]
        The relevant command line options read from the override file.
    """
    log.debug("Reading command line overrides for subcommand %s from URI %s", cmd_name, file_uri)
    config = Config(file_uri)
    return config[cmd_name]


def sortAstropyTable(table: Table, dimensions: list[Dimension], sort_first: list[str] | None = None) -> Table:
    """Sort an astropy table, with prioritization given to columns in this
    order:
    1. the provided named columns
    2. spatial and temporal columns
    3. the rest of the columns

    The table is sorted in-place, and is also returned for convenience.

    Parameters
    ----------
    table : `astropy.table.Table`
        The table to sort
    dimensions : `list` [``Dimension``]
        The dimensions of the dataIds in the table (the dimensions should be
        the same for all the dataIds). Used to determine if the column is
        spatial, temporal, or neither.
    sort_first : `list` [`str`]
        The names of columns that should be sorted first, before spatial and
        temporal columns.

    Returns
    -------
    `astropy.table.Table`
        For convenience, the table that has been sorted.
    """
    # For sorting we want to ignore the id
    # We also want to move temporal or spatial dimensions earlier
    sort_first = sort_first or []
    sort_early: list[str] = []
    sort_late: list[str] = []
    for dim in dimensions:
        if dim.spatial or dim.temporal:
            sort_early.extend(dim.required.names)
        else:
            sort_late.append(str(dim))
    sort_keys = sort_first + sort_early + sort_late
    # The required names above means that we have the possibility of
    # repeats of sort keys. Now have to remove them
    # (order is retained by dict creation).
    sort_keys = list(dict.fromkeys(sort_keys).keys())

    table.sort(sort_keys)
    return table


def catch_and_exit(func: Callable) -> Callable:
    """Decorator which catches all exceptions, prints an exception traceback
    and signals click to exit.
    """

    @wraps(func)
    def inner(*args: Any, **kwargs: Any) -> None:
        try:
            func(*args, **kwargs)
        except (click.exceptions.ClickException, click.exceptions.Exit, click.exceptions.Abort):
            # this is handled by click itself
            raise
        except Exception:
            exc_type, exc_value, exc_tb = sys.exc_info()
            assert exc_type is not None
            assert exc_value is not None
            assert exc_tb is not None
            if exc_tb.tb_next:
                # do not show this decorator in traceback
                exc_tb = exc_tb.tb_next
            log.exception(
                "Caught an exception, details are in traceback:", exc_info=(exc_type, exc_value, exc_tb)
            )
            # tell click to stop, this never returns.
            click.get_current_context().exit(1)

    return inner
