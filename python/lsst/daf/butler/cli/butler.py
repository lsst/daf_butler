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

__all__ = (
    "ButlerCLI",
    "LoaderCLI",
    "cli",
    "main",
)

import abc
import dataclasses
import functools
import logging
import os
import traceback
import types
from collections import defaultdict
from functools import cache
from importlib.metadata import entry_points
from typing import Any

import click
import yaml

from lsst.resources import ResourcePath
from lsst.utils import doImport
from lsst.utils.introspection import get_full_type_name
from lsst.utils.timer import time_this

from .cliLog import CliLog
from .opt import log_file_option, log_label_option, log_level_option, log_tty_option, long_log_option
from .progress import ClickProgressHandler

log = logging.getLogger(__name__)


@functools.lru_cache
def _importPlugin(pluginName: str) -> types.ModuleType | type | None | click.Command:
    """Import a plugin that contains Click commands.

    Parameters
    ----------
    pluginName : `str`
        An importable module whose __all__ parameter contains the commands
        that can be called.

    Returns
    -------
    An imported module or None
        The imported module, or None if the module could not be imported.

    Notes
    -----
    A cache is used in order to prevent repeated reports of failure
    to import a module that can be triggered by ``butler --help``.
    """
    try:
        return doImport(pluginName)
    except Exception as err:
        log.warning("Could not import plugin from %s, skipping.", pluginName)
        log.debug(
            "Plugin import exception: %s\nTraceback:\n%s",
            err,
            "".join(traceback.format_tb(err.__traceback__)),
        )
        return None


@dataclasses.dataclass(frozen=True)
class PluginCommand:
    """A click Command and the plugin it came from."""

    command: click.Command
    """The command (`click.Command`)."""
    source: str
    """Where the command came from (`str`)."""


class LoaderCLI(click.MultiCommand, abc.ABC):
    """Extends `click.MultiCommand`, which dispatches to subcommands, to load
    subcommands at runtime.

    Parameters
    ----------
    *args : `typing.Any`
        Arguments passed to parent constructor.
    **kwargs : `typing.Any`
        Keyword arguments passed to parent constructor.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    @property
    @abc.abstractmethod
    def localCmdPkg(self) -> str:
        """Identifies the location of the commands that are in this
        package.

        `getLocalCommands` assumes that the commands can be found in
        `localCmdPkg.__all__`, if this is not the case then getLocalCommands
        should be overridden.

        Returns
        -------
        package : `str`
            The fully qualified location of this package.
        """
        raise NotImplementedError()

    def getLocalCommands(self) -> defaultdict[str, list[str | PluginCommand]]:
        """Get the commands offered by the local package. This assumes that the
        commands can be found in `localCmdPkg.__all__`, if this is not the case
        then this function should be overridden.

        Returns
        -------
        commands : `defaultdict` [`str`, `list` [`str` | `PluginCommand` ]]
            The key is the command name. The value is a list of package(s) that
            contains the command.
        """
        commandsLocation = _importPlugin(self.localCmdPkg)
        if commandsLocation is None:
            # _importPlugins logs an error, don't need to do it again here.
            return defaultdict(list)
        assert hasattr(commandsLocation, "__all__"), f"Must define __all__ in {commandsLocation}"
        commands = [getattr(commandsLocation, name) for name in commandsLocation.__all__]
        return defaultdict(
            list,
            {
                command.name: [PluginCommand(command, get_full_type_name(commandsLocation))]
                for command in commands
            },
        )

    def list_commands(self, ctx: click.Context) -> list[str]:
        """Get all the commands that can be called by the
        butler command, it is used to generate the --help output.

        Used by Click.

        Parameters
        ----------
        ctx : `click.Context`
            The current Click context.

        Returns
        -------
        commands : `list` [`str`]
            The names of the commands that can be called by the butler command.
        """
        self._setupLogging(ctx)
        commands = self._getCommands()
        self._raiseIfDuplicateCommands(commands)
        return sorted(commands)

    def get_command(self, ctx: click.Context, name: str) -> click.Command | None:
        """Get a single command for execution.

        Used by Click.

        Parameters
        ----------
        ctx : `click.Context`
            The current Click context.
        name : `str`
            The name of the command to return.

        Returns
        -------
        command : `click.Command`
            A Command that wraps a callable command function.
        """
        self._setupLogging(ctx)
        commands = self._getCommands()
        if name not in commands:
            return None
        self._raiseIfDuplicateCommands(commands)
        command = commands[name][0]
        if isinstance(command, str):
            module_str = command + "." + self._cmdNameToFuncName(name)
            # The click.command decorator returns an instance of a class, which
            # is something that doImport is not expecting. We add it in as an
            # option here to appease mypy.
            with time_this(log, msg="Importing command %s (via %s)", args=(name, module_str)):
                plugin = _importPlugin(module_str)
            if not plugin:
                return None
        else:
            plugin = command.command
        if not isinstance(plugin, click.Command):
            raise RuntimeError(
                f"Command {name!r} loaded from {module_str} is not a click Command, is {type(plugin)}"
            )
        return plugin

    def _setupLogging(self, ctx: click.Context | None) -> None:
        """Init the logging system and config it for the command.

        Subcommands may further configure the log settings.
        """
        if isinstance(ctx, click.Context):
            CliLog.initLog(
                longlog=ctx.params.get(long_log_option.name(), False),
                log_tty=ctx.params.get(log_tty_option.name(), True),
                log_file=ctx.params.get(log_file_option.name(), ()),
                log_label=ctx.params.get(log_label_option.name(), ()),
            )
            if log_level_option.name() in ctx.params:
                CliLog.setLogLevels(ctx.params[log_level_option.name()])
        else:
            # This works around a bug in sphinx-click, where it passes in the
            # click.MultiCommand instead of the context.
            # https://github.com/click-contrib/sphinx-click/issues/70
            CliLog.initLog(longlog=False)
            logging.debug(
                "The passed-in context was not a click.Context, could not determine --long-log or "
                "--log-level values."
            )

    @classmethod
    def getPluginList(cls) -> list[ResourcePath]:
        """Get the list of importable yaml files that contain cli data for this
        command.

        Returns
        -------
        `list` [`lsst.resources.ResourcePath`]
            The list of files that contain yaml data about a cli plugin.
        """
        yaml_files = []
        if hasattr(cls, "pluginEnvVar"):
            pluginModules = os.environ.get(cls.pluginEnvVar)
            if pluginModules:
                yaml_files.extend([ResourcePath(p) for p in pluginModules.split(":") if p != ""])

        return yaml_files

    @classmethod
    def _funcNameToCmdName(cls, functionName: str) -> str:
        """Convert function name to the butler command name: change
        underscores, (used in functions) to dashes (used in commands), and
        change local-package command names that conflict with python keywords
        to a legal function name.
        """
        return functionName.replace("_", "-")

    @classmethod
    def _cmdNameToFuncName(cls, commandName: str) -> str:
        """Convert butler command name to function name: change dashes (used in
        commands) to underscores (used in functions), and for local-package
        commands names that conflict with python keywords, change the local,
        legal, function name to the command name.
        """
        return commandName.replace("-", "_")

    @staticmethod
    def _mergeCommandLists(
        a: defaultdict[str, list[str | PluginCommand]], b: defaultdict[str, list[str | PluginCommand]]
    ) -> defaultdict[str, list[str | PluginCommand]]:
        """Combine two dicts whose keys are strings (command name) and values
        are list of string (the package(s) that provide the named command).

        Parameters
        ----------
        a : `defaultdict` [`str`, `list` [`str` | `PluginCommand` ]]
            The key is the command name. The value is a list of package(s) that
            contains the command.
        b : (same as a)

        Returns
        -------
        commands : `defaultdict` [`str`: [`str` | `PluginCommand` ]]
            For convenience, returns a extended with b. ('a' is modified in
            place.)
        """
        for key, val in b.items():
            a[key].extend(val)
        return a

    @classmethod
    def _getPluginCommands(cls) -> defaultdict[str, list[str | PluginCommand]]:
        """Get the commands offered by plugin packages.

        Returns
        -------
        commands : `defaultdict` [`str`, `list` [`str`]]
            The key is the command name. The value is a list of package(s) that
            contains the command.

        Notes
        -----
        Assumes that if entry points are defined, the plugin environment
        variable will not be defined for that same package.
        """
        commands: defaultdict[str, list[str | PluginCommand]] = defaultdict(list)
        for pluginName in cls.getPluginList():
            try:
                resources = defaultdict(list, yaml.safe_load(pluginName.read()))
            except Exception as err:
                log.warning("Error loading commands from %s, skipping. %s", pluginName, err)
                continue
            if "cmd" not in resources:
                log.warning("No commands found in %s, skipping.", pluginName)
                continue
            pluginCommands = {cmd: [resources["cmd"]["import"]] for cmd in resources["cmd"]["commands"]}
            cls._mergeCommandLists(commands, defaultdict(list, pluginCommands))

        if hasattr(cls, "entryPoint"):
            plugins = entry_points(group=cls.entryPoint)
            for p in plugins:
                try:
                    func = p.load()
                except Exception as err:
                    log.warning("Could not import plugin from entry point %s, skipping.", p)
                    log.debug(
                        "Plugin import exception: %s\nTraceback:\n%s",
                        err,
                        "".join(traceback.format_tb(err.__traceback__)),
                    )
                    continue
                func_name = get_full_type_name(func)
                pluginCommands = {cmd.name: [PluginCommand(cmd, func_name)] for cmd in func()}
                cls._mergeCommandLists(commands, defaultdict(list, pluginCommands))

        return commands

    @cache
    def _getCommands(self) -> defaultdict[str, list[str | PluginCommand]]:
        """Get the commands offered by daf_butler and plugin packages.

        Returns
        -------
        commands : `defaultdict` [`str`, `list` [`str` | `PluginCommand` ]]
            The key is the command name. The value is a list of package(s) that
            contains the command.
        """
        return self._mergeCommandLists(self.getLocalCommands(), self._getPluginCommands())

    @staticmethod
    def _raiseIfDuplicateCommands(commands: defaultdict[str, list[str | PluginCommand]]) -> None:
        """If any provided command is offered by more than one package raise an
        exception.

        Parameters
        ----------
        commands : `defaultdict` [`str`, `list` [`str` | `PLuginCommand` ]]
            The key is the command name. The value is a list of package(s) that
            contains the command.

        Raises
        ------
        click.ClickException
            Raised if a command is offered by more than one package, with an
            error message to be displayed to the user.
        """
        msg = ""
        for command, packages in commands.items():
            if len(packages) > 1:
                pkg_names: list[str] = []
                for p in packages:
                    if not isinstance(p, str):
                        p = p.source
                    pkg_names.append(p)
                msg += f"Command '{command}' exists in packages {', '.join(pkg_names)}. "
        if msg:
            raise click.ClickException(message=msg + "Duplicate commands are not supported, aborting.")


class ButlerCLI(LoaderCLI):
    """Specialized command loader implementing the ``butler`` command."""

    localCmdPkg = "lsst.daf.butler.cli.cmd"

    pluginEnvVar = "DAF_BUTLER_PLUGINS"
    entryPoint = "butler.cli"

    @classmethod
    def _funcNameToCmdName(cls, functionName: str) -> str:
        # Docstring inherited from base class.

        # The "import" command name and "butler_import" function name are
        # defined in cli/cmd/commands.py, and if those names are changed they
        # must be changed here as well.
        # It is expected that there will be very few butler command names that
        # need to be changed because of e.g. conflicts with python keywords (as
        # is done here and in _cmdNameToFuncName for the 'import' command). If
        # this becomes a common need then some way of doing this should be
        # invented that is better than hard coding the function names into
        # these conversion functions. An extension of the 'cli/resources.yaml'
        # file (as is currently used in obs_base) might be a good way to do it.
        if functionName == "butler_import":
            return "import"
        return super()._funcNameToCmdName(functionName)

    @classmethod
    def _cmdNameToFuncName(cls, commandName: str) -> str:
        # Docstring inherited from base class.
        if commandName == "import":
            return "butler_import"
        return super()._cmdNameToFuncName(commandName)


class UncachedButlerCLI(ButlerCLI):
    """ButlerCLI that can be used where caching of the commands is disabled."""

    def _getCommands(self) -> defaultdict[str, list[str | PluginCommand]]:  # type: ignore[override]
        """Get the commands offered by daf_butler and plugin packages.

        Returns
        -------
        commands : `defaultdict` [`str`, `list` [`str` | `PluginCommand` ]]
            The key is the command name. The value is a list of package(s) that
            contains the command.
        """
        return self._mergeCommandLists(self.getLocalCommands(), self._getPluginCommands())


@click.command(cls=ButlerCLI, context_settings=dict(help_option_names=["-h", "--help"]))
@log_level_option()
@long_log_option()
@log_file_option()
@log_tty_option()
@log_label_option()
@ClickProgressHandler.option
def cli(log_level: str, long_log: bool, log_file: str, log_tty: bool, log_label: str, progress: bool) -> None:
    """Butler command-line tools.

    Log options apply to all subcommands.
    """
    pass


def main() -> click.Command:
    """Return main entry point for command-line."""
    return cli()
