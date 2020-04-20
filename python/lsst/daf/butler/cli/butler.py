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
import logging
import os

from . import cmd as butlerCommands
from .utils import to_upper
from lsst.utils import doImport

log = logging.getLogger(__name__)


def _initLogging(logLevel):
    numeric_level = getattr(logging, logLevel, None)
    if not isinstance(numeric_level, int):
        raise click.ClickException(f"Invalid log level: {logLevel}")
    logging.basicConfig(level=numeric_level)


def funcNameToCmdName(functionName):
    """Change underscores, used in functions, to dashes, used in commands."""
    return functionName.replace("_", "-")


def cmdNameToFuncName(commandName):
    """Change dashes, used in commands, to underscores, used in functions."""
    return commandName.replace("-", "_")


class LoaderCLI(click.MultiCommand):

    @staticmethod
    def _getPluginList():
        pluginModules = os.environ.get("DAF_BUTLER_PLUGINS")
        if not pluginModules:
            return []
        return pluginModules.split(":")

    @staticmethod
    def _importPlugin(pluginName):
        """Import a plugin that contains Click commands.

        Parameters
        ----------
        pluginName : string
            An importable module whose __all__ parameter contains the commands
            that can be called.

        Returns
        -------
        An imported module or None
            The imported module, or None if the module could not be imported.
        """
        try:
            return doImport(pluginName)
        except (TypeError, ModuleNotFoundError, ImportError) as err:
            log.warning("Could not import plugin from %s, skipping.", pluginName)
            log.debug("Plugin import exception: %s", err)
            return None

    @staticmethod
    def _getLocalCommand(commandName):
        """Search the commands provided by this package and return the command
        if found.

        Parameters
        ----------
        commandName : string
            The name of the command to search for.

        Returns
        -------
        Command object
            A Click command that can be executed.
        """
        try:
            return getattr(butlerCommands, commandName)
        except AttributeError:
            pass
        return None

    @staticmethod
    def _getLocalCommands():
        """Get the commands offered by daf_butler.

        Returns
        -------
        list of string
            The names of the commands.
        """
        return [funcNameToCmdName(f) for f in butlerCommands.__all__]

    def list_commands(self, ctx):
        """Used by Click to get all the commands that can be called by the
        butler command, it is used to generate the --help output.

        Parameters
        ----------
        ctx : click.Context
            The current Click context.

        Returns
        -------
        List of string
            The names of the commands that can be called by the butler command.
        """
        commands = self._getLocalCommands()
        for pluginName in self._getPluginList():
            plugin = self._importPlugin(pluginName)
            if plugin is None:
                continue
            commands.extend([funcNameToCmdName(command) for command in plugin.__all__])
        commands.sort()
        return commands

    def get_command(self, context, name):
        """Used by Click to get a single command for execution.

        Parameters
        ----------
        ctx : click.Context
            The current Click context.
        name : string
            The name of the command to return.

        Returns
        -------
        click.Command
            A Command that wraps a callable command function.
        """
        name = cmdNameToFuncName(name)
        localCmd = self._getLocalCommand(name)
        if localCmd is not None:
            return localCmd
        for pluginName in self._getPluginList():
            plugin = self._importPlugin(pluginName)
            if plugin is None:
                continue
            for command in plugin.__all__:
                if command == name:
                    fullCommand = pluginName + "." + command
                    try:
                        cmd = doImport(fullCommand)
                    except Exception as err:
                        log.debug("Command import exception: %s", err)
                        context.fail("Could not import command {fullCommand}")
                    return cmd
        return None


@click.command(cls=LoaderCLI)
@click.option("--log-level",
              type=click.Choice(["critical", "error", "warning", "info", "debug",
                                 "CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]),
              default="warning",
              help="The Python log level to use.",
              callback=to_upper)
def cli(log_level):
    _initLogging(log_level)


def main():
    return cli()
