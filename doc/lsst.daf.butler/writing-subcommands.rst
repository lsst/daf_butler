.. _daf_butler_cli:

The Butler Command
==================

``daf_butler`` provides a command line interface command called ``butler``. It supports subcommands, some of
which are implemented in ``daf_butler``.

.. _click: https://click.palletsprojects.com/

The ``butler`` command and subcommands are implemented in `click`_, which is well documented and has a good
quickstart guide. There is more to know about click's commands, options, and arguments than is described here,
and the `click`_ documentation is a good resource for that.

This guide includes an overview of how `subcommands`_, `options`_, and `arguments`_ are used with the
``butler`` command.

It then describes how other packages can add subcommands to the butler command by way of a plugin system,
in `Butler Command Plugins`_.

Finanly, the ``butler`` command line framework can be used to write other commands that load subcommands. This
is described in `Command Loader Base Class`_

Subcommands
-----------

Subcommands are like the "pull" in ``git pull``. The subcommand is implemented as a python function and
decorated with ``@click.command``. The name of the function will become the name of the subcommand on the
command line.

Example of a command or subcommand definition:

.. code-block:: py
   :name: command-example

    @click.command
    def pull():
        """Pull docstring, will be shown in the help output.
        ...

Naming
~~~~~~

For two-word commands, the words in the function name should be separated by an underscore. When creating the
command name click will convert the underscore to a dash.

For example, the function

.. code-block:: py
   :name: command-name-example

    @click.command
    def register_instrument():
        ...

Becomes a subcommand of `butler` called ``register-instrument``.

Options
-------

Options are like the ``-a`` and the ``-m <msg>`` in ``git commit -a -m <msg>``. They are declared as options
of a command by decorating a command function with ``click.option``.

If the option has a long name, (like ``--message``), the long name, without dashes, becomes the argument name
(``message``) when click calls the function, otherwise the short name of the option (``-m``) will be used to
create the argument name.

.. code-block:: py
   :name: option-example

    @click.command
    @click.option("-a", "--all", is_flag=True)
    @click.option("-m", "--message")
    def commit(all, message):
        ...

Shared Options
~~~~~~~~~~~~~~

Option definitions can be shared and should be used to improve consistency and reduce code duplication.

Shared Options:

- Should be placed in a package that is as high in the dependcy tree as is reasonable for that option.
  By convention, option definitions go in the file at ``.../cli/opt/options.py`` in the package's python
  directory tree.
- The shared option name should:

  - Match or nearly match the long name of the option.
  - Be all lowercase.
  - Have multiple words separated by underscores.
  - End in ``_option``.

- Should be defined by creating an instance of the class ``lsst.daf.butler.cli.utils.MWOptionDecorator``.
  This class takes the same arguments as ``click.option`` and adds helper methods for the command line
  framework. There are many examples in ``daf_butler``'s ``options.py`` file.

Defines a shared option:

.. code-block:: py
   :name: shared-option-example

    dataset_type_option = MWOptionDecorator("-d", "--dataset-type")

Uses a shared option:

.. code-block:: py
   :name: shared-option-use

    @click.command()
    @dataset_type_option()
    def my_command(dataset_type):
        ...

Option Groups
~~~~~~~~~~~~~

An option group decorator may be created for shared options that will frequently be used together. The option
group decorator can then add all its options to a command with a single decorator call.

Option Group decorators:

- Go in the file at ``.../cli/opt/optionDecorators.py``.
- Inherit from ``lsst.daf.butler.cli.utils.OptionGroup``, which makes it easy to define the option group:

  1. Create a subclass of ``OptionGroup``
  2. In the subclass ``__init__`` function, define a member parameter called ``decorators`` that is a ``list``
     or ``tuple`` of the options that go in that group.

Defines an Option Group decorator:

.. code-block:: py
   :name: option-group-example

    class pipeline_build_options(OptionGroup):  # noqa: N801
    """Decorator to add options to a command function for building a pipeline.
    """

    def __init__(self):
        self.decorators = [
            ctrlMpExecOpts.pipeline_option(),
            ctrlMpExecOpts.task_option()]

Uses an Option Group decorator:

.. code-block:: py
   :name: option-group-use

   @click.command()
   @pipeline_build_options()
   def build(pipeline, task):
       ...

Arguments
---------

Arguments are parameters without flags like ``my_branch`` in ``git checkout my_branch``. They are declared
much like options:

.. code-block:: py

    @click.command
    @click.argument("branch")
    def checkout(branch):
        ...

Shared Arguments
~~~~~~~~~~~~~~~~

Arguments definitions can be shared, similar to options, and also should be used to improve consistency and
reduce code duplication.

Shared Arguments:

- Should be placed in a package that is as high in the dependcy tree as is reasonable for that option. By
  convention, option definitions go in the file at ``.../cli/opt/arguments.py`` in the package's python
  directory tree.
- The shared argument name should:

  - Match or nearly match the long name of the argument.
  - Be all lowercase.
  - Have multiple words separated by underscores.
  - End in ``_argument``.

- Should be defined by creating an instance of the class ``lsst.daf.butler.cli.utils.MWArgumentDecorator``.
  This class takes the same arguments as ``click.argument`` and adds helper methods for the command line
  framework. There are many examples in ``daf_butler``’s ``arguments.py`` file.

Defines a shared argument:

.. code-block:: py
   :name: shared-argument-example

   example_argument = MWArgumentDecorator(
        "example",
       help="An example argument for tutorial.")

Uses a shared option:

.. code-block:: py
   :name: shared-argument-use

   @click.command()
   @example_argument()
   def cmd(example):
       ...

Butler Command Plugins
======================

Packages can add subcommands to the butler command using a plugin system. This section describes how to do
that. To use the plugin system you should also read and understand the sections above about `the butler
command`_. Then, write your subcommands and arrange them as described below in `Package Layout`_. Finally,
declare them as ``butler`` command plugins as described in `Manifest`_.

Package Layout
--------------

- All command line interface code should go in a folder called ``cli`` under the package's python
  hierarchy e.g. ``python/lsst/daf/butler/cli``.
- Commands go in a file ``.../cli/cmd/commands.py``
- Options go in a file ``.../cli/opt/options.py``
- Shared options go in a file ``.../cli/opt/sharedOptions.py``
- Arguments go in a file ``.../cli/opt/arguments.py``
- To make commands importable there must be a manifest file, usually named ``resources.yaml`` in the ``cli``
  folder. ``cli``
- There may be a ``utils.py`` file, also usually in the ``cli`` folder.

.. code-block:: text

   cli
   ├── cmd
   │    └── commands.py
   ├── opt
   │    ├── arguments.py
   │    ├── options.py
   │    └── sharedOptions.py
   ├── resources.yaml
   └── utils.yaml

Manifest
--------

The ``butler`` command finds plugin commands by way of a resource manifest published in an environment
variable.

Create a file ``resources.yaml`` in the ``cli`` folder. ``cmd`` is the section for importable commands;
``import`` names the package that the commands can be imported from and ``commands`` is a list of importable
command names. Use the dash-separated command name, not the underscore-separated function name.

For example, the manifest file for ``butler`` plugin subcommands in ``obs_base`` is like this:

.. code-block:: yaml

    cmd:
      import: lsst.obs.base.cli.cmd
      commands:
        - register-instrument
        - write-curated-calibrations

Publish the resource manifest in an environment variable: in the package's ``ups/<pkg>.table`` file, add a
command to prepend ``DAF_BUTLER_PLUGINS`` with the location of the resource manifest. Make sure to use the
environment variable for the location of the package.

The settings for ``obs_base`` are like this:

.. code-block:: text

    envPrepend(DAF_BUTLER_PLUGINS, $OBS_BASE_DIR/python/lsst/obs/base/cli/resources.yaml)

Command Loader Base Class
=========================

The ``butler`` commmand uses a ``click.MultiCommand`` subclass called ``LoaderCLI`` that dynamically loads
subcomands from the local package and from plugin packages. ``LoaderCLI`` can be used to implement other
commands that dynamically load subcommands.

It's easy to create a new kind of command by copying the template below and making a few small changes:

- Change the value of ``localCmdPkg`` so refers to importable commands in the local package.
- If you will support plugin commands, decide on a new environment variable to refer to the plugin manifests
  and change the value of ``pluginEnvVar`` to that.
- If you will not support plugin commands, simply delete ``pluginEnvVar``.
- Change the class name ``ButlerCLI`` to something more descriptive for your command, and change the argument
  ``@click.command(cls=ButlerCLI,...`` that refers to it.

.. code-block:: py
   :name: loader-example

    import click

    from lsst.daf.butler.cli.butler import LoaderCLI

    # Change the class name to better describe your command.
    class ButlerCLI(LoaderCLI):

        # Replace this value with the import path to your `cmd` module.
        localCmdPkg = "lsst.daf.butler.cli.cmd"

        # Replace this value with the manifest environment variable described
        # above.
        pluginEnvVar = "DAF_BUTLER_PLUGINS"

    # Change ``cls=ButlerCLI`` to be the same as your new class name above.
    @click.command(cls=ButlerCLI,
                   context_settings=dict(help_option_names=["-h", "--help"]))
    # You can remove log_level_option if you do not support it. You can add
    # other command options here. (Subcommand options are declared elsewhere).
    @log_level_option()
    def cli(log_level):
        # Normally you would handle the function arguments here, if there are
        # any, and/or pass them to other functions. `log_level` is unique; it
        # is handled by `LoaderCLI.get_command` and `LoaderCLI.list_commands`,
        # and is called in one of those functions before this function is
        # called.
        pass


    def main():
        return cli()
