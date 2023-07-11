
.. _daf_butler_cli:

The Butler Command
==================

.. note::

    This page describes how to extend the ``butler`` subcommand; see :ref:`lsst.daf.butler-scripts` for documentation on the subcommands defined in `lsst.daf.butler`.
    Additional subcommands are defined in other packages, especially `lsst.obs.base`; use ``butler --help`` to get a complete list.

``daf_butler`` provides a command line interface command called ``butler``. It supports subcommands, some of
which are implemented in ``daf_butler``.

.. _click: https://click.palletsprojects.com/

The ``butler`` command and subcommands are implemented in `click`_, which is well
`documented <https://click.palletsprojects.com/en/7.x/#documentation>`_ and has a good
`quickstart <https://click.palletsprojects.com/en/7.x/quickstart/>`_ guide. There is more to know about
click's commands, options, and arguments than is described here, and the click  documentation is a good
resource for that.

This guide includes an overview of how `subcommands`_, `options`_, and `arguments`_ are used with the
``butler`` command.

It then describes how other packages can add subcommands to the butler command by way of a plugin system,
in `Adding Butler Subcommands`_.

Finally, the ``butler`` command line framework can be used to write other commands that load subcommands. This
is described in `Writing Other Commands`_

Subcommands
-----------

Subcommands are like the "pull" in ``git pull``. The subcommand is implemented as a python function and
decorated with ``@click.command``. The name of the function will become the name of the subcommand on the
command line.

Example of a command or subcommand definition:

.. code-block:: py
   :name: command-example
    # filename: git.py

    import click


    @click.group()
    def git():
        """An example git-style interface."""
        pass


    # Notice this uses "@git" instead of "@click", this adds the command to the
    # git group.
    @git.command()
    def pull():
        """An example 'pull' subcommand."""
        print("pull!")


    if __name__ == "__main__":
        git()

This creates a command that is called by `main`, which has a subcommand `pull`.
It automatically has a `--help` option:

.. code-block:: text

    $ python git.py pull --help
    Usage: git.py pull [OPTIONS]

      An example 'pull' subcommand.

    Options:
      --help  Show this message and exit.

And `pull` can be called as a subcommand:

.. code-block:: text

    $ python git.py pull
    pull!


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

Butler Subcommands
~~~~~~~~~~~~~~~~~~

When creating a subcommand to be loaded by the ``butler`` command, use the decorator ``@click.command()``.
The subcommand loader will find the command and add it to the ``butler`` command. Don't worry about this too
much for now, we will discuss it more later.

An example ``butler`` subcommand implementation:

.. code-block:: py

    @click.command()
    def my_subcommand():
        """An example subcommand that can be loaded by the butler subcommand loader."""
        pass

Options
-------

Options are like the ``--all`` and the ``--message <msg>`` in ``git commit --all --message <msg>``.
They are added to a command by decorating the command function with ``@click.option()``.

In addition to the long flag (like ``--message``) it can have a short flag, like ``-m`` for message.
If it has a long flag, the argument name to the command function is the long flag with the dashes removed.
If there is only a short flag, that will be used to create the argument name (again, without the dash).
If a string with no dashes is passed, that will be used as the argument name, overriding the argument name
that would have been generated using either of the option flags. This is shown below in the section describing
``MWOptionDecorator``.

An example of a subcommand that uses options:

.. code-block:: py
    :name: option-example

    import click


    @click.group()
    def git():
        """An example git-style interface."""
        pass


    @git.command()
    @click.option("-m", "--message", help="commit message")
    @click.option("-a", "--all", help="commit all changed files", is_flag=True)
    def commit(all, message):
        """An example 'commit' subcommand."""
        print(f"commit. all: {all}, message: {message}")


    if __name__ == "__main__":
        git()

The help is automatically generated:

.. code-block:: text

    $ python git.py commit --help
    Usage: git.py commit [OPTIONS]

      An example 'commit' subcommand.

    Options:
      -m, --message TEXT  commit message
      -a, --all           commit all changed files
      --help              Show this message and exit.

And an example of calling the subcommand:

.. code-block:: text

    $ python git.py commit -a -m "example commit message"
    commit. all: True, message: example commit message


Arguments
---------

Arguments are parameters without flags like ``my_branch`` in ``git checkout my_branch``.
They are added to a command by decorating the command function with ``@click.argument()``.

An example of a subcommand that uses arguments:

.. code-block:: py
    :name: argument-example

    import click


    @click.group()
    def git():
        """An example git-style interface."""
        pass


    @git.command()
    @click.argument("branch")
    def checkout(branch):
        """An example 'checkout' subcommand.

        BRANCH In click, arguments are documented in the command function help.
              But you can use MWArgumentDecorator, described later, to
              automatically add argument help to your command function.
        """
        print(f"checkout branch {branch}")


    if __name__ == "__main__":
        git()

The help is automatically generated:

.. code-block:: text

    $ python git.py checkout --help
    Usage: git.py checkout [OPTIONS] BRANCH

      An example 'checkout' subcommand.

      BRANCH In click, arguments are documented in the command function help.
      But you can use MWArgumentDecorator, described later, to
      automatically add argument help to your command function.

    Options:
      --help  Show this message and exit.

And an example of calling the subcommand:

.. code-block:: text

    $ python git.py checkout mybranch
    checkout branch mybranch

Butler Command Line Interface Utilities
=======================================

``daf_butler`` provides utilities that can be used with Click for various
purposes:

Shared Options and Arguments
----------------------------

It can be good to define an option or argument one time and use it with more than one command.
This reduces code duplication and improves consistency in the command line interface.
``daf_butler`` provides ``MWOptionDecorator`` and ``MWArgumentDecorator`` to define reusable option and argument decorators.

- Mostly they take the same arguments as ``@click.option`` and ``@click.argument``.
- ``MWArgumentDecorator`` accepts a ``help`` argument, and inserts that help text in the correct place in the command's help output.
  (The standard ``@click.argument`` decorator does not take a ``help`` argument and instead requires the the argument docstring to be added to the command function.)

An example implementation of ``git checkout`` that uses MWArgumentDecorator and MWOptionDecorator:

.. code-block:: py
    :name: MWDecorator-example

    import click

    from lsst.daf.butler.cli.opt import MWOptionDecorator, MWArgumentDecorator

    branch_argument = MWArgumentDecorator("branch", help="Checkout a branch")

    # Notice a string with no dashes is passed ("make_new_branch"), it is used
    # as the argument name in the command function where it is used. (This is
    # available for any click.option)
    new_branch_option = MWOptionDecorator(
        "-b",
        "make_new_branch",
        help="create and checkout a new branch",
        # is_flag makes the option take no values, uses a bool
        # which is true if the option is passed and false by default.
        is_flag=True,
    )


    @click.group()
    def git():
        """An example git-style interface."""
        pass


    @git.command()
    @branch_argument()
    @new_branch_option()
    def checkout(branch, make_new_branch):
        """An example 'checkout' subcommand."""
        print(f"checkout branch {branch}, make new:{make_new_branch}")


    if __name__ == "__main__":
        git()

By convention:

- Shared options and arguments should be placed in a package that is as high in the dependency tree as is reasonable for that option.
- Shared option definitions go in the file ``.../cli/opt/options.py`` in the package's python directory tree. Shared arguments go in ``.../cli/opt/arguments.py``
- The shared option name should:

  - Match or nearly match the long name of the option or argument.
  - Be all lowercase.
  - Have multiple words separated by underscores.
  - Shared options should end with ``_option``. Shared arguments should end with ``_argument``.

Shared Option Groups
--------------------

An option group decorator may be created for shared options that will frequently be used together.
The option group decorator can then add all its options to a command with a single decorator call.

By convention:

- Option group decorators should go in the file ``.../cli/opt/optionDecorators.py``.
- Option group decorators should inherit from ``lsst.daf.butler.cli.utils.OptionGroup``.
  This makes it easy to define the option group:

  1. Create a subclass of ``OptionGroup``
  2. In the subclass ``__init__`` function, define a member parameter called ``decorators`` that is a ``list``
     or ``tuple`` of the options that go in that group.

Defines an Option Group decorator:

.. code-block:: py
   :name: option-group-example

    class pipeline_build_options(OptionGroup):  # noqa: N801
        """Decorator to add options to a command function for building a pipeline."""

        def __init__(self):
            self.decorators = [
                ctrlMpExecOpts.pipeline_option(),
                ctrlMpExecOpts.task_option(),
            ]

Uses an Option Group decorator:

.. code-block:: py
   :name: option-group-use

   @click.command()
   @pipeline_build_options()
   def build(pipeline, task):
       ...


Callbacks
---------

Options and arguments take a ``callback`` argument whose value is a function to be executed before passing the value to the command function.
This allows the value(s) to be manipulated or acted upon before the command function is executed.
``lsst.daf.butler.cli.utils`` provides several helpful callback functions:

``split_commas``
    Accepts a list of strings that may contain comma separated values and splits them at the commas, returning a single list of values.
``split_kv``
    Accepts a list of strings with key-value pairs that may be comma separated.
    It is very configurable, for example the user can specify the key-value separator token, output container type and ordering, and more.
    The docstring is a good resource to learn more.
``to_upper``
    Converts the value to upper case.
``options_file_option``
    Allows option values to be loaded from a ``yaml`` file.

Adding Butler Subcommands
=========================

Packages can add subcommands to the ``butler`` command using a plugin system. This section describes how to do that.
To use the plugin system you should also read and understand the sections above about `the butler command`_.
Then, write your subcommands and arrange them as described below in `Package Layout`_.
Finally, declare them as ``butler`` command plugins as described in `Manifest`_.

Package Layout
--------------

The following conventions are recommended but not required:

- All command line interface code should go in a folder called ``cli`` under the package's python hierarchy e.g. ``python/lsst/daf/butler/cli``.
- Commands go in a file ``.../cli/cmd/commands.py``
- Options go in a file ``.../cli/opt/options.py``
- Shared options go in a file ``.../cli/opt/sharedOptions.py``
- Arguments go in a file ``.../cli/opt/arguments.py``
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

The ``butler`` command finds plugin commands by way of a resource manifest published in an environment variable.
By convention it is usually in the ``cli`` folder and named ``resources.yaml``.

The ``resources.yaml`` must have a heading ``cmd``, this is the section for importable commands.
It must contain two key-value pairs:

  - A key called ``import`` whose value names the package that the commands can be imported from.
  - A key called ``commands`` that contains a list of importable command names.
    Use the dash-separated command name, not the underscore-separated function name.

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

.. _daf_butler_cli-other_commands:

Writing Other Commands
======================

Other commands that load their commands from plugins as describe above can be implemented using the butler command framework, described below.

The ``butler`` command uses a ``click.MultiCommand`` subclass called ``LoaderCLI``.
It dynamically loads subcommands from the local package and from plugin packages.
``LoaderCLI`` can be used to implement other commands that dynamically load subcommands.

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
    @click.command(cls=ButlerCLI, context_settings=dict(help_option_names=["-h", "--help"]))
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
