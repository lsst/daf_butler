.. _cli_logging_options:

Controlling Log Output from Command-Line Tooling
================================================

Every command-line program has a standard set of options that control how logs are reported.
These can be listed using ``butler --help`` and are available to all command-line programs that :ref:`use the Butler command-line infrastructure <daf_butler_cli-other_commands>`.
The logging options must be given before any subcommand is specified.

By default the ``lsst`` log hierarchy is set to ``INFO`` logging level and the python root logger is left at its default level of ``WARNING``.
Additional loggers (and their descendants) can be treated the same as the default ``lsst`` logger by setting the ``$DAF_BUTLER_DEFAULT_LOGGER`` environment variable to a colon-separated list of logger names.

The logging level can be changed by using the ``--log-level`` command line option.
To change all the default loggers to ``DEBUG`` use ``--log-level DEBUG``.
The help text will list all the supported level names.
To change the logging level of the root python logger use either ``--log-level .=LEVEL`` (where ``LEVEL`` is the required level from the supported list).
Since the default logger is always set to ``INFO``, in order to set all loggers to, say, ``DEBUG`` then both the default and root logger must be set explicitly: ``--log-level "=DEBUG" --log-level DEBUG``.

This syntax demonstrates how to specify finer control of specific loggers.
For example to turn on debug logging solely in Butler datastores use ``--log-level lsst.daf.butler.datastores=DEBUG``.
The ``-log-level`` option can be given multiple times.

As an example of how the log options can be combined:

.. code-block:: bash

    butler --log-level lsst.daf.butler=DEBUG --long-log --log-label MY_ID --log-file log.json

These options will:

* set the ``lsst`` logger to ``INFO`` and the butler logger to ``DEBUG``, leaving all non-lsst loggers at ``WARNING``;
* generate formatted log messages that will use the long format that includes time stamps with every log message containing the special text ``MY_ID``;
* create a log file containing a JSON representation of the log messages.

The JSON output always includes every piece of information from the log record; the ``--long-log`` option has no effect on the content of the JSON file.
