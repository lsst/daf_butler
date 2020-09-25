.. _cli_file_options:

Specifying Command-Line Options from a File
===========================================

Many of the ``butler`` subcommands support the ability to provide default command-line options from an external text file.
This is done with the ``-@`` (or ``--options-file``) option.
A YAML file can be supplied with new default values for any of the options, grouped by subcommand.

.. code-block:: yaml

   config-dump:
     subset: .datastore
   ingest-raws:
     config: keyword=value

For example, if the above file is called ``defaults.yaml`` then calling

.. prompt:: bash

   butler config-dump -@defaults.yaml myrepo

would only report the datastore section of the config.

.. note::

  Options explicitly given on the command-line always take precedence over those specified from an external options file.


