.. note::

   The ``butler`` subcommands documented here are only those defined in the ``daf_butler`` package itself; downstream packages can implement additional subcommands via the plugin system described at :ref:`daf_butler_cli`.
   The best way to get a complete list of subcommands is to use ``butler --help``.

.. click:: lsst.daf.butler.cli.butler:cli
   :prog: butler
   :show-nested:
