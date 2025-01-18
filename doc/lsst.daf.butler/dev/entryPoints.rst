.. _lsst.daf.butler-dev_entry_points:

.. py:currentmodule:: lsst.daf.butler

Support Entry Points
--------------------

Some functionality is enabled by utilizing the Python entry points system.
Entry points are managed by the ``project.entry_points`` section of the ``pyproject.toml`` file.


Command Line Subcommands
^^^^^^^^^^^^^^^^^^^^^^^^

The entry points that support command-line subcommands are documented in :ref:`daf_butler_cli-entry-points`.

ObsCore RecordFactory
^^^^^^^^^^^^^^^^^^^^^

`lsst.daf.butler.registry.obscore.RecordFactory` is the class that generates ObsCore records from Butler datasets.
This conversion generally requires decisions to be made that depend on the dimension universe for the butler.
The default implementation only works with the ``daf_butler`` dimension universe namespace.
Alternative implementations can be registered using the ``butler.obscore_factory`` entry point group.
The label associated with the entry point should correspond to the universe namespace.

For example, a hypothetical entry point for the default namespace could be written in the ``pyproject.toml`` file as:

.. code-block:: TOML

    [project.entry-points.'butler.obscore_factory']
    daf_butler = "lsst.daf.butler.registry.obscore._records.DafButlerRecordFactory"

The function listed for the entry point should return the Python type that should be used to generate records.
It is required to be a subclass of `lsst.daf.butler.registry.obscore.RecordFactory`.
