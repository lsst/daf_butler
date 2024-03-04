.. py:currentmodule:: lsst.daf.butler

.. _lsst.daf.butler:

###############
lsst.daf.butler
###############

.. _lsst.daf.butler-changes:

Changes
=======

.. toctree::
   :maxdepth: 1

   CHANGES.rst


.. _lsst.daf.butler-using:

Using the Butler
================

.. toctree::
  :maxdepth: 1

  configuring.rst
  datastores.rst
  formatters.rst
  organizing.rst
  queries.rst
  use-in-tests.rst

This module provides an abstracted data access interface, known as the Butler.
It can be used to read and write data without having to know the details of file formats or locations.

.. _lsst.daf.butler-dimensions:

The Dimensions System
=====================

.. toctree::
  :maxdepth: 1

  dimensions.rst

Concrete Storage Classes
========================

.. toctree::
  :maxdepth: 1

  concreteStorageClasses.rst

..
   The sphinx+click tooling generates docs using sphinx's built-in
   "program" and "option" directive, but linking to those is broken (in
   sphinx itself): https://github.com/sphinx-doc/sphinx/issues/880

   It seems the best we can do is link to the anchor below (even putting a
   manual anchor in scripts/butler.py.rst does not seem to work).

.. _lsst.daf.butler-scripts:

Butler Command-Line Reference
=============================

.. toctree::
   :maxdepth: 1

   scripts/options-file.rst
   scripts/logging.rst
   scripts/butler

.. _lsst.daf.butler-dev:

Design and Development
======================

``lsst.daf.butler`` is developed at https://github.com/lsst/daf_butler.
You can find Jira issues for this module under the `daf_butler <https://jira.lsstcorp.org/issues/?jql=project%20%3D%20DM%20AND%20component%20%3D%20daf_butler>`_ component.

.. toctree::
   :maxdepth: 1

   dev/dataCoordinate.rst

Butler Command Line Interface Development
-----------------------------------------

.. toctree::
   :maxdepth: 1

   writing-subcommands.rst

.. _lsst.daf.butler-pyapi:

Python API reference
====================

.. automodapi:: lsst.daf.butler
   :no-main-docstr:
   :no-inherited-members:
   :skip: RegistryConfig

Example datastores
------------------

.. automodapi:: lsst.daf.butler.datastores.chainedDatastore
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"
.. automodapi:: lsst.daf.butler.datastores.inMemoryDatastore
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"
.. automodapi:: lsst.daf.butler.datastores.fileDatastore
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"

Example formatters
------------------

.. automodapi:: lsst.daf.butler.formatters.file
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"
.. automodapi:: lsst.daf.butler.formatters.json
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"
.. automodapi:: lsst.daf.butler.formatters.matplotlib
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"
.. automodapi:: lsst.daf.butler.formatters.parquet
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"
.. automodapi:: lsst.daf.butler.formatters.pickle
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"
.. automodapi:: lsst.daf.butler.formatters.yaml
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"

Test utilities
--------------

.. automodapi:: lsst.daf.butler.tests
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"
   :no-inheritance-diagram:

Middleware-Internal API
=======================

.. warning::

   These symbols are used throughout the middleware system and may be used in advanced middleware extensions (e.g. third-party `Datastore` implementations), but are not considered fully public interfaces in terms of stability guarantees.

Datastore utilities
-------------------

.. automodapi:: lsst.daf.butler.datastore
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"
   :skip: Datastore

Registry utilities and interfaces
---------------------------------

.. automodapi:: lsst.daf.butler.registry
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"
   :skip: Registry
   :skip: CollectionType

.. automodapi:: lsst.daf.butler.registry.interfaces
   :headings: ^"
   :no-inherited-members:
   :no-main-docstr:

.. automodapi:: lsst.daf.butler.registry.queries
   :headings: ^"
   :no-inherited-members:
   :no-main-docstr:

.. automodapi:: lsst.daf.butler.registry.wildcards
   :headings: ^"
   :no-inherited-members:
   :no-main-docstr:

Database backends
-----------------

.. automodapi:: lsst.daf.butler.registry.databases.sqlite
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"

.. automodapi:: lsst.daf.butler.registry.databases.postgresql
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"

General utilities
-----------------

.. automodapi:: lsst.daf.butler.arrow_utils
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"

.. automodapi:: lsst.daf.butler.column_spec
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"

.. automodapi:: lsst.daf.butler.ddl
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"

.. automodapi:: lsst.daf.butler.json
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"

.. automodapi:: lsst.daf.butler.logging
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"

.. automodapi:: lsst.daf.butler.mapping_factory
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"

.. automodapi:: lsst.daf.butler.nonempty_mapping
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"

.. automodapi:: lsst.daf.butler.persistence_context
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"

.. automodapi:: lsst.daf.butler.progress
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"

.. automodapi:: lsst.daf.butler.pydantic_utils
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"

.. automodapi:: lsst.daf.butler.repo_relocation
   :no-main-docstr:
   :headings: ^"
   :no-inherited-members:
   :include-all-objects:

.. automodapi:: lsst.daf.butler.time_utils
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"

.. automodapi:: lsst.daf.butler.timespan_database_representation
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"

.. automodapi:: lsst.daf.butler.utils
   :no-main-docstr:
   :no-inherited-members:
   :headings: ^"

Command-Line Interface Utilities
--------------------------------

.. automodapi:: lsst.daf.butler.cli.butler
   :no-main-docstr:
   :headings: ^"

.. automodapi:: lsst.daf.butler.cli.cmd
   :no-main-docstr:
   :include-all-objects:
   :headings: ^"

.. automodapi:: lsst.daf.butler.cli.opt
   :no-main-docstr:
   :include-all-objects:
   :headings: ^"

.. automodapi:: lsst.daf.butler.cli.utils
   :no-main-docstr:
   :headings: ^"

.. automodapi:: lsst.daf.butler.cli.cliLog
   :no-main-docstr:
   :headings: ^"

.. automodapi:: lsst.daf.butler.cli.progress
   :no-main-docstr:
   :headings: ^"
