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
   :skip: ButlerURI

.. py:class:: lsst.daf.butler.ButlerURI(uri)

   ``ButlerURI`` implementation. Exists for backwards compatibility.
   New code should use `lsst.resources.ResourcePath`.

.. automodapi:: lsst.daf.butler.registry
   :no-main-docstr:

.. automodapi:: lsst.daf.butler.registry.interfaces
   :no-main-docstr:

.. automodapi:: lsst.daf.butler.registry.queries
   :no-main-docstr:

.. automodapi:: lsst.daf.butler.registry.wildcards
   :no-main-docstr:

Example datastores
------------------

.. automodapi:: lsst.daf.butler.datastores.chainedDatastore
   :no-main-docstr:
   :headings: ^"
.. automodapi:: lsst.daf.butler.datastores.inMemoryDatastore
   :no-main-docstr:
   :headings: ^"
.. automodapi:: lsst.daf.butler.datastores.fileDatastore
   :no-main-docstr:
   :headings: ^"

Example formatters
------------------

.. automodapi:: lsst.daf.butler.formatters.file
   :no-main-docstr:
   :headings: ^"
.. automodapi:: lsst.daf.butler.formatters.json
   :no-main-docstr:
   :headings: ^"
.. automodapi:: lsst.daf.butler.formatters.matplotlib
   :no-main-docstr:
   :headings: ^"
.. automodapi:: lsst.daf.butler.formatters.parquet
   :no-main-docstr:
   :headings: ^"
.. automodapi:: lsst.daf.butler.formatters.pickle
   :no-main-docstr:
   :headings: ^"
.. automodapi:: lsst.daf.butler.formatters.yaml
   :no-main-docstr:
   :headings: ^"

Database backends
-----------------

.. automodapi:: lsst.daf.butler.registry.databases.sqlite
   :no-main-docstr:
   :headings: ^"
.. automodapi:: lsst.daf.butler.registry.databases.postgresql
   :no-main-docstr:
   :headings: ^"

Support API
-----------

.. automodapi:: lsst.daf.butler.core.utils
   :no-main-docstr:
   :headings: ^"
.. automodapi:: lsst.daf.butler.core.repoRelocation
   :no-main-docstr:
   :headings: ^"
   :include-all-objects:

Test utilities
--------------

.. automodapi:: lsst.daf.butler.tests
   :no-main-docstr:
   :no-inheritance-diagram:

Command Line Interface API
--------------------------

.. warning::
   The command line interface API (everything in ``lsst.daf.butler.cli``) is for only for developer use to write command line interfaces, and is not intended for general use.

.. automodapi:: lsst.daf.butler.cli.butler
   :no-main-docstr:


.. automodapi:: lsst.daf.butler.cli.cmd
   :no-main-docstr:
   :include-all-objects:

.. automodapi:: lsst.daf.butler.cli.opt
   :no-main-docstr:
   :include-all-objects:

.. automodapi:: lsst.daf.butler.cli.utils
   :no-main-docstr:

.. automodapi:: lsst.daf.butler.cli.cliLog
   :no-main-docstr:

.. automodapi:: lsst.daf.butler.cli.progress
   :no-main-docstr:
