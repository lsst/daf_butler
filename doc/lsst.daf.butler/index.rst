.. py:currentmodule:: lsst.daf.butler

.. _lsst.daf.butler:

###############
lsst.daf.butler
###############

.. _lsst.daf.butler-using:

Using the Butler
================

.. toctree::
  :maxdepth: 1

  configuring.rst
  organizing.rst
  queries.rst
  use-in-tests.rst

This module provides an abstracted data access interface, known as the Butler.
It can be used to read and write data without having to know the details of file formats or locations.

.. _lsst.daf.butler-dev:

Design and Development
======================

``lsst.daf.butler`` is developed at https://github.com/lsst/daf_butler.
You can find Jira issues for this module under the `daf_butler <https://jira.lsstcorp.org/issues/?jql=project%20%3D%20DM%20AND%20component%20%3D%20daf_butler>`_ component.

.. toctree::
   :maxdepth: 1

   dev/dataCoordinate.rst

.. _lsst.daf.butler-scripts:

Command Line Scripts
====================

.. click:: lsst.daf.butler.cli.butler:cli
   :prog: butler
   :show-nested:

.. _lsst.daf.butler-dimensions:

The Dimensions System
=====================

.. toctree::
  :maxdepth: 1

  dimensions.rst

.. _lsst.daf.butler-pyapi:

Concrete Storage Classes
========================

.. toctree::
  :maxdepth: 1

  concreteStorageClasses.rst

Python API reference
====================

.. automodapi:: lsst.daf.butler
   :no-main-docstr:

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
.. automodapi:: lsst.daf.butler.datastores.posixDatastore
   :no-main-docstr:
   :headings: ^"
.. automodapi:: lsst.daf.butler.datastores.s3Datastore
   :no-main-docstr:
   :headings: ^"
.. automodapi:: lsst.daf.butler.datastores.webdavDatastore
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
.. automodapi:: lsst.daf.butler.formatters.pexConfig
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
.. automodapi:: lsst.daf.butler.registry.databases.oracle
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
