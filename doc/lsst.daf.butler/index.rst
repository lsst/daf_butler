.. py:currentmodule:: lsst.daf.butler

.. _lsst.daf.butler:

###############
lsst.daf.butler
###############

This module provides an abstracted data access interface, known as the Butler.
It can be used to read and write data without having to know the details of file formats or locations.

.. _lsst.daf.butler-contributing:

Contributing
============

``lsst.daf.butler`` is developed at https://github.com/lsst/daf_butler.
You can find Jira issues for this module under the `daf_butler <https://jira.lsstcorp.org/issues/?jql=project%20%3D%20DM%20AND%20component%20%3D%20daf_butler>`_ component.

.. _lsst.daf.butler-pyapi:

Python API reference
====================

.. automodapi:: lsst.daf.butler

Example Datastores
==================

.. automodapi:: lsst.daf.butler.datastores.posixDatastore
.. automodapi:: lsst.daf.butler.datastores.inMemoryDatastore
.. automodapi:: lsst.daf.butler.datastores.chainedDatastore

Example Registries
==================

.. automodapi:: lsst.daf.butler.registries.sqlRegistry

Example Formatters
==================

.. automodapi:: lsst.daf.butler.formatters.fileFormatter
.. automodapi:: lsst.daf.butler.formatters.jsonFormatter
.. automodapi:: lsst.daf.butler.formatters.yamlFormatter
.. automodapi:: lsst.daf.butler.formatters.pickleFormatter

Support API
===========

.. automodapi:: lsst.daf.butler.core.safeFileIo
.. automodapi:: lsst.daf.butler.core.utils
