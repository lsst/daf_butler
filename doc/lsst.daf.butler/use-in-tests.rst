.. py:currentmodule:: lsst.daf.butler.tests

.. _using-butler-in-tests:

##############################
Using the Butler in unit tests
##############################

This document describes how to use the Butler when testing Butler-dependent code in circumstances where one should not use a repository associated with a specific telescope, such as in unit tests.
It covers tools for creating memory-based repositories and for creating simple mock datasets.

The goal is not to produce a completely realistic production-like environment, as in integration testing, but to provide the minimal functionality needed for higher-level code such as tasks to run in an isolated test environment.

.. _using-butler-in-tests-overview:

Overview
========

The `lsst.daf.butler.tests` module provides tools for using the Butler in tests, including creating and populating minimal repositories.
These tools are designed to give each unit test case its own, customized environment containing only the state needed for that particular test.

.. _using-butler-in-tests-make-repo:

Creating and populating a repository
====================================

The `lsst.daf.butler.tests.makeTestRepo` function creates an in-memory repository.
In addition to the arguments used by `~lsst.daf.butler.Butler.makeRepo`, `makeTestRepo` takes a mapping of the data ID(s) needed by the test.
This mapping does not create any datasets itself, but instead prepares the registry so that datasets with those IDs *could* be created later.

The data ID mapping takes the form of a key for each data ID dimension, and a list of possible values.
For example:

.. code-block:: py

   {"instrument": ["FancyCam"],
    "exposure": [101, 102, 103],
    "detector": [0, 1, 2, 3],
    "skymap": ["map"],
    "tract": [42, 43],
    "patch": [0, 1, 2, 3, 4, 5],
   }

To avoid creating ambiguous data IDs, values are assigned to each other in an arbitrary one-to-one fashion, rather than trying every combination (e.g., in the above example all the patches might only be assigned to tract 42).
In many tests this is not a problem, as a single data ID is all that's needed.
If not, the `expandUniqueId` function can recover which values are assigned to which:

.. code-block:: py

   >>> import lsst.daf.butler.tests as butlerTests
   >>> butlerTests.expandUniqueId(repo, {"detector": 2})
   DataCoordinate({instrument, detector}, ('FancyCam', 2))

The `addDataIdValue` function defines allowed data ID values after the repository has been created.
Unlike the data ID parameter to `makeTestRepo`, `addDataIdValue` lets you optionally specify which values are related to which.
Any required but unspecified relationships are filled arbitrarily, so you need only give the ones your tests depend on.

.. code-block:: py

   from lsst.daf.butlker.tests import addDataIdValue

   addDataIdValue(butler, "skymap", "map")
   # Tract requires a skymap; can assume it's "map" because no other options.
   # Were there more than one skymap, this would choose one unpredictably.
   addDataIdValue(butler, "tract", 42)
   # Explicit specification.
   addDataIdValue(butler, "tract", 43, skymap="map")
   # Can map dimensions in a many-to-many relationship.
   for patch in [0, 1, 2, 3, 4, 5]:
       addDataIdValue(butler, "patch", patch, tract=42)
       addDataIdValue(butler, "patch", patch, tract=43)

The `addDatasetType` function registers any dataset types needed by the test (e.g., "calexp").
As with registering the data IDs, this is a prerequisite for actually reading or writing any datasets of that type during the test.

.. _using-butler-in-tests-make-collection:

Test collections
================

On some systems, `makeTestRepo` can be too slow to run for every test, so at present it should be called in the `~unittest.TestCase.setUpClass` method.
Individual tests can be partly isolated using the `lsst.daf.butler.tests.makeTestCollection` method, which creates a new collection in an existing repository.

.. note::

   While each collection has its own datasets, the set of valid dataset *types* and data IDs is repository-wide.
   Avoid defining these at the collection level, because they may have unpredictable effects on later tests.

Once a test collection is created, datasets can be read or written as usual:

.. code-block:: py
   :emphasize-lines: 1, 4

   butler = butlerTests.makeTestCollection(repo)

   exposure = makeTestExposure()  # user-defined code
   butler.put(exposure, "calexp", dataId)
   processCalexp(dataId)  # user-defined code
