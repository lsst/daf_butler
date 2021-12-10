Butler v23.0.0 2021-12-10
=========================

New Features
------------

- Add ability to cache datasets locally when using a remote file store.
  This can significantly improve performance when retrieving components from a dataset. (`DM-13365 <https://jira.lsstcorp.org/browse/DM-13365>`_)
- Add a new ``butler retrieve-artifacts`` command to copy file artifacts from a Butler datastore. (`DM-27241 <https://jira.lsstcorp.org/browse/DM-27241>`_)
- Add ``butler transfer-datasets`` command-line tool and associated ``Butler.transfer_from()`` API.

  This can be used to transfer datasets between different butlers, with the caveat that dimensions and dataset types must be pre-defined in the receiving butler repository. (`DM-28650 <https://jira.lsstcorp.org/browse/DM-28650>`_)
- Add ``amp`` parameter to the Exposure StorageClass, allowing single-amplifier subimage reads. (`DM-29370 <https://jira.lsstcorp.org/browse/DM-29370>`_)
- Add new ``butler collection-chain`` subcommand for creating collection chains from the command line. (`DM-30373 <https://jira.lsstcorp.org/browse/DM-30373>`_)
- Add ``butler ingest-files`` subcommand to simplify ingest of any external file. (`DM-30935 <https://jira.lsstcorp.org/browse/DM-30935>`_)
- * Add class representing a collection of log records (``ButlerLogRecords``).
  * Allow this class to be stored and retrieved from a Butler datastore.
  * Add special log handler to allow JSON log records to be stored.
  * Add ``--log-file`` option to command lines to redirect log output to file.
  * Add ``--no-log-tty`` to disable log output to terminal. (`DM-30977 <https://jira.lsstcorp.org/browse/DM-30977>`_)
- Registry methods that previously could raise an exception when searching in
  calibrations collections now have an improved logic that skip those
  collections if they were not given explicitly but only appeared in chained
  collections. (`DM-31337 <https://jira.lsstcorp.org/browse/DM-31337>`_)
- Add a confirmation step to ``butler prune-collection`` to help prevent
  accidental removal of collections. (`DM-31366 <https://jira.lsstcorp.org/browse/DM-31366>`_)
- Add ``butler register-dataset-type`` command to register a new dataset type. (`DM-31367 <https://jira.lsstcorp.org/browse/DM-31367>`_)
- Use cached summary information to simplify queries involving datasets and provide better diagnostics when those queries yield no results. (`DM-31583 <https://jira.lsstcorp.org/browse/DM-31583>`_)
- Add a new ``butler export-calibs`` command to copy calibrations and write an export.yaml document from a Butler datastore. (`DM-31596 <https://jira.lsstcorp.org/browse/DM-31596>`_)
- Support rewriting of dataId containing dimension records such as ``day_obs`` and ``seq_num`` in ``butler.put()``.
  This matches the behavior of ``butler.get()``. (`DM-31623 <https://jira.lsstcorp.org/browse/DM-31623>`_)
- Add ``--log-label`` option to ``butler`` command to allow extra information to be injected into the log record. (`DM-31884 <https://jira.lsstcorp.org/browse/DM-31884>`_)
- * The ``Butler.transfer_from`` method no longer registers new dataset types by default.
  * Add the related option ``--register-dataset-types`` to the ``butler transfer-datasets`` subcommand. (`DM-31976 <https://jira.lsstcorp.org/browse/DM-31976>`_)
- Support UUIDs as the primary keys in registry and allow for reproducible UUIDs.

  This change will significantly simplify transferring of data between butler repositories. (`DM-29196 <https://jira.lsstcorp.org/browse/DM-29196>`_)
- Allow registry methods such as ``queryDatasets`` to use a glob-style string when specifying collection or dataset type names. (`DM-30200 <https://jira.lsstcorp.org/browse/DM-30200>`_)
- Add support for updating and replacing dimension records. (`DM-30866 <https://jira.lsstcorp.org/browse/DM-30866>`_)


API Changes
-----------

- A new method ``Datastore.knows()`` has been added to allow a user to ask the datastore whether it knows about a specific dataset but without requiring a check to see if the artifact itself exists.
  Use ``Datastore.exists()`` to check that the datastore knows about a dataset and the artifact exists. (`DM-30335 <https://jira.lsstcorp.org/browse/DM-30335>`_)


Bug Fixes
---------

- Fix handling of ingest_date timestamps.

  Previously there was an inconsistency between ingest_date database-native UTC
  handling and astropy Time used for time literals which resulted in 37 second
  difference. This updates makes consistent use of database-native time
  functions to resolve this issue. (`DM-30124 <https://jira.lsstcorp.org/browse/DM-30124>`_)
- Fix butler repository creation when a seed config has specified a registry manager override.

  Previously only that manager was recorded rather than the full set.
  We always require a full set to be recorded to prevent breakage of a butler when a default changes. (`DM-30372 <https://jira.lsstcorp.org/browse/DM-30372>`_)
- Stop writing a temporary datastore cache directory every time a ``Butler`` object was instantiated.
  Now only create one when one is requested. (`DM-30743 <https://jira.lsstcorp.org/browse/DM-30743>`_)
- Fix ``Butler.transfer_from()`` such that it registers any missing dataset types and also skips any datasets that do not have associated datastore artifacts. (`DM-30784 <https://jira.lsstcorp.org/browse/DM-30784>`_)
- Add support for click 8.0. (`DM-30855 <https://jira.lsstcorp.org/browse/DM-30855>`_)
- Replace UNION ALL with UNION for subqueries for simpler query plans. (`DM-31429 <https://jira.lsstcorp.org/browse/DM-31429>`_)
- Fix parquet formatter error when reading tables with no indices.

  Previously, this would cause butler.get to fail to read valid parquet tables. (`DM-31700 <https://jira.lsstcorp.org/browse/DM-31700>`_)
- Fix problem in ButlerURI where transferring a file from one URI to another would overwrite the existing file even if they were the same actual file (for example because of soft links in the directory hierarchy). (`DM-31826 <https://jira.lsstcorp.org/browse/DM-31826>`_)


Performance Enhancement
-----------------------

- Make collection and dataset pruning significantly more efficient. (`DM-30140 <https://jira.lsstcorp.org/browse/DM-30140>`_)
- Add indexes to make certain spatial join queries much more efficient. (`DM-31548 <https://jira.lsstcorp.org/browse/DM-31548>`_)
- Made 20x speed improvement for ``Butler.transfer_from``.
  The main slow down is asking the datastore whether a file artifact exists.
  This is now parallelized and the result is cached for later. (`DM-31785 <https://jira.lsstcorp.org/browse/DM-31785>`_)
- Minor efficiency improvements when accessing `lsst.daf.butler.Config` hierarchies. (`DM-32305 <https://jira.lsstcorp.org/browse/DM-32305>`_)
- FileDatastore: Improve removing of datasets from the trash by at least a factor of 10. (`DM-29849 <https://jira.lsstcorp.org/browse/DM-29849>`_)

Other Changes and Additions
---------------------------

- Enable serialization of ``DatasetRef`` and related classes to JSON format. (`DM-28678 <https://jira.lsstcorp.org/browse/DM-28678>`_)
- `ButlerURI` ``http`` schemes can now handle non-WebDAV endpoints.
  Warnings are only issued if WebDAV functionality is requested. (`DM-29708 <https://jira.lsstcorp.org/browse/DM-29708>`_)
- Switch logging such that all logging messages are now forwarded to Python ``logging`` from ``lsst.log``.
  Previously all Python ``logging`` messages were being forwarded to ``lsst.log``. (`DM-31120 <https://jira.lsstcorp.org/browse/DM-31120>`_)
- Add formatter and storageClass information for FocalPlaneBackground. (`DM-22534 <https://jira.lsstcorp.org/browse/DM-22534>`_)
- Add formatter and storageClass information for IsrCalib. (`DM-29531 <https://jira.lsstcorp.org/browse/DM-29531>`_)
- Change release note creation to use [Towncrier](https://towncrier.readthedocs.io/en/actual-freaking-docs/index.html). (`DM-30291 <https://jira.lsstcorp.org/browse/DM-30291>`_)
- Add a Butler configuration for an execution butler that has pre-defined registry entries but no datastore records.

  The `Butler.put()` will return the pre-existing dataset ref but will still fail if a datastore record is found. (`DM-30335 <https://jira.lsstcorp.org/browse/DM-30335>`_)
- If an unrecognized dimension is used as a look up key in a configuration file (using the ``+`` syntax) a warning is used suggesting a possible typo rather than a confusing `KeyError`.
  This is no longer a fatal error and the key will be treated as a name. (`DM-30685 <https://jira.lsstcorp.org/browse/DM-30685>`_)
- Add ``split`` transfer mode that can be used when some files are inside the datastore and some files are outside the datastore.
  This is equivalent to using `None` and ``direct`` mode dynamically. (`DM-31251 <https://jira.lsstcorp.org/browse/DM-31251>`_)

Butler v22.0 2021-04-01
=======================

New Features
------------

* A Butler instance can now be configured with dataId defaults such as an instrument or skymap. [DM-27153]
* Add ``butler prune-datasets`` command. [DM-26689]
* Add ``butler query-dimension-records`` command [DM-27344]
* Add ``--unlink`` option to ``butler prune-collection`` command. [DM-28857]
* Add progress reporting option for long-lived commands. [DM-28964]
* Add ``butler associate`` command to add existing datasets to a tagged collection. [DM-26688]
* Add officially-supported JSON serialization for core Butler classes. [DM-28314]
* Allow ``butler.get()`` to support dimension record values such as exposure observing day or detector name in the dataID. [DM-27152]
* Add "direct" ingest mode to allow a file to be ingested retaining the full path to the original file. [DM-27478]

Bug Fixes
---------

* Fix temporal queries and clarify ``Timespan`` behavior. [DM-27985]

Other Changes and Additions
---------------------------

* Make ``ButlerURI`` class immutable. [DM-29073]
* Add ``ButlerURI.findFileResources`` method to walk the directory tree and return matching files. [DM-29011]
* Improve infrastructure for handling test repositories. [DM-23862]

Butler Datastores
-----------------

New Features
~~~~~~~~~~~~

* Implement basic file caching for use with remote datastores. [DM-29383]
* Require that a DataId always be available to a ``Formatter``. This allows formatters to do a consistency check such as comparing the physical filter in a dataId with that read from a file. [DM-28583]
* Add special mode to datastore to instruct it to ignore registry on ``get``. This is useful for Execution Butlers where registry knows in advance about all datasets but datastore does not. [DM-28648]
* Add ``forget`` method to instruct datastore to remove all knowledge of a dataset without deleting the file artifact. [DM-29106]

Butler Registry
---------------

New Features
~~~~~~~~~~~~

* Avoid long-lived connections to database. [DM-26302]
* Add option to flatten when setting a collection chain. [DM-29203]
