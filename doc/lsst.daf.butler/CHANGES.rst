Butler v25.0.0 2023-02-27
=========================

New Features
------------

- * Added ``StorageClass.is_type`` method to compare a type with that of the storage class itelf.
  * Added keys, values, items, and iterator for ``StorageClassFactory``. (`DM-29835 <https://jira.lsstcorp.org/browse/DM-29835>`_)
- Updated parquet backend to use Arrow Tables natively, and add converters to and from pandas DataFrames, Astropy Tables, and Numpy structured arrays. (`DM-34874 <https://jira.lsstcorp.org/browse/DM-34874>`_)
- ``Butler.transfer_from()`` can now copy dimension records as well as datasets.
  This significantly enhances the usability of this method when transferring between disconnected Butlers.
  The ``butler transfer-datasets`` command will transfer dimension records by default but this can be disabled with the ``--no-transfer-dimensions`` option (which can be more efficient if you know that the destination Butler contains all the records). (`DM-34887 <https://jira.lsstcorp.org/browse/DM-34887>`_)
- ``butler query-data-ids`` will now determine default dimensions to use if a dataset type and collection is specified.
  The logical AND of all supplied dataset types will be used.
  Additionally, if no results are returned a reason will now be given in many cases. (`DM-35391 <https://jira.lsstcorp.org/browse/DM-35391>`_)
- Added ``DataFrameDelegate`` to allow DataFrames to be used with ``lsst.pipe.base.InMemoryDatasetHandle``. (`DM-35803 <https://jira.lsstcorp.org/browse/DM-35803>`_)
- Add ``StorageClass.findStorageClass`` method to find a storage class from a python type. (`DM-35815 <https://jira.lsstcorp.org/browse/DM-35815>`_)
- The optional dependencies of ``lsst-resources`` can be requested as optional dependencies of ``lsst-daf-butler`` and will be passed down to the underlying package.
  This allows callers of ``lsst.daf.butler`` to specify the type of resources they want to be able to access without being aware of the role of ``lsst.resources`` as an implementation detail. (`DM-35886 <https://jira.lsstcorp.org/browse/DM-35886>`_)
- Requires Python 3.10 or greater for better type annotation support. (`DM-36174 <https://jira.lsstcorp.org/browse/DM-36174>`_)
- Bind values in Registry queries can now specify list/tuple of numbers for identifiers appearing on the right-hand side of ``IN`` expression. (`DM-36325 <https://jira.lsstcorp.org/browse/DM-36325>`_)
- It is now possible to override the python type returned by ``butler.get()`` (if the types are compatible with each other) by using the new ``readStorageClass`` parameter.
  Deferred dataset handles can also be overridden.

  For example, to return an `astropy.table.Table` from something that usually returns an ``lsst.afw.table.Catalog`` you would do:

  .. code-block:: python

      table = butler.getDirect(ref, readStorageClass="AstropyTable")

  Any parameters given to the ``get()`` must still refer to the native storage class. (`DM-4551 <https://jira.lsstcorp.org/browse/DM-4551>`_)


API Changes
-----------

- Deprecate support for accessing data repositories with integer dataset IDs, and disable creation of new data repositories with integer dataset IDs, as per `RFC-854 <https://jira.lsstcorp.org/browse/RFC-854>`_. (`DM-35063 <https://jira.lsstcorp.org/browse/DM-35063>`_)
- ``DimensionUniverse`` now has a ``isCompatibleWith()`` method to check if two universes are compatible with each other.
  The initial test is very basic but can be improved later. (`DM-35082 <https://jira.lsstcorp.org/browse/DM-35082>`_)
- Deprecated support for components in `Registry.query*` methods, per `RFC-879 <https://jira.lsstcorp.org/browse/RFC-879>`_. (`DM-36312 <https://jira.lsstcorp.org/browse/DM-36312>`_)
- Multiple minor API changes to query methods from `RFC-878 <https://jira.lsstcorp.org/browse/RFC-878>`_ and `RFC-879 <https://jira.lsstcorp.org/browse/RFC-879>_`.

  This includes:

  - ``CollectionSearch`` is deprecated in favor of ``Sequence[str]`` and the new ``CollectionWildcard`` class.
  - ``queryDatasetTypes`` and ``queryCollections`` now return `~collections.abc.Iterable` (representing an unspecified in-memory collection) and `~collections.abc.Sequence`, respectively, rather than iterators.
  - ``DataCoordinateQueryResults.findDatasets`` now raises ``MissingDatasetTypeError`` when the given dataset type is not registered.
  - Passing regular expressions and other patterns as dataset types to ``queryDataIds`` and ``queryDimensionRecords`` is deprecated.
  - Passing unregistered dataset types ``queryDataIds`` and ``queryDimensionRecords`` is deprecated; in the future this will raise ``MissingDatasetTypeError`` instead of returning no query results.
  - Query result class ``explain_no_results`` now returns `~collections.abc.Iterable` instead of `~collections.abc.Iterator`. (`DM-36313 <https://jira.lsstcorp.org/browse/DM-36313>`_)
- A method has been added to ``DatasetRef`` and ``DatasetType``, named ``overrideStorageClass``, to allow a new object to be created that has a different storage class associated with it. (`DM-4551 <https://jira.lsstcorp.org/browse/DM-4551>`_)


Bug Fixes
---------

- Fixed a bug in the parquet reader where a single string column name would be interpreted as an iterable. (`DM-35803 <https://jira.lsstcorp.org/browse/DM-35803>`_)
- Fixed bug in ``elements`` argument to various export methods that prevented it from doing anything. (`DM-36111 <https://jira.lsstcorp.org/browse/DM-36111>`_)
- A bug has been fixed in ``DatastoreCacheManager`` that triggered if two processes try to cache the same dataset simultaneously. (`DM-36412 <https://jira.lsstcorp.org/browse/DM-36412>`_)
- Fixed bug in pandas ``dataframe`` to arrow conversion that would crash with some pandas object data types. (`DM-36775 <https://jira.lsstcorp.org/browse/DM-36775>`_)
- Fixed bug in pandas ``dataframe`` to arrow conversion that would crash with partially nulled string columns. (`DM-36795 <https://jira.lsstcorp.org/browse/DM-36795>`_)


Other Changes and Additions
---------------------------

- For command-line options that split on commas, it is now possible to specify parts of the string not to split by using ``[]`` to indicate comma-separated list content. (`DM-35917 <https://jira.lsstcorp.org/browse/DM-35917>`_)
- Moved the typing workaround for the built-in `Ellipsis` (`...`) singleton to ``lsst.utils``. (`DM-36108 <https://jira.lsstcorp.org/browse/DM-36108>`_)
- Now define regions for data IDs with multiple spatial dimensions to the intersection of those dimensions' regions. (`DM-36111 <https://jira.lsstcorp.org/browse/DM-36111>`_)
- Added support for in-memory datastore to roll back a call to ``datastore.trash()``.
  This required that the ``bridge.moveToTrash()`` method now takes an additional ``transaction`` parameter (that can be `None`). (`DM-36172 <https://jira.lsstcorp.org/browse/DM-36172>`_)
- Restructured internal Registry query system methods to share code better and prepare for more meaningful changes. (`DM-36174 <https://jira.lsstcorp.org/browse/DM-36174>`_)
- Removed unnecessary table-locking in dimension record insertion.

  Prior to this change, we used explicit full-table locks to guard against a race condition that wasn't actually possible, which could lead to deadlocks in rare cases involving insertion of governor dimension records. (`DM-36326 <https://jira.lsstcorp.org/browse/DM-36326>`_)
- Chained Datastore can now support "move" transfer mode for ingest.
  Files are copied to each child datastore unless only one child datastore is accepting the incoming files, in which case "move" is used. (`DM-36410 <https://jira.lsstcorp.org/browse/DM-36410>`_)
- ``DatastoreCacheManager`` can now use an environment variable, ``$DAF_BUTLER_CACHE_DIRECTORY_IF_UNSET``, to specify a cache directory to use if no explicit directory has been specified by configuration or by the ``$DAF_BUTLER_CACHE_DIRECTORY`` environment variable.
  Additionally, a ``DatastoreCacheManager.set_fallback_cache_directory_if_unset()`` class method has been added that will set this environment variable with a suitable value.
  This is useful for multiprocessing where each forked or spawned subprocess needs to share the same cache directory. (`DM-36412 <https://jira.lsstcorp.org/browse/DM-36412>`_)
- Added support for ``ChainedDatastore.export()``. (`DM-36517 <https://jira.lsstcorp.org/browse/DM-36517>`_)
- Reworked transaction and connection management for compatibility with transaction-level connection pooling on the server.

  Butler clients still hold long-lived connections, via delegation to SQLAlchemy's connection pooling, which can handle disconnections transparently most of the time.  But we now wrap all temporary table usage and cursor iteration in transactions. (`DM-37249 <https://jira.lsstcorp.org/browse/DM-37249>`_)


An API Removal or Deprecation
-----------------------------

- Removed deprecated filterLabel exposure component access. (`DM-27811 <https://jira.lsstcorp.org/browse/DM-27811>`_)


Butler v24.0.0 2022-08-26
=========================

New Features
------------

- Support LSST-style visit definitions where a single exposure is part of a set of related exposures all taken with the same acquisition command.
  Each exposure knows the "visit" it is part of.

  * Modify the ``exposure`` dimension record to include ``seq_start`` and ``seq_end`` metadata.
  * Modify ``visit`` record to include a ``seq_num`` field.
  * Remove ``visit_system`` dimension and add ``visit_system_membership`` record to allow a visit to be associated with multiple visit systems. (`DM-30948 <https://jira.lsstcorp.org/browse/DM-30948>`_)
- ``butler export-calibs`` now takes a ``--transfer`` option to control how data are exported (use ``direct`` to do in-place export) and a ``--datasets`` option to limit the dataset types to be exported.
  It also now takes a default collections parameter (all calibration collections). (`DM-32061 <https://jira.lsstcorp.org/browse/DM-32061>`_)
- Iterables returned from registry methods `queryDataIds` and `queryDimensionRecords` have two new methods - `order_by` and `limit`. (`DM-32403 <https://jira.lsstcorp.org/browse/DM-32403>`_)
- Builds using ``setuptools`` now calculate versions from the Git repository, including the use of alpha releases for those associated with weekly tags. (`DM-32408 <https://jira.lsstcorp.org/browse/DM-32408>`_)
- Butler can now support lookup of repositories by label if the user environment is correctly configured.
  This is done using the new `~lsst.daf.butler.Butler.get_repo_uri()` and `~lsst.daf.butler.Butler.get_known_repos()` APIs. (`DM-32491 <https://jira.lsstcorp.org/browse/DM-32491>`_)
- Add a butler command line command called ``butler remove-collections`` that can remove non-RUN collections. (`DM-32687 <https://jira.lsstcorp.org/browse/DM-32687>`_)
- Add a butler command line command called ``butler remove-runs`` that can remove RUN collections and contained datasets. (`DM-32831 <https://jira.lsstcorp.org/browse/DM-32831>`_)
- It is now possible to register type conversion functions with storage classes.
  This can allow a dataset type definition to change storage class in the registry whilst allowing datasets that have already been serialized using one python type to be returned using the new python type.
  The ``storageClasses.yaml`` definitions can now look like:

  .. code-block:: yaml

     TaskMetadata:
       pytype: lsst.pipe.base.TaskMetadata
       converters:
         lsst.daf.base.PropertySet: lsst.pipe.base.TaskMetadata.from_metadata

  Declares that if a ``TaskMetadata`` is expected then a ``PropertySet`` can be converted to the correct python type. (`DM-32883 <https://jira.lsstcorp.org/browse/DM-32883>`_)
- Dimension record imports now ignore conflicts (without checking for consistency) instead of failing. (`DM-33148 <https://jira.lsstcorp.org/browse/DM-33148>`_)
- Storage class converters can now also be used on `~lsst.daf.butler.Butler.put`. (`DM-33155 <https://jira.lsstcorp.org/browse/DM-33155>`_)
- If a `~lsst.daf.butler.DatasetType` has been constructed that differs from the registry definition, but in a way that is compatible through `~lsst.daf.butler.StorageClass` conversion, then using that in a `lsst.daf.butler.Butler.get()` call will return a python type that matches the user-specified `~lsst.daf.butler.StorageClass` instead of the internal python type. (`DM-33303 <https://jira.lsstcorp.org/browse/DM-33303>`_)
- The dataset ID can now be used in a file template for datastore (using ``{id}``). (`DM-33414 <https://jira.lsstcorp.org/browse/DM-33414>`_)
- Add `Registry.getCollectionParentChains` to find the `CHAINED` collections that another collection belongs to. (`DM-33643 <https://jira.lsstcorp.org/browse/DM-33643>`_)
- Added ``has_simulated`` to the ``exposure`` record to indicate that some content of this exposure was simulated. (`DM-33728 <https://jira.lsstcorp.org/browse/DM-33728>`_)
- The command-line tooling has changed how it sets the default logger when using ``--log-level``.
  Now only the default logger(s) (``lsst`` and the colon-separated values stored in the ``$DAF_BUTLER_ROOT_LOGGER``) will be affected by using ``--log-level`` without a specific logger name.
  By default only this default logger will be set to ``INFO`` log level and all other loggers will remain as ``WARNING``.
  Use ``--log-level '.=level'`` to change the root logger (this will not change the default logger level and so an additional call to ``--log-level DEBUG`` may be needed to turn on debugging for all loggers). (`DM-33809 <https://jira.lsstcorp.org/browse/DM-33809>`_)
- Added ``azimuth`` to the ``exposure`` and ``visit`` records. (`DM-33859 <https://jira.lsstcorp.org/browse/DM-33859>`_)
- If repository aliases have been defined for the site they can now be used in place of the Butler repository URI in both the `~lsst.daf.butler.Butler` constructor and command-line tools. (`DM-33870 <https://jira.lsstcorp.org/browse/DM-33870>`_)
- * Added ``visit_system`` to ``instrument`` record and allowed it to be used as a tie breaker in dataset determination if a dataId is given using ``seq_num`` and ``day_obs`` and it matches multiple visits.
  * Modify export YAML format to include the dimension universe version and namespace.
  * Allow export files with older visit definitions to be read (this does not fill in the new metadata records).
  * `DimensionUniverse` now supports the ``in`` operator to check if a dimension is part of the universe. (`DM-33942 <https://jira.lsstcorp.org/browse/DM-33942>`_)
- * Added a definition for using healpix in skypix definitions.
  * Change dimension universe caching to support a namespace in addition to a version number. (`DM-33946 <https://jira.lsstcorp.org/browse/DM-33946>`_)
- Added a formatter for `lsst.utils.packages.Packages` Python types in `lsst.daf.butler.formatters.packages.PackagesFormatter`. (`DM-34105 <https://jira.lsstcorp.org/browse/DM-34105>`_)
- Added an optimization that speeds up ``butler query-datasets`` when using ``--show-uri``. (`DM-35120 <https://jira.lsstcorp.org/browse/DM-35120>`_)


API Changes
-----------

- Many internal utilities from ``lsst.daf.butler.core.utils`` have been relocated to the ``lsst.utils`` package. (`DM-31722 <https://jira.lsstcorp.org/browse/DM-31722>`_)
- The ``ButlerURI`` class has now been removed from this package.
  It now exists as `lsst.resources.ResourcePath`.
  All code should be modified to use the new class name. (`DM-31723 <https://jira.lsstcorp.org/browse/DM-31723>`_)
- `lsst.daf.butler.Registry.registerRun` and `lsst.daf.butler.Registry.registerCollection` now return a Booelan indicating whether the collection was created or already existed. (`DM-31976 <https://jira.lsstcorp.org/browse/DM-31976>`_)
- A new optional parameter, ``record_validation_info`` has been added to `~lsst.daf.butler.Butler.ingest` (and related datastore APIs) to allow the caller to declare that file attributes such as the file size or checksum should not be recorded.
  This can be useful if the file is being monitored by an external system or it is known that the file might be compressed in-place after ingestion. (`DM-33086 <https://jira.lsstcorp.org/browse/DM-33086>`_)
- Added a new `DatasetType.is_compatible_with` method.
  This method determines if two dataset types are compatible with each other, taking into account whether the storage classes allow type conversion. (`DM-33278 <https://jira.lsstcorp.org/browse/DM-33278>`_)
- The `run` parameter has been removed from Butler method `lsst.daf.butler.Butler.pruneDatasets`.
  It was never used in Butler implementation, client code should simply remove it. (`DM-33488 <https://jira.lsstcorp.org/browse/DM-33488>`_)
- Registry methods now raise exceptions belonging to a class hierarchy rooted at `lsst.daf.butler.registry.RegistryError`.
  See also :ref:`daf_butler_query_error_handling` for details. (`DM-33600 <https://jira.lsstcorp.org/browse/DM-33600>`_)
- Added ``DatasetType.storageClass_name`` property to allow the name of the storage class to be retrieved without requiring that the storage class exists.
  This is possible if people have used local storage class definitions or a test ``DatasetType`` was created temporarily. (`DM-34460 <https://jira.lsstcorp.org/browse/DM-34460>`_)


Bug Fixes
---------

- ``butler export-calibs`` can now copy files that require the use of a file template (for example if a direct URI was stored in datastore) with metadata records.
  File templates that use metadata records now complain if the record is not attached to the ``DatasetRef``. (`DM-32061 <https://jira.lsstcorp.org/browse/DM-32061>`_)
- Make it possible to run `queryDimensionRecords` while constraining on the existence of a dataset whose dimensions are not a subset of the record element's dependencies (e.g. `raw` and `exposure`). (`DM-32454 <https://jira.lsstcorp.org/browse/DM-32454>`_)
- Butler constructor can now take a `os.PathLike` object when the ``butler.yaml`` is not included in the path. (`DM-32467 <https://jira.lsstcorp.org/browse/DM-32467>`_)
- In the butler presets file (used by the ``--@`` option), use option names that match the butler CLI command option names (without leading dashes).
  Fail if option names used in the presets file do not match options for the current butler command. (`DM-32986 <https://jira.lsstcorp.org/browse/DM-32986>`_)
- The butler CLI command ``remove-runs`` can now unlink RUN collections from parent CHAINED collections. (`DM-33619 <https://jira.lsstcorp.org/browse/DM-33619>`_)
- Improves ``butler query-collections``:

  * TABLE output formatting is easier to read.
  * Adds INVERSE modes for TABLE and TREE output, to view CHAINED parent(s) of collections (non-INVERSE lists children of CHAINED collections).
  * Sorts datasets before printing them. (`DM-33902 <https://jira.lsstcorp.org/browse/DM-33902>`_)
- Fix garbled printing of raw-byte hashes in query-dimension-records. (`DM-34007 <https://jira.lsstcorp.org/browse/DM-34007>`_)
- The automatic addition of ``butler.yaml`` to the Butler configuration URI now also happens when a ``ResourcePath`` instance is given. (`DM-34172 <https://jira.lsstcorp.org/browse/DM-34172>`_)
- Fix handling of "doomed" (known to return no results even before execution) follow-up queries for datasets.
  This frequently manifested as a `KeyError` with a message about dataset type registration during `QuantumGraph` generation. (`DM-34202 <https://jira.lsstcorp.org/browse/DM-34202>`_)
- Fix `~lsst.daf.butler.Registry.queryDataIds` bug involving dataset constraints with no dimensions. (`DM-34247 <https://jira.lsstcorp.org/browse/DM-34247>`_)
- The `click.Path` API changed, change from ordered arguments to keyword arguments when calling it. (`DM-34261 <https://jira.lsstcorp.org/browse/DM-34261>`_)
- Fix `~lsst.daf.butler.Registry.queryCollections` bug in which children of chained collections were being alphabetically sorted instead of ordered consistently with the order in which they would be searched. (`DM-34328 <https://jira.lsstcorp.org/browse/DM-34328>`_)
- Fixes the bug introduced in `DM-33489 <https://jira.lsstcorp.org/browse/DM-33489>`_ (appeared in w_2022_15) which causes not-NULL constraint violation for datastore component column. (`DM-34375 <https://jira.lsstcorp.org/browse/DM-34375>`_)
- Fixes an issue where the command line tools were caching argument and option values but not separating option names from option values correctly in some cases. (`DM-34812 <https://jira.lsstcorp.org/browse/DM-34812>`_)


Other Changes and Additions
---------------------------

- Add a `NOT NULL` constraint to dimension implied dependency columns.

  `NULL` values in these columns already cause the query system to misbehave. (`DM-21840 <https://jira.lsstcorp.org/browse/DM-21840>`_)
- Update parquet writing to use default per-column compression. (`DM-31963 <https://jira.lsstcorp.org/browse/DM-31963>`_)
- Tidy up ``remove-runs`` subcommand confirmation report by sorting dataset types and filtering out those with no datasets in the collections to be deleted. (`DM-33584 <https://jira.lsstcorp.org/browse/DM-33584>`_)
- The constraints on collection names have been relaxed.
  Previously collection names were limited to ASCII alphanumeric characters plus a limited selection of symbols (directory separator, @-sign).
  Now all unicode alphanumerics can be used along with emoji. (`DM-33999 <https://jira.lsstcorp.org/browse/DM-33999>`_)
- File datastore now always writes a temporary file and renames it even for local file system datastores.
  This minimizes the risk of a corrupt file being written if the process writing the file is killed at the wrong time. (`DM-35458 <https://jira.lsstcorp.org/browse/DM-35458>`_)


An API Removal or Deprecation
-----------------------------

- The ``butler prune-collections`` command line command is now deprecated.
  Please consider using ``remove-collections`` or ``remove-runs`` instead. Will be removed after v24. (`DM-32499 <https://jira.lsstcorp.org/browse/DM-32499>`_)
- All support for reading and writing `~lsst.afw.image.Filter` objects has been removed.
  The old ``filter`` component for exposures has been removed, and replaced with a new ``filter`` component backed by `~lsst.afw.image.FilterLabel`.
  It functions identically to the ``filterLabel`` component, which has been deprecated. (`DM-27177 <https://jira.lsstcorp.org/browse/DM-27177>`_)


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
