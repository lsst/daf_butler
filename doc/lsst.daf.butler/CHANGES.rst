Butler 27.0.0 2024-05-28
========================

Now supports Python 3.12.

New Features
------------

- Updated the open-source license to allow for the code to be distributed with either GPLv3 or BSD 3-clause license. (`DM-37231 <https://rubinobs.atlassian.net/browse/DM-37231>`_)
- Added new storage class and formatter for ``NNModelPackagePayload`` -- an interface between butler and pretrained neural networks, currently implemented in pytorch. (`DM-38454 <https://rubinobs.atlassian.net/browse/DM-38454>`_)
- Improved support for finding calibrations and spatially-joined datasets as follow-ups to data ID queries. (`DM-38498 <https://rubinobs.atlassian.net/browse/DM-38498>`_)
- Added a storage class and associated formatter for the Spectractor ``FitParameters`` class, which holds the fitted ``LIBRADTRAN`` atmospheric parameters. (`DM-38745 <https://rubinobs.atlassian.net/browse/DM-38745>`_)
- Added support for serialization and deserialization of Arrow schemas via Parquet, and added support for translation of ``doc`` and ``units`` to/from arrow/astropy schemas. (`DM-40582 <https://rubinobs.atlassian.net/browse/DM-40582>`_)
- Added ``DimensionElement.schema`` as a less SQL-oriented way to inspect the fields of a ``DimensionRecord``.

  Also added two high-level containers (``DimensionRecordSet`` and ``DimensionRecordTable``) for ``DimensionRecord`` objects, but these should be considered experimental and unstable until they are used in public ``Butler`` APIs. (`DM-41113 <https://rubinobs.atlassian.net/browse/DM-41113>`_)
- Added new ``Butler`` APIs migrated from registry: ``Butler.get_dataset_type()``, ``Butler.get_dataset()``, and ``Butler.find_dataset()``. (`DM-41365 <https://rubinobs.atlassian.net/browse/DM-41365>`_)
- Butler server can now be configured to use a ``ChainedDatastore``. (`DM-41880 <https://rubinobs.atlassian.net/browse/DM-41880>`_)
- * Added new API ``Butler.transfer_dimension_records_from()`` to copy dimension records out of some refs and add them to the target butler.
  * This and ``Butler.transfer_from()`` now copy related dimension records as well as the records associated directly with the refs.
    For example, if ``visit`` is being transferred additional records such as ``visit_definition`` will also be copied.
    This requires a full Butler and not a limited Butler (such as the one backed by a quantum graph). (`DM-41966 <https://rubinobs.atlassian.net/browse/DM-41966>`_)
- Added ``LabeledButlerFactory``, a factory class for constructing Butler instances.  This is intended for use in long-lived services that need to be able to create a Butler instance for each incoming client request. (`DM-42188 <https://rubinobs.atlassian.net/browse/DM-42188>`_)
- Added a new optional dependency set ``remote``, which can be used to install the dependencies required by the client half of Butler client/server. (`DM-42190 <https://rubinobs.atlassian.net/browse/DM-42190>`_)
- "Cloned" Butler instances returned from ``Butler(butler=otherButler)`` and ``LabeledButlerFactory`` no longer share internal state with their parent instance.  This makes it safe to use the new instance concurrently with the original in separate threads.  It is still unsafe to use a single ``Butler`` instance concurrently from multiple threads. (`DM-42317 <https://rubinobs.atlassian.net/browse/DM-42317>`_)
- * Released ``DimensionUniverse`` version 6
    * ``group`` and ``day_obs`` are now true dimensions.
    * ``exposure`` now implies both ``group`` and ``day_obs``, and ``visit`` implies ``day_obs``.
  * Exported YAML files using universe version 1 and newer can be imported and converted to universe version 6. (`DM-42636 <https://rubinobs.atlassian.net/browse/DM-42636>`_)
- The Butler repository index can now be configured by a new environment variable ``$DAF_BUTLER_REPOSITORIES``, which contains the configuration directly instead of requiring lookup via a URI. (`DM-42660 <https://rubinobs.atlassian.net/browse/DM-42660>`_)
- Added ``can_see_sky`` metadata field to ``exposure`` dimension record (dimension universe v7).
  This field can indicate whether the detector received photons from the sky taking into account the camera shutter and the dome and telescope alignment. (`DM-43101 <https://rubinobs.atlassian.net/browse/DM-43101>`_)
- Added additional collection chain methods to the ``Butler.collection_chains`` interface: ``extend_chain``, ``remove_from_chain``, and ``redefine_chain``.  These methods are all "atomic" functions that can safely be used concurrently from multiple processes. (`DM-43315 <https://rubinobs.atlassian.net/browse/DM-43315>`_)
- Added a ``timespan`` parameter to ``Butler.get()`` (for direct and remote butler).
  This parameter can be used to specify an explicit time for calibration selection without requiring a temporal coordinate be included in the data ID.
  Additionally, if no time span is specified and no time span can be found in the data ID a default full-range time span will be used for calibration selection.
  This allows a calibration to be selected if there is only one matching calibration in the collection. (`DM-43499 <https://rubinobs.atlassian.net/browse/DM-43499>`_)
- Added a new method ``Butler.collection_chains.prepend_chain``.  This allows you to insert collections at the beginning of a chain. It is an "atomic" operation that can be safely used concurrently from multiple processes. (`DM-43671 <https://rubinobs.atlassian.net/browse/DM-43671>`_)
- Added ``MatchingKernel`` storage class for persisting the PSF-matching kernel from image differencing. (`DM-43736 <https://rubinobs.atlassian.net/browse/DM-43736>`_)
- Made ``Timespan`` a Pydantic model and added a ``SerializableRegion`` type alias that allows ``lsst.sphgeom.Region`` to be used directly as a Pydantic model field. (`DM-43769 <https://rubinobs.atlassian.net/browse/DM-43769>`_)


API Changes
-----------

- Deprecated most public APIs that use ``Dimension`` or ``DimensionElement`` objects.

  This implements `RFC-834 <https://rubinobs.atlassian.net/browse/RFC-834>`_, deprecating the ``DimensionGraph`` class (in favor of the new, similar ``DimensionGroup``) and a large number of ``DataCoordinate`` methods and attributes, including its `collections.abc.Mapping` interface.

  This includes:

  - use ``DataCoordinate.dimensions`` instead of ``DataCoordinate.graph`` (likewise for arguments to ``DataCoordinate.standardize``);
  - use ``dict(DataCoordinate.required)`` as a drop-in replacement for ``DataCoordinate.byName()``, but consider whether you want ``DataCoordinate.required`` (a `~collections.abc.Mapping` view rather than a `dict`) or ``DataCoordinate.mapping`` (a `~collections.abc.Mapping` with all *available* key-value pairs, not just the required ones);
  - also use ``DataCoordinate.mapping`` or ``DataCoordinate.required`` instead of treating ``DataCoordinate`` itself as a `~collections.abc.Mapping`, *except* square-bracket indexing, which is still very much supported;
  - use ``DataCoordinate.dimensions.required.names`` or ``DataCoordinate.required.keys()`` as a drop-in replacement for ``DataCoordinate.keys().names`` or ``DataCoordinate.names``, but consider whether you actually want ``DataCoordinate.dimensions.names`` or ``DataCoordinate.mapping.keys`` instead.

  ``DimensionGroup`` is almost identical to ``DimensionGraph``, but it and its subset attributes are not directly iterable (since those iterate over ``Dimension`` and ``DimensionElement`` objects); use the ``.names`` attribute to iterate over names instead (just as names could be iterated over in ``DimensionGraph``).

  ``DimensionGraph`` is still used in some ``lsst.daf.butler`` APIs (most prominently ``DatasetType.dimensions``) that may be accessed without deprecation warnings being emitted, but iterating over that object or its subset attributes *will* yield deprecation warnings.
  And ``DimensionGraph`` is still accepted along with ``DimensionGroup`` without warning in most public APIs.
  When ``DimensionGraph`` is removed, methods and properties that return ``DimensionGraph`` will start returning ``DimensionGroup`` instead.

  Rare code (mostly in downstream middleware packages) that does need access to ``Dimension`` or ``DimensionElement`` objects should obtain them directly from the ``DimensionUniverse``.
  For the pattern of checking whether a dimension is a skypix level, test whether its name is in ``DimensionUniverse.skypix_dimensions`` or ``DimensionGroup.skypix`` instead of obtaining a ``Dimension`` instance and calling ``isinstance(dimension, SkyPixDimension)``. (`DM-34340 <https://rubinobs.atlassian.net/browse/DM-34340>`_)
- Added new ``transfer_option_no_short`` that creates the ``--transfer`` option without the associated ``-t`` alias. (`DM-35599 <https://rubinobs.atlassian.net/browse/DM-35599>`_)
- - ``Butler`` class became an abstract base class, original ``Butler`` was renamed to ``DirectButler``.
  - Clients that need an access to ``DirectButler`` class will have to import it from ``lsst.daf.butler.direct_butler``.
  - ``Butler.from_config(...)`` should be used to make ``Butler`` instances. ``Butler(...)`` still works and is identical to ``Butler.from_config(...)``, but will generate ``mypy`` errors. (`DM-41116 <https://rubinobs.atlassian.net/browse/DM-41116>`_)
- ``SqlRegistry`` does not inherit now from ``Registry`` or any other interface, and has been moved to ``registry.sql_registry`` module. (`DM-41235 <https://rubinobs.atlassian.net/browse/DM-41235>`_)
- Added ``Butler._query`` context manager which will support building of the complex queries for data in Butler.
  For now ``Butler._query`` provides access to just three convenience methods similar to query methods in ``Registry``.
  This new API should be considered experimental and potentially unstable, its use should be limited to downstream middleware code for now. (`DM-41761 <https://rubinobs.atlassian.net/browse/DM-41761>`_)
- * Added ``dry_run`` parameter to ``Butler.transfer_from`` to allow the transfer to run without doing the transfer. (`DM-42306 <https://rubinobs.atlassian.net/browse/DM-42306>`_)
- The ``Datastore`` base class was changed so that subclasses are no longer
  required to have the same constructor parameters as the base class.
  Subclasses are now required to implement ``_create_from_config`` for creating an instance
  from the ``Datastore.fromConfig`` static method, and ``clone`` for creating a
  copy of an existing instance. (`DM-42317 <https://rubinobs.atlassian.net/browse/DM-42317>`_)
- Added ``Timespan.from_day_obs()`` to construct a 24-hour time span from an observing day specified as a YYYYMMDD integer. (`DM-42636 <https://rubinobs.atlassian.net/browse/DM-42636>`_)


Bug Fixes
---------

- Fixed QuantumGraph-load breakage introduced on `DM-41043 <https://rubinobs.atlassian.net/browse/DM-41043>`_. (`DM-41164 <https://rubinobs.atlassian.net/browse/DM-41164>`_)
- ``DirectButler.transfer_from`` no longer requires expanded dataset refs under certain circumstances.
  However, providing expanded refs in advance is still recommended for efficiency. (`DM-41165 <https://rubinobs.atlassian.net/browse/DM-41165>`_)
- Fixed caching in ``DatasetRef`` deserialization that caused the serialized storage class to be ignored.

  This caused intermittent failures when running pipelines that use multiple storage classes for the same dataset type. (`DM-41562 <https://rubinobs.atlassian.net/browse/DM-41562>`_)
- Stopped accepting and ignoring unrecognized keyword arguments in ``DimensionRecord`` constructors.

  Passing an invalid field to a ``DimensionRecord`` now raises `TypeError`.

  This also prevents ``DimensionRecord`` construction from reinterpreting ``timespan=None`` as ``timespan=Timespan(None, None)``. (`DM-41724 <https://rubinobs.atlassian.net/browse/DM-41724>`_)
- Enabled collection-information caching in several contexts, especially during dataset query result iteration.

  This fixed a performance- and database-load regression introduced on `DM-41117 <https://rubinobs.atlassian.net/browse/DM-41117>`_, in which we emitted many redundant queries for collection information. (`DM-42216 <https://rubinobs.atlassian.net/browse/DM-42216>`_)
- Fixed miscellaneous thread-safety issues in ``DimensionUniverse``, ``DimensionGroup``, and ``StorageClassFactory``. (`DM-42317 <https://rubinobs.atlassian.net/browse/DM-42317>`_)
- ``butler query-collections --chains=TABLE`` now lists children in search order, not alphabetical order. (`DM-42605 <https://rubinobs.atlassian.net/browse/DM-42605>`_)
- Fixed problem with serialization of ``exposure`` dimension records with Pydantic v2. (`DM-42812 <https://rubinobs.atlassian.net/browse/DM-42812>`_)
- ``Butler.exists`` now throws a ``NoDefaultCollectionError`` when attempting to query for a ``DataId`` without specifying any collections to search.  Previously it would return `False`, hiding the user error. (`DM-42945 <https://rubinobs.atlassian.net/browse/DM-42945>`_)
- Reading masked parquet columns into astropy Tables now uses appropriate
  fill values.  In addition, floating point columns will be filled with ``NaN``
  instead of using a masked column.  This fixes discrepancies when accessing
  masked columns with ``.filled()`` or ``not``. (`DM-43187 <https://rubinobs.atlassian.net/browse/DM-43187>`_)
- Reverted/fixed part of `DM-43187 <https://rubinobs.atlassian.net/browse/DM-43187>`_.
  Now masked floating point columns will retain their masked status on read.
  The underlying array value and fill value are still ``NaN`` for consistency when using ``filled()`` or ``not`` for these masked
  columns. (`DM-43570 <https://rubinobs.atlassian.net/browse/DM-43570>`_)
- The ``flatten`` flag for the ``butler collection-chain`` CLI command now works as documented: it only flattens the specified children instead of flattening the entire collection chain.

  ``registry.setCollectionChain`` will no longer throw unique constraint violation exceptions when there are concurrent calls to this function. Instead, all calls will succeed and the last write will win. As a side-effect of this change, if calls to ``setCollectionChain`` occur within an explicit call to ``Butler.transaction``, other processes attempting to modify the same chain will block until the transaction completes. (`DM-43671 <https://rubinobs.atlassian.net/browse/DM-43671>`_)
- Fixed an issue where ``registry.setCollectionChain`` would raise a `KeyError` when assigning to a collection that was present in the collection cache. (`DM-43750 <https://rubinobs.atlassian.net/browse/DM-43750>`_)


Performance Enhancement
-----------------------

- ``FileDatastore.knows()`` no longer requires database I/O if its input ``DatasetRef`` has datastore records attached. (`DM-41880 <https://rubinobs.atlassian.net/browse/DM-41880>`_)
- Made significant performance enhancements when transferring hundreds of thousands of datasets.

  * Datastore now declares to ``ResourcePath`` when a resource is known to be a file.
  * Sped up file template validation.
  * Only request dimension metadata for template formatting if that metadata is needed.
  * Sped up cloning of ``Location`` instances.
  * No longer merge formatter ``kwargs`` unless there is something to merge.
  * Declared when a file location is trusted to be within the datastore. (`DM-42306 <https://rubinobs.atlassian.net/browse/DM-42306>`_)


Other Changes and Additions
---------------------------

- Reorganized internal subpackages, renamed modules, and adjusted symbol lifting.

  This included moving some symbols that we had always intended to be private
  (or public only to other middleware packages) that were not clearly marked as such
  (e.g., with leading underscores) before. (`DM-41043 <https://rubinobs.atlassian.net/browse/DM-41043>`_)
- Dropped support for Pydantic 1.x. (`DM-42302 <https://rubinobs.atlassian.net/browse/DM-42302>`_)
- Created Dimension Universe 5 which increases the size of the instrument name field in the ``instrument`` dimension from 16 to 32 characters. (`DM-42896 <https://rubinobs.atlassian.net/browse/DM-42896>`_)


An API Removal or Deprecation
-----------------------------

- * Removed dataset type component query support from all Registry methods.
    The main ``Registry.query*`` methods now warn if a ``components`` parameter is given and raise if it has a value other than `False`.
    The components parameters will be removed completely after v27.
  * Removed ``CollectionSearch`` class.
    A simple `tuple` is now used for this. (`DM-36303 <https://rubinobs.atlassian.net/browse/DM-36303>`_)
- Removed various already-deprecated factory methods for ``DimensionPacker`` objects and their support code, as well as the concrete ``ObservationDimensionPacker``.

  While ``daf_butler`` still defines the ``DimensionPacker`` abstract interface, all construction logic has moved to downstream packages. (`DM-38687 <https://rubinobs.atlassian.net/browse/DM-38687>`_)
- * Removed ``Butler.datastore`` property. The datastore can no longer be accessed directly.
  * Removed ``Butler.datasetExists`` (and the "direct" variant). Please use ``Butler.exists()`` and ``Butler.stored()`` instead.
  * Removed ``Butler.getDirect`` and related APIs. ``Butler.get()`` et al now use the ``DatasetRef`` directly if one is given.
  * Removed the ``run`` and ``ideGenerationMode`` parameters from ``Butler.ingest()``. They were no longer being used.
  * Removed the ``--reuse-ids`` option for the ``butler import`` command-line. This option was no longer used now that UUIDs are used throughout.
  * Removed the ``reconsitutedDimension`` parameter from ``Quantum.from_simple``. (`DM-40150 <https://rubinobs.atlassian.net/browse/DM-40150>`_)


Butler v26.0.0 2023-09-22
=========================

Now supports Python 3.11.

New Features
------------

- Added the ability to remove multiple dataset types at once, including expansion of wildcards, with ``Registry.removeDatasetType`` and ``butler remove-dataset-type``. (`DM-34568 <https://rubinobs.atlassian.net/browse/DM-34568>`_)
- Added the ``ArrowNumpyDict`` storage class to Parquet formatter. (`DM-37279 <https://rubinobs.atlassian.net/browse/DM-37279>`_)
- Added support for columns with array values (1D and multi-dimensional) in Parquet tables accessed via arrow/astropy/numpy.
  Pandas does not support array-valued columns. (`DM-37425 <https://rubinobs.atlassian.net/browse/DM-37425>`_)
- Integrated an experimental Butler server into distribution.
  ``lsst.daf.butler.server`` will likely not be in this location permanently.
  The interface is also evolving and should be considered extremely unstable.
  Some testing of the remote registry code has been included. (`DM-37609 <https://rubinobs.atlassian.net/browse/DM-37609>`_)
- Added support for writing/reading masked columns in astropy tables.
  This also adds support for masked columns in pandas dataframes, with limited support for conversion between the two. (`DM-37757 <https://rubinobs.atlassian.net/browse/DM-37757>`_)
- Dimension records are now available via attribute access on ``DataCoordinate`` instances, allowing syntax like ``data_id.exposure.day_obs``. (`DM-38054 <https://rubinobs.atlassian.net/browse/DM-38054>`_)
- Added default row groups (targeting a size of <~ 1GB) for Parquet files. (`DM-38063 <https://rubinobs.atlassian.net/browse/DM-38063>`_)
- ``Butler.get()`` and ``Butler.put()`` can now be used with resolved ``DatasetRef``. (`DM-38210 <https://rubinobs.atlassian.net/browse/DM-38210>`_)
- ``Butler.transfer_from()`` can now be used in conjunction with a ``ChainedDatastore``.
  Additionally, datastore constraints are now respected. (`DM-38240 <https://rubinobs.atlassian.net/browse/DM-38240>`_)
- * Modified ``Butler.import_()`` (and by extension the ``butler import`` command-line) to accept URIs for the directory and export file.
  * Modified ``butler ingest-files`` to accept a remote URI for the table file. (`DM-38492 <https://rubinobs.atlassian.net/browse/DM-38492>`_)
- Added support for multi-index dataframes with ``DataFrameDelegate`` and ``InMemoryDatastore``. (`DM-38642 <https://rubinobs.atlassian.net/browse/DM-38642>`_)
- Added new APIs to support the deprecation of ``LimitedButler.datastore``:

  * ``LimitedButler.get_datastore_roots`` can be used to retrieve any root URIs associated with attached datastores.
    If a datastore does not support the concept it will return `None` for its root URI.
  * ``LimitedButler.get_datastore_names`` can be used to retrieve the names of the internal datastores.
  * ``LimitedButler.get_many_uris`` allows for the bulk retrieval of URIs from a list of refs.
  * Also made ``getURI`` and ``getURIs`` available for ``LimitedButler``. (`DM-39915 <https://rubinobs.atlassian.net/browse/DM-39915>`_)
- Modified to fully support Pydantic version 2.x and version 1.x. (`DM-40002 <https://rubinobs.atlassian.net/browse/DM-40002>`_; `DM-40303 <https://rubinobs.atlassian.net/browse/DM-40303>`_)


API Changes
-----------

- Added new APIs for checking dataset existence.

  * `~lsst.daf.butler.LimitedButler.stored` checks whether the datastore artifact(s) exists for a single `~lsst.daf.butler.DatasetRef`.
  * `~lsst.daf.butler.LimitedButler.stored_many` is a bulk version of `~lsst.daf.butler.LimitedButler.stored` that can be used for many `~lsst.daf.butler.DatasetRef`.
  * `~lsst.daf.butler.Butler.exists` checks whether registry and datastore know about a single `~lsst.daf.butler.DatasetRef` and can optionally check for artifact existence.
    The results are returned in an `~enum.Flag` object (specifically `~lsst.daf.butler.DatasetExistence`) that evaluates to `True` if the dataset is available for retrieval.

  Additionally `~lsst.daf.butler.DatasetRef` now has a new method for checking whether two `~lsst.daf.butler.DatasetRef` only differ by compatible storage classes. (`DM-32940 <https://rubinobs.atlassian.net/browse/DM-32940>`_)
- `lsst.daf.Butler.transfer_from` method now accepts ``LimitedButler`` as a source Butler.
  In cases when a full butler is needed as a source it will try to cast it to a ``Butler``. (`DM-33497 <https://rubinobs.atlassian.net/browse/DM-33497>`_)
- * Creating an unresolved dataset reference now issues an ``UnresolvedRefWarning`` and is deprecated (and subsequently removed).
  * A resolved `~lsst.daf.butler.DatasetRef` can now be created by specifying the run without the ID -- the constructor will now automatically issue an ID.
    Previously this was an error.
    To support ID generation a new optional parameter ``id_generation_mode`` can now be given to the constructor to allow the ID to be constructed in different ways. (`DM-37703 <https://rubinobs.atlassian.net/browse/DM-37703>`_)
- - `~lsst.daf.butler.DatasetRef` constructor now requires ``run`` argument in all cases and always constructs a resolved reference.
  - Methods ``DatasetRef.resolved()``, ``DatasetRef.unresolved()``, and ``DatasetRef.getCheckedId()`` were removed. (`DM-37704 <https://rubinobs.atlassian.net/browse/DM-37704>`_)
- Added ``StorageClassDelegate.copy()`` method.
  By default this method calls `copy.deepcopy()` but subclasses can override as needed. (`DM-38694 <https://rubinobs.atlassian.net/browse/DM-38694>`_)
- ``Database.fromUri`` and ``Database.makeEngine`` methods now accept `sqlalchemy.engine.URL` instances in addition to strings. (`DM-39484 <https://rubinobs.atlassian.net/browse/DM-39484>`_)
- Added new parameter ``without_datastore`` to the ``Butler`` and ``ButlerConfig`` constructors to allow a butler to be created that can not access a datastore.
  This can be helpful if you want to query registry without requiring the overhead of the datastore. (`DM-40120 <https://rubinobs.atlassian.net/browse/DM-40120>`_)


Bug Fixes
---------

- Fixed race condition in datastore cache involving the possibility of multiple processes trying to retrieve the same file simultaneously and one of those processes deleting the file on exit of the context manager. (`DM-37092 <https://rubinobs.atlassian.net/browse/DM-37092>`_)
- Made ``Registry.findDataset`` respect the storage class of a `~lsst.daf.butler.DatasetType` that is passed to it.
  This also makes direct ``PipelineTask`` execution respect storage class conversions in the same way that execution butler already did. (`DM-37450 <https://rubinobs.atlassian.net/browse/DM-37450>`_)
- Can now properly retrieve astropy full table metadata with ``butler.get``. (`DM-37530 <https://rubinobs.atlassian.net/browse/DM-37530>`_)
- Fixed an order-of-operations bug in the query system (and as a result, ``QuantumGraph`` generation) that manifested as a "Custom operation find_first not supported by engine iteration" message. (`DM-37625 <https://rubinobs.atlassian.net/browse/DM-37625>`_)
- ``Butler.put`` is fixed to raise a correct exception for duplicate put attempts for ``DatasetRef`` with the same dataset ID. (`DM-37704 <https://rubinobs.atlassian.net/browse/DM-37704>`_)
- Fixed parsing of order by terms to treat direct references to dimension primary key columns as references to the dimensions. (`DM-37855 <https://rubinobs.atlassian.net/browse/DM-37855>`_)
- Fixed bugs involving CALIBRATION-collection skipping and long dataset type names that were introduced on `DM-31725 <https://rubinobs.atlassian.net/browse/DM-31725>`_. (`DM-37868 <https://rubinobs.atlassian.net/browse/DM-37868>`_)
- Now check for big-endian arrays when serializing to Parquet.
  This allows astropy FITS tables to be easily serialized. (`DM-37913 <https://rubinobs.atlassian.net/browse/DM-37913>`_)
- Fixed bugs in spatial query constraints introduced in `DM-31725 <https://rubinobs.atlassian.net/browse/DM-31725>`_. (`DM-37930 <https://rubinobs.atlassian.net/browse/DM-37930>`_)
- Fixed additional bugs in spatial query constraints introduced in `DM-31725 <https://rubinobs.atlassian.net/browse/DM-31725>`_. (`DM-37938 <https://rubinobs.atlassian.net/browse/DM-37938>`_)
- Fixed occasional crashes in ``Butler`` ``refresh()`` method due to a race condition in dataset types refresh. (`DM-38305 <https://rubinobs.atlassian.net/browse/DM-38305>`_)
- Fixed query manipulation logic to more aggressively move operations from Python postprocessing to SQL.

  This fixes a bug in ``QuantumGraph`` generation that occurs when a dataset type that is actually present in an input collection has exactly the same dimensions as the graph as a whole, manifesting as a mismatch between ``daf_relation`` engines. (`DM-38402 <https://rubinobs.atlassian.net/browse/DM-38402>`_)
- Add check for ``ListType`` when pandas converts a list object into Parquet. (`DM-38845 <https://rubinobs.atlassian.net/browse/DM-38845>`_)
- Few registry methods treated empty collection list in the same way as `None`, meaning that Registry-default run collection was used.
  This has been fixed now to mean that queries always return empty result set, with explicit "doomed by" messages. (`DM-38915 <https://rubinobs.atlassian.net/browse/DM-38915>`_)
- Fixed a bug in ``butler query-data-ids`` that caused a cryptic "the query has deferred operations..." error message when a spatial join is involved. (`DM-38943 <https://rubinobs.atlassian.net/browse/DM-38943>`_)
- Fixed more issues with storage class conversion. (`DM-38952 <https://rubinobs.atlassian.net/browse/DM-38952>`_)
- Fixed a SQL generation bug for queries that involve the common ``skypix`` dimension and at least two other spatial dimensions. (`DM-38954 <https://rubinobs.atlassian.net/browse/DM-38954>`_)
- Fixed bugs in storage class conversion in ``FileDatastore``, as used by ``QuantumBackedButler``. (`DM-39198 <https://rubinobs.atlassian.net/browse/DM-39198>`_)
- Fixed the bug in initializing PostgreSQL registry which resulted in "password authentication failed" error.
  The bug appeared during the SQLAlchemy 2.0 transition which changed default rendering of URL to string. (`DM-39484 <https://rubinobs.atlassian.net/browse/DM-39484>`_)
- Fixed a rare bug in follow-up dataset queries involving relation commutators.

  This occurred when building QuantumGraphs where a "warp" dataset type was an overall input to the pipeline and present in more than one input RUN collection. (`DM-40184 <https://rubinobs.atlassian.net/browse/DM-40184>`_)
- Ensureed ``Datastore`` record exports (as used in quantum-backed butler) are deduplicated when necessary. (`DM-40381 <https://rubinobs.atlassian.net/browse/DM-40381>`_)


Performance Enhancement
-----------------------

- When passing lazy query-results objects directly to various registry methods (``associate``, ``disassociate``, ``removeDatasets``, and ``certify``), query and process one dataset type at a time instead of querying for all of them and grouping by type in Python. (`DM-39939 <https://rubinobs.atlassian.net/browse/DM-39939>`_)


Other Changes and Additions
---------------------------

- Rewrote the registry query system, using the new ``daf_relation`` package.

  This change should be mostly invisible to users, but there are some subtle behavior changes:

  - ``Registry.findDatasets`` now respects the given storage class when passed a full `~lsst.daf.butler.DatasetType` instance, instead of replacing it with storage class registered with that dataset type.  This causes storage class overrides in ``PipelineTask`` input connections to be respected in more contexts as well; in at least some cases these were previously being incorrectly ignored.
  - ``Registry.findDatasets`` now utilizes cached summaries of which dataset types and governor dimension values are present in each collection.  This should result in fewer and simpler database calls, but it does make the result vulnerable to stale caches (which, like `~lsst.daf.butler.Registry` methods more generally, must be addressed manually via calls to ``Registry.refresh``.
  - The diagnostics provided by the ``explain_no_results`` methods on query result object (used prominently in the reporting on empty quantum graph builds) have been significantly improved, though they now use ``daf_relation`` terminology that may be unfamiliar to users.
  - `~lsst.daf.butler.Registry` is now more consistent about raising ``DataIdValueError`` when given invalid governor dimension values, while not raising (but providing ``explain_no_results`` diagnostics) for all other invalid dimension values, as per `RFC-878 <https://rubinobs.atlassian.net/browse/RFC-878>`_.
  - `~lsst.daf.butler.Registry` methods that take a ``where`` argument are now typed to expect a `str` that is not `None`, with the default no-op value now an empty string (before either an empty `str` or `None` could be passed, and meant the same thing).  This should only affect downstream type checking, as the runtime code still just checks for whether the argument evaluates as `False` in a boolean context. (`DM-31725 <https://rubinobs.atlassian.net/browse/DM-31725>`_)
- Added dimensions config entries that declare that the ``visit`` dimension "populates" various dimension elements that define many-to-many relationships.

  In the future, this will be used to ensure the correct records are included in exports of dimension records. (`DM-34589 <https://rubinobs.atlassian.net/browse/DM-34589>`_)
- Added converter config to allow ``lsst.ip.isr.IntermediateTransmissionCurve`` and subclasses to be used for ``lsst.afw.image.TransmissionCurve``. (`DM-36597 <https://rubinobs.atlassian.net/browse/DM-36597>`_)
- ``Butler.getURIs`` no longer checks the file system to see if the file exists before returning a URI if the datastore thinks it knows about the file.
  This does mean that if someone has removed the file from the file system without deleting it from datastore that a URI could be retrieved for something that does not exist. (`DM-37173 <https://rubinobs.atlassian.net/browse/DM-37173>`_)
- * Enhanced the JSON and YAML formatters so that they can both handle dataclasses and Pydantic models (previously JSON supported Pydantic and YAML supported dataclasses).
  * Rationalized the storage class conversion handling to always convert from a `dict` to the original type even if the caller is requesting a `dict`.
    Without this change it was possible to have some confusion where a Pydantic model's serialization did not match the `dict`-like view it was emulating. (`DM-37214 <https://rubinobs.atlassian.net/browse/DM-37214>`_)
- Added an `obsCoreTableManager` property to `~lsst.daf.butler.Registry` for access to the ObsCore table manager.
  This will be set to `None` when repository lacks an ObsCore table.
  It should only be used by a limited number of clients, e.g. ``lsst.obs.base.DefineVisitsTask``, which need to update the table. (`DM-38205 <https://rubinobs.atlassian.net/browse/DM-38205>`_)
- * Modified ``Butler.ingest()`` such that it can now ingest resolved ``DatasetRef``.
    If unresolved refs are given (which was the previous requirement for ingest and is no longer possible) they are resolved internally but a warning is issued.
  * Added ``repr()`` support for ``RegistryDefaults`` class. (`DM-38779 <https://rubinobs.atlassian.net/browse/DM-38779>`_)
- The behavior of ``FileDatastore.transfer_from()`` has been clarified regarding what to do when an absolute URI (from a direct ingest) is found in the source butler.
  If ``transfer="auto"`` (the default) the absolute URI will be stored in the target butler.
  If any other transfer mode is used the absolute URI will be copied/linked into the target butler. (`DM-38870 <https://rubinobs.atlassian.net/browse/DM-38870>`_)
- Made minor modifications to the StorageClass system to support mock storage classes (in ``pipe_base``) for testing. (`DM-38952 <https://rubinobs.atlassian.net/browse/DM-38952>`_)
- Replaced the use of ``lsst.utils.ellipsis`` mypy workaround with the native type `type.EllipsisType` available since Python 3.10. (`DM-39410 <https://rubinobs.atlassian.net/browse/DM-39410>`_)
- Moved Butler repository aliasing resolution into `~lsst.daf.butler.ButlerConfig` so that it is available everywhere without having to do the resolving each time. (`DM-39563 <https://rubinobs.atlassian.net/browse/DM-39563>`_)
- Added ability for some butler primitives to be cached and re-used on deserialization through a special interface. (`DM-39582 <https://rubinobs.atlassian.net/browse/DM-39582>`_)
- * Replaced usage of ``Butler.registry.dimensions`` with ``Butler.dimensions``.
  * Modernized type annotations.
  * Fixed some documentation problems.
  * Made some Minor modernizations to use set notation and f-strings. (`DM-39605 <https://rubinobs.atlassian.net/browse/DM-39605>`_)
- Changed all Butler code and tests to use conforming DataIDs.
  Removed the fake ``DataCoordinate`` classes from the datastore tests.
  Improved type annotations in some test files. (`DM-39665 <https://rubinobs.atlassian.net/browse/DM-39665>`_)
- Added various optimizations to ``QuantumGraph`` loading. (`DM-40121 <https://rubinobs.atlassian.net/browse/DM-40121>`_)
- Fixed docs on referring to timespans in queries, and made related error messages more helpful. (`DM-38084 <https://rubinobs.atlassian.net/browse/DM-38084>`_)
- Clarified that ``butler prune-datasets --purge`` always removes dataset entries and clarified when the run argument is used. (`DM-39086 <https://rubinobs.atlassian.net/browse/DM-39086>`_)

An API Removal or Deprecation
-----------------------------

- Deprecated methods for constructing or using ``DimensionPacker`` instances.

  The ``DimensionPacker`` interface is not being removed, but all concrete implementations will now be downstream of ``daf_butler`` and will not satisfy the assumptions of the current interfaces for constructing them. (`DM-31924 <https://rubinobs.atlassian.net/browse/DM-31924>`_)
- ``Butler.datasetExists`` has been deprecated and will be removed in a future release.
  It has been replaced by ``Butler.stored()`` (specifically to check if the datastore has the artifact) and ``Butler.exists()`` which will check registry and datastore and optionally check whether the artifact exists. (`DM-32940 <https://rubinobs.atlassian.net/browse/DM-32940>`_)
- Removed the ``Spectraction`` storage class.
  This was a temporary storage class added for convenience during development, which was a roll-up-and-pickle of all the potentially relevant parts of the extraction.
  All the necessary information is now stored inside the ``SpectractorSpectrum`` storage class. (`DM-33932 <https://rubinobs.atlassian.net/browse/DM-33932>`_)
- * Removed deprecated ``ButlerURI`` (use ``lsst.resources.ResourcePath`` instead).
  * Removed deprecated ``kwargs`` parameter from ``DeferredDatasetHandle``.
  * Removed the deprecated ``butler prune-collection`` command.
  * Removed the deprecated ``checkManagerDigests`` from butler registry. (`DM-37534 <https://rubinobs.atlassian.net/browse/DM-37534>`_)
- * Deprecated ``Butler.getDirect()`` and ``Butler.putDirect()``.
    We have modified the ``get()`` and ``put()`` variants to recognize the presence of a resolved ``DatasetRef`` and use it directly.
    For ``get()`` we no longer unpack the ``DatasetRef`` and re-run the query, but return exactly the dataset being requested.
  * Removed ``Butler.pruneCollections``.
    This method was replaced by ``Butler.removeRuns`` and ``Registry.removeCollections`` a long time ago and the command-line interface was removed previously. (`DM-38210 <https://rubinobs.atlassian.net/browse/DM-38210>`_)
- Code that calculates schema digests was removed, registry will no longer store digests in the database.
  Previously we saved schema digests, but we did not verify them since w_2022_22 in v24.0. (`DM-38235 <https://rubinobs.atlassian.net/browse/DM-38235>`_)
- Support for integer dataset IDs in registry has now been removed.
  All dataset IDs must now be `uuid.UUID`. (`DM-38280 <https://rubinobs.atlassian.net/browse/DM-38280>`_)
- Removed support for non-UUID dataset IDs in ``Butler.transfer_from()``.
  The ``id_gen_map`` parameter has been removed and the ``local_refs`` parameter has been removed from ``Datastore.transfer_from()``. (`DM-38409 <https://rubinobs.atlassian.net/browse/DM-38409>`_)
- Deprecated ``reconstituteDimensions`` argument from ``Quantum.from_simple``. (`DM-39582 <https://rubinobs.atlassian.net/browse/DM-39582>`_)
- The semi-public ``Butler.datastore`` property has now been deprecated.
  The ``LimitedButler`` API has been expanded such that there is no longer any need for anyone to access the datastore class directly. (`DM-39915 <https://rubinobs.atlassian.net/browse/DM-39915>`_)
- ``lsst.daf.butler.registry.DbAuth`` class has been moved to the ``lsst-utils`` package and can be imported from the ``lsst.utils.db_auth`` module. (`DM-40462 <https://rubinobs.atlassian.net/browse/DM-40462>`_)


Butler v25.0.0 2023-02-27
=========================

This is the last release that can access data repositories using integer dataset IDs.
Please either recreate these repositories or convert them to use UUIDs using `the butler migrate tooling <https://github.com/lsst-dm/daf_butler_migrate>`_.

New Features
------------

- * Added ``StorageClass.is_type`` method to compare a type with that of the storage class itelf.
  * Added keys, values, items, and iterator for ``StorageClassFactory``. (`DM-29835 <https://rubinobs.atlassian.net/browse/DM-29835>`_)
- Updated parquet backend to use Arrow Tables natively, and add converters to and from pandas DataFrames, Astropy Tables, and Numpy structured arrays. (`DM-34874 <https://rubinobs.atlassian.net/browse/DM-34874>`_)
- ``Butler.transfer_from()`` can now copy dimension records as well as datasets.
  This significantly enhances the usability of this method when transferring between disconnected Butlers.
  The ``butler transfer-datasets`` command will transfer dimension records by default but this can be disabled with the ``--no-transfer-dimensions`` option (which can be more efficient if you know that the destination Butler contains all the records). (`DM-34887 <https://rubinobs.atlassian.net/browse/DM-34887>`_)
- ``butler query-data-ids`` will now determine default dimensions to use if a dataset type and collection is specified.
  The logical AND of all supplied dataset types will be used.
  Additionally, if no results are returned a reason will now be given in many cases. (`DM-35391 <https://rubinobs.atlassian.net/browse/DM-35391>`_)
- Added ``DataFrameDelegate`` to allow DataFrames to be used with ``lsst.pipe.base.InMemoryDatasetHandle``. (`DM-35803 <https://rubinobs.atlassian.net/browse/DM-35803>`_)
- Add ``StorageClass.findStorageClass`` method to find a storage class from a python type. (`DM-35815 <https://rubinobs.atlassian.net/browse/DM-35815>`_)
- The optional dependencies of ``lsst-resources`` can be requested as optional dependencies of ``lsst-daf-butler`` and will be passed down to the underlying package.
  This allows callers of ``lsst.daf.butler`` to specify the type of resources they want to be able to access without being aware of the role of ``lsst.resources`` as an implementation detail. (`DM-35886 <https://rubinobs.atlassian.net/browse/DM-35886>`_)
- Requires Python 3.10 or greater for better type annotation support. (`DM-36174 <https://rubinobs.atlassian.net/browse/DM-36174>`_)
- Bind values in Registry queries can now specify list/tuple of numbers for identifiers appearing on the right-hand side of ``IN`` expression. (`DM-36325 <https://rubinobs.atlassian.net/browse/DM-36325>`_)
- It is now possible to override the python type returned by ``butler.get()`` (if the types are compatible with each other) by using the new ``readStorageClass`` parameter.
  Deferred dataset handles can also be overridden.

  For example, to return an `astropy.table.Table` from something that usually returns an ``lsst.afw.table.Catalog`` you would do:

  .. code-block:: python

      table = butler.getDirect(ref, readStorageClass="AstropyTable")

  Any parameters given to the ``get()`` must still refer to the native storage class. (`DM-4551 <https://rubinobs.atlassian.net/browse/DM-4551>`_)


API Changes
-----------

- Deprecate support for accessing data repositories with integer dataset IDs, and disable creation of new data repositories with integer dataset IDs, as per `RFC-854 <https://rubinobs.atlassian.net/browse/RFC-854>`_. (`DM-35063 <https://rubinobs.atlassian.net/browse/DM-35063>`_)
- ``DimensionUniverse`` now has a ``isCompatibleWith()`` method to check if two universes are compatible with each other.
  The initial test is very basic but can be improved later. (`DM-35082 <https://rubinobs.atlassian.net/browse/DM-35082>`_)
- Deprecated support for components in `Registry.query*` methods, per `RFC-879 <https://rubinobs.atlassian.net/browse/RFC-879>`_. (`DM-36312 <https://rubinobs.atlassian.net/browse/DM-36312>`_)
- Multiple minor API changes to query methods from `RFC-878 <https://rubinobs.atlassian.net/browse/RFC-878>`_ and `RFC-879 <https://rubinobs.atlassian.net/browse/RFC-879>_`.

  This includes:

  - ``CollectionSearch`` is deprecated in favor of ``Sequence[str]`` and the new ``CollectionWildcard`` class.
  - ``queryDatasetTypes`` and ``queryCollections`` now return `~collections.abc.Iterable` (representing an unspecified in-memory collection) and `~collections.abc.Sequence`, respectively, rather than iterators.
  - ``DataCoordinateQueryResults.findDatasets`` now raises ``MissingDatasetTypeError`` when the given dataset type is not registered.
  - Passing regular expressions and other patterns as dataset types to ``queryDataIds`` and ``queryDimensionRecords`` is deprecated.
  - Passing unregistered dataset types ``queryDataIds`` and ``queryDimensionRecords`` is deprecated; in the future this will raise ``MissingDatasetTypeError`` instead of returning no query results.
  - Query result class ``explain_no_results`` now returns `~collections.abc.Iterable` instead of `~collections.abc.Iterator`. (`DM-36313 <https://rubinobs.atlassian.net/browse/DM-36313>`_)
- A method has been added to ``DatasetRef`` and ``DatasetType``, named ``overrideStorageClass``, to allow a new object to be created that has a different storage class associated with it. (`DM-4551 <https://rubinobs.atlassian.net/browse/DM-4551>`_)


Bug Fixes
---------

- Fixed a bug in the parquet reader where a single string column name would be interpreted as an iterable. (`DM-35803 <https://rubinobs.atlassian.net/browse/DM-35803>`_)
- Fixed bug in ``elements`` argument to various export methods that prevented it from doing anything. (`DM-36111 <https://rubinobs.atlassian.net/browse/DM-36111>`_)
- A bug has been fixed in ``DatastoreCacheManager`` that triggered if two processes try to cache the same dataset simultaneously. (`DM-36412 <https://rubinobs.atlassian.net/browse/DM-36412>`_)
- Fixed bug in pandas ``dataframe`` to arrow conversion that would crash with some pandas object data types. (`DM-36775 <https://rubinobs.atlassian.net/browse/DM-36775>`_)
- Fixed bug in pandas ``dataframe`` to arrow conversion that would crash with partially nulled string columns. (`DM-36795 <https://rubinobs.atlassian.net/browse/DM-36795>`_)


Other Changes and Additions
---------------------------

- For command-line options that split on commas, it is now possible to specify parts of the string not to split by using ``[]`` to indicate comma-separated list content. (`DM-35917 <https://rubinobs.atlassian.net/browse/DM-35917>`_)
- Moved the typing workaround for the built-in `Ellipsis` (`...`) singleton to ``lsst.utils``. (`DM-36108 <https://rubinobs.atlassian.net/browse/DM-36108>`_)
- Now define regions for data IDs with multiple spatial dimensions to the intersection of those dimensions' regions. (`DM-36111 <https://rubinobs.atlassian.net/browse/DM-36111>`_)
- Added support for in-memory datastore to roll back a call to ``datastore.trash()``.
  This required that the ``bridge.moveToTrash()`` method now takes an additional ``transaction`` parameter (that can be `None`). (`DM-36172 <https://rubinobs.atlassian.net/browse/DM-36172>`_)
- Restructured internal Registry query system methods to share code better and prepare for more meaningful changes. (`DM-36174 <https://rubinobs.atlassian.net/browse/DM-36174>`_)
- Removed unnecessary table-locking in dimension record insertion.

  Prior to this change, we used explicit full-table locks to guard against a race condition that wasn't actually possible, which could lead to deadlocks in rare cases involving insertion of governor dimension records. (`DM-36326 <https://rubinobs.atlassian.net/browse/DM-36326>`_)
- Chained Datastore can now support "move" transfer mode for ingest.
  Files are copied to each child datastore unless only one child datastore is accepting the incoming files, in which case "move" is used. (`DM-36410 <https://rubinobs.atlassian.net/browse/DM-36410>`_)
- ``DatastoreCacheManager`` can now use an environment variable, ``$DAF_BUTLER_CACHE_DIRECTORY_IF_UNSET``, to specify a cache directory to use if no explicit directory has been specified by configuration or by the ``$DAF_BUTLER_CACHE_DIRECTORY`` environment variable.
  Additionally, a ``DatastoreCacheManager.set_fallback_cache_directory_if_unset()`` class method has been added that will set this environment variable with a suitable value.
  This is useful for multiprocessing where each forked or spawned subprocess needs to share the same cache directory. (`DM-36412 <https://rubinobs.atlassian.net/browse/DM-36412>`_)
- Added support for ``ChainedDatastore.export()``. (`DM-36517 <https://rubinobs.atlassian.net/browse/DM-36517>`_)
- Reworked transaction and connection management for compatibility with transaction-level connection pooling on the server.

  Butler clients still hold long-lived connections, via delegation to SQLAlchemy's connection pooling, which can handle disconnections transparently most of the time.  But we now wrap all temporary table usage and cursor iteration in transactions. (`DM-37249 <https://rubinobs.atlassian.net/browse/DM-37249>`_)


An API Removal or Deprecation
-----------------------------

- Removed deprecated filterLabel exposure component access. (`DM-27811 <https://rubinobs.atlassian.net/browse/DM-27811>`_)


Butler v24.0.0 2022-08-26
=========================

New Features
------------

- Support LSST-style visit definitions where a single exposure is part of a set of related exposures all taken with the same acquisition command.
  Each exposure knows the "visit" it is part of.

  * Modify the ``exposure`` dimension record to include ``seq_start`` and ``seq_end`` metadata.
  * Modify ``visit`` record to include a ``seq_num`` field.
  * Remove ``visit_system`` dimension and add ``visit_system_membership`` record to allow a visit to be associated with multiple visit systems. (`DM-30948 <https://rubinobs.atlassian.net/browse/DM-30948>`_)
- ``butler export-calibs`` now takes a ``--transfer`` option to control how data are exported (use ``direct`` to do in-place export) and a ``--datasets`` option to limit the dataset types to be exported.
  It also now takes a default collections parameter (all calibration collections). (`DM-32061 <https://rubinobs.atlassian.net/browse/DM-32061>`_)
- Iterables returned from registry methods `queryDataIds` and `queryDimensionRecords` have two new methods - `order_by` and `limit`. (`DM-32403 <https://rubinobs.atlassian.net/browse/DM-32403>`_)
- Builds using ``setuptools`` now calculate versions from the Git repository, including the use of alpha releases for those associated with weekly tags. (`DM-32408 <https://rubinobs.atlassian.net/browse/DM-32408>`_)
- Butler can now support lookup of repositories by label if the user environment is correctly configured.
  This is done using the new `~lsst.daf.butler.Butler.get_repo_uri()` and `~lsst.daf.butler.Butler.get_known_repos()` APIs. (`DM-32491 <https://rubinobs.atlassian.net/browse/DM-32491>`_)
- Add a butler command line command called ``butler remove-collections`` that can remove non-RUN collections. (`DM-32687 <https://rubinobs.atlassian.net/browse/DM-32687>`_)
- Add a butler command line command called ``butler remove-runs`` that can remove RUN collections and contained datasets. (`DM-32831 <https://rubinobs.atlassian.net/browse/DM-32831>`_)
- It is now possible to register type conversion functions with storage classes.
  This can allow a dataset type definition to change storage class in the registry whilst allowing datasets that have already been serialized using one python type to be returned using the new python type.
  The ``storageClasses.yaml`` definitions can now look like:

  .. code-block:: yaml

     TaskMetadata:
       pytype: lsst.pipe.base.TaskMetadata
       converters:
         lsst.daf.base.PropertySet: lsst.pipe.base.TaskMetadata.from_metadata

  Declares that if a ``TaskMetadata`` is expected then a ``PropertySet`` can be converted to the correct python type. (`DM-32883 <https://rubinobs.atlassian.net/browse/DM-32883>`_)
- Dimension record imports now ignore conflicts (without checking for consistency) instead of failing. (`DM-33148 <https://rubinobs.atlassian.net/browse/DM-33148>`_)
- Storage class converters can now also be used on `~lsst.daf.butler.Butler.put`. (`DM-33155 <https://rubinobs.atlassian.net/browse/DM-33155>`_)
- If a `~lsst.daf.butler.DatasetType` has been constructed that differs from the registry definition, but in a way that is compatible through `~lsst.daf.butler.StorageClass` conversion, then using that in a `lsst.daf.butler.Butler.get()` call will return a python type that matches the user-specified `~lsst.daf.butler.StorageClass` instead of the internal python type. (`DM-33303 <https://rubinobs.atlassian.net/browse/DM-33303>`_)
- The dataset ID can now be used in a file template for datastore (using ``{id}``). (`DM-33414 <https://rubinobs.atlassian.net/browse/DM-33414>`_)
- Add `Registry.getCollectionParentChains` to find the `CHAINED` collections that another collection belongs to. (`DM-33643 <https://rubinobs.atlassian.net/browse/DM-33643>`_)
- Added ``has_simulated`` to the ``exposure`` record to indicate that some content of this exposure was simulated. (`DM-33728 <https://rubinobs.atlassian.net/browse/DM-33728>`_)
- The command-line tooling has changed how it sets the default logger when using ``--log-level``.
  Now only the default logger(s) (``lsst`` and the colon-separated values stored in the ``$DAF_BUTLER_ROOT_LOGGER``) will be affected by using ``--log-level`` without a specific logger name.
  By default only this default logger will be set to ``INFO`` log level and all other loggers will remain as ``WARNING``.
  Use ``--log-level '.=level'`` to change the root logger (this will not change the default logger level and so an additional call to ``--log-level DEBUG`` may be needed to turn on debugging for all loggers). (`DM-33809 <https://rubinobs.atlassian.net/browse/DM-33809>`_)
- Added ``azimuth`` to the ``exposure`` and ``visit`` records. (`DM-33859 <https://rubinobs.atlassian.net/browse/DM-33859>`_)
- If repository aliases have been defined for the site they can now be used in place of the Butler repository URI in both the `~lsst.daf.butler.Butler` constructor and command-line tools. (`DM-33870 <https://rubinobs.atlassian.net/browse/DM-33870>`_)
- * Added ``visit_system`` to ``instrument`` record and allowed it to be used as a tie breaker in dataset determination if a dataId is given using ``seq_num`` and ``day_obs`` and it matches multiple visits.
  * Modify export YAML format to include the dimension universe version and namespace.
  * Allow export files with older visit definitions to be read (this does not fill in the new metadata records).
  * `DimensionUniverse` now supports the ``in`` operator to check if a dimension is part of the universe. (`DM-33942 <https://rubinobs.atlassian.net/browse/DM-33942>`_)
- * Added a definition for using healpix in skypix definitions.
  * Change dimension universe caching to support a namespace in addition to a version number. (`DM-33946 <https://rubinobs.atlassian.net/browse/DM-33946>`_)
- Added a formatter for `lsst.utils.packages.Packages` Python types in `lsst.daf.butler.formatters.packages.PackagesFormatter`. (`DM-34105 <https://rubinobs.atlassian.net/browse/DM-34105>`_)
- Added an optimization that speeds up ``butler query-datasets`` when using ``--show-uri``. (`DM-35120 <https://rubinobs.atlassian.net/browse/DM-35120>`_)


API Changes
-----------

- Many internal utilities from ``lsst.daf.butler.core.utils`` have been relocated to the ``lsst.utils`` package. (`DM-31722 <https://rubinobs.atlassian.net/browse/DM-31722>`_)
- The ``ButlerURI`` class has now been removed from this package.
  It now exists as `lsst.resources.ResourcePath`.
  All code should be modified to use the new class name. (`DM-31723 <https://rubinobs.atlassian.net/browse/DM-31723>`_)
- `lsst.daf.butler.Registry.registerRun` and `lsst.daf.butler.Registry.registerCollection` now return a Booelan indicating whether the collection was created or already existed. (`DM-31976 <https://rubinobs.atlassian.net/browse/DM-31976>`_)
- A new optional parameter, ``record_validation_info`` has been added to `~lsst.daf.butler.Butler.ingest` (and related datastore APIs) to allow the caller to declare that file attributes such as the file size or checksum should not be recorded.
  This can be useful if the file is being monitored by an external system or it is known that the file might be compressed in-place after ingestion. (`DM-33086 <https://rubinobs.atlassian.net/browse/DM-33086>`_)
- Added a new `DatasetType.is_compatible_with` method.
  This method determines if two dataset types are compatible with each other, taking into account whether the storage classes allow type conversion. (`DM-33278 <https://rubinobs.atlassian.net/browse/DM-33278>`_)
- The `run` parameter has been removed from Butler method `lsst.daf.butler.Butler.pruneDatasets`.
  It was never used in Butler implementation, client code should simply remove it. (`DM-33488 <https://rubinobs.atlassian.net/browse/DM-33488>`_)
- Registry methods now raise exceptions belonging to a class hierarchy rooted at `lsst.daf.butler.registry.RegistryError`.
  See also :ref:`daf_butler_query_error_handling` for details. (`DM-33600 <https://rubinobs.atlassian.net/browse/DM-33600>`_)
- Added ``DatasetType.storageClass_name`` property to allow the name of the storage class to be retrieved without requiring that the storage class exists.
  This is possible if people have used local storage class definitions or a test ``DatasetType`` was created temporarily. (`DM-34460 <https://rubinobs.atlassian.net/browse/DM-34460>`_)


Bug Fixes
---------

- ``butler export-calibs`` can now copy files that require the use of a file template (for example if a direct URI was stored in datastore) with metadata records.
  File templates that use metadata records now complain if the record is not attached to the ``DatasetRef``. (`DM-32061 <https://rubinobs.atlassian.net/browse/DM-32061>`_)
- Make it possible to run `queryDimensionRecords` while constraining on the existence of a dataset whose dimensions are not a subset of the record element's dependencies (e.g. `raw` and `exposure`). (`DM-32454 <https://rubinobs.atlassian.net/browse/DM-32454>`_)
- Butler constructor can now take a `os.PathLike` object when the ``butler.yaml`` is not included in the path. (`DM-32467 <https://rubinobs.atlassian.net/browse/DM-32467>`_)
- In the butler presets file (used by the ``--@`` option), use option names that match the butler CLI command option names (without leading dashes).
  Fail if option names used in the presets file do not match options for the current butler command. (`DM-32986 <https://rubinobs.atlassian.net/browse/DM-32986>`_)
- The butler CLI command ``remove-runs`` can now unlink RUN collections from parent CHAINED collections. (`DM-33619 <https://rubinobs.atlassian.net/browse/DM-33619>`_)
- Improves ``butler query-collections``:

  * TABLE output formatting is easier to read.
  * Adds INVERSE modes for TABLE and TREE output, to view CHAINED parent(s) of collections (non-INVERSE lists children of CHAINED collections).
  * Sorts datasets before printing them. (`DM-33902 <https://rubinobs.atlassian.net/browse/DM-33902>`_)
- Fix garbled printing of raw-byte hashes in query-dimension-records. (`DM-34007 <https://rubinobs.atlassian.net/browse/DM-34007>`_)
- The automatic addition of ``butler.yaml`` to the Butler configuration URI now also happens when a ``ResourcePath`` instance is given. (`DM-34172 <https://rubinobs.atlassian.net/browse/DM-34172>`_)
- Fix handling of "doomed" (known to return no results even before execution) follow-up queries for datasets.
  This frequently manifested as a `KeyError` with a message about dataset type registration during `QuantumGraph` generation. (`DM-34202 <https://rubinobs.atlassian.net/browse/DM-34202>`_)
- Fix `~lsst.daf.butler.Registry.queryDataIds` bug involving dataset constraints with no dimensions. (`DM-34247 <https://rubinobs.atlassian.net/browse/DM-34247>`_)
- The `click.Path` API changed, change from ordered arguments to keyword arguments when calling it. (`DM-34261 <https://rubinobs.atlassian.net/browse/DM-34261>`_)
- Fix `~lsst.daf.butler.Registry.queryCollections` bug in which children of chained collections were being alphabetically sorted instead of ordered consistently with the order in which they would be searched. (`DM-34328 <https://rubinobs.atlassian.net/browse/DM-34328>`_)
- Fixes the bug introduced in `DM-33489 <https://rubinobs.atlassian.net/browse/DM-33489>`_ (appeared in w_2022_15) which causes not-NULL constraint violation for datastore component column. (`DM-34375 <https://rubinobs.atlassian.net/browse/DM-34375>`_)
- Fixes an issue where the command line tools were caching argument and option values but not separating option names from option values correctly in some cases. (`DM-34812 <https://rubinobs.atlassian.net/browse/DM-34812>`_)


Other Changes and Additions
---------------------------

- Add a `NOT NULL` constraint to dimension implied dependency columns.

  `NULL` values in these columns already cause the query system to misbehave. (`DM-21840 <https://rubinobs.atlassian.net/browse/DM-21840>`_)
- Update parquet writing to use default per-column compression. (`DM-31963 <https://rubinobs.atlassian.net/browse/DM-31963>`_)
- Tidy up ``remove-runs`` subcommand confirmation report by sorting dataset types and filtering out those with no datasets in the collections to be deleted. (`DM-33584 <https://rubinobs.atlassian.net/browse/DM-33584>`_)
- The constraints on collection names have been relaxed.
  Previously collection names were limited to ASCII alphanumeric characters plus a limited selection of symbols (directory separator, @-sign).
  Now all unicode alphanumerics can be used along with emoji. (`DM-33999 <https://rubinobs.atlassian.net/browse/DM-33999>`_)
- File datastore now always writes a temporary file and renames it even for local file system datastores.
  This minimizes the risk of a corrupt file being written if the process writing the file is killed at the wrong time. (`DM-35458 <https://rubinobs.atlassian.net/browse/DM-35458>`_)


An API Removal or Deprecation
-----------------------------

- The ``butler prune-collections`` command line command is now deprecated.
  Please consider using ``remove-collections`` or ``remove-runs`` instead. Will be removed after v24. (`DM-32499 <https://rubinobs.atlassian.net/browse/DM-32499>`_)
- All support for reading and writing `~lsst.afw.image.Filter` objects has been removed.
  The old ``filter`` component for exposures has been removed, and replaced with a new ``filter`` component backed by `~lsst.afw.image.FilterLabel`.
  It functions identically to the ``filterLabel`` component, which has been deprecated. (`DM-27177 <https://rubinobs.atlassian.net/browse/DM-27177>`_)


Butler v23.0.0 2021-12-10
=========================

New Features
------------

- Add ability to cache datasets locally when using a remote file store.
  This can significantly improve performance when retrieving components from a dataset. (`DM-13365 <https://rubinobs.atlassian.net/browse/DM-13365>`_)
- Add a new ``butler retrieve-artifacts`` command to copy file artifacts from a Butler datastore. (`DM-27241 <https://rubinobs.atlassian.net/browse/DM-27241>`_)
- Add ``butler transfer-datasets`` command-line tool and associated ``Butler.transfer_from()`` API.

  This can be used to transfer datasets between different butlers, with the caveat that dimensions and dataset types must be pre-defined in the receiving butler repository. (`DM-28650 <https://rubinobs.atlassian.net/browse/DM-28650>`_)
- Add ``amp`` parameter to the Exposure StorageClass, allowing single-amplifier subimage reads. (`DM-29370 <https://rubinobs.atlassian.net/browse/DM-29370>`_)
- Add new ``butler collection-chain`` subcommand for creating collection chains from the command line. (`DM-30373 <https://rubinobs.atlassian.net/browse/DM-30373>`_)
- Add ``butler ingest-files`` subcommand to simplify ingest of any external file. (`DM-30935 <https://rubinobs.atlassian.net/browse/DM-30935>`_)
- * Add class representing a collection of log records (``ButlerLogRecords``).
  * Allow this class to be stored and retrieved from a Butler datastore.
  * Add special log handler to allow JSON log records to be stored.
  * Add ``--log-file`` option to command lines to redirect log output to file.
  * Add ``--no-log-tty`` to disable log output to terminal. (`DM-30977 <https://rubinobs.atlassian.net/browse/DM-30977>`_)
- Registry methods that previously could raise an exception when searching in
  calibrations collections now have an improved logic that skip those
  collections if they were not given explicitly but only appeared in chained
  collections. (`DM-31337 <https://rubinobs.atlassian.net/browse/DM-31337>`_)
- Add a confirmation step to ``butler prune-collection`` to help prevent
  accidental removal of collections. (`DM-31366 <https://rubinobs.atlassian.net/browse/DM-31366>`_)
- Add ``butler register-dataset-type`` command to register a new dataset type. (`DM-31367 <https://rubinobs.atlassian.net/browse/DM-31367>`_)
- Use cached summary information to simplify queries involving datasets and provide better diagnostics when those queries yield no results. (`DM-31583 <https://rubinobs.atlassian.net/browse/DM-31583>`_)
- Add a new ``butler export-calibs`` command to copy calibrations and write an export.yaml document from a Butler datastore. (`DM-31596 <https://rubinobs.atlassian.net/browse/DM-31596>`_)
- Support rewriting of dataId containing dimension records such as ``day_obs`` and ``seq_num`` in ``butler.put()``.
  This matches the behavior of ``butler.get()``. (`DM-31623 <https://rubinobs.atlassian.net/browse/DM-31623>`_)
- Add ``--log-label`` option to ``butler`` command to allow extra information to be injected into the log record. (`DM-31884 <https://rubinobs.atlassian.net/browse/DM-31884>`_)
- * The ``Butler.transfer_from`` method no longer registers new dataset types by default.
  * Add the related option ``--register-dataset-types`` to the ``butler transfer-datasets`` subcommand. (`DM-31976 <https://rubinobs.atlassian.net/browse/DM-31976>`_)
- Support UUIDs as the primary keys in registry and allow for reproducible UUIDs.

  This change will significantly simplify transferring of data between butler repositories. (`DM-29196 <https://rubinobs.atlassian.net/browse/DM-29196>`_)
- Allow registry methods such as ``queryDatasets`` to use a glob-style string when specifying collection or dataset type names. (`DM-30200 <https://rubinobs.atlassian.net/browse/DM-30200>`_)
- Add support for updating and replacing dimension records. (`DM-30866 <https://rubinobs.atlassian.net/browse/DM-30866>`_)


API Changes
-----------

- A new method ``Datastore.knows()`` has been added to allow a user to ask the datastore whether it knows about a specific dataset but without requiring a check to see if the artifact itself exists.
  Use ``Datastore.exists()`` to check that the datastore knows about a dataset and the artifact exists. (`DM-30335 <https://rubinobs.atlassian.net/browse/DM-30335>`_)


Bug Fixes
---------

- Fix handling of ingest_date timestamps.

  Previously there was an inconsistency between ingest_date database-native UTC
  handling and astropy Time used for time literals which resulted in 37 second
  difference. This updates makes consistent use of database-native time
  functions to resolve this issue. (`DM-30124 <https://rubinobs.atlassian.net/browse/DM-30124>`_)
- Fix butler repository creation when a seed config has specified a registry manager override.

  Previously only that manager was recorded rather than the full set.
  We always require a full set to be recorded to prevent breakage of a butler when a default changes. (`DM-30372 <https://rubinobs.atlassian.net/browse/DM-30372>`_)
- Stop writing a temporary datastore cache directory every time a ``Butler`` object was instantiated.
  Now only create one when one is requested. (`DM-30743 <https://rubinobs.atlassian.net/browse/DM-30743>`_)
- Fix ``Butler.transfer_from()`` such that it registers any missing dataset types and also skips any datasets that do not have associated datastore artifacts. (`DM-30784 <https://rubinobs.atlassian.net/browse/DM-30784>`_)
- Add support for click 8.0. (`DM-30855 <https://rubinobs.atlassian.net/browse/DM-30855>`_)
- Replace UNION ALL with UNION for subqueries for simpler query plans. (`DM-31429 <https://rubinobs.atlassian.net/browse/DM-31429>`_)
- Fix parquet formatter error when reading tables with no indices.

  Previously, this would cause butler.get to fail to read valid parquet tables. (`DM-31700 <https://rubinobs.atlassian.net/browse/DM-31700>`_)
- Fix problem in ButlerURI where transferring a file from one URI to another would overwrite the existing file even if they were the same actual file (for example because of soft links in the directory hierarchy). (`DM-31826 <https://rubinobs.atlassian.net/browse/DM-31826>`_)


Performance Enhancement
-----------------------

- Make collection and dataset pruning significantly more efficient. (`DM-30140 <https://rubinobs.atlassian.net/browse/DM-30140>`_)
- Add indexes to make certain spatial join queries much more efficient. (`DM-31548 <https://rubinobs.atlassian.net/browse/DM-31548>`_)
- Made 20x speed improvement for ``Butler.transfer_from``.
  The main slow down is asking the datastore whether a file artifact exists.
  This is now parallelized and the result is cached for later. (`DM-31785 <https://rubinobs.atlassian.net/browse/DM-31785>`_)
- Minor efficiency improvements when accessing `lsst.daf.butler.Config` hierarchies. (`DM-32305 <https://rubinobs.atlassian.net/browse/DM-32305>`_)
- FileDatastore: Improve removing of datasets from the trash by at least a factor of 10. (`DM-29849 <https://rubinobs.atlassian.net/browse/DM-29849>`_)

Other Changes and Additions
---------------------------

- Enable serialization of ``DatasetRef`` and related classes to JSON format. (`DM-28678 <https://rubinobs.atlassian.net/browse/DM-28678>`_)
- `ButlerURI` ``http`` schemes can now handle non-WebDAV endpoints.
  Warnings are only issued if WebDAV functionality is requested. (`DM-29708 <https://rubinobs.atlassian.net/browse/DM-29708>`_)
- Switch logging such that all logging messages are now forwarded to Python ``logging`` from ``lsst.log``.
  Previously all Python ``logging`` messages were being forwarded to ``lsst.log``. (`DM-31120 <https://rubinobs.atlassian.net/browse/DM-31120>`_)
- Add formatter and storageClass information for FocalPlaneBackground. (`DM-22534 <https://rubinobs.atlassian.net/browse/DM-22534>`_)
- Add formatter and storageClass information for IsrCalib. (`DM-29531 <https://rubinobs.atlassian.net/browse/DM-29531>`_)
- Change release note creation to use [Towncrier](https://towncrier.readthedocs.io/en/actual-freaking-docs/index.html). (`DM-30291 <https://rubinobs.atlassian.net/browse/DM-30291>`_)
- Add a Butler configuration for an execution butler that has pre-defined registry entries but no datastore records.

  The `Butler.put()` will return the pre-existing dataset ref but will still fail if a datastore record is found. (`DM-30335 <https://rubinobs.atlassian.net/browse/DM-30335>`_)
- If an unrecognized dimension is used as a look up key in a configuration file (using the ``+`` syntax) a warning is used suggesting a possible typo rather than a confusing `KeyError`.
  This is no longer a fatal error and the key will be treated as a name. (`DM-30685 <https://rubinobs.atlassian.net/browse/DM-30685>`_)
- Add ``split`` transfer mode that can be used when some files are inside the datastore and some files are outside the datastore.
  This is equivalent to using `None` and ``direct`` mode dynamically. (`DM-31251 <https://rubinobs.atlassian.net/browse/DM-31251>`_)

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
