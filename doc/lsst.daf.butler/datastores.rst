.. _daf_butler-datastores:

#######################
Datastore Configuration
#######################

.. py:currentmodule:: lsst.daf.butler

A Butler `Datastore` is configured in the ``datastore`` section of the top-level Butler YAML configuration.
The only mandatory entry in the datastore configuration is the ``cls`` key.
This specifies the fully qualified class name of the Python class implementing the datastore.
The default Butler configuration uses the `~datastores.fileDatastore.FileDatastore`.
All other keys depend on the specific datastore class that is selected.


.. note::

  The default configuration values can be inspected at ``$DAF_BUTLER_DIR/python/lsst/daf/butler/configs`` (they can be accessed directly as Python package resources) and current values can be obtained by calling ``butler config-dump`` on a Butler repository.

The supported datastores are:

* :ref:`daf_butler-datastores-file` (local POSIX along with remote datastores such as S3)
* :ref:`daf_butler-datastores-memory`
* :ref:`daf_butler-datastores-chain`

.. _daf_butler-datastores-file:

File-Based Datastores
=====================

There is a single file-based datastore (`~datastores.fileDatastore.FileDatastore`) that handles local POSIX file system and remote object stores.
This datastore uses formatters to read datasets from files and write datasets to files.
Data access is entirely mediated by the URI used to specify the datastore root and currently supports S3 and WebDAV in addition to local files.
If an absolute URI is stored directly in the datastore it can use a different URI scheme from that used to locate the root of the datastore.

The supported configurations are:

**root**
    The location of the "root" of the datastore "file system".
    Usually the default value of ``<butlerRoot>/datastore`` can be left unchanged.
    Here ``<butlerRoot>`` is a magic value that is replaced either with the location of the Butler configuration file or the top-level ``root`` as set in that ``butler.yaml`` configuration file.
**records**
    This sections defines the name of the registry table that should be used to hold details about datasets stored in the datastore (such as the path within the datastore and the associated formatter).
    This only needs to be set if multiple datastores are to be used simultaneously within one Butler repository since the table names should not clash.
**create**
    A Boolean to define whether an attempt should be made to initialize the datastore by creating the directory.  Defaults to `True`, and that default should normally not be changed.
**templates**
    The template to use to construct "files" within the datastore.
    The template uses data dimensions to do this.
    Generally the default setting will be usable although it can be tuned per `DatasetType`, `StorageClass` or data ID.
    Changes to this template only apply to new datasets since datastore remembers the names associated with previous datasets.
    Templates are formatted as `FileTemplate` instances.
**formatters**
    Mapping of `DatasetType`, `StorageClass` or data ID to a specific formatter class that understands the associated Python type and will serialize it to a file artifact.
    The formatters section also supports the definitions of write recipes (bulk configurations that can be selected for specific formatters) and write parameters (parameters that control how the dataset is serialized; note it is required that all serialized artifacts be readable by a formatter without knowing which write parameters were used).
    Once a formatter is associated with a particular dataset it is permanently associated with that dataset even if the configuration is later modified to specify a different formatter.
**constraints**
    Specify `DatasetType`, `StorageClass` or data ID that will be accepted or rejected by this datastore.
**composites**
    Controls whether composite datasets are disassembled by the datastore.
    By default composites are not disassembled.
    Disassembly can be controlled by `DatasetType`, `StorageClass` or data ID.

.. _daf_butler-config-lookups:

Name Matching
^^^^^^^^^^^^^

Templates, formatters, constraints, and composites all use a standard look up priority.
The order is:

#. If there is an ``instrument`` in the data ID the first look up will be for a key that matches ``instrument<INSTRUMENT_NAME>``.
   If there is a match the items within that part of the hierarchy will be matched in preference to those at the top-level.
#. The highest priority is then the `DatasetType` name.
#. If the `DatasetType` corresponds to a component of a composite the composite name will then be checked.
#. If there is still no match the dimensions will be used.
   Dimensions are specified by the presence of a ``+`` as a separator.
   For example ``instrument+physical_filter+visit`` would match any `DatasetType` that uses those three dimensions.
#. The final match is against the `StorageClass` name.

.. _daf_butler-datastores-memory:

In-Memory Datastore
===================

The `~datastores.inMemoryDatastore.InMemoryDatastore` currently only supports the ``constraints`` field.
This allows the datastore to accept specific dataset types.

In the future more features will be added to allow some form of cache expiry.

.. _daf_butler-datastores-chain:

Chained Datastores
==================

The `~datastores.chainedDatastore.ChainedDatastore` datastore enables multiple other datastores to be combined into one.
The datastore will be sent to every datastore in the chain and success is reported if any of the datastores accepts the dataset.
When a dataset is retrieved each datastore is asked for the dataset in turn and the first match is sufficient.
This allows an in-memory datastore to be combined with a file-based datastore to enable simple in-memory retrieval for a dataset that has been persisted to disk.
A file-based datastore can be turned into a a chained datastore after the fact, for example by adding an in-memory caching datastore.
The only constraint is that all the datasets in registry are associated with at least one of the datastores in the chain.

`~datastores.chainedDatastore.ChainedDatastore` has a ``datastores`` key that contains a list of datastore configurations that can match the ``datastore`` contents from other datastores.
Additionally, a `~datastores.chainedDatastore.ChainedDatastore` can also support ``constraints`` definitions.
