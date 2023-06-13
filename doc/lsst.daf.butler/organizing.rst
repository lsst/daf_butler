.. _daf_butler_organizing_datasets:

Organizing and identifying datasets
===================================

.. py:currentmodule:: lsst.daf.butler

Each dataset in a repository is associated with an opaque unique integer ID, which we currently call its ``dataset_id``, and it's usually seen in Python code as the value of `DatasetRef.id`.
This is the number used as the primary key in most `Registry` tables that refer to datasets, and it's the only way the contents of a `Datastore` are matched to those in a `Registry`.
With that number, the dataset is fully identified, and anything else about it can be unambiguously looked up.
We call a `DatasetRef` whose `~DatasetRef.id` attribute is not `None` a *resolved* `DatasetRef`.

.. note::

    In most data repositories, dataset IDs are 128-bit UUIDs that are guaranteed to be unique across all data repositories, not just within one; if two datasets share the same UUID in different data repositories, they must be identical (this is possible because of the extraordinarily low probability of a collision between two random 128-bit numbers, and our reservation of deterministic UUIDs for very special datasets).
    As a result, we also frequently refer to the dataset ID as the UUID, especially in contexts where UUIDs are actually needed or can be safely assumed.

Most of the time, however, users identify a dataset using a combination of three other attributes:

 - a dataset type;
 - a data ID (also known as data coordinates);
 - a collection.

Most collections are constrained to contain only one dataset with a particular dataset type and data ID, so this combination is usually enough to resolve a dataset (see :ref:`daf_butler_collections` for exceptions).

A dataset's type and data ID are intrinsic to it --- while there may be many datasets with a particular dataset type and/or data ID, the dataset type and data ID associated with a dataset are set and fixed when it is created.
A `DatasetRef` always has both a dataset type attribute and a data ID, though the latter may be empty.
Dataset types are discussed below in :ref:`daf_butler_dataset_types`, while data IDs are one aspect of the larger :ref:`Dimensions <lsst.daf.butler-dimensions_overview>` system and are discussed in :ref:`lsst.daf.butler-dimensions_data_ids`.

In contrast, the relationship between datasets and collections is many-to-many: a collection typically contains many different datasets, and a particular dataset may belong to multiple collections.
As a result, is is common to search for datasets in multiple collections (often in a well-defined order), and interfaces that provide that functionality can accept a collection search path in :ref:`many different forms <daf_butler_collection_expressions>`.
Collections are discussed further below in :ref:`daf_butler_collections`.

.. _daf_butler_dataset_types:

Dataset types
-------------

The names "dataset" and "dataset type" (which ``daf_butler`` inherits from its ``daf_persistence`` predecessor) are intended to evoke the relationship between an instance and its class in object-oriented programming, but this is a metaphor, *not* a relationship that maps to any particular Python objects: we don't have any Python class that fully represents the *dataset* concept (`DatasetRef` is the closest), and the `DatasetType` class is a regular class, not a metaclass.
So a *dataset type* is represented in Python as a `DatasetType` *instance*.

A dataset type defines both the dimensions used in a dataset's data ID (so all data IDs for a particular dataset type have the same keys, at least when put in standard form) and the storage class that corresponds to its in-memory Python type and maps to the file format (or generalization thereof) used by a `Datastore` to store it.
These are associated with an arbitrary string name.

Beyond that definition, what a dataset type *means* isn't really specified by the butler itself, but we expect higher-level code that *uses* butler to make that clear, and one anticipated case is worth calling out here: a dataset type roughly corresponds to the role its datasets play in a processing pipeline.
In other words, a particular pipeline will typically accept particular dataset types as inputs and produce particular dataset types as outputs (and may produce and consume other dataset types as intermediates).
And while the exact dataset types used may be configurable, changing a dataset type will generally involve substituting one dataset type for a very similar one (most of the time with the same dimensions and storage class).

.. _daf_butler_collections:

Collections
-----------

Collections are lightweight groups of datasets defined in the `Registry`.
Groups of self-consistent calibration datasets, the outputs of a processing run, and the set of all raw images for a particular instrument are all examples of collections.
Collections are referred to in code simply as `str` names; various `Registry` methods can be used to manage them and obtain additional information about them when relevant.

There are multiple types of collections, corresponding to the different values of the `CollectionType` enum.
All collection types are usable in the same way in any context where existing datasets are being queried or retrieved, though the actual searches may be implemented quite differently in terms of database queries.
Collection types behave completely differently in terms of how and when datasets can be added to or remove from them.

Run Collections
^^^^^^^^^^^^^^^

A dataset is always added to a `CollectionType.RUN` collection when it is inserted into the `Registry`, and can never be removed from it without fully removing the dataset from the `Registry`.
There is no other way to add a dataset to a ``RUN`` collection.
The run collection name *must* be used in any file path templates used by any `Datastore` in order to guarantee uniqueness (other collection types are too flexible to guarantee continued uniqueness over the life of the dataset).

The name "run" reflects the fact that we expect most ``RUN`` collections to be used to store the outputs of processing runs, but they should also be used in any other context in which their lack of flexibility is acceptable, as they are the most efficient type of collection to store and query.

``RUN`` collections that do represent the outputs of processing runs can be associated with a host name string and a timespan, and are expected to be the way in which some provenance is associated with datasets (e.g. a dataset that contains a list of software versions would have the same ``RUN`` as the datasets produced by a processing run that used those versions).

Like most collections, a ``RUN`` can contain at most one dataset with a particular dataset type and data ID.

Tagged Collections
^^^^^^^^^^^^^^^^^^

`CollectionType.TAGGED` collections are the most flexible type of collection; datasets can be `associated <Registry.associate>` with or `disassociated <Registry.disassociate>` from a ``TAGGED`` collection at any time, as long as the usual contraint on a collection having only one dataset with a particular dataset type and data ID is maintained.
Membership in a ``TAGGED`` collection is implemented in the `Registry` database as a single row in a many-to-many join table (a "tag") and is completely decoupled from the actual storage of the dataset.

Tags are thus both extremely lightweight relative to copies or re-ingests of files or other `Datastore` content, and *slightly* more expensive to store and possibly query than the ``RUN`` or ``CHAINED`` collection representations (which have no per-dataset costs).
The latter is rarely important, but higher-level code should avoid  automatically creating ``TAGGED`` collections that may not ever be used.

Calibration Collections
^^^^^^^^^^^^^^^^^^^^^^^

`CollectionType.CALIBRATION` collections associate each dataset they contain with a temporal validity range.
The usual constraint on dataset type and data ID uniqueness is enforced as a function of time, not collection-wide - so for any particular dataset type and data ID combination, the validity range timespans may not overlap (but may be - and usually are - adjacent).

In other respects, ``CALIBRATION`` collections closely resemble ``TAGGED`` collections: they are also backed by a many-to-many join table (where each row has a timespan as well as a collection identifier and a dataset identifier), and datasets can be associated or disassociated from them similarly freely.
We use slightly different nomenclature for these operations, reflecting the high-level actions they represent: `certifying <Registry.certify>` a dataset adds it to a ``CALIBRATION`` collection with a particular validity range, and `decertifying <Registry.decertify>` a dataset removes some or all of that validity range.

The same dataset can be present in a ``CALIBRATION`` collection multiple times with different validity ranges.

Chained Collections
^^^^^^^^^^^^^^^^^^^

A `CollectionType.CHAINED` collection is essentially a multi-collection search path that has been saved in the `Registry` database and associated with a name of its own.
Querying a ``CHAINED`` collection simply queries its child collections in order, and a ``CHAINED`` collection is always (and only) updated when its child collections are.

``CHAINED`` collections may contain other chained collections, as long as they do not contain cycles, and they can also include restrictions on the dataset types to search for within each child collection (see :ref:`daf_butler_collection_expressions`).

The usual constraint on dataset type and data ID uniqueness within a collection is only lazily enforced for chained collections: operations that query them either deduplicate results themselves or terminate single-dataset searches after the first match in a child collection is found.
In some methods, like `Registry.queryDatasets`, this behavior is optional: passing ``findFirst=True`` will enforce the constraint, while ``findFirst=False`` will not.
