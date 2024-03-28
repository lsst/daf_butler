.. _lsst.daf.butler-dimensions_overview:

.. py:currentmodule:: lsst.daf.butler

Overview
--------
Dimensions are astronomical concepts that are used to label and organize datasets.
In the `Registry` database, most dimensions are associated with a table that contains not just the primary key values for dimensions, but foreign key relationships to other dimension tables, metadata columns, and in some cases spatial regions and time intervals as well.
Examples of dimensions include instruments, detectors, visits, and tracts.

Instances of the `Dimension` class represent one of these concepts, not values of the type of one of those concepts (e.g. "detector", not a particular detector).
In fact, a dimension "value" can mean different things in different contexts: it could mean the value of the primary key or other unique identifier for a particular entity (the integer ID or string name for a particular detector), or it could represent a complete record in the table for that dimension.

The dimensions schema also has some tables that do not map directly to `Dimension` instances.
Some of these provide extra metadata fields for combinations of dimensions, and are represented by the `DimensionElement` class in Python (this is also the base class of the `Dimension` class, and provides much of its functionality).
Others represent the overlaps between spatial dimensions, and are discussed in :ref:`lsst.daf.butler-dimensions_spatial_and_temporal`.

.. _lsst.daf.butler-dimensions_relationships_and_containers:

Dimension Relationships and Containers
--------------------------------------

Dimensions may have relationships, and in fact these relationships are used almost exclusively to define relationships between datasets, which have no direct relationships between them.
There are two kinds of relationships:

 - If dimension ``A`` is a *required* dependency of dimension ``B``, ``A``'s primary key value must be provided in order to uniquely identify ``B``, and ``A`` has a one-to-many relationship with ``B``.
   For example, the detector dimension has a required dependency on the instrument dimension, and hence one uses both an instrument name and a detector ID (or detector name) to fully identify a detector.
   When both dimensions are associated with database tables, a required dependency involves having the dependency's primary key fields both foreign key fields and part of a compound primary key in the dependent table.

 - If dimension ``C`` is an *implied* dependency of dimension ``D``, a value for ``D`` implies a value for ``C``, and ``C`` has a one-to-many relationship with ``D``.
   For example, the visit dimension has an implied dependency on the physical filter dimension, because a visit is observed through exactly one filter and hence each visit ID determines a filter name.
   When both dimensions are associated with database tables, an implied dependency involves having a foreign key field in the dependent table that is not part of a primary key in the dependent table.

A `DimensionGroup` is an immutable, set-like container of dimensions that is guaranteed to (recursively) include all dependencies of any dimension in the set.
It also categorizes those dimensions into `~DimensionGroup.required` and `~DimensionGroup.implied` subsets, which have roughly the same meaning for a set of dimensions as they do for a single dimension: once the primary key values of all of the required dimensions are known, the primary key values of all implied dimensions are known as well.
`DimensionGroup` also guarantees a deterministic and topological sort order for its elements.

The complete set of all recognized dimensions is managed by a `DimensionUniverse`.
A dimension universe is constructed from configuration, and is responsible for constructing all `Dimension` and `DimensionElement` instances; within a universe, there is exactly one `Dimension` instance that is always used to represent a particular dimension.

`DimensionUniverse` instances themselves are held in a global map keyed by the version number in the configuration used for construction, so they behave somewhat like singletons.

.. _lsst.daf.butler-dimensions_data_ids:

Data IDs
--------

The most common way butler users encounter dimensions is as the keys in a *data ID*, a dictionary that maps dimensions to their primary key values.
Different datasets with the same `DatasetType` are always identified by the same set of dimensions (i.e. the same set of data ID keys), and hence a `DatasetType` instance holds a `DimensionGroup` that contains exactly those keys.

Many data IDs are simply Python dictionaries that use the string names of dimensions or actual `Dimension` instances as keys.
Most `Butler` and `Registry` APIs that accept data IDs as input accept both dictionaries and keyword arguments that are added to these dictionaries automatically.

The data IDs returned by the `Butler` or `Registry` (and most of those used internally) are usually instances of the `DataCoordinate` class.
`DataCoordinate` instances can have different states of knowledge about the dimensions they identify.
They always contain at least the key-value pairs that correspond to its `DimensionGroup`\ 's `~DimensionGroup.required` subset -- that is, the minimal set of keys needed to fully identify all other dimensions in the set.
They can also contain key-value pairs for the `~DimensionGroup.implied` subset (a state indicated by `DataCoordinate.hasFull()` returning `True`).
And if `DataCoordinate.hasRecords` returns `True`, the data ID also holds all of the metadata records associated with its dimensions, both as a mapping in
the `~DataCoordinate.records` attribute and via dynamic attribute access, e.g.
``data_id.exposure.day_obs``.

`DataCoordinate` objects can of course be used with standard Python built-in containers, but an interface (`DataCoordinateIterable`) and a few simple adapters (`DataCoordinateSet`, `DataCoordinateSequence`) also exist to provide a bit more functionality for homogenous collections of data IDs (in which all data IDs identify the same dimensions, and generally have the same `~DataCoordinate.hasFull` / `~DataCoordinate.hasRecords` state).

.. _lsst.daf.butler-dimensions_spatial_and_temporal:

Spatial and Temporal Dimensions
-------------------------------

Dimensions can be *spatial* or *temporal* (or both, or neither), meaning that each record is associated with a region on the sky or a timespan (respectively).
The overlaps between regions and timespans define many-to-many relationships between dimensions that --- along with the one-to-many ID-based dependencies --- generally provide a way to fully relate any set of dimensions.
This produces a natural, concise query system; dimension relationships can be used to construct the full ``JOIN`` clause of a SQL ``SELECT`` with no input from the user, allowing them to specify just the ``WHERE`` clause (see `Registry.queryDataIds` and `Registry.queryDatasets`).
It is also possible to associate a region or timespan with a combination of dimensions (such as the region for a visit and a detector), by defining a `DimensionElement` for that combination.

One kind of spatial dimension is special: a `SkyPixDimension` represents a complete pixelization of the sky, as defined by an `lsst.sphgeom.Pixelization` object.
These are typically hierarchical pixelizations, like :py:class:`Hierarchical Triangular Mesh (HTM) <lsst.sphgeom.HtmPixelization>`), :py:class:`Q3C <lsst.sphgeom.Q3cPixelization>`, or HEALPix (which has no `lsst.sphgeom` implementation currently), but a skypix dimension encodes both the pixelization scheme and a level, defining a unique mapping from points on the sky to integer IDs.
By convention, the name of a skypix dimension starts with a short, lowercase name for the pixelization scheme followed by the integer level (e.g. "htm7").

A moderately efficient database representation of temporal relationships is straightforward: these are overlaps of 1-d intervals, so we can use regular (i.e. B-tree) indexes to join directly on overlap expressions of intervals expressed as pairs of columns (though more specialized indexing that reflects the non-overlapping nature of many of these intervals may be necessary in the future).

The same is not true of regions (especially regions on the sphere), at least not without assuming a particular RDBMS.
Instead, spatial regions for dimensions are stored as opaque, ``base64``-encoded strings in the database, but we also create an overlap table for each spatial dimension element that relates it to a special "common" skypix dimension (see `DimensionUniverse.commonSkyPix`).
We can then use a regular index on the common skypix ID to make spatial joins efficient, to the extent that proximity in skypix ID corresponds to proximity on sky.
In practice, these IDs correspond to some space-filling curve, which yields good typical-case performance with a reasonable choice of pixelization level, but no guarantees on worst-case performance.

.. _lsst.daf.butler-dimensions_universe_history:

Dimension Universe Change History
---------------------------------

Each project can have their own universe definition.
The dimension universe data model evolves over time.
A universe is uniquely identified by its vesion number and its namespace.
The default universe had a namespace of ``daf_butler``.

The default namespace has had the following version changes:

1. Added ``healpix`` dimension.
   Removed ``visit.seeing``.
   Updated storage classes for many dimensions.
   Defined governor dimensions (``instrument`` and ``skymap``).
   Added ``day_obs`` and ``seq_num`` to ``visit`` and ``exposure``.
   Renamed ``exposure.name`` to ``exposure.obs_id``.
2. Modified how visits are defined
   (Added the default ``visit_system`` to ``instrument``,
   added ``visit_system_membership`` dimension, removed ``visit_system`` from ``visit`` dimension).
   Added ``has_simulated``, ``azimuth``, ``seq_start``, and ``seq_end`` to ``exposure``.
   Added ``seq_num``, ``azimuth`` to ``visit``.
3. Updated the length of the ``observation_reason`` field from 32 to 68 in ``exposure`` and ``visit``.
4. Added "populated_by" information to visit fields to allow related dimensions to be discovered automatically.
5. Changed the length of the ``instrument.name`` field from 16 to 32 characters.
6. Made ``day_obs`` and ``group`` full dimensions.
   Dropped ``group_id`` from ``exposure`` and renamed ``group_name`` to ``group``.
7. Added ``can_see_sky`` metadata field to ``exposure``.
   This field can indicate whether the detector received photons from the sky taking into account the camera shutter and the dome and telescope alignment.

Prior to October 2020 there were no version numbers for the dimension universe.
