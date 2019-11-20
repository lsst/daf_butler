Registry Refactoring To-Do
==========================

- Make DatasetRef immutable/always-complete.
- Refactor Registry by adding Backend.
- Switch to per-DatasetType `dataset` tables.
- Add multi-collection search to Butler.
- Add collection table.
- Move to per-Collection `dataset_collection` tables?
- Add label to `quantum` table.

Architecture
------------

RegistryLayer

- ABC; specialized per RDBMS
- ABC declares table specifications
- abstracts a namespace in a particular RDBMS
- has a connection
- manages all static tables
- manages creation of all dynamic tables

OpaqueRecordStorage

- concrete
- abstracts a named opaque table (usually) needed by a Datastore
- cannot be chained; a Datastore's internals must not depend on which Registry layers the client is initialized with.

DimensionRecordStorage

- ABC; specialized for SQL vs SkyPix vs ...
- abstracts a table for a particular dimension element + its common skypix overlap table
- can be chained

QuantumRecordStorage

- ABC; specialized for SQL

DatasetRecordStorage
