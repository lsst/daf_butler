Registry Refactoring To-Do
==========================

- Make DatasetRef immutable/always-complete.
- Refactor Registry by adding Backend.
- Switch to per-DatasetType `dataset` tables.
- Add multi-collection search to Butler.
- Add collection table.
- Move to per-Collection `dataset_collection` tables?
- Add label to `quantum` table.

Construction Patterns
---------------------

Constructing clients to an existing repo:

- Identify Registry class.
- Identify Datastore class (maybe recursive, if Chained).
- Assemble complete configuration.


Types of Collection and DatasetType
-----------------------------------

Global DatasetTypes
"""""""""""""""""""

- always ingested
- uids are calculated from files (via data IDs?), using reserved site ID
- data IDs are globally unique, not just unique within a given collection

Standard DatasetTypes
"""""""""""""""""""""

 - all we have now
 - uids are autoincrement + site
 - DatasetType + data ID is unique within any collection

Nonsingular DatasetTypes
""""""""""""""""""""""""

- uids are autoincrement + site
- use case: configs and environments for pipeline runs
- probably use case: master calibrations
- can have multiple nonsingular datasets with the same data ID in a collection;
frequently have minimal or nonexistent data IDs
- cannot be retrieved with just a data ID (unless iterating); need uids

Tagged Collections
""""""""""""""""""

- all we have now
- most flexible type of collection: datasets can be added and removed at will

Run Collections
"""""""""""""""

- special implied collection for any dataset
- datasets can never be removed from this collection

Calibration Collections
"""""""""""""""""""""""

- associate each dataset with a validity range
- validity ranges for all instances of a dataset type are non-overlapping

Virtual Collection
""""""""""""""""""

- ordered list of collections to be searched

Dataset Table Partitioning
--------------------------

dataset_common
""""""""""""""
incr_id: int NOT NULL (autoincrement)
site_id: int NOT NULL
dataset_type_name: str NOT NULL
run_id: int NOT NULL
quantum_id: int

PRIMARY KEY (incr_id, site_id)

dataset_<name>
""""""""""""""
incr_id: int NOT NULL
site_id: int NOT NULL
<dimensions>

PRIMARY KEY (incr_id, site_id)


dataset_collection
""""""""""""""""""
incr_id: int NOT NULL
site_id: int NOT NULL
<collection>
<hash-of-dimensions>