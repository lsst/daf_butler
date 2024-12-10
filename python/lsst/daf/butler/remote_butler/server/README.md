# Butler server

## Concurrency and caching

The server internally uses `DirectButler` instances to retrieve data from the
database.  `DirectButler` instances are not thread-safe, so we need a separate
instance for each request.  It is expensive to create a `DirectButler` instance
from scratch, so we create a "template" `DirectButler` the first a repository
is accessed, and clone it for each request.  (The cloning process is managed by
`LabeledButlerFactory`.)

Within `DirectButler`, there are a number of internal caches.  Some of these
caches assume that no external processes will be mutating the repository during
the lifetime of the `DirectButler` instance, and lock in state at the first
time the data is accessed.  This behavior is OK in the context of a single HTTP
request in Butler server. However, it is problematic across requests if the
repository can be changing in the background, because new requests wouldn't be
able to see updated data.

It is expected that the main data release repositories will be immutable,
which would allow for more aggressive caching, but we don't yet have a way to
configure "mutable" vs "immutable" repositories in the server.

### Summary of caches that exist in `DirectButler`

Caches that are shared globally between all Butler instances within a process:

- `DimensionUniverse`
- `DimensionGroup`
- `StorageClassFactory`

Caches shared between instances cloned from the same parent Butler instance:

- Most SqlAlchemy `Table` objects created during managers' `initialize()` classmethods
- Tables created by `register()` in `ByNameOpaqueTableStorageManager`
- Dataset "tag" and "calib" tables created by
  `ByDimensionsDatasetRecordStorageManagerUUID` and stored in `DatasetTypeCache`

Caches copied between instances at the time of cloning:

- `DimensionRecordCache`
- `DatasetType` and `DynamicTables` instances in `DatasetTypeCache`

Caches that start empty in a newly cloned instance:

- Collection cache and collection summary cache in `CachingContext`
- `DimensionGraph` cache in `_DimensionGroupStorage`
- `DatastoreCacheManager` in `FileDatastore` (not relevant to Butler server,
  since the server does not access files on the filesystem.)

### Caching caveats

There is not currently a way to detect all changes to a Butler repository and invalidate the caches.  The Butler server must be restarted if a repository is changed in any of the following ways:

- Any additions/deletions/modifications to governor dimension records
- Updating a dataset type (deleting it and re-registering the same name with different values)
- Deleting a dataset type
