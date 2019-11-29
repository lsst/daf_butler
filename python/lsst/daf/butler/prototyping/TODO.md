Registry Refactoring To-Do
==========================

- Make DatasetRef immutable/always-complete.
- Add multi-collection search to Butler.
- Add label to `quantum` table.

Primary key categories in Registry
----------------------------------

Named entities with surrogate IDs
"""""""""""""""""""""""""""""""""

Examples: `collection` (including `run`), `dataset_type`.

From the user perspective, these are always identified by a globally unique name (which humans are responsible for coming up with).

In the past, we used these strings directly as primary key and foreign keys, but in the future we will use autoincrement integer IDs as the internal primary key and foreign key value.

We do not use an origin field as part of these keys.

When referenced by another entity in a registry layer, that layer always needs its own copy of the named entity, which will in general result in different IDs for the same entity in different registry layers.  This makes it easy to define foreign key constraints on these entities, as they're strictly internal to a single layer.

These entities are always "synced" or "registered" rather than explicitly inserted.  This performs an insert only if a record with a matching name does not exist, and checks whether the rest of the definition conflicts if a matching record does exist.  These operations cannot be vectorized, and may require their own transaction.

Unnamed entities with autoincrement+origin primary keys
"""""""""""""""""""""""""""""""""""""""""""""""""""""""

Examples: `dataset`, `quantum`

These entities are uniquely identified by a compound primary key consisting of an autoincrement integer `id` column and a non-autoincrement integer `origin` column that corresponds at least roughly to the RegistryLayer that created the entity.

When transferring between disconnected data repositories, these should retain their original `origin`.

These entities should not be copied between layers in a single registry, though this complicates foreign key constraints (we could just remove the constraints themselves; unclear if we can define a foreign key on a union view, or whether we would want to).

These entities are always inserted or updated explicitly.  Insertion APIs are vectorized (though only some databases may actually implement them as a bulk insert, as we need to get inserted IDs back), and can happen as part of larger transactions.  We may wrap these APIs in savepoint calls to simplify transaction logic elsewhere, on the assumption that callers will need to use vectorization to ensure savepoints don't happen too often.

In Python, these entities must be represented by objects that know the `id` and `origin`.  These may be mutable objects that can have these fields updated after insert, or immutable objects that have a variant (e.g. another class in the same hierarchy) that does not know the `id` and `origin`, to represent pre-insertion objects.

Entities with fully-external primary keys
"""""""""""""""""""""""""""""""""""""""""

Examples: dimensions

The primary keys for these entities may be strings or integers, but they are always provided fully by users or external Python code.

These entities should not be copied between layers in a single registry, though this complicates foreign key constraints (we could just remove the constraints themselves; unclear if we can define a foreign key on a union view, or whether we would want to).

These entities are always inserted explicitly, and are never updated.  Insertion APIs are vectorized (all databases can probably implement those via bulk inserts of some kind), and can happen as a part of larger transactions.  We may wrap these APIs in savepoint calls to simplify transaction logic elsewhere, on the assumption that callers will need to use vectorization to ensure savepoints don't happen too often.
