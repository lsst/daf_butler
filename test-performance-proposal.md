# Speeding up the daf_butler test suite

## Summary

Repository setup, not SQL, dominates the daf_butler test suite.
`Butler.makeRepo` and `Butler.from_config` account for **151.0s of the suite's 378.3s of test time (40%)**.

The originally proposed change, teaching `SqliteDatabase` to accept a shared-cache in-memory URI, is **not worth doing**.
Its measured ceiling for Butler-level tests is about 9%, not the 3.7x seen for registry-level tests, because the SQLite driver is only about 5.6% of Butler test runtime.

Two changes are proposed instead.
The first is a test helper that wraps `Butler.makeRepo` and serves repeated requests from a cached pristine template repository.
This has now been implemented on `tickets/DM-55885`; see the **Implemented result** section below.
The second, larger and later, reuses a live template `Butler` via `clone()` and resets it with a SQLite snapshot.
It measures 6.5ms to 14.8ms per test against 83ms to 118ms today, but it requires new private reset APIs in the library.

## Implemented result

Repository setup fell from **151.0s to 87.8s**, a 42% reduction in setup time.
Total instrumented test time fell from 378.3s to 314.6s, a drop of 63.7s that matches the 63.2s setup saving almost exactly, so the whole saving is attributable to this change.
That is a **16.8% reduction in total test suite time**.

Per file, the largest absolute movements were:

| File | Setup before | Setup after |
| --- | --- | --- |
| `tests/test_butler.py` | 52.19s | 29.67s |
| `tests/test_parquet.py` | 17.92s | 8.83s |
| `tests/test_remote_butler.py` | 19.01s | 11.79s |
| `tests/test_sqlite.py` | 20.49s | 7.92s |

`tests/test_sqlite.py` improved despite being listed as out of scope, because it reaches repository creation indirectly through `makeTestRepo`.

The figure above comes from direct instrumentation of the repository-setup call paths, measured identically before and after.
End-to-end A/B runs on this machine were too noisy to be useful, giving 7.8% and 13.3% on successive repetitions of the same comparison.
Direct attribution should be preferred for any future measurement of this kind.

## Recommendation

1. Land a test helper that wraps `Butler.makeRepo`, and migrate test call sites to it.
   This is the seam both optimizations need.
2. Implement the template repository cache inside that helper.
   Measured 21.1% on `test_butler.py`; expected to generalize to about 40% of total suite time.
3. Only then consider the `clone()` plus snapshot route, and only behind supported private reset APIs with their own tests.
4. Close the shared-cache in-memory SQLite ticket as not worthwhile, keeping the existing in-memory default for registry-level and database-level tests where the 3.7x is real.

## Methodology and a warning about measurement

All figures below were taken with `lsst_distrib` tag b8412 and a local `lsst_resources` clone.

**Wall-clock timings on this machine are unreliable.**
Two runs of an identical configuration with identical cache statistics measured 74.40s and 94.68s, a 27% swing caused by unrelated load.
Every headline figure in this document is therefore **CPU time** (user plus system), which reproduced to within 0.5% across repetitions.
Per-call microbenchmarks are means of 20 iterations and were stable.

Anyone re-measuring this work should use CPU time and interleave the A and B runs.

## Investigation outcomes

### The original premise does not hold for Butler-level tests

A shared-cache in-memory database works fine at the SQLAlchemy layer.
A second engine sees the first engine's data, and `BEGIN IMMEDIATE` behaves correctly, with or without `StaticPool`.
The route is technically available.

It is not worth taking.
Disabling SQLite durability entirely (`PRAGMA synchronous=OFF` plus `journal_mode=MEMORY`) removes every fsync that an in-memory database would also remove, and measured only a 9% improvement.
In a profile of a representative subset, `sqlite3.Cursor.execute` and `sqlite3.Connection.commit` together account for about 5.6% of runtime.

Two further obstacles would need solving for no real gain.
`PRAGMA database_list` returns an empty filename for a shared-cache in-memory database, so `_find_database_filename` yields `None` and `SqliteDatabase` misreports itself.
`mode=memory` and `mode=ro` are mutually exclusive in a SQLite URI, so read-only opens would need `PRAGMA query_only` instead.

### Where the time actually goes

Instrumenting the full suite gives the following attribution.

| Phase | Calls | Wall time |
| --- | --- | --- |
| `Butler.makeRepo` | 315 | 26.32s |
| `Butler.from_config` | 492 | 25.88s |

Those figures are for `tests/test_butler.py` alone, where setup is **52.2s of 95.5s (55%)**.
Note that reopens outnumber creations, so `from_config` matters as much as `makeRepo`.

Across the whole suite, repository setup is 151.0s of 378.3s.
Six files hold 129s of that.

| File | Setup | File total | Calls | Setup share |
| --- | --- | --- | --- | --- |
| `tests/test_butler.py` | 52.19s | 95.47s | 807 | 55% |
| `tests/test_sqlite.py` | 20.49s | 65.15s | 354 | 31% |
| `tests/test_remote_butler.py` | 19.01s | 41.27s | 190 | 46% |
| `tests/test_parquet.py` | 17.92s | 22.27s | 284 | 80% |
| `tests/test_query_remote.py` | 9.80s | 24.02s | 102 | 41% |
| `tests/test_simpleButler.py` | 9.57s | 32.09s | 124 | 30% |

About a dozen `test_cliCmd*.py` files show the same shape at smaller scale, between 70% and 89% setup each.
They share the pattern and would be fixed by the same helper.

### The database content is free to copy

The cost of repository setup is Python object construction, not database work.

| Operation | Cost |
| --- | --- |
| `makeTestRepo` (current) | 83.1ms |
| `Butler.from_config` on an existing repo | 34.7ms to 42.4ms |
| `shutil.copytree` of a template repo | 1.2ms |
| `shutil.copyfile` of `gen3.sqlite3` (824KB) | 0.8ms |
| `sqlite3.Connection.backup()` into `:memory:` | 0.1ms |
| `sqlite3.Connection.deserialize()` | under 0.05ms |

A freshly created test repository is two files totaling 844KB, and `butler.yaml` retains the `<butlerRoot>` token, so it is relocatable by a plain directory copy.

### `declareStaticTables` cannot be short-circuited

Roughly 48 tables are converted into SQLAlchemy `Table` objects on **every** `Database` instantiation, including read-only reopens that create nothing.
Across the suite this is 320 calls with `create=True` costing 11.51s and 768 calls with `create=False` costing 6.26s.

This work cannot simply be skipped.
The `Table` objects that `declareStaticTables` builds are the query surface that every manager uses to construct subsequent SQL.
Reuse is the only available lever, and naive reuse is unsafe: sharing one `DatabaseMetadata` across `Database` instances broke 45 tests, because `DatabaseMetadata.add_table` caches by bare table name and dynamic tables leak between repositories.

## Proposed change 1: a test helper wrapping `Butler.makeRepo`

### Rationale

`makeTestRepo` exists but is used in only six places.
`tests/test_butler.py` calls `Butler.makeRepo` directly, and those direct calls are 270 to 302 of the cacheable requests.
A helper-only change would therefore barely move the suite.

Migrating the direct `Butler.makeRepo` call sites in tests to a wrapper creates a single seam through which any repository-setup optimization can be applied.
It also keeps the optimization out of the public `Butler.makeRepo`, so tests that genuinely exercise repository creation remain unaffected by construction.

### Concrete edits

Add to `python/lsst/daf/butler/tests/_testRepo.py`:

```python
def make_repo_for_test(root, config=None, dimensionConfig=None, **kwargs):
    """Create a test repository, reusing a cached template when possible."""
```

The signature should mirror `Butler.makeRepo` (`_butler.py:394`) and return the same `Config`.
Migrate the direct `Butler.makeRepo` calls in the test files listed in the attribution table to call this instead.

### Cache design

Build the repository once per distinct configuration, then serve later requests by copying the template directory.

Cache key:

- the `config` mapping, expanded to a plain dict,
- the `dimensionConfig`,
- `forceConfigRoot`,
- `os.environ.get("DAF_BUTLER_CONFIG_PATH")`.

`DAF_BUTLER_CONFIG_PATH` (`_config.py:59`, consumed at `_config.py:1279`) is the **only** environment variable in the config assembly path, so it is the only one the key needs.

Bypass the cache entirely when `outfile`, `standalone`, or `overwrite` is set, or when `searchPaths` is supplied.

Two details that caused real failures in the prototype and must be handled:

1. **Return a config re-read from the copy**, not the template's config object.
   Otherwise callers see the template's root rather than their own.
2. **Gate on `isinstance(rp, FileResourcePath)`, not on `ResourcePath.isLocal`.**
   `RemoteTestResourcePath` subclasses `FileResourcePath` and overrides `isLocal = False` while remaining backed by a genuine local path, so `isLocal` excludes repositories that are perfectly copyable.
   Using the wrong gate left 45 calls uncached and cost 4.5 percentage points.

Use `shutil.copytree(..., dirs_exist_ok=True)`, since some callers pass a root that already exists.

### Measured result

`tests/test_butler.py`, interleaved A/B, CPU time.

| Variant | Rep 1 | Rep 2 | Mean saving |
| --- | --- | --- | --- |
| Baseline | 98.54s | 96.95s | |
| Template cache, `isLocal` gate | 79.23s | 85.47s | 16.6% |
| Template cache, `FileResourcePath` gate | 77.46s | 76.70s | **21.1%** |

All 259 tests pass in both configurations, with counts identical to baseline.
The cache served 302 requests from 46 templates, with 13 calls bypassing.

Across the whole suite, measured once in each configuration by CPU time:

| Variant | CPU | Result |
| --- | --- | --- |
| Baseline | 380.26s | 1717 passed, 1 pre-existing failure |
| Template cache | 311.61s | 1716 passed, 1 pre-existing failure plus 1 new |

That is an **18.1% reduction** in total suite CPU, with the cache serving 852 requests from 60 templates and only 14 calls bypassing.

### Why the prototype's one new failure argues for scoping the cache to a helper

The prototype patched `Butler.makeRepo` globally, and that broke
`tests/test_cliLog.py::CliLogTestCase::testFileLogging`.
The test runs `butler create` and asserts that more than ten DEBUG records reach the log file.
When repository creation is served from a cached template, the work is not done, so only one record is emitted and the assertion fails on `1 not greater than 10`.

This is the failure mode to take seriously.
The cache changes an observable side effect for any test that asserts on the *process* of repository creation rather than its result, and there is no general way to detect such tests.

Scoping the cache to a test helper removes this class of problem by construction.
`butler create` reaches repository creation through production code, so it would continue to call the real `Butler.makeRepo` and would continue to emit its log records.

The corollary is that the helper-scoped version will measure somewhat below 18.1%, because repository creations driven through the CLI stay uncached.
The `test_cliCmd*.py` files account for roughly 15s of setup in total, so the expected figure is in the mid teens rather than 18%.

### Known follow-up

The remote-test classes bake their per-test datastore root into the configuration, producing 46 distinct templates where 14 would otherwise suffice.
Excluding the datastore root from the cache key should collapse those and recover a few more points.

## Proposed change 2: template `Butler` reuse via `clone()` and snapshot reset

### Rationale

`DirectButler.clone()` (`direct_butler/_direct_butler.py:238`) costs 0.62ms against 34.7ms for `Butler.from_config`.
Combined with a SQLite snapshot restore for isolation, a per-test setup costs 6.5ms with `InMemoryDatastore`, or 14.8ms when each test also gets its own `FileDatastore` root.
Today the equivalent is 83ms to 118ms.

### Required reset steps

Restoring the database alone is **not sufficient**.
All three steps below are required, and steps 2 and 3 were each found by way of a real test failure.

1. Restore pristine database contents with `sqlite3.Connection.backup()` from a snapshot held open for the session.
2. Remove dynamic tables from `DatabaseMetadata` (`registry/interfaces/_database.py:2028`), both from its `_tables` dict and from the underlying `MetaData`.
   Without this, the next test fails with `no such table: dataset_tags_00000001`, because `ensureTableExists` finds the stale cached `Table` and skips creation.
3. Clear the dataset type cache **including its shared table cache**.
   `DatasetTypeCache.clear()` (`registry/datasets/byDimensions/_dataset_type_cache.py:138`) does not touch `tables`, and `clone()` deliberately shares that cache by reference (`_dataset_type_cache.py:83`).
   `DynamicTables.create` (`registry/datasets/byDimensions/tables.py:528`) short-circuits on it, so a stale entry silently suppresses table creation.

`SqlRegistry.refresh()` (`registry/sql_registry.py:346`) handles the remaining manager state and costs 0.05ms.

### Concrete edits

These steps reach into private state that will drift, and a drift failure presents as a **wrong test result rather than an error**.
They must therefore be supported APIs with their own tests, not a test helper reaching into internals.

Add, for testing use only:

- `Database.restore_snapshot(...)` and a matching snapshot accessor on `SqliteDatabase`, covering step 1.
- A metadata reset covering step 2, most naturally a method on `DatabaseMetadata` that drops everything added since the static schema was declared.
- A `reset_caches()` on the dataset record storage manager covering step 3, which must clear `tables` as well as the by-name, by-id, and by-dimensions caches.

Each needs a direct unit test asserting that stale dynamic tables and stale cache entries do not survive a reset.

### Validation performed

A prototype passed five isolation scenarios:

- new dimension records do not leak between tests,
- chained collections do not leak,
- calibration collections work across resets, exercising the second kind of dynamic table,
- opaque and datastore records do not leak,
- a dataset type name can be reused with a different storage class in a later test.

A per-test `FileDatastore` root also works, at 14.8ms against 42.4ms for `from_config`.

### Projection

About 2,200 setup calls across the suite currently average about 68ms.
At 15ms each the total falls from 151s to roughly 33s.
This is a **projection** from stable per-call measurements, not an end-to-end measurement, and it assumes call sites can adopt the route.

## Rejected options

| Option | Measured | Why rejected |
| --- | --- | --- |
| Shared-cache in-memory SQLite | about 9% ceiling | SQLite driver is only 5.6% of runtime; needs `database_list` and read-only mode workarounds |
| `PRAGMA synchronous=OFF`, `journal_mode=MEMORY` | 9% | Weakens crash durability; must not leak into production configs. Available cheaply for tests alone if wanted |
| Sharing one `DatabaseMetadata` across `Database` instances | 9%, **45 test failures** | Dynamic tables leak between repositories |
| Caching parsed config files by URI and mtime | no gain | The cost is the in-memory merge, not file parsing |

## Interaction with pytest-xdist

`pytest-xdist` 3.8.0 is installed and is **not** currently enabled; `addopts` in `pyproject.toml:141` is only `--ignore=tests_integration`.

On 20 cores, `tests/test_butler.py` runs in 30.92s with `-n 8 --dist loadscope` against 97s single-process.
The template cache still helps under xdist, but less: 30.92s to 26.65s, about 14%.

The two approaches compete for the same seconds.
Caches are per-process, so template builds multiply by worker count while the saved work divides.
The distinction that matters is that xdist spends eight cores to shorten one run, whereas these changes reduce the total work, which is what helps shared CI capacity.

## Risks and non-goals

- The template cache must live in a test helper.
  Putting it in `Butler.makeRepo` changes public behavior and affects tests whose purpose is to exercise repository creation.
  This is not hypothetical: a globally patched prototype broke `test_cliLog.py::CliLogTestCase::testFileLogging`, which asserts on the volume of log output produced while creating a repository.
- Change 2 must not ship without the supported reset APIs described above.
  Cache drift there produces incorrect test results rather than failures.
- Neither change should alter production code paths.
  The durability pragmas in particular must be confined to test configuration.
- The `clone()` route shares one registry between a template and its clones.
  Tests that legitimately need two independent repositories with the same configuration, such as the transfer tests, need separate templates.

## Unrelated observation

`tests/test_server.py::ButlerClientServerAuthorizationTestCase::test_group_authorization` fails on `tickets/DM-55885` with an unmodified working tree.
It is unrelated to this work but will confuse anyone re-running the suite.
