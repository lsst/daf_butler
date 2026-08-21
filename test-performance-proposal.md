# Speeding up the daf_butler test suite

## Outcome

The test suite is about 25% cheaper.
Total CPU across `tests/` fell from **378s to 286s**, and `tests/test_butler.py` from **95s to 62s** without PostgreSQL installed.

Six changes landed on `tickets/DM-55885`, each independently revertable:

| Commit | Change | Effect |
| --- | --- | --- |
| `e82ae5941` | Template cache for test repository creation | repository setup 151.0s to 87.8s |
| `35db7d4ed` | Only cache repositories with a SQLite registry | template builds 171 to 48 over the PostgreSQL modules |
| `a59300a70` | Split `Butler.makeRepo` into two phases | behaviour-neutral |
| `4f0a7f317` | Cache configuration and database separately | about 1s |
| `88e0dd63d` | `Config` merge dict fast path | merge 0.192ms to 0.084ms |
| `debc418e0` | Dedicated copy routine in `Config.__init__` | copy 111us to 48us |
| `b9c1b1949` | Reuse storage class definitions from an identical config | 18s, `addFromConfig` 8.80ms to 0.37ms |

The originally proposed change, teaching `SqliteDatabase` to accept a shared-cache in-memory URI, was **not worth doing** and should be closed.
Its measured ceiling for Butler-level tests was about 9%, because the SQLite driver is only about 5.6% of Butler test runtime.

## What was wrong with the initial premise

An in-memory registry is 3.7 times faster than a file-backed one for registry-level tests, and that gain is real.
It does not transfer to Butler-level tests.

Disabling SQLite durability entirely, with `PRAGMA synchronous=OFF` and `journal_mode=MEMORY`, removes every fsync that an in-memory database would also remove.
That measured a 9% improvement, which bounds what any in-memory scheme could achieve.
In a profile of a representative subset, `sqlite3.Cursor.execute` and `sqlite3.Connection.commit` together account for about 5.6% of runtime.

A shared-cache in-memory database does work at the SQLAlchemy layer: a second engine sees the first engine's data, and `BEGIN IMMEDIATE` behaves, with or without `StaticPool`.
Two obstacles would still need solving for that 9%.
`PRAGMA database_list` returns an empty filename for such a database, so `_find_database_filename` yields `None` and `SqliteDatabase` misreports itself.
`mode=memory` and `mode=ro` are mutually exclusive in a SQLite URI, so read-only opens would need `PRAGMA query_only`.

## Where the time actually went

Repository setup, not SQL, dominated.
Before any change, `Butler.makeRepo` and `Butler.from_config` accounted for **151.0s of 378.3s (40%)**.
Reopens outnumbered creations, 492 calls against 315 in `tests/test_butler.py` alone, so `from_config` mattered as much as `makeRepo`.

Six files held 129s of that 151s.

| File | Setup before | Setup after |
| --- | --- | --- |
| `tests/test_butler.py` | 52.19s | 29.67s |
| `tests/test_sqlite.py` | 20.49s | 7.92s |
| `tests/test_remote_butler.py` | 19.01s | 11.79s |
| `tests/test_parquet.py` | 17.92s | 8.83s |
| `tests/test_query_remote.py` | 9.80s | 5.31s |
| `tests/test_simpleButler.py` | 9.57s | 6.72s |

`tests/test_sqlite.py` improved despite reaching repository creation only indirectly, through `makeTestRepo`.

### The database content is free to copy

The cost of repository setup is Python object construction, not database work.

| Operation | Cost |
| --- | --- |
| `makeTestRepo` (original) | 83.1ms |
| `Butler.from_config` on an existing repository | 34.7ms |
| `shutil.copytree` of a template repository | 1.2ms |
| `sqlite3.Connection.backup()` into `:memory:` | 0.1ms |
| `sqlite3.Connection.deserialize()` | under 0.05ms |

A fresh test repository is two files totalling 844KB, and `butler.yaml` keeps the `<butlerRoot>` token, so it is relocatable by a plain directory copy.

## The changes that landed

### Template cache for repository creation

`make_repo_for_test` in `python/lsst/daf/butler/tests/_repo_template_cache.py` builds each distinct configuration once and copies it afterwards.
Test modules call it instead of `Butler.makeRepo`.

Two details each caused a real failure while developing it.

The "can this be copied" test must be `isinstance(path, FileResourcePath)`, **not** `ResourcePath.isLocal`.
`RemoteTestResourcePath` subclasses `FileResourcePath` and overrides `isLocal = False` while remaining backed by a local path.
Gating on `isLocal` silently excluded 45 copyable repositories and cost 4.5 percentage points.

The returned `Config` must be re-read from the copy, not returned from the template, or callers see the template's root.

The cache must live in a test helper rather than in `Butler.makeRepo`.
A prototype that patched the public method broke `tests/test_cliLog.py::CliLogTestCase::testFileLogging`, which runs `butler create` and asserts that more than ten DEBUG records reach the log file.
Served from a template the work is not done, so one record is emitted.
Any test that asserts on the *process* of repository creation rather than its result breaks this way, and there is no general way to detect such tests.
Scoping the cache to a helper removes the problem by construction, because production code still calls the real `Butler.makeRepo`.

### Only cache repositories with a SQLite registry

A client/server registry keeps its contents outside the repository directory, so copying the directory does not copy the registry.
`TemporaryPostgresInstance.patch_butler_config` also assigns a fresh `registry.namespace` from `secrets.token_hex` on every call, so every PostgreSQL configuration is unique and every lookup is a miss.
Caching them built a template that was used exactly once and then retained until exit, which is more work than creating the repository directly.

Over the PostgreSQL-backed modules this reduced template builds from 171 to 48.
Verified with `~/pyenv` and Homebrew `postgresql@16`, since `testing.postgresql` is not in the conda environment: all 139 PostgreSQL tests pass.

### Splitting `makeRepo`, and caching its two halves

`Butler._make_repo_butler_config` and `Butler._make_repo_registry` separate configuration assembly from database creation.
Measured on a default test repository the configuration phase costs about 10ms and the database phase about 36ms.

Only the registry and dimension configurations reach the database.
Two repositories differing only in datastore configuration produce **byte-identical databases**, verified by comparing canonical SQLite dumps.
The dimension universe does reach it: the four `tests/config/dimensions/*.yaml` files plus the default produce five distinct databases.

Caching the database on registry and dimensions alone, and `butler.yaml` on the whole configuration, reduced databases built across the suite from 116 to 19.

**Both halves must be cached.**
Caching only the database makes things worse, because the configuration phase then runs on every request: repository creation went from 9.07s to 23.62s with only the database cached, and to 7.98s with both.
The net gain over caching whole directories is about 1s, so this change is worth taking for its clearer model of what affects the database rather than for its speed.

### Configuration handling

`Config` merging tested every node against `collections.abc.Mapping`.
That abstract check costs about 143ns against 49ns for an exact `dict` check, and ran once per node of every subtree merged.

`Config.__init__` populated an empty dict by merging into it, so every value paid for a target lookup and a target type check that could not matter.

A census over the suite found only **12 distinct value types across 18.2 million merged values**: strings 53.5%, `Config` 22.0%, `dict` 11.0%, `list` 7.3%, then `bool`, `float`, `NoneType`, `int` and four `Config` subclasses.
There are no arbitrary complex objects, so the defensive comment about deep copy failing described a case that does not occur.
Lists were, and remain, shared by reference rather than copied.

### Storage class definitions

This was the largest single win after the template cache, worth about 18s.

`addFromConfig` cost the same 8.5ms on its tenth call as on its first, because every Butler re-derived identical definitions.
The cost was not the compatibility check.

| Phase | Share |
| --- | --- |
| `StorageClassConfig(config)` | 85% |
| `StorageClass` construction, 120 objects | 26% |
| pydantic `model_validate`, 120 models | 12% |
| `registerStorageClass`, the compatibility check | **4%** |

Recognising a configuration that has already been processed avoids all of the first three.
The key is hashed from the supplied configuration **without expanding it**, which is what makes it worthwhile: about 53us against 4ms to build the `StorageClassConfig`.
`pydantic_core.to_json` is 2.3 times faster than `json.dumps(sort_keys=True)` for this and is used instead.
It does not sort keys, so configurations holding the same definitions in a different order would miss the cache; measured over the suite this costs nothing, with 7 misses for 7 distinct configurations.

Registration still runs for every class, so a conflicting redefinition is reported exactly as before.
Verified against the known reproducer: 284 conflict errors, unchanged.

The cache is keyed on content and holds derived definitions rather than acting as a registry, so it does not assume a single global factory and survives the planned removal of the `StorageClassFactory` singleton.

## Investigated and deliberately not done

### `declareStaticTables`

This is now the largest remaining component of `Butler.from_config`, at 11.88ms of roughly 34ms.
It is left alone because every avenue measured is either slower or unsafe.

It cannot be short-circuited.
The `Table` objects it builds are the query surface every manager uses to construct subsequent SQL.

The work is not wasteful.
68% of it is SQLAlchemy constructing 48 tables and 193 columns; only 16% is daf_butler code.
Reflection, the obvious suspect, is **0.04ms**.

It is called twice per `from_config`, but the calls are not symmetric: the first declares one table in 0.80ms so that `loadRepo` can read the dimensions configuration out of the database, and the second declares all 48 in 11.88ms.
Removing the first would save under a millisecond.

Building the schema once and cloning it per `Database` does not work.
`Table.to_metadata` for the whole schema costs **15.20ms**, more than the 11.88ms of building it from specifications, because SQLAlchemy redoes the same construction and adds copying.

Sharing one `DatabaseMetadata` across `Database` instances is the only remaining route.
It **broke 45 tests**, because `DatabaseMetadata.add_table` caches by bare table name and dynamic tables leak between repositories.
Making it safe requires separating static from dynamic tables in the metadata, which is a design change whose failure mode is cross-test contamination rather than an error.

### Template `Butler` reuse via `clone()` and a snapshot

`DirectButler.clone()` costs 0.62ms against 34.7ms for `Butler.from_config`.
Combined with a SQLite snapshot restore for isolation, a per-test setup costs 6.5ms with `InMemoryDatastore`, or 14.8ms when each test also gets its own `FileDatastore` root.

Restoring the database alone is not sufficient.
Three steps are required, and the second and third were each found by way of a real test failure:

1. restore pristine contents with `sqlite3.Connection.backup()`;
2. remove dynamic tables from `DatabaseMetadata`, or the next test fails with `no such table: dataset_tags_00000001`;
3. clear the dataset type cache **including its shared table cache**, which `DatasetTypeCache.clear()` does not touch and which `clone()` deliberately shares by reference.

A prototype passed five isolation scenarios: new dimension records, chained collections, calibration collections, opaque and datastore records, and reusing a dataset type name with a different storage class.

This is not proposed for now.
Steps 2 and 3 reach into private state whose drift would present as a **wrong test result rather than an error**, so it needs supported reset APIs with their own tests before it could be trusted.

### Other rejected options

| Option | Measured | Why rejected |
| --- | --- | --- |
| Shared-cache in-memory SQLite | about 9% ceiling | SQLite driver is only 5.6% of runtime |
| `PRAGMA synchronous=OFF`, `journal_mode=MEMORY` | 9% | Weakens crash durability; must not reach production configuration |
| Sharing one `DatabaseMetadata` | 9%, **45 failures** | Dynamic tables leak between repositories |
| Caching parsed configuration files by URI and mtime | no gain | The cost is the in-memory merge, not file parsing |
| `Config.__getitem__` returning a view | about 12s | Changes a documented public contract; team has code relying on the deep copy |
| pickle round-trip instead of `deepcopy` | under 1s available | `copy.deepcopy` totals only 1.39s across the suite, and 63% of its calls pass `None`, where pickle is 4 times slower |
| Cython or Numba for the merge | not viable | Numba cannot handle 12 heterogeneous value types; Cython still calls the same dict C API, for maybe 2s |

## Interaction with pytest-xdist

`pytest-xdist` 3.8.0 is installed and **not** currently enabled; `addopts` in `pyproject.toml` is only `--ignore=tests_integration`.

On 20 cores, `tests/test_butler.py` ran in 30.92s with `-n 8 --dist loadscope` against 97s single-process.
The template cache still helped under xdist, but less: 30.92s to 26.65s, about 14%.

The two approaches compete for the same seconds.
Caches are per-process, so template builds multiply by worker count while the saved work divides.
The distinction that matters is that xdist spends eight cores to shorten one run, whereas these changes reduce total work, which is what helps shared CI capacity.

## A warning about measurement

**Wall-clock timings on this machine are unreliable.**
Two runs of an identical configuration with identical cache statistics measured 74.40s and 94.68s, a 27% swing caused by unrelated load.
An interleaved A/B of the same comparison gave 7.8% and 13.3% on successive repetitions.

Every headline figure here is either **CPU time** from `/usr/bin/time` with interleaved runs, or a direct instrumented measurement of the code path in question.
Direct attribution proved far more reliable than end-to-end timing and should be preferred.

Two measurements silently produced nothing and were caught only because the numbers were implausibly small.
A fresh `git worktree` cannot run this suite: it lacks the gitignored generated `version.py`, so all 63 test modules fail to import, and it lacks the `butler` entry point.
Passing a list of test files through a shell variable can collapse into a single argument, after which pytest exits in about a second having run nothing.

## Unrelated problems found

`tests/test_parquet.py` fails wholesale, 142 tests, when run after `tests/test_testRepo.py`.
`StorageClassFactory` is a process-wide singleton and the two modules define `StructuredDataDictYaml` with different `delegate` values, so the second registration raises and every subsequent Butler construction fails.
This is present at `78e7f2904`, before any of this work.
A full-suite run does not show it because alphabetical ordering happens to avoid the collision; any reordering, a different selection, `-p randomly`, or xdist redistribution can expose it.

`tests/test_server.py::ButlerClientServerAuthorizationTestCase::test_group_authorization` fails on this branch with an unmodified working tree.

## Corrections to earlier estimates in this document's history

Two projections here were wrong and are recorded so the reasoning is not repeated.

Repository creation was projected to save 5 to 7s from splitting the cache; it saved about 1s.
The projection extrapolated from a 10.4ms configuration-phase microbenchmark when real test configurations cost about 23ms, and assumed more redundancy among full configurations than exists.

The storage class work was projected at about 22s on the basis of *eliminating* the redundant derivation.
When that was ruled out, the approach changed to making the underlying merge faster, which halves a fraction rather than removing it, and the expected return should have been restated at that point.
Eliminating the derivation was later done differently, by content-keyed reuse, and did deliver about 18s.
