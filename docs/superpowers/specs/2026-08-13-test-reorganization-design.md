# Test suite reorganization and pytest migration

Status: draft for review
Date: 2026-08-13
Ticket: to be assigned before implementation begins

## Problem

The test suite re-runs large numbers of tests against slightly different backend
configurations by way of class inheritance.
The cost is paid on every GitHub Actions run, on all four Python versions in the
matrix.

A full single-threaded baseline was measured on 2026-08-13 (see the appendix for
method and caveats):
20m30s wall clock, 1542 passed, 540 skipped, 2086 tests collected.

| Location | Time | Note |
| --- | --- | --- |
| `tests/test_butler.py` total | 543s (44%) | of which... |
| → `S3DatastoreButlerTestCase` | 257s / 33 tests | 21% of the entire suite, ~7.8s per test |
| → `ChainedDatastoreButlerTestCase` | 52s / 33 tests | full butler suite again |
| → `ButlerExplicitRootTestCase` | 43s / 40 tests | reruns all of `PosixDatastoreButlerTestCase` |
| → `ChainedDatastoreTransfers` | 47s / 14 tests | reruns all of `PosixDatastoreTransfers` |
| `tests/test_sqlite.py` total | 188s | 6 × the full 59-test `RegistryTests` suite |
| `tests/test_cliLog.py` | 80s | 2 tests |
| `tests/test_storageClass.py` | 43s | 1 test |
| `tests/test_packages.py` | 42s | 1 test |

The mixin suites involved are large and are instantiated many times:

| Module | Lines | Tests | Instantiations |
| --- | --- | --- | --- |
| `python/lsst/daf/butler/registry/tests/_registry.py` | 4151 | 59 | 7 |
| `python/lsst/daf/butler/tests/butler_queries.py` | 2688 | 34 | 3 |
| `python/lsst/daf/butler/registry/tests/_database.py` | 1359 | — | 3 |
| `tests/test_butler.py` | 3752 | — | ~10 concrete cases |
| `tests/test_datastore.py` | 2467 | — | ~9 concrete cases |

The structural cause is that adding one test method to a mixin such as
`ButlerTests` silently multiplies it by the number of concrete subclasses.
The reorganization must remove the existing duplication and also remove the
mechanism that lets it regrow unnoticed.

## Goals

- Reduce per-job CI wall clock substantially, primarily by not running every
  test against every backend variant.
- Migrate to native pytest: fixtures and parametrization instead of inheritance,
  bare `assert` instead of `unittest` assertion methods.
- Prove that code coverage does not decrease, at line and branch granularity.
- Leave the suite easier to extend, such that adding a test does not implicitly
  add ten.

## Non-goals

- Changing what CI covers.
  All backends continue to run on all four Python versions; matrix jobs run in
  parallel, so only per-job work matters.
- Changing library behavior.
  See the no-library-changes rule in the coverage gate section.
- Improving test coverage.
  Coverage must not decrease; increasing it is separate work.

## Key decisions

| Decision | Choice |
| --- | --- |
| Shipped mixin suites | In scope; nothing outside daf_butler imports them |
| Redundancy | Aggressive, guarded by measured marginal coverage |
| CI matrix | Unchanged: all backends on all Python versions |
| Coverage gate | Covered line and arc set diff must be empty |
| Delivery | One ticket branch, incremental commits, green at each step |
| Layout | Hierarchical `tests/<area>/`, gated on a sconsUtils change |
| Style | Fully pytest-native, all files |
| Shared fixtures | Shipped pytest plugin, registered via entry point |
| Triage method | Coverage-differential, from one instrumented baseline run |

## Prerequisite tickets in other repositories

Three changes outside this branch are part of the overall design.
All three are improvements in their own right rather than test-only
conveniences, which is why they belong upstream rather than as local
workarounds.

### sconsUtils: hierarchical test discovery

`tests/SConscript` calls `scripts.BasicSConscript.tests(pyList=[])`, and the
current behavior does not discover tests in subdirectories.
This is the only thing forcing a flat `tests/` directory.

A suite of this size is much clearer split by subsystem, so sconsUtils should be
taught to discover tests in a hierarchy.
GitHub Actions invokes pytest directly and is unaffected; only the Jenkins/scons
path needs this.
The hierarchical layout therefore cannot merge until this lands.

### lsst.resources: a native test-remote scheme

daf_butler contains no `.scheme ==` checks anywhere in `datastores/` or
`datastore/`.
All "remote" behavior is mediated by 13 `isLocal` sites plus `as_local()` and the
cache manager:

- `_formatter.py:553`, `:722`, `:1001`, `:1069`
- `datastores/fileDatastore.py:352`, `:1081`, `:3150`
- `datastore/cache_manager.py:565`
- `_butler_config.py:122`
- `repo_relocation.py:89`
- `_rubin/temporary_for_ingest.py:106`, `:128`, `:165`

A `ResourcePath` that reports `isLocal = False` while being backed by the local
filesystem can therefore exercise all of it, without moto and without an S3
mock.

The faithful implementation is a `FileResourcePath` subclass with
`isLocal = False` **and** a real `_as_local()` that copies to a temporary file
and cleans up.
Overriding `isLocal` alone is not sufficient and would be a false pass:
`FileResourcePath._as_local()` is just `yield self`, so the `not uri.isLocal`
branches would fire against a file that was never copied, leaving the
cache-population and temporary-file-cleanup paths untested.

Scheme dispatch in `ResourcePath.__new__` is a hardcoded if/elif chain with no
plugin registry, so the scheme must be registered in lsst.resources itself.
Monkeypatching the dispatch from a fixture was considered and rejected.

Every downstream package that handles remote URIs can use this to test its
non-local paths, so it belongs in resources regardless of this work.
`requirements.txt` already pins `lsst-resources @ git+...@main`, so GitHub
Actions picks the scheme up as soon as it merges.

### daf_butler: shared-cache in-memory SQLite

An in-memory registry is 3.7× faster than a file-backed one in the measured
baseline:

| Variant | Time | Per test |
| --- | --- | --- |
| `SqliteFileRegistry*` | ~49s / 59 tests | 0.83s |
| `SqliteMemoryRegistry*` | ~13s / 60 tests | 0.22s |

That gain is available today for registry-level and database-level tests, and
this design adopts it as their default backend.

It is **not** available for Butler-level tests.
`Butler.makeRepo` writes a config and `Butler.from_config` reopens it, and
reopening `:memory:` yields a fresh empty database.
Measured directly, `makeTestRepo` with `sqlite:///:memory:` fails with
`LookupError: Registry attribute config:dimensions.json is missing from
database`.
The convenience at `registry/databases/sqlite.py:284-292` recreates the schema on
reconnect but cannot restore the data or the registry attributes.

SQLite's own mechanism for this is a shared-cache in-memory database
(`file:name?mode=memory&cache=shared`), which persists across connections while
one connection stays open.
That route is closed today: `registry/databases/sqlite.py:198-205` explicitly
raises `NotImplementedError` for connection strings containing `uri=true`.

Teaching `SqliteDatabase` to accept a shared-cache in-memory URI would extend
the 3.7× gain to Butler-level tests, which is where most of the remaining time
sits.
It is a library change, so it must be a separate ticket and must not ride on this
branch.
If it merges first, the coverage baseline has to be re-taken against it.

## Coverage measurement and the gate

The coverage requirement is an input to the design, not a check applied
afterwards.
Which variants may be dropped is decided from measured data.

### One instrumented baseline run

A single run with per-test coverage contexts supplies everything needed:

```
pytest --cov=lsst.daf.butler --cov-branch --cov-context=test
```

The resulting `.coverage` SQLite database records which test covered which line
and arc.
It answers both questions:

- **Marginal coverage per variant.**
  For a variant such as the 33 `S3DatastoreButlerTestCase` contexts, compute the
  lines and arcs it covers that no other test covers.
  An empty marginal set means the variant is pure duplication and can be dropped
  wholesale.
  A non-empty set is attributed back to individual test contexts, and only those
  tests stay parametrized over that axis.
- **The before/after gate.**
  Extract the global covered line and arc set; after conversion, `baseline minus
  new` must be empty.
  Arcs matter as much as lines, because a dropped variant shows up in branch
  coverage first.

Ten separate per-variant coverage runs are not needed.

### Rules the gate imposes

**No library changes on this branch.**
Comparing covered line sets is only meaningful if `python/lsst/daf/butler/`
source line numbers are stable.
Any genuine bug found during the migration goes on a separate ticket.
Without this rule the gate cannot fail, and so proves nothing.

**The gate measures library code only.**
The measured set is `lsst.daf.butler` excluding the test-support modules being
relocated (`registry/tests/`, `tests/butler_queries.py` within the package).
Test-code coverage is not comparable across a rewrite that renames every file, so
`--cov=tests` stays in CI for reporting continuity but is not part of the gate.

**Skip count must not increase.**
A test that silently starts skipping usually appears in the line diff, but not if
another test happens to cover the same lines.
Silent skipping is a failure mode this migration is particularly prone to, so it
gets its own cheap check.

### Tooling

A single script, roughly 100 lines, querying the `coverage` database directly and
offering two operations:

- `marginal <variant>` — lines and arcs unique to a set of test contexts.
- `gate <baseline.coverage> <new.coverage>` — the covered-to-uncovered set,
  which must be empty.

It must be re-runnable at every commit in the series, so it lives on the branch
under a clearly marked path and is deleted in the closing commit.

### Measurement environment

Measurement happens in a dedicated venv built from `requirements.txt` and
`requirements/test.in`, mirroring what CI installs.
The shared EUPS stack is unsuitable: it lacks `fastapi` and
`testing.postgresql`, which skips 26% of tests, including the postgres and server
variants whose marginal coverage matters most.
Pip-installing into the stack conda environment is not permitted.

## Fixture architecture

### Axes

The current inheritance encodes four independent axes.
The fixtures make them explicit and separately selectable.

| Axis | Values |
| --- | --- |
| Registry backend | sqlite-file, sqlite-memory, postgres |
| Registry managers | collection manager (name-key, synth-int-key), ingest-date type (default, astropy) |
| Datastore | posix file, in-memory, chained, test-remote file, null |
| Butler client | direct, remote over test server, cloned |

### Opt-in parametrization

The governing idiom is optional indirect parametrization, so that one default
variant is what you get by writing a test normally, and additional variants are
opted into explicitly.

```python
@pytest.fixture
def registry_backend(request) -> str:
    return getattr(request, "param", "sqlite-file")   # default when not parametrized
```

A test written normally runs once, against the default combination.
A test that the marginal-coverage query identifies as backend-sensitive opts in:

```python
@pytest.mark.parametrize("registry_backend", ["sqlite-file", "postgres"], indirect=True)
def test_ingest_date_handling(butler): ...
```

Two properties motivate this shape over a plainly parametrized fixture:

- The default costs nothing to write.
  Multiplication becomes opt-in and visible in the diff, so the duplication
  cannot silently regrow.
  Long term this matters more than the one-off cleanup.
- Axes compose without a class explosion.
  `ButlerExplicitRootTestCase` currently reruns 40 tests to check one behavior;
  as a fixture axis it is one parametrized test.

`ButlerExplicitRootTestCase`, `ClonedSqliteButlerTestCase`,
`ClonedPostgresPosixDatastoreButlerTestCase`, `ChainedDatastoreTransfers` and the
three `ButlerMakeRepoOutfile*` classes all collapse this way, replacing roughly
140 test executions with a handful of parametrized cases, subject in each case to
the marginal-coverage check.

### Where the fixtures live

The fixtures ship as a pytest plugin in the package, registered by entry point:

```toml
[project.entry-points.pytest11]
daf_butler = "lsst.daf.butler.tests.fixtures"
```

The distinction that produced the current problem is between shipping test
*suites* that get subclassed and shipping test *fixtures* that get composed.
The first is the cause of the inheritance explosion.
The second is what `lsst.daf.butler.tests` already does well and what downstream
packages already consume, so exposing `butler`, `registry_backend`,
`datastore_type` and friends as fixtures gives those packages a working `butler`
fixture instead of a hand-rolled one.

Per-area `conftest.py` files hold only what is genuinely local to an area.

## Expensive-resource fixtures

Two tiers, plus a documented option.

**Tier 1, the default: function-scoped copy of a reference repo.**
A template repo is built once per xdist worker with the standard dimension
records populated.
Each test copies it and opens a Butler.
Measured on local disk:

| Operation | Time |
| --- | --- |
| `makeTestRepo`, file registry with dimension records | 0.130s |
| `copytree` of the 825 KiB template | 0.001s |
| `copytree` + `Butler.from_config` | 0.051s |

Copy-and-open is 2.5× cheaper than building fresh.
Note that essentially all of the remaining 0.051s is `Butler.from_config`, which
templating cannot avoid, since each test needs its own Butler instance.

**Tier 2, rare and explicit: built from scratch.**
For a non-standard dimension universe, a different collection manager, or a
deliberately empty registry.

Postgres gets the analogous treatment.
`TemporaryPostgresInstance` already provides one server per session with a fresh
namespace per test; the improvement is a template database created once, then
`CREATE DATABASE ... TEMPLATE ...` per test, which postgres implements as a cheap
file-level copy rather than a full schema build.

Under xdist, session-scoped fixtures are per worker, so the template is built
three or four times per run instead of roughly a thousand.
No `--dist` change is needed; the default `load` mode is fine because each worker
is self-sufficient.

### Documented option: a shared writeable repo

A shared repo with a unique run collection per test is viable for tests that are
orthogonal to each other under a given Butler configuration.
`butler.put` inserts dataset rows and a run collection without touching dimension
records, which is why the pattern works in pipe_base.

It is not the default here because the preconditions do not hold for the files
that dominate the runtime.
Much of the butler suite issues broad queries (`queryDatasets` across
collections, `queryCollections`, `queryDatasetTypes`) or performs destructive
operations (`removeRuns`, `pruneDatasets`, the `testImportExport` family).
Those tests observe and disturb each other, making results order-dependent, and
under xdist's default `load` distribution the order is not even stable between
runs.

The payoff is also small.
Templating already reduces setup from 130ms to 51ms; a shared repo saves at most
the remaining ~51ms, or roughly 4% of the suite, against a third of the suite
available from deduplication with no coupling risk.

**Preconditions for adopting it in a specific file:** every test uses a unique run
collection; no test queries across collections; no test performs destructive
operations.
Where those hold and profiling shows setup dominates, a shared-repo fixture
scoped to that file is a legitimate local optimization.
It should be justified by profile data rather than assumed.

## Removing the dependence on moto

Most of the S3 testing does not test S3.
It tests the not-local path, which the test-remote scheme covers directly.

The one place daf_butler depends on S3-specific behavior rather than on
remoteness is `remote_butler/server/handlers/_file_info.py:59`, which calls
`generate_presigned_get_url`.

- The **test-remote scheme** covers the datastore, formatter and cache layers:
  all 13 `isLocal` sites, `as_local` downloads, cache population, checksum
  skipping, and absolute-URI and direct-mode path handling.
- **moto** shrinks to the presigned-URL server path, one or two tests, retained as
  insurance at the single point of genuine S3 dependence.

`transfer="direct"` does not need S3.
`fileDatastore.py:1063-1100` sets `tgtLocation = None`, skips the transfer, and
stores `str(srcUri)`; the `isLocal` check at line 1081 is inside the `else`
branch, so `file://` exercises direct mode identically.
Similarly, the `testAbsoluteURITransfer*` family tests that an absolute URI round
trips without being rewritten as relative, which a `test-remote://` URI checks at
least as well as `s3://` and arguably better, since it cannot pass accidentally
by being local.

The underlying principle is that testing real S3 semantics belongs to
lsst.resources.
daf_butler needs to test only that it handles a non-local URI correctly.

Expected effect: the S3 block falls from 257s across 33 tests to roughly 20s
across 8 to 10 tests, with the marginal-coverage query confirming nothing was
lost.

## Layout and splitting

Hierarchical, by subsystem: `tests/butler/`, `tests/registry/`,
`tests/datastore/`, `tests/cli/`, `tests/queries/`.
Target no file over roughly 800 lines.

The structural payoff comes first: files that exist only to instantiate variants
stop existing.
`test_sqlite.py`, `test_postgresql.py`, `test_remote_butler.py`,
`test_query_direct_sqlite.py`, `test_query_direct_postgresql.py` and
`test_query_remote.py` are backend-instantiation shells whose content becomes
parametrization on the real test files.
The same applies to the `ButlerServerTests` classes inside `test_butler.py`:
"remote butler over a test server" becomes a client-axis value rather than a
subclass.

`tests/test_butler.py` (3752 lines) splits into `tests/butler/`:

| New file | Contents |
| --- | --- |
| `test_put_get.py` | `ButlerPutGetTests` core: put/get, composites, storage-class overrides, pytype coercion |
| `test_ingest.py` | `testIngest`, `test_ingest_zip`, `test_temporary_for_ingest` |
| `test_collections.py` | collection chain redefine/prepend/extend/remove, dataset-type caching |
| `test_import_export.py` | `testImportExport*`, `testRemoveRuns`, `testPruneDatasets` |
| `test_transfers.py` | `DatastoreTransfers`, `TransferDatasetsInPlace` |
| `test_lifecycle.py` | constructor, close, GC, pickle, stringification, `makeRepo`, transactions, dataId rewriting, metrics |
| `test_config_repo.py` | `ButlerConfigTests`, `ButlerMakeRepoOutfile*` |
| `test_null_datastore.py` | `NullDatastoreTestCase` |

`tests/test_datastore.py` (2467 lines) splits into `tests/datastore/`:
`test_file.py`, `test_constraints.py`, `test_cache.py` (the 485-line
`DatastoreCacheTestCase`), `test_records.py` (`StoredFileInfo`,
`TestDatastoreRecordTable`, `DatasetRefURIs`), `test_null.py`.

The relocated mixins split the same way.
`registry/tests/_registry.py` (4151 lines) becomes `tests/registry/`:
`test_collections.py`, `test_dataset_types.py`, `test_datasets.py`,
`test_dimensions.py`, `test_calibs.py`, `test_transactions.py`.
`registry/tests/_database.py` (1359 lines) becomes `tests/registry/test_database.py`.
`python/lsst/daf/butler/tests/butler_queries.py` (2688 lines) splits by concern
into `tests/queries/`.

## Conversion mechanics

Largely mechanical, with a codemod doing the bulk and ruff preventing
backsliding.

| From | To |
| --- | --- |
| `self.assertEqual/True/In/IsNone` | bare `assert` |
| `self.assertRaises(X)` | `pytest.raises(X)` |
| `self.assertLogs`, `self.assertWarns` | `caplog`, `pytest.warns` |
| `self.assertAlmostEqual` | `pytest.approx` |
| `self.subTest` (~40 sites) | `@pytest.mark.parametrize` |
| `setUp`, `tearDown`, `addCleanup` | fixtures with `yield` |
| `setUpClass` | module- or session-scoped fixtures |
| `@unittest.skipIf`, `@unittest.expectedFailure` | `@pytest.mark.skipif`, `xfail` |
| `unittest.IsolatedAsyncioTestCase` | `pytest-asyncio`, strict mode |

`pytest-subtests` is deliberately not adopted.
All ~40 `subTest` sites express better as `parametrize`, which also yields
independent test IDs and lets xdist distribute them.

The `TestCaseMixin` `TYPE_CHECKING` construct at
`python/lsst/daf/butler/tests/utils.py:59-69` exists only so that unittest mixins
type-check, and is deleted outright.

Ruff's `PT` ruleset (flake8-pytest-style) is added to `[tool.ruff.lint] select`.
It catches the idioms a codemod gets syntactically right but stylistically wrong.
All changes must remain ruff and mypy clean.

### The mapping file

The diff is too large to review by reading, and test count cannot serve as a
check because deduplication reduces it deliberately.

The migration therefore produces a mapping file recording, for every original
test nodeid, either its new nodeid or nodeids, or `dropped: <reason>` citing the
marginal-coverage result that justified the removal.
A reviewer checks the dropped list against the coverage evidence rather than
reading 30k lines of diff.
The coverage gate proves nothing was lost; the mapping file explains why each
removal was safe.

## Configuration and CI

`[tool.pytest.ini_options]` gains:

```toml
asyncio_mode = "strict"
timeout = 300
xfail_strict = true
markers = ["postgres", "s3", "server", "slow"]
```

The markers are for local development rather than CI, which runs everything.
`-m "not postgres and not server"` is the difference between a 20-minute and a
5-minute local iteration, which matters for anyone working on this suite.

New test dependencies in `requirements/test.in` and
`[project.optional-dependencies] test`:

- `pytest-asyncio` — required.
  `tests/test_gafaelfawr.py:45` and `tests/test_server.py:1040` are
  `unittest.IsolatedAsyncioTestCase`; once they are no longer `TestCase` there is
  no async runner.
- `pytest-timeout` — advisable.
  The postgres and server tests are the ones that hang, and a hung xdist worker
  currently consumes the whole job's budget.

`moto` and `testing.postgresql` are already present in `requirements/test.in`.

`--cov-context=test` stays out of the permanent CI command.
It is measurably slower and is needed only for the migration measurement runs.

### An independent CI change

`.github/workflows/build.yaml` carries the comment "We have two cores so we can
speed up the testing with xdist" and passes `-n 3`.
Standard `ubuntu-latest` runners now have more than two vCPUs, so `-n auto` may
be a free wall-clock reduction with no code change at all.
This should be verified against a real run rather than assumed, and landed as its
own commit so its effect is measurable separately from the reorganization.

## Migration order

Prerequisites, in other repositories, can proceed in parallel: sconsUtils
hierarchical discovery, and the lsst.resources test-remote scheme.
The shared-cache sqlite ticket is a daf_butler library change and deliberately
does not ride on this branch.

One ticket branch, a commit series, green at every step.

1. **`-n auto` alone**, isolated so its contribution is attributable.
2. **Baseline.**
   Coverage tooling script; instrumented run in the CI-mirroring venv; baseline
   `.coverage` and durations archived.
3. **Fixture plugin skeleton**, entry point, test dependencies.
   No test changes; the suite must stay green.
4. **Pattern setter: `tests/datastore/test_cache.py`.**
   The 485-line `DatastoreCacheTestCase` is self-contained with no backend axis,
   so it establishes the conventions in a small, cheaply reviewed commit before
   the large files.
5. **`test_butler.py`** — split, convert, deduplicate.
   One commit per resulting file.
6. **`test_datastore.py`** — the same.
7. **Relocate and split `registry/tests/`**, deleting the `test_sqlite.py` and
   `test_postgresql.py` instantiation shells.
8. **Relocate and split `butler_queries.py`**, deleting the three `test_query_*`
   shells.
9. **S3 onto the test-remote scheme**, moto reduced to the presigned-URL case.
10. **Bulk mechanical conversion** of the remaining simple files.
11. **Close out.**
    Gate run, mapping file complete, migration tooling deleted, CI configuration
    finalized.

Optional, pending evidence: the outliers `test_cliLog.py` (80s, 2 tests),
`test_storageClass.py::testFactoryFind` (43s) and
`test_packages.py::testPackages` (42s).
Together these are 14% of the local baseline, but all three are import-bound or
environment-scanning bound and were measured in a large shared-filesystem conda
stack.
They may be close to free on a CI runner.
This should be checked against real CI timings before any effort is spent, and no
saving should be claimed for them until it is.

## Risks

| Risk | Mitigation |
| --- | --- |
| A dropped variant loses coverage | The gate compares line *and* arc sets and must show an empty regression set |
| A test silently starts skipping | Skip count is checked separately, since the line diff may not reveal it |
| The gate is invalidated by library edits | No library changes on this branch; genuine fixes go on separate tickets |
| A faked remote path passes without exercising the download | The test-remote scheme implements a real copying `_as_local`, not just `isLocal = False` |
| The diff is unreviewable | The mapping file makes every removal auditable against coverage evidence |
| Duplication regrows later | Multiplication is opt-in and visible in the diff, rather than implicit in a base class |
| Shared fixtures leak state between tests | The shared-repo tier is not the default; adopting it requires stated preconditions and profile data |
| Hierarchy breaks Jenkins | The hierarchical layout does not merge until the sconsUtils change lands |

## Appendix: measured baseline

Method: `pytest tests/ -q -rs --durations=0`, single-threaded, run through the
EUPS stack at `lsst_distrib` tag `w_2026_33` on 2026-08-13.

Result: 20m30s (1230s), 1542 passed, 540 skipped, 4 xfailed, 1203 subtests passed,
2086 tests collected.

Caveats that limit how far these numbers should be trusted:

- This environment lacks `fastapi` and `testing.postgresql`, so 540 tests (26%)
  were skipped, including the postgres and server variants.
  Those are likely the second-largest cost after S3 and are absent from this
  baseline entirely.
- The environment is a large conda stack on a shared filesystem, which inflates
  import-bound and filesystem-bound tests relative to a CI runner.
- The authoritative baseline for the gate is the instrumented run in the
  CI-mirroring venv described above, not this one.

Repo-creation micro-benchmarks were run on local disk (`/tmp`) and are reported in
the expensive-resource fixtures section.

Time by file, top entries:

| File | Time |
| --- | --- |
| `test_butler.py` | 542.9s |
| `test_sqlite.py` | 187.9s |
| `test_cliLog.py` | 79.7s |
| `test_parquet.py` | 67.4s |
| `test_obscore.py` | 45.5s |
| `test_storageClass.py` | 43.4s |
| `test_simpleButler.py` | 42.6s |
| `test_packages.py` | 41.5s |
| `test_dimensions_versions.py` | 32.4s |
| `test_cliPluginLoader.py` | 22.3s |
| `test_datastore.py` | 17.3s |

`test_butler.py` by class:

| Class | Time | Tests |
| --- | --- | --- |
| `S3DatastoreButlerTestCase` | 256.9s | 33 |
| `ChainedDatastoreButlerTestCase` | 51.5s | 33 |
| `PosixDatastoreTransfers` | 49.6s | 14 |
| `PosixDatastoreButlerTestCase` | 48.5s | 39 |
| `ChainedDatastoreTransfers` | 46.5s | 14 |
| `ButlerExplicitRootTestCase` | 42.7s | 40 |
| `InMemoryDatastoreButlerTestCase` | 18.5s | 26 |
| `ClonedSqliteButlerTestCase` | 16.8s | 26 |

`test_sqlite.py` by class:

| Class | Time | Tests |
| --- | --- | --- |
| `ClonedSqliteFileRegistryNameKeyCollMgrUUIDTestCase` | 50.0s | 59 |
| `SqliteFileRegistryNameKeyCollMgrUUIDTestCase` | 49.0s | 59 |
| `SqliteFileRegistrySynthIntKeyCollMgrUUIDTestCase` | 47.1s | 59 |
| `SqliteMemoryRegistryAstropyIngestDateTestCase` | 13.4s | 60 |
| `SqliteMemoryRegistryNameKeyCollMgrUUIDTestCase` | 13.3s | 60 |
| `SqliteMemoryRegistrySynthIntKeyCollMgrUUIDTestCase` | 13.0s | 60 |
