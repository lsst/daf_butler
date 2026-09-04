# Pytest migration and deduplication of the butler and datastore test files

Status: draft for review
Date: 2026-08-25
Ticket: DM-55822

This supersedes the parts of `2026-08-13-test-reorganization-design.md` that
cover `tests/test_butler.py` and `tests/test_datastore.py`.
The earlier document's measurements predate DM-55824 and DM-55885 and no longer
describe the suite.

## Problem

The two files encode four independent axes as a lattice of `unittest`
subclasses.
Adding one method to a mixin such as `ButlerTests` silently multiplies it by the
number of concrete subclasses, and most of those multiplications test nothing
that the axis affects.

The goal is to remove the existing duplication and to remove the mechanism that
lets it regrow unnoticed.

## What changed since the earlier design

Two tickets have landed, and they invalidate the earlier document's
justification rather than merely improving on it.

- **DM-55824** replaced the moto-backed S3 datastore tests with a fake remote
  URI scheme.
  `S3DatastoreButlerTestCase`, which was 257s and 21% of the entire suite, no
  longer exists.
- **DM-55885** added the repo template cache in
  `python/lsst/daf/butler/tests/_repo_template_cache.py`.

The full suite fell from 1230s to 172s, and `tests/test_butler.py` from 543s to
40s.

Two consequences for this design:

- **The expensive-resource fixture design in the earlier document is moot.**
  It proposed a two-tier scheme built on copying a template repo.
  Setup across all of `tests/test_butler.py` now totals **0.11s** against 37.1s
  of call time, because `make_repo_for_test` already caches.
  There is nothing left for a fixture tier to save.
- **This is a maintainability change, not a performance change.**
  It sheds time as a side effect.
  The deduplication ceiling is roughly 37% of these two files, or about 18% of
  the whole suite before accounting for tests that the coverage evidence will
  require us to keep.

### Measured baseline

Environment: `uv sync --locked --all-extras --dev`, which is what CI installs.
Postgres, the test server, and moto are all live in it.
Measured at `ae3f97620`:

```
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run pytest \
    tests/test_butler.py tests/test_datastore.py -q -p no:randomly --durations=0

535 passed, 10 skipped, 10 xfailed, 271 subtests passed in 86.16s
```

| Class | Time | Tests | Axis coordinates |
| --- | --- | --- | --- |
| `ButlerServerPostgresTests` | 11.50s | 34 | postgres x server |
| `ButlerServerSqliteTests` | 10.45s | 32 | sqlite x server |
| `ClonedPostgresPosixDatastoreButlerTestCase` | 9.37s | 34 | postgres x cloned |
| `PostgresPosixDatastoreButlerTestCase` | 9.27s | 34 | postgres x direct |
| `PosixDatastoreTestCase` | 7.50s | 29 | datastore |
| `ChainedDatastoreTestCase` | 4.07s | 29 | datastore |
| `PosixDatastoreButlerTestCase` | 3.80s | 39 | sqlite x posix x direct |
| `ChainedDatastoreTransfers` | 3.76s | 14 | rerun of `PosixDatastoreTransfers` |
| `ButlerExplicitRootTestCase` | 3.71s | 40 | rerun of Posix for one behavior |
| `ChainedDatastoreButlerTestCase` | 3.63s | 31 | sqlite x chained x direct |
| `RemoteTestDatastoreButlerTestCase` | 3.39s | 33 | sqlite x remote-test x direct |
| `DatastoreCacheTestCase` | 2.11s | 10 | none |

The postgres and server variants are 47% of the two files.
They were absent from the earlier document's baseline because that environment
lacked `fastapi` and `testing.postgresql`.

## Goals

- Replace the subclass lattice with explicit, independently selectable fixture
  axes, so that multiplication is opt-in and visible in the diff.
- Convert both files to native pytest.
  No `unittest` survives in either: no `TestCase`, no `assertX`, no `setUp`.
  This is an acceptance criterion rather than a preference, because a leftover
  `TestCase` base silently disables `parametrize` on that class.
- Prove that library code coverage does not decrease, at line and branch
  granularity.

## Non-goals

- Changing library behavior.
  See the no-library-changes rule below.
- Improving coverage.
  Coverage must not decrease; increasing it is separate work.
- Changing what CI covers.
- The hierarchical `tests/<area>/` layout.
  Deferred; see "Layout".

## Scope

`tests/test_butler.py` and `tests/test_datastore.py` only.

`tests/test_datastore.py` is in scope despite costing only 5.8s in isolation,
because it shares the datastore axis and most of the fixture surface with
`tests/test_butler.py`.
Splitting them across two tickets would mean designing the same fixtures twice.

Out of scope, each to its own later ticket: `registry/tests/`,
`tests/butler_queries.py`, the `test_sqlite.py` / `test_postgresql.py` /
`test_query_*` instantiation shells, and the bulk mechanical conversion of the
remaining simple files.

## Key decisions

| Decision | Choice |
| --- | --- |
| Scope | `test_butler.py` and `test_datastore.py` |
| Layout | Flat, split by filename prefix |
| Fixture delivery | Shipped module, opted into via `pytest_plugins` |
| Redundancy | Aggressive, guarded by measured marginal coverage |
| Coverage gate | Covered line and arc set diff must be empty |
| Codemod | `ruff --select PT --fix --unsafe-fixes` |
| Sequencing | Convert first, deduplicate second, as two separate passes |

### Layout: flat, not hierarchical

The earlier document proposed `tests/butler/`, `tests/registry/` and so on,
gated on a sconsUtils change, on the grounds that
`scripts.BasicSConscript.tests(pyList=[])` does not discover tests in
subdirectories.

That reading looks wrong.
An empty `pyList` puts `runPythonTests` into what it calls "automated test
discovery mode", in which pytest is invoked from the repository root with no
path arguments and a list of `--ignore` options, and therefore recurses.

The real hazard is different and is not fixed upstream: under rootdir-based
imports without `__init__.py`, `tests/butler/test_collections.py` and
`tests/registry/test_collections.py` collide on module basename.

Neither risk is worth carrying on a ticket whose purpose is elsewhere, so this
branch stays flat and splits by filename prefix.
The hierarchy remains available later, on its own ticket, where the collision
question can be settled properly.

### Fixture delivery

Fixtures live in `python/lsst/daf/butler/tests/fixtures.py` and are activated by
`pytest_plugins = ["lsst.daf.butler.tests.fixtures"]` in `tests/conftest.py`.

A `pytest11` entry point was considered and rejected.
It would auto-register the plugin in every environment that has daf_butler
installed, so any pytest run anywhere would import these fixtures at startup and
a bug in them would break unrelated test collection.
Opt-in via `pytest_plugins` gives downstream packages the same working `butler`
fixture with none of that.

The distinction that produced the current problem is between shipping test
*suites* that get subclassed and shipping test *fixtures* that get composed.
The first is the cause of the inheritance explosion.
The second is what `lsst.daf.butler.tests` already does well.

### Rejected: `-n auto` in CI

`.github/workflows/build.yaml` passes `-n 3` with a comment about having two
cores.
Raising it to `-n auto` was proposed on the theory that runners now have more
vCPUs.

Measured on GitHub Actions: `-n auto` selected 2 workers and was **slower** than
`-n 3`.
`-n 3` stays.
This is recorded so the idea is not retried.

## Fixture architecture

### Why the subclasses exist

The lattice is held together by class attributes, not by behavior:
`configFile`, `fullConfigKey`, `validationCanFail`, `datastoreStr`,
`datastoreName`, `registryStr`, `predictionSupported`, `useTempRoot`.
Any parametrization scheme has to replace those first or it just relocates the
problem.

Most of them are datastore-shaped and become a frozen dataclass keyed by
datastore type.
The two that are not, `registryStr` and `useTempRoot`, belong to the registry
and repo-layout axes respectively and are supplied by those fixtures.

The datastore dataclass:

```python
@dataclasses.dataclass(frozen=True)
class DatastoreProfile:
    """Everything that varies between datastore configurations."""

    config_file: str
    full_config_key: str | None
    validation_can_fail: bool
    datastore_str: list[str]
    datastore_name: list[str] | None
    prediction_supported: bool = True


DATASTORE_PROFILES: dict[str, DatastoreProfile] = {...}
```

### Axes

| Fixture | Default | Other values |
| --- | --- | --- |
| `registry_backend` | `sqlite` | `postgres` |
| `datastore_type` | `posix` | `in_memory`, `chained`, `remote_test`, `null` |
| `butler_client` | `direct` | `cloned`, `server` |
| `repo_layout` | `in_repo` | `explicit_root`, `outfile`, `outfile_dir`, `outfile_uri` |

Each is an independently overridable fixture defaulting to one value, using
optional indirect parametrization:

```python
@pytest.fixture
def datastore_type(request) -> str:
    return getattr(request, "param", "posix")
```

Two derived fixtures compose them: `butler_config`, which builds the repo and
returns the config URI, and `butler`, which opens the requested client kind.

A test written plainly runs once, against the default combination.
A test that the marginal-coverage query shows to be axis-sensitive opts in, and
the multiplication is visible in the diff:

```python
@pytest.mark.parametrize("registry_backend", ["sqlite", "postgres"], indirect=True)
def test_ingest_date_handling(butler): ...
```

Two properties motivate this over a plainly parametrized fixture.
The default costs nothing to write, so duplication cannot silently regrow.
And axes compose without a class explosion: `ButlerExplicitRootTestCase`
currently reruns 40 tests to check one behavior, and as a fixture axis it is one
parametrized test.

`ButlerServerTests` already overrides eight methods to opt out of the server
axis.
That is the inheritance-shaped version of what this design builds, and it
suggests the server variant's true marginal set is small.

## File split

Flat, prefix-named.
Target no file over roughly 800 lines; the split below averages about 460.

`tests/test_butler.py`, 3682 lines:

| New file | Contents |
| --- | --- |
| `test_butler_put_get.py` | basic put/get, composite concrete and virtual, storage-class override get, `ComponentFromOverriddenStorageClass` and `...Warns`, pytype coercion, deferred collection passing |
| `test_butler_ingest.py` | `testIngest`, `test_ingest_zip`, `test_temporary_for_ingest`, `test_specialized_file_datasets_functions` |
| `test_butler_collections.py` | collection chain redefine, prepend, extend, remove; `testGetDatasetCollectionCaching`; `testGetDatasetTypes` |
| `test_butler_import_export.py` | `testImportExport`, `testImportExportVirtualComposite`, `testRemoveRuns`, `testPruneDatasets`, `testExportTransferCopy` |
| `test_butler_transfers.py` | `DatastoreTransfers`, `TransferDatasetsInPlace`, `test_transfer_dimension_records_from` |
| `test_butler_lifecycle.py` | constructor, path constructor, close, garbage collection, pickle, stringification, transaction, dataId rewriting, metrics, provenance, `testDafButlerRepositories` |
| `test_butler_config_repo.py` | `ButlerConfigTests`, `testMakeRepo`, `ButlerMakeRepoOutfile*`, `testFileLocations`, `testPutTemplates` |
| `test_butler_null_datastore.py` | `NullDatastoreTestCase` |

`tests/test_datastore.py`, 2467 lines:

| New file | Contents |
| --- | --- |
| `test_datastore_file.py` | `DatastoreTests` core, no-checksums, trash, cleanup |
| `test_datastore_constraints.py` | `DatastoreConstraintsTests`, per-store constraints |
| `test_datastore_cache.py` | `DatastoreCacheTestCase` |
| `test_datastore_records.py` | `DatasetRefURIsTestCase`, `StoredFileInfoTestCase`, `TestDatastoreRecordTable` |
| `test_datastore_null.py` | `NullDatastoreTestCase` |

## Deduplication targets

Every class below is a pure axis rerun.
None is dropped on judgement.
Each drop is proposed, then justified or refused by the marginal-coverage query.

| Current class | Executions | Time | Becomes |
| --- | --- | --- | --- |
| `ButlerServerPostgresTests` | 34 | 11.50s | `registry_backend="postgres"` opt-in on server tests with marginal coverage |
| `ClonedPostgresPosixDatastoreButlerTestCase` | 34 | 9.37s | `butler_client="cloned"` on tests with marginal coverage |
| `ButlerExplicitRootTestCase` | 40 | 3.71s | `repo_layout="explicit_root"` on `testFileLocations` plus any marginal |
| `ChainedDatastoreTransfers` | 14 | 3.76s | `datastore_type="chained"` opt-in on marginal transfer tests |
| `ClonedSqliteButlerTestCase` | 22 | 1.29s | folded into the same `cloned` axis |
| `TrashDatastoreTestCase` | 31 | 0.53s | trash-specific tests only |
| `PosixDatastoreNoChecksumsTestCase` | 31 | 0.51s | checksum axis opt-in |
| `ButlerMakeRepoOutfileDirTestCase`, `...UriTestCase` | 8 | 0.71s | `repo_layout` parametrization on `testConfigExistence` |
| `ChainedDatastoreConstraintsTestCase`, `...NativeTestCase` | 4 | 0.19s | constraint parametrization |

That is 218 of 535 executions and 31.6s of 86.16s, which is the ceiling if every
marginal set comes back empty.
It will not.
The postgres axis in particular exercises genuinely different SQL, so several
tests are expected to survive as explicit opt-ins.

## Coverage measurement and the gate

The coverage requirement is an input to the design, not a check applied
afterwards.
Which variants may be dropped is decided from measured data.

### One instrumented baseline run

Taken against `ae3f97620` in the CI-mirroring environment:

```
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run pytest tests/ -p no:randomly \
    --cov=lsst.daf.butler --cov-branch --cov-context=test
```

The resulting `.coverage` database records which test covered which line and
arc, and answers both questions:

- **Marginal coverage per variant.**
  For a set of test contexts, the lines and arcs it covers that no other test
  covers.
  An empty marginal set means the variant is pure duplication and can be dropped
  wholesale.
  A non-empty set is attributed back to individual contexts, and only those
  tests stay parametrized over that axis.
- **The before-and-after gate.**
  The covered line and arc sets, such that after conversion `baseline minus new`
  must be empty.
  Arcs matter as much as lines, because a dropped variant shows up in branch
  coverage first.

### Scope of the measured set

Library code only: `lsst.daf.butler` excluding `lsst/daf/butler/tests/` and
`lsst/daf/butler/registry/tests/`.

This branch adds `fixtures.py` under the first of those, and test-support
coverage is not comparable across a rewrite that renames every file.
`--cov=tests` stays in CI for reporting continuity but is not part of the gate.

### Rules the gate imposes

**No library changes on this branch.**
Comparing covered line sets is only meaningful if `python/lsst/daf/butler/`
source line numbers are stable.
Any genuine bug found during the migration goes on a separate ticket.
Without this rule the gate cannot fail, and so proves nothing.

The single exception is the new `lsst/daf/butler/tests/fixtures.py`, which is
outside the measured set by construction.

**Skip count must not increase.**
A test that silently starts skipping usually appears in the line diff, but not
if another test happens to cover the same lines.
Silent skipping is a failure mode this migration is particularly prone to, so it
gets its own cheap check.

### Tooling

`tests/_migration/coverage_tool.py`, roughly 100 lines, querying the `coverage`
database directly, offering three operations:

- `marginal <context-pattern>` — lines and arcs unique to a set of test
  contexts.
  Drives every drop decision.
- `gate <baseline.coverage> <new.coverage>` — the covered-to-uncovered set,
  which must be empty.
- `skips <baseline-report> <new-report>` — skip count, which must not increase.

It must be re-runnable at every commit in the series.
It is deleted in the closing commit.

### What is and is not committed

The baseline `.coverage` database is not committed.
With per-test contexts over roughly 2100 tests it is large and binary.
It lives at a path documented in the ticket, outside the repository.

What is committed is the tool, the mapping file, and a small
`tests/_migration/baseline_summary.json` holding per-file covered line and arc
counts, as a cheap tripwire that the out-of-repository database is still the
right one.

### Subtests blur marginal attribution

These two files run 271 subtest executions, and coverage contexts under
`subTest` attribute to the parent test rather than to the subtest.

This does not, however, require converting them before the baseline, and cannot.

`pytest.mark.parametrize` does not work on a `unittest.TestCase` method: it
collects a single case and fails with `TypeError: missing 1 required positional
argument`.
All six convertible sites live in mixins that combine into `TestCase`
subclasses, so they can only be converted once those classes are gone.

The ordering turns out not to matter.
Every marginal-coverage query runs against the **post-conversion** database, by
which point every test is a plain function and each case is its own context.
The baseline database feeds only the gate, which compares global covered line
and arc sets — a quantity subtests do not affect.

The six sites are therefore converted as part of the split that removes their
`TestCase` base, and the remaining two, being loops within a single test, need
no treatment at all.
The global gate is unaffected either way; this is only about attribution
granularity for the `marginal` query.

### Measurement environment

`uv sync --locked --all-extras --dev`, matching what
`.github/workflows/build.yaml` installs.

`PYTHONPATH` and `DYLD_LIBRARY_PATH` must be cleared when invoking `uv run`.
Otherwise a configured EUPS stack leaks in, and the installed daf_butler and
sphgeom shadow the virtual environment.

The shared EUPS stack is unsuitable as a measurement environment: it lacks
`fastapi` and `testing.postgresql`, which skips the postgres and server
variants — precisely the variants whose marginal coverage matters most here.

## Conversion mechanics

### Ruff is the codemod

Verified against a copy of `tests/test_datastore.py`:

```
ruff check --select PT --fix --unsafe-fixes   ->  350 errors, 345 fixed, 5 remaining
pytest on the result                          ->  186 passed, 8 skipped, 104 subtests
```

It rewrites `self.assertEqual(a, b)` to `assert a == b` and `self.assertRaises`
to `pytest.raises`, and adds the `import pytest`.
No hand-written codemod is needed.

### The autofix leftovers are fixed, not ignored

Counted after running the autofix over copies of both files:

| Rule | In scope | Repo-wide |
| --- | --- | --- |
| `PT011`, too-broad `pytest.raises` | 14 | 117 |
| `PT012`, multi-statement `raises` block | 4 | 7 |
| `PT027`, `assertRaises` the fix cannot reach | 3 | 5 |

Eighteen sites of `PT011` and `PT012` in the two files in scope is small enough
to fix on this ticket, so neither rule is added to `ignore`.
`PT011` sites gain a `match=` argument, which is a genuine improvement: a bare
`pytest.raises(ValueError)` passes on any `ValueError`, including one raised for
the wrong reason.
`PT012` sites have the non-asserting statements lifted out of the `with` block.

The three `PT027` sites have to be converted by hand regardless, since the
autofix leaves them as `self.assertRaises` in a file that no longer has a
`TestCase`.

The remaining repo-wide balance, roughly 103 `PT011` and 3 `PT012` in the other
71 files, is held by the ratchet below and falls to whichever ticket converts
each file.

### Remaining manual surface

Counted across both files:

| From | To | Sites |
| --- | --- | --- |
| `self.assert*` | bare `assert` | 814, automated |
| `self.assertRaises` | `pytest.raises` | 140, of which 137 automated |
| `self.subTest` | `@pytest.mark.parametrize` | 6 of 8; 2 stay loops |
| `self.assertLogs` | `caplog` | 10 |
| `enterContext` | fixtures with `yield` | 25 |
| `setUp`, `tearDown` | fixtures with `yield` | per class |
| `setUpClass` | module-scoped fixtures | 7 |
| `@unittest.expectedFailure` | `@pytest.mark.xfail` | 2 |

Three scope reductions against the earlier document, from counting rather than
estimating:

- **`pytest-asyncio` is not needed on this ticket.**
  Neither file contains an `IsolatedAsyncioTestCase`; those are in
  `tests/test_gafaelfawr.py` and `tests/test_server.py`, which are out of scope.
  The dependency moves to whichever later ticket takes them.
- **Eight `subTest` sites, not roughly 40.**
  The larger figure was suite-wide.
- **No `assertWarns`, no `assertAlmostEqual`, no `addCleanup` in either file.**

`pytest-subtests` is deliberately not adopted.

Six of the eight sites express better as `parametrize`, which yields independent
test IDs and lets xdist distribute them.
The two that do not are loops over data produced at runtime:
`tests/test_butler.py:322` sits inside `runPutGetTest`, a helper called by many
tests rather than a test itself, and `tests/test_butler.py:2039` iterates over
datasets the test has just created.
Neither has a parameter list that exists at collection time.
Both keep their loop and drop the `subTest` wrapper, with the iteration identity
moved into the assertion message.

This costs per-iteration isolation in those two loops: the first failure now
ends the loop instead of reporting every failing case.
That is accepted, because both loops are inside a single test either way, so
neither was contributing separable coverage contexts to begin with.

### The ruff ratchet

`PT` is added to `[tool.ruff.lint] select` in full, with no rules in `ignore`,
and `per-file-ignores` entries disabling it for the 71 test files not yet
converted.
Each later ticket deletes entries, and in doing so takes on that file's share of
the repo-wide `PT011` and `PT012` balance.

This is 71 lines of `pyproject.toml`, shrinking over time.
The alternative, deferring `PT` until the closing ticket, leaves the newly
written files unguarded during the very ticket that writes them, which defeats
the purpose.

All changes must remain ruff and mypy clean.

### Configuration

`[tool.pytest.ini_options]` gains:

```toml
xfail_strict = true
markers = ["postgres", "server", "slow"]
```

The markers are for local development rather than CI, which runs everything.
`-m "not postgres and not server"` is the difference between an 86-second and a
45-second iteration on these two files.

`pytest-timeout` is added to the `dev` dependency group with `timeout = 300`.
The postgres and server tests are the ones that hang, and a hung xdist worker
currently consumes the whole job's budget.

`--cov-context=test` stays out of the permanent CI command.
It is measurably slower and is needed only for the migration measurement runs.

## Migration order

One ticket branch, a commit series, green at every step.

1. **Baseline.**
   `tests/_migration/coverage_tool.py`, the instrumented run,
   `baseline_summary.json`, and an empty mapping file.
2. **Fixture module and conftest.**
   `python/lsst/daf/butler/tests/fixtures.py` and `tests/conftest.py` with
   `pytest_plugins`.
   Nothing consumes them yet; the suite stays green.
3. **Ruff `PT` ratchet** and the `pyproject.toml` configuration above.
4. **Pattern setter: `test_datastore_cache.py`.**
   485 lines, 10 tests, no backend axis, self-contained.
   It establishes the conventions in a commit that is cheap to review, before
   the large files.
5. **Split and convert `tests/test_butler.py`**, one commit per resulting file.
6. **Split and convert `tests/test_datastore.py`**, one commit per resulting
   file.
7. **Deduplicate**, one commit per axis, each commit message citing the
   `marginal` output that justifies it.
8. **Close out.**
   Gate run, mapping file complete, `tests/_migration/` deleted, configuration
   finalized.

### Convert first, deduplicate second

Steps 5 and 6 preserve every test execution.
Step 7 performs every deletion.

This buys a property worth the extra commits: at the end of step 6 the gate must
show an empty diff **and** an unchanged test count, which proves the conversion
was faithful on its own terms.
Every deletion then lands in step 7, isolated, one axis per commit, each
traceable to its evidence.
If the gate fails, the two-pass shape says immediately whether the fault is in
the conversion or in a deletion.

### The mapping file

The diff is too large to review by reading, and test count cannot serve as a
check because deduplication reduces it deliberately.

`tests/_migration/mapping.md` records, for every original test nodeid, either
its new nodeid or nodeids, or `dropped: <axis> — marginal set empty (N lines,
N arcs checked)`.

A reviewer checks the dropped list against the coverage evidence rather than
reading a very large diff.
The coverage gate proves nothing was lost; the mapping file explains why each
removal was safe.
It is pasted into the ticket and deleted in the closing commit.

## Risks

| Risk | Mitigation |
| --- | --- |
| A dropped variant loses coverage | The gate compares line *and* arc sets and must show an empty regression set |
| A test silently starts skipping | Skip count is checked separately, since the line diff may not reveal it |
| The gate is invalidated by library edits | No library changes on this branch; genuine fixes go on separate tickets |
| The conversion itself loses a test | Convert and deduplicate are separate passes; after the conversion pass, test count must be unchanged |
| Subtest contexts hide marginal coverage | Marginal queries run against the post-conversion database, where every case is its own context; the baseline feeds only the gate, which subtests do not affect |
| The diff is unreviewable | The mapping file makes every removal auditable against coverage evidence |
| Duplication regrows later | Multiplication is opt-in and visible in the diff, rather than implicit in a base class |
| The postgres axis is dropped too aggressively | Postgres exercises different SQL; its marginal set is expected to be non-empty, and drops are refused where it is |
| Measurement environment drifts from CI | The environment is `uv sync --locked --all-extras --dev`, the same command CI runs |
