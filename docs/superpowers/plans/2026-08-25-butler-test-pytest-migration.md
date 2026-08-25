# Butler Test Pytest Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `unittest` subclass lattice in `tests/test_butler.py` and `tests/test_datastore.py` with native pytest fixture axes, and delete the duplicate test executions the lattice produces, without losing any library coverage. The end state contains **no `unittest` at all** in the files these tasks produce — no `TestCase`, no `assertX`, no `setUp`. That is a hard acceptance criterion, checked in Tasks 12, 17 and 23, not merely a stylistic preference.

**Architecture:** Four independently selectable fixture axes (registry backend, datastore type, butler client, repo layout) replace roughly fourteen concrete `TestCase` subclasses. Each axis fixture defaults to one value, so a test written plainly runs once and multiplication is opt-in and visible in the diff. Conversion and deduplication are two separate passes: after the conversion pass the test count must be unchanged, which proves the conversion was faithful on its own terms; every deletion then lands in the deduplication pass, one axis per commit, each justified by a marginal-coverage query.

**Tech Stack:** pytest, pytest-cov (coverage contexts), ruff (`flake8-pytest-style` as the codemod), uv, `testing.postgresql`, `lsst.daf.butler.tests` helpers (`make_repo_for_test`, `create_test_server`, `setup_postgres_test_db`).

**Spec:** `docs/superpowers/specs/2026-08-25-butler-test-pytest-migration-design.md`

## Global Constraints

- **Ticket branch:** `tickets/DM-55822`. Never push to a remote.
- **No library changes.** Nothing under `python/lsst/daf/butler/` may change except the new `python/lsst/daf/butler/tests/fixtures.py`. Source line numbers must stay stable or the coverage gate is meaningless. Any genuine bug found goes on a separate ticket, recorded in `tests/_migration/mapping.md` and not fixed here.
- **Green at every commit.** Every task ends with a passing run of the files it touched.
- **No `unittest` in produced files.** Every file a task creates must pass `rg -c "unittest|self\.assert" <file>` with no matches. `parametrize` silently breaks on `unittest.TestCase` methods — it collects one case and raises `TypeError: missing 1 required positional argument` — so a leftover `TestCase` base is not cosmetic, it disables parametrization.
- **Ruff and mypy clean.** The repo has a pre-commit hook that runs `ruff check` and `ruff format`; it will reject a commit and modify files. Re-`git add` and re-commit when it does.
- **Test environment.** Always `env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev ...`. Both parts are load-bearing:
  - Clearing `PYTHONPATH`/`DYLD_LIBRARY_PATH` stops a configured EUPS stack shadowing the venv; without it collection dies on `dlopen ... libsphgeom.dylib`.
  - `--all-extras --dev` on **every** `uv run`, not just the initial `uv sync`. A bare `uv run` re-syncs the environment to the default dependency groups and silently *uninstalls* the extras, taking `fastapi` with it. The symptom is not an error but 231 spurious test errors and a much larger skip count — a baseline taken that way understates covered lines and makes the gate trivially passable. This was hit for real on the first baseline attempt.
- **Line length 110**, `target-version = "py311"`, numpydoc docstring convention. American English in prose. One sentence per line in Markdown.
- **Coverage measured set:** `lsst.daf.butler` excluding `lsst/daf/butler/tests/` and `lsst/daf/butler/registry/tests/`.
- **Baseline commit:** `ae3f97620`. Baseline runtime for the two files in scope: 535 passed, 10 skipped, 10 xfailed, 271 subtests, 86.16s.

---

## File Structure

**Created:**

| Path | Responsibility |
| --- | --- |
| `python/lsst/daf/butler/tests/fixtures.py` | The four axis fixtures, `DatastoreProfile`, the `ButlerHarness` hierarchy. Shipped, opted into by `pytest_plugins`. |
| `tests/conftest.py` | Activates the plugin; supplies `test_directory`, which the shipped module cannot know. |
| `tests/_migration/coverage_tool.py` | `marginal`, `gate`, `skips`. Deleted in the final task. |
| `tests/_migration/mapping.md` | Original nodeid to new nodeid(s), or `dropped:` with evidence. Deleted in the final task. |
| `tests/_migration/baseline_summary.json` | Per-file covered line and arc counts. Deleted in the final task. |
| `tests/test_butler_*.py` (8 files) | The split of `tests/test_butler.py`. |
| `tests/test_datastore_*.py` (5 files) | The split of `tests/test_datastore.py`. |
| `doc/changes/DM-55822.misc.md` | Changelog fragment. |

**Deleted:** `tests/test_butler.py`, `tests/test_datastore.py`.

**Modified:** `pyproject.toml` (ruff `PT` select plus per-file-ignores ratchet, pytest markers, `pytest-timeout`).

---

## Task 1: Coverage tooling and the instrumented baseline

**Files:**
- Create: `tests/_migration/coverage_tool.py`
- Create: `tests/_migration/mapping.md`
- Create: `tests/_migration/baseline_summary.json`
- Create: `tests/_migration/README.md`

**Interfaces:**
- Produces: `coverage_tool.py` with three subcommands, invoked as `uv run --all-extras --dev python tests/_migration/coverage_tool.py <subcommand> ...`:
  - `marginal <coverage-db> <context-pattern>` — prints the count of lines and arcs covered only by contexts matching the SQL `LIKE` pattern, then the first 50 of each as `path:line` / `path:from->to`.
  - `gate <baseline-db> <new-db>` — prints counts and exits non-zero if any line or arc covered in baseline is uncovered in new.
  - `summary <coverage-db> <out.json>` — writes per-file covered line and arc counts.

- [ ] **Step 1: Write the tool**

The `coverage` package writes a SQLite database. The relevant schema is `file(id, path)`, `context(id, context)`, `line_bits(file_id, context_id, numbits)` and `arc(file_id, context_id, fromno, tono)`. `line_bits.numbits` is a packed bitmap; `coverage.numbits.numbits_to_nums` unpacks it.

```python
# This file is migration scaffolding for DM-55822 and is deleted before merge.
"""Query per-test coverage contexts to justify test deduplication."""

from __future__ import annotations

import json
import sqlite3
import sys
from collections import defaultdict

from coverage.numbits import numbits_to_nums

EXCLUDED = ("/lsst/daf/butler/tests/", "/lsst/daf/butler/registry/tests/")


def _included(path: str) -> bool:
    return "/lsst/daf/butler/" in path and not any(e in path for e in EXCLUDED)


def _load(db_path: str) -> tuple[dict[int, set[tuple[str, int]]], dict[int, set[tuple[str, int, int]]], dict[int, str]]:
    """Return per-context line and arc sets, plus the context id to name map."""
    conn = sqlite3.connect(db_path)
    files = {i: p for i, p in conn.execute("select id, path from file") if _included(p)}
    contexts = dict(conn.execute("select id, context from context"))

    lines: dict[int, set[tuple[str, int]]] = defaultdict(set)
    for file_id, ctx_id, numbits in conn.execute("select file_id, context_id, numbits from line_bits"):
        if file_id in files:
            lines[ctx_id].update((files[file_id], n) for n in numbits_to_nums(numbits))

    arcs: dict[int, set[tuple[str, int, int]]] = defaultdict(set)
    for file_id, ctx_id, fromno, tono in conn.execute("select file_id, context_id, fromno, tono from arc"):
        if file_id in files:
            arcs[ctx_id].add((files[file_id], fromno, tono))

    conn.close()
    return lines, arcs, contexts


def marginal(db_path: str, pattern: str) -> int:
    """Print lines and arcs covered only by contexts matching ``pattern``."""
    lines, arcs, contexts = _load(db_path)
    matched = {i for i, name in contexts.items() if _matches(name, pattern)}
    if not matched:
        print(f"NO CONTEXTS matched {pattern!r} -- check the pattern", file=sys.stderr)
        return 2
    others = set(contexts) - matched

    for label, table in (("lines", lines), ("arcs", arcs)):
        mine: set = set().union(*(table[i] for i in matched)) if matched else set()
        theirs: set = set().union(*(table[i] for i in others)) if others else set()
        unique = mine - theirs
        print(f"{label}: {len(matched)} contexts, {len(mine)} covered, {len(unique)} unique")
        for item in sorted(unique)[:50]:
            print("   ", item)
    return 0


def _matches(name: str, pattern: str) -> bool:
    return pattern.strip("%") in name


def gate(baseline_db: str, new_db: str) -> int:
    """Exit non-zero if anything covered in baseline is uncovered in new."""
    b_lines, b_arcs, _ = _load(baseline_db)
    n_lines, n_arcs, _ = _load(new_db)

    status = 0
    for label, before, after in (("lines", b_lines, n_lines), ("arcs", b_arcs, n_arcs)):
        b_all: set = set().union(*before.values()) if before else set()
        n_all: set = set().union(*after.values()) if after else set()
        lost = b_all - n_all
        print(f"{label}: baseline {len(b_all)}, new {len(n_all)}, lost {len(lost)}")
        for item in sorted(lost)[:50]:
            print("    LOST", item)
        if lost:
            status = 1
    return status


def summary(db_path: str, out_path: str) -> int:
    """Write per-file covered line and arc counts as a tripwire."""
    lines, arcs, _ = _load(db_path)
    per_file: dict[str, dict[str, int]] = defaultdict(lambda: {"lines": 0, "arcs": 0})
    for path, _ in set().union(*lines.values()) if lines else set():
        per_file[path]["lines"] += 1
    for path, _, _ in set().union(*arcs.values()) if arcs else set():
        per_file[path]["arcs"] += 1
    with open(out_path, "w") as fh:
        json.dump(dict(sorted(per_file.items())), fh, indent=2, sort_keys=True)
    print(f"wrote {out_path} covering {len(per_file)} files")
    return 0


if __name__ == "__main__":
    cmd, *rest = sys.argv[1:]
    sys.exit({"marginal": marginal, "gate": gate, "summary": summary}[cmd](*rest))
```

- [ ] **Step 2: Verify the tool runs against a cheap database first**

Do not spend twenty minutes on the full run only to find a schema mistake.

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_datastore.py \
  -p no:randomly --cov=lsst.daf.butler --cov-branch --cov-context=test \
  --cov-report= -q
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev python tests/_migration/coverage_tool.py \
  marginal .coverage "DatastoreCacheTestCase"
```

Expected: a non-zero context count and non-zero covered-line count. If it prints `NO CONTEXTS matched`, the context naming differs from the assumption — inspect with `sqlite3 .coverage "select context from context limit 20"` and fix `_matches` before continuing.

- [ ] **Step 3: Take the full instrumented baseline**

This takes several minutes. `-p no:randomly` matters: a stable order makes context names reproducible.

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/ \
  -p no:randomly --cov=lsst.daf.butler --cov-branch --cov-context=test \
  --cov-report= -q -rs > /tmp/dm55822-baseline.txt 2>&1
tail -3 /tmp/dm55822-baseline.txt
mkdir -p ~/dm55822
cp .coverage ~/dm55822/baseline.coverage
cp /tmp/dm55822-baseline.txt ~/dm55822/baseline-report.txt
```

The database is **not** committed; it is large and binary. Record the absolute path and the skip count in `tests/_migration/README.md`.

- [ ] **Step 4: Generate the committed tripwire**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev python tests/_migration/coverage_tool.py \
  summary ~/dm55822/baseline.coverage tests/_migration/baseline_summary.json
```

- [ ] **Step 5: Write `tests/_migration/README.md` and the empty mapping file**

`README.md` records: the baseline commit (`ae3f97620`, unmodified — this is the first task that changes anything), the absolute path of the out-of-repository database, the baseline pass/skip/xfail counts from Step 3, and the three tool invocations. State plainly that the whole directory is deleted before merge.

`mapping.md` starts as a table header:

```markdown
# DM-55822 test mapping

| Original nodeid | New nodeid(s) or disposition |
| --- | --- |
```

- [ ] **Step 6: Commit**

```bash
git add tests/_migration/
git commit -m "Add coverage tooling and baseline for DM-55822

Migration scaffolding, deleted before merge. The instrumented .coverage
database lives outside the repository because it is large and binary;
baseline_summary.json is the committed tripwire that it is still the
right one."
```

---

## Task 2: The fixture module and conftest

Nothing consumes these yet. The suite must stay green, which at this point means unchanged.

**Files:**
- Create: `python/lsst/daf/butler/tests/fixtures.py`
- Create: `tests/conftest.py`

**Interfaces:**
- Produces, and every later task consumes:
  - `DatastoreProfile` — frozen dataclass with fields `config_file: str`, `full_config_key: str | None`, `validation_can_fail: bool`, `datastore_str: list[str]`, `datastore_name: list[str] | None`.
  - `DATASTORE_PROFILES: dict[str, DatastoreProfile]` keyed by `"posix"`, `"in_memory"`, `"chained"`, `"remote_test"`.
  - `ButlerHarness` with attributes `butler`, `profile`, `config_file: str`, `root: str`, `default_run: str`, `storage_class_factory: StorageClassFactory`, `registry_str: str`, `prediction_supported: bool`, `trust_mode_supported: bool`; and methods `create_empty_butler(run=None, writeable=None, metrics=None, cleanup=True) -> Butler`, `create_butler(run, storage_class, dataset_type_name, metrics=None) -> tuple[Butler, DatasetType]`, `are_uris_equivalent(uri1, uri2) -> bool`, `remove_dataset_out_of_band(butler, ref) -> None`.
  - `ClonedButlerHarness(ButlerHarness)` and `ServerButlerHarness(ButlerHarness)`.
  - `TestRepo` — dataclass with `config_file: str`, `root: str`, `profile: DatastoreProfile`, `dir1: str | None`, `dir2: str | None`. Some layouts override the profile (`explicit_root` clears `full_config_key`; `remote_test` computes `datastore_str`/`datastore_name` from the generated URI), so **always read the profile off the repo or harness, never out of `DATASTORE_PROFILES` directly**.
  - `add_dataset_type(dataset_type_name, dimensions, storage_class, registry) -> DatasetType` — was `ButlerPutGetTests.addDatasetType`.
  - `DEFAULT_RUN` — the `ingésτ😺` run name, was `ButlerPutGetTests.default_run`.
  - Fixtures: `registry_backend`, `datastore_type`, `butler_client`, `repo_layout` (all `str`, all overridable by indirect parametrize), `storage_class_factory` (session), `postgres_instance` (session), `butler_repo` (`TestRepo` — **not** `butler_config`; it must carry the layout dirs and the effective profile, which a bare path cannot), `butler_harness` (`ButlerHarness`), `butler` (`Butler`, an empty Butler opened on `DEFAULT_RUN`), and `test_directory` (`str`, supplied by `tests/conftest.py`).

- [ ] **Step 1: Write the profile table**

The values come verbatim from the class attributes being replaced. `BUTLER_ROOT_TAG` is already importable from `lsst.daf.butler`.

```python
@dataclasses.dataclass(frozen=True)
class DatastoreProfile:
    """Everything that varies between datastore configurations."""

    config_file: str
    """Path relative to the tests directory, of the butler config to use."""

    full_config_key: str | None
    """Key expected in the full config but not the limited one, or `None` if
    the configuration has no such key."""

    validation_can_fail: bool
    """Whether ``validateConfiguration`` can fail for this datastore."""

    datastore_str: list[str]
    """Fragments expected in the datastore's string representation."""

    datastore_name: list[str] | None
    """Expected datastore names, or `None` if they are computed per test."""


DATASTORE_PROFILES: dict[str, DatastoreProfile] = {
    "posix": DatastoreProfile(
        config_file="config/basic/butler.yaml",
        full_config_key=".datastore.formatters",
        validation_can_fail=True,
        datastore_str=["/tmp"],
        datastore_name=[f"FileDatastore@{BUTLER_ROOT_TAG}"],
    ),
    "in_memory": DatastoreProfile(
        config_file="config/basic/butler-inmemory.yaml",
        full_config_key=None,
        validation_can_fail=False,
        datastore_str=["datastore='InMemory"],
        datastore_name=["InMemoryDatastore@"],
    ),
    "chained": DatastoreProfile(
        config_file="config/basic/butler-chained.yaml",
        full_config_key=".datastore.datastores.1.formatters",
        validation_can_fail=True,
        datastore_str=["datastore='InMemory", "/FileDatastore_1/,", "/FileDatastore_2/'"],
        datastore_name=[
            "InMemoryDatastore@",
            f"FileDatastore@{BUTLER_ROOT_TAG}/FileDatastore_1",
            "SecondDatastore",
        ],
    ),
    "remote_test": DatastoreProfile(
        config_file="config/basic/butler-remotetest-store.yaml",
        full_config_key=None,
        validation_can_fail=True,
        datastore_str=[],  # computed per test from the generated root URI
        datastore_name=None,
    ),
}
```

- [ ] **Step 2: Write the four axis fixtures**

```python
@pytest.fixture
def registry_backend(request: pytest.FixtureRequest) -> str:
    """Registry backend for this test: ``sqlite`` or ``postgres``."""
    return getattr(request, "param", "sqlite")


@pytest.fixture
def datastore_type(request: pytest.FixtureRequest) -> str:
    """Datastore configuration: a key of `DATASTORE_PROFILES`."""
    return getattr(request, "param", "posix")


@pytest.fixture
def butler_client(request: pytest.FixtureRequest) -> str:
    """Butler client: ``direct``, ``cloned`` or ``server``."""
    return getattr(request, "param", "direct")


@pytest.fixture
def repo_layout(request: pytest.FixtureRequest) -> str:
    """Where the config sits relative to the repo root: ``in_repo``,
    ``explicit_root``, ``outfile``, ``outfile_dir`` or ``outfile_uri``."""
    return getattr(request, "param", "in_repo")
```

The `getattr(request, "param", default)` idiom is the whole design: a test that does not parametrize gets the default and runs once.

- [ ] **Step 3: Write the harness hierarchy**

Port the bodies verbatim from the classes named in each comment. Do not improve them; a behavior change here invalidates the gate.

```python
class ButlerHarness:
    """A Butler plus the client-specific hooks tests need."""

    def __init__(self, butler, profile, config_file, root, default_run,
                 storage_class_factory, registry_str, exit_stack):
        ...

    prediction_supported = True
    trust_mode_supported = True

    def create_empty_butler(self, run=None, writeable=None, metrics=None, cleanup=True) -> Butler:
        # Port verbatim from tests/test_butler.py:215-229
        # (ButlerPutGetTests.create_empty_butler), replacing self.tmpConfigFile
        # with self.config_file and self.enterContext with self._exit_stack.
        ...

    def create_butler(self, run, storage_class, dataset_type_name, metrics=None):
        # Port verbatim from tests/test_butler.py:231-287
        # (ButlerPutGetTests.create_butler).
        ...

    def are_uris_equivalent(self, uri1: ResourcePath, uri2: ResourcePath) -> bool:
        # Port verbatim from tests/test_butler.py:640-645
        # (ButlerTests.are_uris_equivalent).
        return uri1 == uri2

    def remove_dataset_out_of_band(self, butler: Butler, ref: DatasetRef) -> None:
        # Port verbatim from tests/test_butler.py:2108-2115
        # (FileDatastoreButlerTests.remove_dataset_out_of_band).
        ...


class ClonedButlerHarness(ButlerHarness):
    """Harness that hands out cloned Butlers."""

    def create_butler(self, run, storage_class, dataset_type_name, metrics=None):
        butler, dataset_type = super().create_butler(run, storage_class, dataset_type_name, metrics=metrics)
        return butler.clone(run=run, metrics=metrics), dataset_type


class ServerButlerHarness(ButlerHarness):
    """Harness backed by a RemoteButler talking to a test server."""

    prediction_supported = False
    trust_mode_supported = False

    def __init__(self, server_instance, *args, **kwargs):
        ...

    def create_empty_butler(self, run=None, writeable=None, metrics=None, cleanup=True) -> Butler:
        return self._server_instance.hybrid_butler.clone(run=run, metrics=metrics)

    def are_uris_equivalent(self, uri1: ResourcePath, uri2: ResourcePath) -> bool:
        # S3 pre-signed URLs may differ in expiration query parameters.
        return uri1.scheme == uri2.scheme and uri1.netloc == uri2.netloc and uri1.path == uri2.path

    def remove_dataset_out_of_band(self, butler: Butler, ref: DatasetRef) -> None:
        # Cannot delete via S3 signed URLs, so reach in through DirectButler.
        self._server_instance.direct_butler.getURI(ref).remove()
```

`ClonedSqliteButlerTestCase.create_butler` calls `butler.clone(run=run)` without metrics while `ClonedPostgresPosixDatastoreButlerTestCase` passes `metrics=metrics`. Use the postgres form, which passes metrics, for both. Note the discrepancy in `tests/_migration/mapping.md` — it is a pre-existing inconsistency, not a behavior change this branch is making, and the gate will confirm it costs nothing.

- [ ] **Step 4: Write the composed fixtures**

```python
@pytest.fixture(scope="session")
def storage_class_factory(test_directory: str) -> StorageClassFactory:
    """Storage classes loaded once per worker from the test config."""
    factory = StorageClassFactory()
    factory.addFromConfig(os.path.join(test_directory, "config/basic/butler.yaml"))
    return factory


@pytest.fixture(scope="session")
def postgres_instance() -> Iterator[TemporaryPostgresInstance]:
    """One postgres server per session, matching the previous setUpClass."""
    with setup_postgres_test_db() as instance:
        yield instance


@pytest.fixture
def butler_repo(request, test_directory, registry_backend, datastore_type, repo_layout) -> Iterator[TestRepo]:
    """Build a repo for the requested axis combination and clean it up."""
    ...


@pytest.fixture
def butler_harness(request, test_directory, butler_repo, butler_client,
                   registry_backend, storage_class_factory) -> Iterator[ButlerHarness]:
    """Open a Butler of the requested client kind and yield its harness."""
    ...


@pytest.fixture
def butler(butler_harness: ButlerHarness) -> Butler:
    """Return the Butler under test, for tests that need nothing else."""
    return butler_harness.create_empty_butler(run=butler_harness.default_run)
```

**This task is implemented; read `python/lsst/daf/butler/tests/fixtures.py` rather than this sketch.** The repo construction is split into `_make_remote_test_repo`, `_make_explicit_root_repo` and `_make_outfile_repo`, each porting the `setUp` named in its docstring.

Two constraints that are load-bearing and easy to undo by accident:

- `postgres_instance` and `create_test_server` are resolved with `request.getfixturevalue(...)` and a function-local import, never as fixture parameters or module-level imports. Declaring `postgres_instance` as a parameter would start a postgres server for every sqlite test, and `server.py` imports `fastapi` at module level, so an eager import breaks collection in any environment without it.
- Some layouts override the profile: `explicit_root` clears `full_config_key` and sets `datastore_str=["dir1"]`, and `remote_test` computes both from the generated URI. Always read the profile off `butler_repo.profile` or `butler_harness.profile`, never out of `DATASTORE_PROFILES`.

The server client needs `create_test_server(test_directory, postgres=...)`, so `butler_harness` likewise resolves `postgres_instance` lazily when `butler_client == "server"` and `registry_backend == "postgres"`.

- [ ] **Step 5: Write `tests/conftest.py`**

```python
import os

import pytest

pytest_plugins = ["lsst.daf.butler.tests.fixtures"]


@pytest.fixture(scope="session")
def test_directory() -> str:
    """Absolute path of this tests directory.

    The shipped fixture plugin cannot know where a consuming package keeps its
    test configuration, so each package supplies this.
    """
    return os.path.abspath(os.path.dirname(__file__))
```

- [ ] **Step 6: Verify the plugin loads and nothing else changed**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/ -p no:randomly \
  --fixtures 2>&1 | grep -E "^(butler|registry_backend|datastore_type|butler_client|repo_layout|butler_repo|butler_harness|test_directory|storage_class_factory|postgres_instance)\b"
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest \
  tests/test_butler.py tests/test_datastore.py -q -p no:randomly 2>&1 | tail -2
```

Expected: every fixture listed; counts identical to the baseline recorded in the Global Constraints, since nothing has changed the tests yet.

- [ ] **Step 7: Verify ruff and mypy are clean on the new module**

```bash
env -u PYTHONPATH uv run --all-extras --dev ruff check python/lsst/daf/butler/tests/fixtures.py tests/conftest.py
env -u PYTHONPATH uv run --all-extras --dev mypy python/lsst/daf/butler/tests/fixtures.py
```

- [ ] **Step 8: Commit**

```bash
git add python/lsst/daf/butler/tests/fixtures.py tests/conftest.py
git commit -m "Add pytest fixture axes for butler tests

Four independently selectable axes replace the unittest subclass lattice:
registry backend, datastore type, butler client and repo layout. Each
defaults to one value, so a test written plainly runs once and any
multiplication is opt-in and visible in the diff.

Shipped in the package but activated by pytest_plugins rather than a
pytest11 entry point, so nothing auto-registers in unrelated environments.

Nothing consumes these yet."
```

---

## Task 3: Ruff PT ratchet and pytest configuration

**Files:**
- Modify: `pyproject.toml`

**Interfaces:**
- Produces: `PT` enforced on `tests/conftest.py` and every file created from Task 4 onward; suppressed on the 71 files not yet converted.

- [ ] **Step 1: Generate the per-file-ignores list**

```bash
ls tests/test_*.py | sed 's|.*|"&" = ["PT"],|' | sort
```

That is 73 entries. Remove `tests/test_butler.py` and `tests/test_datastore.py` from the output only when those files are deleted in Tasks 12 and 17 — until then they are still unconverted and need the entry.

- [ ] **Step 2: Add `PT` to select and paste the ignores**

In `[tool.ruff.lint] select`, after `"RUF022",  # sort __all__`, add:

```toml
    "PT",  # flake8-pytest-style
```

Add nothing to `ignore`. `PT011` and `PT012` are fixed rather than suppressed; see Task 4 onward.

Under `[tool.ruff.lint.per-file-ignores]`, after the existing `parserYacc.py` entry, add a commented block:

```toml
# DM-55822 ratchet: files not yet migrated to native pytest. Delete an entry
# when its file is converted. When this list is empty, delete the whole block.
"tests/test_astropyTableFormatter.py" = ["PT"],
...
```

- [ ] **Step 3: Verify the ratchet holds**

```bash
env -u PYTHONPATH uv run --all-extras --dev ruff check tests/ python/lsst/daf/butler/tests/fixtures.py
```

Expected: clean. If a file reports `PT` errors, its entry is missing or misspelled.

- [ ] **Step 4: Verify RUF100 does not fire on the ratchet**

`RUF100` is in `extend-select` and warns about unused suppressions. A `per-file-ignores` entry for a file with no `PT` violations is not a `noqa` and does not trigger it, but confirm:

```bash
env -u PYTHONPATH uv run --all-extras --dev ruff check --select RUF100 tests/
```

Expected: clean.

- [ ] **Step 5: Add pytest configuration**

Under `[tool.pytest.ini_options]`, keeping the existing `addopts`:

```toml
xfail_strict = true
timeout = 300
markers = [
    "postgres: test requires a postgres server",
    "server: test requires the butler server (fastapi)",
    "slow: test is slow enough to skip during local iteration",
]
```

Add `pytest-timeout>=2.3.0` to the `dev` dependency group, then `uv lock` and re-`uv sync --locked --all-extras --dev`.

- [ ] **Step 6: Verify `xfail_strict` does not break the existing xfails**

`xfail_strict = true` turns an unexpectedly-passing xfail into a failure. There are 10 xfails in the two files in scope and more elsewhere.

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/ -q -p no:randomly 2>&1 | tail -3
```

Expected: no `XPASS(strict)` failures. If any appear, that xfail was passing all along — record it in `tests/_migration/mapping.md` as a finding for a separate ticket and mark it `@pytest.mark.xfail(strict=False)` with a comment rather than fixing it here, since fixing it would be a library change.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "Enforce flake8-pytest-style with a per-file ratchet

PT is selected in full with nothing in ignore. The 71 files not yet
converted are suppressed individually in per-file-ignores; each later
ticket deletes its entries and takes on that file's share of the
PT011 and PT012 balance.

Also adds pytest-timeout, since the postgres and server tests are the
ones that hang and a hung xdist worker consumes the whole job budget."
```

---

## Task 4: Pattern setter — `tests/test_datastore_cache.py`

`DatastoreCacheTestCase` is 485 lines, 10 tests, 2.11s, self-contained, and has no backend axis. It establishes the conventions in a commit that is cheap to review before the large files.

**Files:**
- Create: `tests/test_datastore_cache.py`
- Modify: `tests/test_datastore.py` (delete `DatastoreCacheTestCase`, lines 1677-2161)
- Modify: `pyproject.toml` (no `PT` entry for the new file)
- Modify: `tests/_migration/mapping.md`

**Interfaces:**
- Consumes: nothing from `fixtures.py`; this class has no butler.
- Produces: the file-level conventions every later split task follows — module docstring, `TESTDIR` constant, module-scoped fixtures replacing `setUpClass`, function-scoped fixtures replacing `setUp`/`tearDown`, plain module-level test functions.

- [ ] **Step 1: Copy the class out and run the autofix on the copy**

```bash
sed -n '1677,2161p' tests/test_datastore.py > /tmp/cache_body.py
```

Assemble `tests/test_datastore_cache.py` with the licence header from `tests/test_datastore.py:1-30`, the imports it needs, and that body. Then:

```bash
env -u PYTHONPATH uv run --all-extras --dev ruff check --select PT --fix --unsafe-fixes tests/test_datastore_cache.py
```

Expected: most `PT009`/`PT027` fixed automatically, a handful of `PT011`/`PT012` left.

- [ ] **Step 2: Replace `setUpClass` with module-scoped fixtures**

```python
@pytest.fixture(scope="module")
def universe() -> DimensionUniverse:
    return DimensionUniverse()


@pytest.fixture(scope="module")
def storage_class_factory() -> StorageClassFactory:
    factory = StorageClassFactory()
    factory.addFromConfig(os.path.join(TESTDIR, "config/basic/storageClasses.yaml"))
    return factory
```

This module defines its own `storage_class_factory` loading `storageClasses.yaml`, which shadows the plugin's session fixture loading `butler.yaml`. That is deliberate and matches the original `setUpClass`. Name it distinctly — `cache_storage_class_factory` — to avoid a confusing shadow, and note the rename in the mapping file.

- [ ] **Step 3: Replace `setUp`/`tearDown` with one function-scoped fixture**

The `setUp` builds 10 refs with files, 3 composite refs with component files, and a temp root; `tearDown` removes the root. Use `tmp_path` instead of `tempfile.mkdtemp()` so pytest handles cleanup, and return a small dataclass rather than setting attributes:

```python
@dataclasses.dataclass
class CacheFixtures:
    """Refs and files shared by the cache tests."""

    root: str
    refs: list[DatasetRef]
    files: list[ResourcePath]
    composite_refs: list[DatasetRef]
    comp_files: list[list[ResourcePath]]
    comp_refs: list[list[DatasetRef]]


@pytest.fixture
def cache_fixtures(tmp_path, universe, cache_storage_class_factory) -> CacheFixtures:
    # Body from DatastoreCacheTestCase.setUp, with self.root replaced by
    # str(tmp_path) and the trailing self.<attr> assignments returned instead.
    ...
```

`self.id = 0` in the original feeds `DatasetTestHelper.makeDatasetRef`. Keep an explicit counter in the fixture rather than relying on instance state.

- [ ] **Step 4: Convert the 10 test methods to module-level functions**

Each `def testX(self)` becomes `def test_x(cache_fixtures, universe)` requesting only the fixtures it uses. Keep the original method names' meaning but move to `snake_case`, and record every rename in the mapping file — the gate cannot see renames, so the mapping file is the only record.

`_make_cache_manager` becomes a module-level helper taking `universe`:

```python
def _make_cache_manager(config_str: str, universe: DimensionUniverse) -> DatastoreCacheManager:
    config = Config.fromYaml(config_str)
    return DatastoreCacheManager(DatastoreCacheManagerConfig(config), universe=universe)
```

- [ ] **Step 5: Fix the remaining PT011 and PT012**

```bash
env -u PYTHONPATH uv run --all-extras --dev ruff check --select PT tests/test_datastore_cache.py
```

For each `PT011`, add `match=` with a distinctive fragment of the real message. Get the message by running the test with the `raises` removed if you cannot tell from the source. For each `PT012`, move the statements that are not expected to raise out of the `with` block.

- [ ] **Step 6: Delete the class from the original file**

Remove `tests/test_datastore.py:1677-2161`, and remove any imports it alone used. Ruff's `F401` catches those.

- [ ] **Step 7: Run both files and check the count is conserved**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest \
  tests/test_datastore.py tests/test_datastore_cache.py -q -p no:randomly 2>&1 | tail -3
env -u PYTHONPATH uv run --all-extras --dev ruff check tests/test_datastore_cache.py tests/test_datastore.py
```

Expected: the same total as before this task. `DatastoreCacheTestCase` had 10 tests; `tests/test_datastore_cache.py` must have exactly 10.

- [ ] **Step 8: Record the 10 mappings and commit**

```bash
git add tests/test_datastore_cache.py tests/test_datastore.py tests/_migration/mapping.md pyproject.toml
git commit -m "Split datastore cache tests into their own pytest-native file

DatastoreCacheTestCase is self-contained with no backend axis, so it sets
the conventions for the larger splits: module-scoped fixtures for what was
setUpClass, a dataclass-returning function fixture for what was setUp, and
module-level test functions.

Test count is conserved at 10."
```

---

## Tasks 5 to 12: Split and convert `tests/test_butler.py`

**These tasks preserve every test execution.** No class is deleted, no axis is dropped. `ButlerExplicitRootTestCase` and friends become explicit `parametrize` decorators that produce the *same number of cases* they produce today. Deduplication is Task 18 onward.

Each task follows the same shape. The verification that matters is per-task: the number of collected tests across the whole of the original file plus its extracted parts must not change.

**Before starting Task 5, record the reference count:**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_butler.py \
  -p no:randomly --collect-only -q 2>&1 | tail -1
```

Every task from 6 to 13 re-runs the equivalent count across `tests/test_butler*.py` and compares against it.

---

### Task 5: `tests/test_butler_null_datastore.py`

Start with the smallest, to shake out the harness before the large files.

**Files:**
- Create: `tests/test_butler_null_datastore.py`
- Modify: `tests/test_butler.py` (delete `NullDatastoreTestCase`, lines 3516-3572)
- Modify: `tests/_migration/mapping.md`, `pyproject.toml`

**Interfaces:**
- Consumes: nothing from `fixtures.py` — this class builds its own butler with a null datastore.
- Produces: 2 tests.

- [ ] **Step 1: Extract the class body**

```bash
sed -n '3516,3572p' tests/test_butler.py > /tmp/null_body.py
```

Assemble the new file with the licence header from `tests/test_butler.py:1-30`, `TESTDIR = os.path.abspath(os.path.dirname(__file__))`, and the imports the body needs.

- [ ] **Step 2: Run the autofix**

```bash
env -u PYTHONPATH uv run --all-extras --dev ruff check --select PT --fix --unsafe-fixes tests/test_butler_null_datastore.py
```

- [ ] **Step 3: Convert `setUp` to a fixture and the methods to functions**

Follow the conventions from Task 4: `setUp` becomes a function-scoped fixture returning a small dataclass or a single object; each `def testX(self)` becomes a module-level `def test_x(<fixtures>)`.

- [ ] **Step 4: Fix any remaining PT011 and PT012**

```bash
env -u PYTHONPATH uv run --all-extras --dev ruff check tests/test_butler_null_datastore.py
```

- [ ] **Step 5: Delete the class from `tests/test_butler.py` and drop dead imports**

- [ ] **Step 6: Verify the count is conserved**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_butler*.py \
  -p no:randomly --collect-only -q 2>&1 | tail -1
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_butler*.py -q -p no:randomly 2>&1 | tail -2
```

Expected: collected count equals the Task 5 reference count exactly; zero failures.

- [ ] **Step 7: Record mappings and commit**

```bash
git add tests/test_butler_null_datastore.py tests/test_butler.py tests/_migration/mapping.md pyproject.toml
git commit -m "Split NullDatastoreTestCase into its own pytest-native file

Test count conserved at 2."
```

---

### Task 6: `tests/test_butler_transfers.py`

**Files:**
- Create: `tests/test_butler_transfers.py`
- Modify: `tests/test_butler.py` (delete `DatastoreTransfers` 2884-3180, `PosixDatastoreTransfers` 3181-3391, `ChainedDatastoreTransfers` 3392-3397, `ButlerServerDatastoreTransfers` 3398-3424, `TransferDatasetsInPlace` 3425-3515)
- Modify: `tests/_migration/mapping.md`, `pyproject.toml`

**Interfaces:**
- Consumes: `butler_harness`, `datastore_type`, `butler_client` from `fixtures.py`.
- Produces: 31 tests (14 posix + 14 chained + 1 server + 2 in-place).

- [ ] **Step 1: Extract and autofix**

```bash
sed -n '2884,3515p' tests/test_butler.py > /tmp/transfers_body.py
env -u PYTHONPATH uv run --all-extras --dev ruff check --select PT --fix --unsafe-fixes tests/test_butler_transfers.py
```

- [ ] **Step 2: Turn the four concrete classes into two axis parametrizations**

`PosixDatastoreTransfers` and `ChainedDatastoreTransfers` differ only in `configFile`. The 14 shared tests become module-level functions carrying, **for now**, an explicit two-value parametrize that reproduces both runs:

```python
@pytest.mark.parametrize("datastore_type", ["posix", "chained"], indirect=True)
def test_transfer_uuid_to_uuid(butler_harness: ButlerHarness) -> None:
    ...
```

`ButlerServerDatastoreTransfers` contributes one test, `test_transfers_from_remote_to_direct`, which gets `@pytest.mark.parametrize("butler_client", ["server"], indirect=True)` and `@pytest.mark.server`.

Do **not** reduce `["posix", "chained"]` to `["posix"]` here. That is Task 20, and it needs coverage evidence.

- [ ] **Step 3: Do not confuse the two `create_butler` methods**

`DatastoreTransfers.create_butler` at `tests/test_butler.py:2907` has the
signature `(self, manager, label, config_file=None) -> Butler`, which is
unrelated to `ButlerPutGetTests.create_butler` at `:231`. It stays a local
helper in this file and must **not** be folded into `ButlerHarness`.
`PosixDatastoreTransfers.create_butlers` at `:3193` is likewise local.

- [ ] **Step 4: Convert `TransferDatasetsInPlace`**

Its two tests build their own butlers from `butler.yaml` and `butler-chained-posix.yaml` inside the test bodies (lines 3431 and 3448), not from class attributes. Leave that structure; just convert the assertions and the `setUp`.

- [ ] **Step 5: Fix remaining PT011 and PT012, then delete from the original**

- [ ] **Step 6: Verify the count is conserved**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_butler*.py \
  -p no:randomly --collect-only -q 2>&1 | tail -1
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_butler*.py -q -p no:randomly 2>&1 | tail -2
```

Expected: equals the Task 5 reference count; zero failures.

- [ ] **Step 7: Record mappings and commit**

```bash
git add tests/test_butler_transfers.py tests/test_butler.py tests/_migration/mapping.md pyproject.toml
git commit -m "Split butler transfer tests into their own pytest-native file

The posix and chained transfer suites become one datastore_type
parametrization, still producing both runs. Deduplication is a later
commit. Test count conserved at 31."
```

---

### Task 7: `tests/test_butler_config_repo.py`

**Files:**
- Create: `tests/test_butler_config_repo.py`
- Modify: `tests/test_butler.py` (delete `ButlerConfigTests` 145-165, `ButlerExplicitRootTestCase` 2751-2783, `ButlerMakeRepoOutfileTestCase` 2784-2812, `ButlerMakeRepoOutfileDirTestCase` 2813-2831, `ButlerMakeRepoOutfileUriTestCase` 2832-2844; move `testMakeRepo` and `testPutTemplates` out of the mixins)
- Modify: `tests/_migration/mapping.md`, `pyproject.toml`

**Interfaces:**
- Consumes: `butler_harness`, `repo_layout`, `butler_repo` from `fixtures.py`.
- Produces: the config and repo-creation tests, count unchanged.

- [ ] **Step 1: Extract and autofix**

- [ ] **Step 2: Make `repo_layout` carry the outfile variants**

The three `ButlerMakeRepoOutfile*` classes differ only in what `outfile` is passed to `make_repo_for_test`: a file in a second root, a directory, and a URI. Those become `repo_layout` values `outfile`, `outfile_dir`, `outfile_uri`, already handled by `_make_outfile_repo` in `fixtures.py`. The two tests become:

```python
@pytest.mark.parametrize("repo_layout", ["outfile", "outfile_dir", "outfile_uri"], indirect=True)
def test_config_existence(butler_harness: ButlerHarness) -> None:
    ...
```

That still produces 3 x 2 = 6 executions where the classes produced 12 (3 classes x 4 tests). Check that arithmetic against the real file: `ButlerMakeRepoOutfileTestCase` inherits `testDeferredCollectionPassing` and `testPutGet` from `ButlerPutGetTests` as well as defining `testConfigExistence` and `testPutGet`. If the counts do not match, the parametrize list is wrong, not the count — reproduce the exact set before moving on.

- [ ] **Step 3: Make `ButlerExplicitRootTestCase` a `repo_layout` value**

Its `setUp` logic now lives in `_make_explicit_root_repo`; it writes the repo into `dir1`, moves the config to `dir2/butler2.yaml` with an explicit `root` key, and deletes the original. That logic already lives in `_make_explicit_root_repo`.

`testFileLocations` becomes a single parametrized test. The other 39 inherited tests get, **for now**, `@pytest.mark.parametrize("repo_layout", ["in_repo", "explicit_root"], indirect=True)` so the execution count is preserved. Task 19 removes the `explicit_root` value from the ones with no marginal coverage.

Note that `ButlerExplicitRootTestCase` sets `fullConfigKey = None` and `datastoreStr = ["dir1"]`, overriding the posix profile. `_make_explicit_root_repo` already applies that override, so read it from `butler_repo.profile`.

- [ ] **Step 4: Fix remaining PT011 and PT012, then delete from the original**

- [ ] **Step 5: Verify the count is conserved**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_butler*.py \
  -p no:randomly --collect-only -q 2>&1 | tail -1
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_butler*.py -q -p no:randomly 2>&1 | tail -2
```

- [ ] **Step 6: Record mappings and commit**

```bash
git add tests/test_butler_config_repo.py tests/test_butler.py tests/_migration/mapping.md pyproject.toml
git commit -m "Split butler config and repo-creation tests

ButlerExplicitRootTestCase and the three ButlerMakeRepoOutfile classes
become repo_layout axis values, still producing every execution they
produced as subclasses."
```

---

### Task 8: `tests/test_butler_import_export.py`

**Files:**
- Create: `tests/test_butler_import_export.py`
- Modify: `tests/test_butler.py` (move `testImportExport`, `testImportExportVirtualComposite`, `testRemoveRuns`, `testPruneDatasets` out of `FileDatastoreButlerTests`; `testExportTransferCopy` out of `PosixDatastoreButlerTestCase`)
- Modify: `tests/_migration/mapping.md`, `pyproject.toml`

**Interfaces:**
- Consumes: `butler_harness`, `datastore_type`, `registry_backend`, `butler_client`.
- Produces: the import/export tests, count unchanged.

- [ ] **Step 1: Extract and autofix**

- [ ] **Step 2: Reproduce the current axis coverage exactly**

These live in `FileDatastoreButlerTests`, so today they run for `posix`, `postgres-posix`, `cloned-postgres-posix`, `chained`, `remote_test`, `server-sqlite` and `server-postgres`. Reproduce that with explicit parametrize lists on each function, for example:

```python
@pytest.mark.parametrize(
    ("registry_backend", "datastore_type", "butler_client"),
    [
        ("sqlite", "posix", "direct"),
        ("postgres", "posix", "direct"),
        ("postgres", "posix", "cloned"),
        ("sqlite", "chained", "direct"),
        ("sqlite", "remote_test", "direct"),
    ],
    indirect=True,
)
def test_import_export(butler_harness: ButlerHarness) -> None:
    ...
```

Define that list once as a module constant, `FILE_DATASTORE_AXES`, and reference it, so Task 18 onward changes one place.

`ChainedDatastoreButlerTestCase` overrides `testPruneDatasets` to a no-op because out-of-band file manipulation is impossible with an InMemoryDatastore in the chain. Reproduce that by excluding `chained` from that one function's list, with the original comment carried over.

`testExportTransferCopy` is posix-only today. Give it no parametrize at all, so it uses the defaults.

- [ ] **Step 3: Drop the subTest wrapper in `testImportExport`**

At `tests/test_butler.py:2039` the loop iterates over `datasets`, which the test
has just created, so there is no parameter list at collection time and this
cannot become `parametrize`. Delete the `with self.subTest(ref=repr(ref)):`
line, dedent the body, and carry the identity into the assertions:

```python
assert butler.exists(ref), f"dataset {ref!r} missing after import"
```

The first failure now ends the loop rather than reporting every failing
dataset. That is accepted: the loop is inside a single test either way.

- [ ] **Step 4: Fix remaining PT011 and PT012, then delete from the original**

- [ ] **Step 5: Verify the count is conserved**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_butler*.py \
  -p no:randomly --collect-only -q 2>&1 | tail -1
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_butler*.py -q -p no:randomly 2>&1 | tail -2
```

- [ ] **Step 6: Record mappings and commit**

```bash
git add tests/test_butler_import_export.py tests/test_butler.py tests/_migration/mapping.md pyproject.toml
git commit -m "Split butler import and export tests

Introduces FILE_DATASTORE_AXES, the explicit list reproducing what
FileDatastoreButlerTests currently produces by inheritance."
```

---

### Task 9: `tests/test_butler_collections.py`

**Files:**
- Create: `tests/test_butler_collections.py`
- Modify: `tests/test_butler.py` (move `testCollectionChainRedefine`, `testCollectionChainPrepend`, `testCollectionChainExtend`, `testCollectionChainRemove`, `testGetDatasetCollectionCaching`, `testGetDatasetTypes` out of `ButlerTests`)
- Modify: `tests/_migration/mapping.md`, `pyproject.toml`

**Interfaces:**
- Consumes: `butler_harness`, `FILE_DATASTORE_AXES` — import it from `tests/test_butler_import_export.py` or, better, move it to `tests/conftest.py` when this task needs it in a second file.
- Produces: the collection tests, count unchanged.

- [ ] **Step 1: Move `FILE_DATASTORE_AXES` to `tests/conftest.py`**

Two files now need it. Move it, and add `BUTLER_TESTS_AXES` alongside for the wider set `ButlerTests` covers (which adds `in_memory` and `cloned-sqlite` to the file-datastore set).

- [ ] **Step 2: Extract and autofix**

- [ ] **Step 3: Convert, parametrizing with `BUTLER_TESTS_AXES`**

`ButlerServerTests.testGetDatasetTypes` is a no-op override, because the test is mostly about `validateConfiguration`, which is not relevant to `RemoteButler`. Exclude the two server axes from that function's list and carry the comment over.

- [ ] **Step 4: Fix remaining PT011 and PT012, then delete from the original**

- [ ] **Step 5: Verify the count is conserved**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_butler*.py \
  -p no:randomly --collect-only -q 2>&1 | tail -1
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_butler*.py -q -p no:randomly 2>&1 | tail -2
```

- [ ] **Step 6: Record mappings and commit**

```bash
git add tests/test_butler_collections.py tests/test_butler.py tests/conftest.py tests/_migration/mapping.md pyproject.toml
git commit -m "Split butler collection tests

Moves the axis lists to conftest now that a second file needs them."
```

---

### Task 10: `tests/test_butler_ingest.py`

**Files:**
- Create: `tests/test_butler_ingest.py`
- Modify: `tests/test_butler.py` (move `testIngest`, `test_ingest_zip` out of `ButlerTests`; `test_temporary_for_ingest`, `test_specialized_file_datasets_functions` out of `PosixDatastoreButlerTestCase`)
- Modify: `tests/_migration/mapping.md`, `pyproject.toml`

**Interfaces:**
- Consumes: `butler_harness`, `BUTLER_TESTS_AXES`.
- Produces: the ingest tests, count unchanged.

- [ ] **Step 1: Extract and autofix**

- [ ] **Step 2: Reproduce the in-memory opt-out**

`InMemoryDatastoreButlerTestCase` overrides both `testIngest` and `test_ingest_zip` with `pass`, because an InMemoryDatastore cannot ingest files. Exclude `in_memory` from those two functions' axis lists rather than keeping an empty function. Add a comment saying why, ported from the original.

`test_temporary_for_ingest` and `test_specialized_file_datasets_functions` are posix-only. Give them no parametrize.

- [ ] **Step 3: Fix remaining PT011 and PT012, then delete from the original**

- [ ] **Step 4: Verify the count is conserved**

Note that this task *reduces* the collected count by two, because two `pass` overrides that pytest counted as tests are now excluded axis values instead. That is the one legitimate count change in Tasks 5 to 12. Record the new reference count and the reason in `tests/_migration/mapping.md` as `dropped: empty override, InMemoryDatastore cannot ingest`.

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_butler*.py \
  -p no:randomly --collect-only -q 2>&1 | tail -1
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_butler*.py -q -p no:randomly 2>&1 | tail -2
```

- [ ] **Step 5: Record mappings and commit**

```bash
git add tests/test_butler_ingest.py tests/test_butler.py tests/conftest.py tests/_migration/mapping.md pyproject.toml
git commit -m "Split butler ingest tests

The two empty InMemoryDatastore overrides become excluded axis values
rather than no-op test methods, which reduces the collected count by two."
```

---

### Task 11: `tests/test_butler_put_get.py`

The largest extraction. `ButlerPutGetTests` is lines 166-621 and holds `runPutGetTest`, the helper most other tests call.

**Files:**
- Create: `tests/test_butler_put_get.py`
- Modify: `tests/test_butler.py` (move `ButlerPutGetTests` 166-621 and the storage-class tests from `ButlerTests`)
- Modify: `tests/_migration/mapping.md`, `pyproject.toml`

**Interfaces:**
- Consumes: `butler_harness`, `BUTLER_TESTS_AXES`.
- Produces: `run_put_get_test(harness, storage_class, dataset_type_name, ...)` as a module-level helper, imported by Tasks 8, 10 and 13's files if they need it. Check which do before writing the signature.

- [ ] **Step 1: Find every caller of `runPutGetTest` before moving it**

```bash
rg -n "runPutGetTest" tests/
```

Every hit outside this new file needs an import. If the callers are spread across four files, put the helper in `tests/conftest.py` as a fixture instead, so nothing imports across test modules.

- [ ] **Step 2: Extract and autofix**

- [ ] **Step 3: Convert `runPutGetTest` to a module-level or fixture-provided helper**

It currently takes `self` for `self.default_run`, `self.storageClassFactory` and `self.create_butler`. All three are on `ButlerHarness`, so the signature becomes:

```python
def run_put_get_test(
    harness: ButlerHarness,
    storage_class: StorageClass | str,
    dataset_type_name: str,
    metrics: ButlerMetrics | None = None,
) -> Butler:
    ...
```

Its internal loop over `((ref,), (datasetTypeName, dataId), (datasetType, dataId))` loops over data built inside the helper, so it cannot be parametrized. Drop the `with self.subTest(args=repr(args)):` wrapper at `tests/test_butler.py:322`, dedent the body, and carry the identity into the assertions:

```python
assert isinstance(ref, DatasetRef), f"put with args {args!r}"
```

Keep the surrounding loop, the `counter`, and the distinct `this_run` per iteration. The comment there explains that distinct run collections exist to stop cascading failures, and with `subTest` gone that is now the only thing preventing a cascade.

- [ ] **Step 4: Convert `assertGetComponents` to a module-level helper the same way**

- [ ] **Step 5: Fix remaining PT011 and PT012, then delete from the original**

- [ ] **Step 6: Verify the count is conserved**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_butler*.py \
  -p no:randomly --collect-only -q 2>&1 | tail -1
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_butler*.py -q -p no:randomly 2>&1 | tail -2
```

- [ ] **Step 7: Record mappings and commit**

```bash
git add tests/test_butler_put_get.py tests/test_butler.py tests/conftest.py tests/_migration/mapping.md pyproject.toml
git commit -m "Split butler put/get tests

runPutGetTest becomes a plain helper taking a ButlerHarness rather than
a TestCase, which is what lets the remaining files stop inheriting."
```

---

### Task 12: `tests/test_butler_lifecycle.py` and deleting `tests/test_butler.py`

Whatever remains after Tasks 5 to 11 goes here, and the original file disappears.

**Files:**
- Create: `tests/test_butler_lifecycle.py`
- Delete: `tests/test_butler.py`
- Modify: `tests/_migration/mapping.md`, `pyproject.toml` (delete the `tests/test_butler.py` ratchet entry)

**Interfaces:**
- Consumes: `butler_harness`, `BUTLER_TESTS_AXES`, `run_put_get_test`.
- Produces: the lifecycle tests. After this task, `tests/test_butler.py` does not exist.

- [ ] **Step 1: Confirm what is left**

```bash
rg -n "^class |^    def test" tests/test_butler.py
```

Expected remainder: `testConstructor`, `testPathConstructor`, `testClose`, `testGarbageCollection`, `testDafButlerRepositories`, `testPickle`, `testTransaction`, `testStringification`, `testButlerRewriteDataId`, `test_transfer_dimension_records_from`, `test_butler_metrics`, `test_provenance`, `testPytypeCoercion`, plus `clean_environment`, `makeExampleMetrics`, `TransactionTestError`, `setup_module` and `_get_test_data_path`.

- [ ] **Step 2: Move the module-level helpers to `tests/conftest.py`**

`clean_environment`, `makeExampleMetrics`, `TransactionTestError` and `_get_test_data_path` are used across several of the new files. Put them in `tests/conftest.py` — the first three as plain functions, `clean_environment` as an autouse fixture if `setup_module` was calling it for the whole module. Check `tests/test_butler.py:3671` to see what `setup_module` does before deciding.

- [ ] **Step 3: Extract, autofix and convert the remainder**

`ButlerServerTests` contributes overrides for `testConstructor`, `testDafButlerRepositories`, `testMakeRepo`, `testPickle` (an `expectedFailure`), `testStringification`, `testTransaction`. Reproduce each as an axis exclusion plus, where the server has its own assertion, a separate server-only test function. `testPickle`'s `@unittest.expectedFailure` becomes `@pytest.mark.xfail(reason="Pickling not yet implemented for RemoteButler/HybridButler")` on the server-axis case only. With `xfail_strict = true` this will fail loudly if it ever starts passing, which is the desired behavior.

- [ ] **Step 4: Delete `tests/test_butler.py` and its ratchet entry**

```bash
git rm tests/test_butler.py
```

Remove `"tests/test_butler.py" = ["PT"],` from `per-file-ignores`.

- [ ] **Step 5: Verify the count against the Task 10 reference**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_butler*.py \
  -p no:randomly --collect-only -q 2>&1 | tail -1
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_butler*.py -q -p no:randomly 2>&1 | tail -2
env -u PYTHONPATH uv run --all-extras --dev ruff check tests/
rg -n "unittest|self\.assert" tests/test_butler_*.py
```

Expected: equal to the Task 10 reference count (the original minus the two InMemory ingest no-ops); zero failures; ruff clean; and the `rg` finds **nothing**.

If `rg` matches, a class kept its `TestCase` base. Any `parametrize` on it is silently collecting one case instead of many, so the count check above may be passing for the wrong reason. Fix it before committing.

- [ ] **Step 6: Run the whole suite**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/ -q -p no:randomly 2>&1 | tail -3
```

- [ ] **Step 7: Record mappings and commit**

```bash
git add -A tests/ pyproject.toml
git commit -m "Split butler lifecycle tests and delete test_butler.py

Completes the conversion pass for test_butler.py. Every execution the
subclass lattice produced is now produced by an explicit parametrize,
with the two InMemory ingest no-ops the only removals."
```

---

## Tasks 13 to 17: Split and convert `tests/test_datastore.py`

Same shape as Tasks 5 to 12. `DatastoreCacheTestCase` already moved in Task 4.

**Before starting Task 13, record the reference count:**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_datastore*.py \
  -p no:randomly --collect-only -q 2>&1 | tail -1
```

---

### Task 13: `tests/test_datastore_records.py`

**Files:**
- Create: `tests/test_datastore_records.py`
- Modify: `tests/test_datastore.py` (delete `DatasetRefURIsTestCase` 2217-2255, `StoredFileInfoTestCase` 2256-2336, `TestDatastoreRecordTable` 2337-end)
- Modify: `tests/_migration/mapping.md`, `pyproject.toml`

**Interfaces:**
- Consumes: nothing from `fixtures.py`; these are pure unit tests over record types.
- Produces: the record tests, count unchanged.

- [ ] **Step 1: Extract and autofix**

```bash
sed -n '2217,$p' tests/test_datastore.py > /tmp/records_body.py
env -u PYTHONPATH uv run --all-extras --dev ruff check --select PT --fix --unsafe-fixes tests/test_datastore_records.py
```

- [ ] **Step 2: Convert to module-level functions with fixtures**

- [ ] **Step 3: Fix remaining PT011 and PT012, then delete from the original**

- [ ] **Step 4: Verify the count is conserved and commit**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_datastore*.py \
  -p no:randomly --collect-only -q 2>&1 | tail -1
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_datastore*.py -q -p no:randomly 2>&1 | tail -2
git add tests/test_datastore_records.py tests/test_datastore.py tests/_migration/mapping.md pyproject.toml
git commit -m "Split datastore record tests into their own pytest-native file"
```

---

### Task 14: `tests/test_datastore_null.py`

**Files:**
- Create: `tests/test_datastore_null.py`
- Modify: `tests/test_datastore.py` (delete `NullDatastoreTestCase` 2162-2216)
- Modify: `tests/_migration/mapping.md`, `pyproject.toml`

**Interfaces:**
- Consumes: nothing from `fixtures.py`.
- Produces: the null-datastore tests, count unchanged.

- [ ] **Step 1: Extract and autofix**

- [ ] **Step 2: Convert to module-level functions with fixtures**

- [ ] **Step 3: Fix remaining PT011 and PT012, then delete from the original**

- [ ] **Step 4: Verify the count is conserved and commit**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_datastore*.py \
  -p no:randomly --collect-only -q 2>&1 | tail -1
git add tests/test_datastore_null.py tests/test_datastore.py tests/_migration/mapping.md pyproject.toml
git commit -m "Split null datastore tests into their own pytest-native file"
```

---

### Task 15: `tests/test_datastore_constraints.py`

**Files:**
- Create: `tests/test_datastore_constraints.py`
- Modify: `tests/test_datastore.py` (delete `DatastoreConstraintsTests` 1487-1543 and the five concrete constraint classes, 1544-1676)
- Modify: `tests/_migration/mapping.md`, `pyproject.toml`

**Interfaces:**
- Consumes: a local `datastore` fixture, not `butler_harness` — these tests build a `Datastore` directly, not a `Butler`.
- Produces: the constraint tests, count unchanged.

- [ ] **Step 1: Extract and autofix**

- [ ] **Step 2: Define the datastore axis locally**

The five concrete classes are `PosixDatastoreConstraintsTestCase`, `InMemoryDatastoreConstraintsTestCase`, `ChainedDatastoreConstraintsNativeTestCase`, `ChainedDatastoreConstraintsTestCase`, `ChainedDatastoreMemoryConstraintsTestCase`. They differ in `configFile` and in whether the constraint is expected to accept or reject. Build a module-level list of `(config_file, expected)` tuples and parametrize over it, reproducing exactly the current five combinations.

`ChainedDatastorePerStoreConstraintsTests` (1584-1676) is structurally different — it asserts per-datastore behavior within one chain. Keep it as its own set of functions.

- [ ] **Step 3: Convert the two subTest sites now that the classes are gone**

`parametrize` does not work on `unittest.TestCase` methods — it collects one
case and fails with `TypeError: missing 1 required positional argument`. That is
why this could not be done earlier. Now that these are module-level functions,
it works.

`tests/test_datastore.py:1520` and `:1627` both loop over a zip of dataset-type
names and storage classes built in the test body. Lift each to a module-level
list of `(dataset_type_name, storage_class_name)` tuples and parametrize:

```python
_CONSTRAINT_CASES = [
    ("metric", "StructuredDataJson"),
    # ... the exact pairs from the original zip, in order
]


@pytest.mark.parametrize(("dataset_type_name", "sc_name"), _CONSTRAINT_CASES)
def test_constraints(dataset_type_name: str, sc_name: str, ...) -> None:
    ...
```

Keep the `testfile_j if sc.name.endswith("Json") else testfile_y` selection in
the body — it depends on fixture state, not on the parameter list.

- [ ] **Step 4: Fix remaining PT011 and PT012, then delete from the original**

- [ ] **Step 5: Verify the count is conserved and commit**

The collected count **rises** here: each former subtest is now its own test.
Record the before and after in `tests/_migration/mapping.md`, and confirm the
rise equals the number of tuples in the two lists.

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_datastore*.py \
  -p no:randomly --collect-only -q 2>&1 | tail -1
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_datastore*.py -q -p no:randomly 2>&1 | tail -2
git add tests/test_datastore_constraints.py tests/test_datastore.py tests/_migration/mapping.md pyproject.toml
git commit -m "Split datastore constraint tests

The five concrete constraint classes become one parametrization over
(config_file, expected), reproducing every current combination."
```

---

### Task 16: `tests/test_datastore_file.py`

The bulk: `DatastoreTestsBase` (122-150), `DatastoreTests` (151-1152) and the six concrete classes.

**Files:**
- Create: `tests/test_datastore_file.py`
- Modify: `tests/test_datastore.py` (delete everything from 122 to 1486)
- Modify: `tests/_migration/mapping.md`, `pyproject.toml`

**Interfaces:**
- Consumes: a local `datastore` fixture built from a config path, plus `DatasetTestHelper` and `DatastoreTestHelper` from `lsst.daf.butler.tests`.
- Produces: the core datastore tests, count unchanged.

- [ ] **Step 1: Enumerate the concrete classes and their differences first**

```bash
rg -n "^class (Posix|InMemory|Chained|Trash|Cleanup)" tests/test_datastore.py
rg -n "configFile|ingestTransferModes|canIngestNoTransferAuto|isEphemeral|rootKeys|validationCanFail" tests/test_datastore.py
```

Write the resulting matrix into `tests/_migration/mapping.md` before converting. The classes are `PosixDatastoreTestCase`, `PosixDatastoreNoChecksumsTestCase`, `TrashDatastoreTestCase`, `CleanupPosixDatastoreTestCase`, `InMemoryDatastoreTestCase`, `ChainedDatastoreTestCase`, `ChainedDatastoreMemoryTestCase`.

- [ ] **Step 2: Extract and autofix**

- [ ] **Step 3: Build a `DatastoreTestProfile` and parametrize over it**

This mirrors `DatastoreProfile` in `fixtures.py` but is local to this file, because these attributes are datastore-test-specific and no butler test needs them. Do not add them to the shipped module.

- [ ] **Step 4: Reproduce every current combination**

`TrashDatastoreTestCase` and `PosixDatastoreNoChecksumsTestCase` both subclass `PosixDatastoreTestCase`, so each currently reruns all 29 of its tests. Reproduce that with a three-value profile parametrization for now. Task 21 reduces it.

- [ ] **Step 5: Convert the four subTest sites now that the classes are gone**

`parametrize` does not work on `unittest.TestCase` methods, which is why this
could not be done before the split. These are now module-level functions.

`tests/test_datastore.py:857` (`testIngestTransfer`) is the cleanest — a loop
over a literal tuple:

```python
@pytest.mark.parametrize(
    "mode", ["copy", "move", "link", "hardlink", "symlink", "relsymlink", "auto"]
)
def test_ingest_transfer(mode: str, ...) -> None:
    ...
```

`:778` (`testIngestNoTransfer`) has a guard on profile state, so its `continue`
becomes a visible skip:

```python
@pytest.mark.parametrize("mode", [None, "auto"])
def test_ingest_no_transfer(mode: str | None, datastore_profile, ...) -> None:
    if mode == "auto" and "auto" in datastore_profile.ingest_transfer_modes \
            and not datastore_profile.can_ingest_no_transfer_auto:
        pytest.skip("Datastore supports auto but cannot transfer in place.")
    ...
```

This is the one place the suite's skip count legitimately rises. Task 17 Step 5
checks that rise explicitly, so use exactly this skip message.

`:522` loops over composite storage classes built from a literal name tuple.
Lift the tuple to a module constant and parametrize over `(index, name)`,
keeping the distinct `metric_comp_{i}` dataset type name — the original comment
explains it exists to stop file clashes between cases, and that reason survives:

```python
_COMPOSITE_STORAGE_CLASS_NAMES = (
    "StructuredComposite",
    "StructuredCompositeTestA",
    "StructuredCompositeTestB",
    "StructuredCompositeReadComp",
    "StructuredData",  # No disassembly
    "StructuredCompositeReadCompNoDisassembly",
)


@pytest.mark.parametrize(("i", "sc_name"), list(enumerate(_COMPOSITE_STORAGE_CLASS_NAMES)))
def test_composites(i: int, sc_name: str, ...) -> None:
    ...
```

`:1432` loops over two formatters:

```python
@pytest.mark.parametrize("formatter", [BadWriteFormatter, BadNoWriteFormatter])
```

- [ ] **Step 6: Fix remaining PT011 and PT012, then delete from the original**

- [ ] **Step 7: Verify the count is conserved and commit**

The collected count **rises** here as former subtests become their own tests.
Record before and after in `tests/_migration/mapping.md` and confirm the rise
equals 7 + 2 + 6 + 2 = 17 per datastore profile that runs them, minus the
`testIngestNoTransfer` cases that now skip.

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_datastore*.py \
  -p no:randomly --collect-only -q 2>&1 | tail -1
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_datastore*.py -q -p no:randomly 2>&1 | tail -2
git add tests/test_datastore_file.py tests/test_datastore.py tests/_migration/mapping.md pyproject.toml
git commit -m "Split core datastore tests

The seven concrete datastore classes become one profile parametrization,
still producing every execution. Deduplication is a later commit."
```

---

### Task 17: Delete `tests/test_datastore.py` and run the conversion gate

**Files:**
- Delete: `tests/test_datastore.py`
- Modify: `pyproject.toml` (delete the `tests/test_datastore.py` ratchet entry)
- Modify: `tests/_migration/mapping.md`

**Interfaces:**
- Produces: the proof that the conversion pass was faithful. Every later task depends on this having passed.

- [ ] **Step 1: Confirm the file is empty of tests and delete it**

```bash
rg -n "^class |^def test" tests/test_datastore.py
git rm tests/test_datastore.py
rg -n "unittest|self\.assert" tests/test_datastore_*.py
```

Remove `"tests/test_datastore.py" = ["PT"],` from `per-file-ignores`.

The `rg` must find nothing. A surviving `TestCase` base silently disables every
`parametrize` on that class, so the axis reproduction from Tasks 15 and 16 would
be collecting one case where it should collect many.

- [ ] **Step 2: Run the full suite**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/ -q -p no:randomly -rs 2>&1 | tail -5
```

- [ ] **Step 3: Take the post-conversion coverage run**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/ \
  -p no:randomly --cov=lsst.daf.butler --cov-branch --cov-context=test \
  --cov-report= -q -rs > /tmp/dm55822-postconvert.txt 2>&1
cp .coverage ~/dm55822/postconvert.coverage
```

- [ ] **Step 4: Run the gate — this is the checkpoint the two-pass design exists for**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev python tests/_migration/coverage_tool.py \
  gate ~/dm55822/baseline.coverage ~/dm55822/postconvert.coverage
```

Expected: `lost 0` for both lines and arcs, exit status 0.

If anything is lost, **stop**. A conversion-pass loss means a test was dropped or silently changed, and it must be found before any deduplication begins. Bisect by running the gate against the individual new files.

- [ ] **Step 5: Check the skip count**

```bash
grep -oE "[0-9]+ skipped" /tmp/dm55822-baseline.txt /tmp/dm55822-postconvert.txt
```

Expected: the post-conversion count is the baseline count **plus** the `testIngestNoTransfer` delta. That test's loop over `(None, "auto")` currently uses a silent `continue` when a datastore supports `auto` but cannot transfer in place; parametrizing it turns each skipped iteration into a visible `pytest.skip`. Confirm the rise equals exactly the number of datastore profiles that hit that branch, and that every added skip names that reason. With `-rs`, pytest aggregates skips as `SKIPPED [N] <location>: <reason>`, so read the bracketed `N`, not the line count:

```bash
grep -E "^SKIPPED" /tmp/dm55822-postconvert.txt | grep "cannot transfer in place"
```

Any other increase is a test that silently stopped running — investigate before proceeding.

- [ ] **Step 6: Commit**

```bash
git add -A tests/ pyproject.toml
git commit -m "Delete test_datastore.py; conversion pass complete

Coverage gate against the pre-migration baseline reports zero lost lines
and zero lost arcs, and the skip count is unchanged. Every execution the
subclass lattices produced is still produced. Deduplication follows."
```

---

## Tasks 18 to 22: Deduplicate, one axis per commit

Every task here has the same three-part shape: query the marginal coverage, reduce the parametrize list to what the query justifies, and prove nothing was lost. Each commit message must quote the numbers.

**Use `~/dm55822/postconvert.coverage` for the marginal queries**, not the baseline — the context names now match the new test ids.

---

### Task 18: The cloned-butler axis

`ClonedSqliteButlerTestCase` (22 executions, 1.29s) and `ClonedPostgresPosixDatastoreButlerTestCase` (34 executions, 9.37s) exist to check that `Butler.clone()` does not break anything.

**Files:**
- Modify: `tests/conftest.py` (the axis lists), and whichever `tests/test_butler_*.py` files carry `cloned` in a parametrize
- Modify: `tests/_migration/mapping.md`

- [ ] **Step 1: Query the marginal coverage**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev python tests/_migration/coverage_tool.py \
  marginal ~/dm55822/postconvert.coverage "cloned"
```

- [ ] **Step 2: Decide from the output, and write the decision down before editing**

If `unique` is 0 for both lines and arcs, the axis is pure duplication: reduce it to a single explicit test that clones a butler and does one put and one get. If `unique` is non-zero, list the covered locations and keep `cloned` only on the tests that reach them.

Record the actual numbers in `tests/_migration/mapping.md` for every execution removed.

- [ ] **Step 3: Reduce the axis lists**

- [ ] **Step 4: Prove nothing was lost**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/ \
  -p no:randomly --cov=lsst.daf.butler --cov-branch --cov-context=test \
  --cov-report= -q -rs > /tmp/dm55822-dedup19.txt 2>&1
cp .coverage ~/dm55822/dedup19.coverage
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev python tests/_migration/coverage_tool.py \
  gate ~/dm55822/baseline.coverage ~/dm55822/dedup19.coverage
grep -oE "[0-9]+ skipped" /tmp/dm55822-dedup19.txt
```

Expected: `lost 0`, and the skip count not above the baseline.

- [ ] **Step 5: Commit, quoting the evidence**

```bash
git add -A tests/
git commit -m "Deduplicate the cloned-butler axis

marginal 'cloned' over the post-conversion coverage database reported
<N> unique lines and <M> unique arcs. <Decision, in one sentence.>

Removes <K> test executions. Gate against the pre-migration baseline
reports zero lost lines and zero lost arcs."
```

---

### Task 19: The explicit-root repo layout

`ButlerExplicitRootTestCase` reruns 40 tests to check that a config in one directory can refer to a root in another.

**Files:**
- Modify: `tests/test_butler_config_repo.py` and any other file carrying `explicit_root`
- Modify: `tests/_migration/mapping.md`

- [ ] **Step 1: Query the marginal coverage**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev python tests/_migration/coverage_tool.py \
  marginal ~/dm55822/postconvert.coverage "explicit_root"
```

- [ ] **Step 2: Reduce to `test_file_locations` plus whatever the query justifies**

- [ ] **Step 3: Prove nothing was lost**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/ \
  -p no:randomly --cov=lsst.daf.butler --cov-branch --cov-context=test \
  --cov-report= -q -rs > /tmp/dm55822-dedup20.txt 2>&1
cp .coverage ~/dm55822/dedup20.coverage
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev python tests/_migration/coverage_tool.py \
  gate ~/dm55822/baseline.coverage ~/dm55822/dedup20.coverage
grep -oE "[0-9]+ skipped" /tmp/dm55822-dedup20.txt
```

- [ ] **Step 4: Commit, quoting the evidence**

```bash
git add -A tests/
git commit -m "Deduplicate the explicit-root repo layout

marginal 'explicit_root' over the post-conversion coverage database
reported <N> unique lines and <M> unique arcs. <Decision, one sentence.>

ButlerExplicitRootTestCase reran 40 tests to check that a config in one
directory can refer to a root in another; that is now <K> parametrized
cases. Gate against the pre-migration baseline reports zero lost lines
and zero lost arcs."
```

---

### Task 20: The chained-datastore transfer axis and the outfile layouts

`ChainedDatastoreTransfers` (14 executions, 3.76s) reruns `PosixDatastoreTransfers`; `ButlerMakeRepoOutfileDirTestCase` and `...UriTestCase` (8 executions) rerun `ButlerMakeRepoOutfileTestCase`.

**Files:**
- Modify: `tests/test_butler_transfers.py`, `tests/test_butler_config_repo.py`
- Modify: `tests/_migration/mapping.md`

- [ ] **Step 1: Query both**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev python tests/_migration/coverage_tool.py \
  marginal ~/dm55822/postconvert.coverage "chained"
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev python tests/_migration/coverage_tool.py \
  marginal ~/dm55822/postconvert.coverage "outfile_"
```

- [ ] **Step 2: Reduce each axis list to what its query justifies**

- [ ] **Step 3: Prove nothing was lost**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/ \
  -p no:randomly --cov=lsst.daf.butler --cov-branch --cov-context=test \
  --cov-report= -q -rs > /tmp/dm55822-dedup21.txt 2>&1
cp .coverage ~/dm55822/dedup21.coverage
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev python tests/_migration/coverage_tool.py \
  gate ~/dm55822/baseline.coverage ~/dm55822/dedup21.coverage
grep -oE "[0-9]+ skipped" /tmp/dm55822-dedup21.txt
```

Expected: `lost 0`, and the skip count not above the baseline.

- [ ] **Step 4: Commit, quoting both sets of numbers**

```bash
git add -A tests/
git commit -m "Deduplicate the chained transfer and outfile layout axes

marginal 'chained' reported <N> unique lines and <M> unique arcs;
marginal 'outfile_' reported <N> unique lines and <M> unique arcs.
<Decision, one sentence each.>

Removes <K> test executions. Gate against the pre-migration baseline
reports zero lost lines and zero lost arcs."
```

---

### Task 21: The datastore trash, no-checksum and chained-constraint axes

`TrashDatastoreTestCase` and `PosixDatastoreNoChecksumsTestCase` each rerun all 29 tests of `PosixDatastoreTestCase`. `ChainedDatastoreConstraintsTestCase` and `ChainedDatastoreConstraintsNativeTestCase` (4 executions between them) rerun `PosixDatastoreConstraintsTestCase`.

**Files:**
- Modify: `tests/test_datastore_file.py`, `tests/test_datastore_constraints.py`
- Modify: `tests/_migration/mapping.md`

- [ ] **Step 1: Find the real test ids before guessing at patterns**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_datastore_file.py \
  tests/test_datastore_constraints.py -p no:randomly --collect-only -q 2>&1 | head -40
```

The profile parametrization from Tasks 15 and 16 chooses the id text. Use what it actually produces in the queries below rather than the illustrative strings.

- [ ] **Step 2: Query all three axes**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev python tests/_migration/coverage_tool.py \
  marginal ~/dm55822/postconvert.coverage "trash"
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev python tests/_migration/coverage_tool.py \
  marginal ~/dm55822/postconvert.coverage "nochecksum"
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev python tests/_migration/coverage_tool.py \
  marginal ~/dm55822/postconvert.coverage "chained-constraint"
```

- [ ] **Step 3: Reduce each to the tests its query justifies**

`TrashDatastoreTestCase` defines trash-specific behavior on top of the posix suite; expect its marginal set to be non-empty and confined to the trash tests. `PosixDatastoreNoChecksumsTestCase` differs only in a config flag, so expect a small set. Record the actual numbers for every execution removed.

- [ ] **Step 4: Prove nothing was lost**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/ \
  -p no:randomly --cov=lsst.daf.butler --cov-branch --cov-context=test \
  --cov-report= -q -rs > /tmp/dm55822-dedup22.txt 2>&1
cp .coverage ~/dm55822/dedup22.coverage
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev python tests/_migration/coverage_tool.py \
  gate ~/dm55822/baseline.coverage ~/dm55822/dedup22.coverage
grep -oE "[0-9]+ skipped" /tmp/dm55822-dedup22.txt
```

Expected: `lost 0`, and the skip count not above the baseline.

- [ ] **Step 5: Commit, quoting the evidence**

```bash
git add -A tests/
git commit -m "Deduplicate the datastore trash, no-checksum and constraint axes

marginal queries over the post-conversion coverage database reported
<N> unique lines / <M> unique arcs for trash, <N>/<M> for no-checksum
and <N>/<M> for the chained constraint variants.

Removes <K> test executions. Gate against the pre-migration baseline
reports zero lost lines and zero lost arcs."
```

---

### Task 22: The postgres and server axes

The largest and the riskiest: `ButlerServerPostgresTests` (34 executions, 11.50s) and `ClonedPostgresPosixDatastoreButlerTestCase` (34, 9.37s, if Task 18 did not already remove it). Postgres exercises genuinely different SQL, so expect a non-empty marginal set and expect to keep several opt-ins.

**Files:**
- Modify: `tests/conftest.py` and the `tests/test_butler_*.py` files carrying `postgres` or `server`
- Modify: `tests/_migration/mapping.md`

- [ ] **Step 1: Query each axis separately**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev python tests/_migration/coverage_tool.py \
  marginal ~/dm55822/postconvert.coverage "postgres"
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev python tests/_migration/coverage_tool.py \
  marginal ~/dm55822/postconvert.coverage "server"
```

- [ ] **Step 2: Query the cross product, which is the actual target**

The claim being tested is not "postgres is redundant" or "server is redundant" but "postgres *and* server together add nothing over each separately". Query the intersection by pattern:

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev python tests/_migration/coverage_tool.py \
  marginal ~/dm55822/postconvert.coverage "postgres-server"
```

Adjust the pattern to the real id format. If the ids do not make the cross product addressable by a substring, add the axis values to the test id with `ids=` in the parametrize before querying.

- [ ] **Step 3: Reduce, keeping every test with non-empty marginal coverage**

Be conservative here. A postgres-only code path that only one test reaches is exactly what this axis exists for. When the query is ambiguous, keep the test.

- [ ] **Step 4: Add the `postgres` and `server` markers**

Every remaining test on those axes gets `@pytest.mark.postgres` or `@pytest.mark.server`, so `-m "not postgres and not server"` works for local iteration as the spec promises.

- [ ] **Step 5: Prove nothing was lost**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/ \
  -p no:randomly --cov=lsst.daf.butler --cov-branch --cov-context=test \
  --cov-report= -q -rs > /tmp/dm55822-dedup23.txt 2>&1
cp .coverage ~/dm55822/dedup23.coverage
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev python tests/_migration/coverage_tool.py \
  gate ~/dm55822/baseline.coverage ~/dm55822/dedup23.coverage
grep -oE "[0-9]+ skipped" /tmp/dm55822-dedup23.txt
```

Expected: `lost 0`, and the skip count not above the baseline.

- [ ] **Step 6: Verify the markers work**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/test_butler*.py tests/test_datastore*.py \
  -q -p no:randomly -m "not postgres and not server" 2>&1 | tail -2
```

- [ ] **Step 7: Commit, quoting the numbers**

```bash
git add -A tests/
git commit -m "Deduplicate the postgres and server axes

marginal 'postgres' reported <N> unique lines and <M> unique arcs;
marginal 'server' reported <N>/<M>; the postgres-and-server cross
product reported <N>/<M>. <Decision, one sentence.>

Every test with non-empty marginal coverage on either axis is kept and
now carries the postgres or server marker, so -m 'not postgres and not
server' works for local iteration.

Removes <K> test executions. Gate against the pre-migration baseline
reports zero lost lines and zero lost arcs."
```

---

## Task 23: Close out

**Files:**
- Delete: `tests/_migration/`
- Create: `doc/changes/DM-55822.misc.md`
- Modify: `pyproject.toml` if any ratchet entries can go

- [ ] **Step 1: Run the final gate**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/ \
  -p no:randomly --cov=lsst.daf.butler --cov-branch --cov-context=test \
  --cov-report= -q -rs > /tmp/dm55822-final.txt 2>&1
cp .coverage ~/dm55822/final.coverage
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev python tests/_migration/coverage_tool.py \
  gate ~/dm55822/baseline.coverage ~/dm55822/final.coverage
grep -oE "[0-9]+ skipped" /tmp/dm55822-baseline.txt /tmp/dm55822-final.txt
tail -3 /tmp/dm55822-final.txt
```

Expected: `lost 0` for lines and arcs; final skip count not above baseline.

- [ ] **Step 2: Measure the result honestly**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest \
  tests/test_butler*.py tests/test_datastore*.py -q -p no:randomly 2>&1 | tail -2
```

Compare against the 86.16s / 535-test baseline in the Global Constraints. Report what was actually achieved, not the 31.6s ceiling — if the coverage evidence forced keeping most of the postgres axis, say so.

- [ ] **Step 3: Check the mapping file is complete**

Every test in the pre-migration file list must appear exactly once. Cross-check against the baseline report:

```bash
grep -oE "tests/test_(butler|datastore)\.py::[A-Za-z0-9_]+::[A-Za-z0-9_]+" /tmp/dm55822-baseline.txt | sort -u > /tmp/dm55822-orig-ids.txt
wc -l /tmp/dm55822-orig-ids.txt
```

For each id, confirm a row in `tests/_migration/mapping.md`. Paste the finished table into the DM-55822 ticket before deleting it.

- [ ] **Step 4: Write the changelog fragment**

`doc/changes/DM-55822.misc.md`:

```markdown
Reorganized the butler and datastore test suites to use pytest fixtures instead of unittest subclass hierarchies.
The four configuration axes (registry backend, datastore type, butler client, and repository layout) are now independently selectable fixtures, so adding a test no longer implicitly adds one per backend combination.
Shared fixtures are available to downstream packages via `pytest_plugins = ["lsst.daf.butler.tests.fixtures"]`.
```

- [ ] **Step 5: Delete the migration scaffolding**

```bash
git rm -r tests/_migration/
```

- [ ] **Step 6: Final verification**

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/ -q -p no:randomly 2>&1 | tail -3
env -u PYTHONPATH uv run --all-extras --dev ruff check .
env -u PYTHONPATH uv run --all-extras --dev mypy python/lsst/daf/butler/tests/fixtures.py
rg -n "unittest|self\.assert" tests/test_butler_*.py tests/test_datastore_*.py tests/conftest.py
git status --short
```

Expected: suite green, ruff clean, mypy clean, the `rg` finds nothing, and the working tree is clean apart from the staged deletions.

Also confirm the run without a fixed order and under xdist, since CI uses both:

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest \
  tests/test_butler*.py tests/test_datastore*.py -q 2>&1 | tail -2
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest \
  tests/test_butler*.py tests/test_datastore*.py -q -n 3 2>&1 | tail -2
```

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "Close out DM-55822 test reorganization

Final gate against the pre-migration baseline reports zero lost lines and
zero lost arcs, with the skip count unchanged. The migration scaffolding
is deleted; the test mapping is recorded on the ticket."
```

---

## Notes for the executor

**When the gate fails.** Do not adjust the tool to make it pass. A non-empty lost set means a real code path stopped being exercised. Find the test that covered it in the baseline database:

```bash
sqlite3 ~/dm55822/baseline.coverage \
  "select c.context from context c join arc a on a.context_id = c.id
   join file f on f.id = a.file_id
   where f.path like '%<file>%' and a.fromno = <n>"
```

**When a marginal query returns nothing.** Check the pattern against real context names before concluding the axis is redundant — `NO CONTEXTS matched` and "0 unique" mean opposite things, and the tool distinguishes them deliberately.

**When a test only passes in one order.** `-p no:randomly` is used throughout for reproducibility, but CI runs with random ordering and `-n 3`. Before the final commit, run once without `-p no:randomly` and once with `-n 3` to catch order dependence introduced by a shared fixture.

**Do not fix library bugs.** If a test fails because the library is wrong, record it in the mapping file, mark the test `xfail` with a reason naming the new ticket, and move on. A library edit invalidates every coverage comparison on the branch.
