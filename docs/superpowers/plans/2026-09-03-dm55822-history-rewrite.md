# DM-55822 History Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite the `tickets/DM-55822` branch so its history shows the test migration as four reviewable phases, each with a line-by-line diff, then force-push to PR #1420.

**Architecture:** The final tree is already correct and must not change. Build the branch forwards from `main`, but *derive* the intermediate states backwards from `HEAD` wherever a state can be derived, so the end state is guaranteed to match. Only the phase-1 state has to be hand-written. The filenames `tests/test_butler.py` and `tests/test_datastore.py` survive until the last phase, which is the entire point: a reviewer who could previously see only "new file, 3682 lines" now sees each change in place.

**Tech Stack:** pytest 9, pytest-xdist, uv, git rebase/reset, ruff, mypy.

**Spec:** `docs/superpowers/specs/2026-08-25-butler-test-pytest-migration-design.md` (the original migration design, unchanged by this plan) and the review on <https://github.com/lsst/daf_butler/pull/1420> by dhirving, whose four numbered steps this plan implements.

## Global Constraints

- **The tree at the final commit MUST be byte-identical to the current `HEAD` tree** (`e88c1d563` plus `820444f40`). This is the acceptance test for the whole rewrite. Verify with `git diff --stat <old-head> <new-head>` returning empty.
- **Every commit must be green.** Run the migrated suite at every phase boundary and at every commit that changes test code. A red intermediate commit makes `git bisect` and per-commit CI useless, which is the reason the phases exist.
- **Test environment is uv, never the EUPS stack.** `~/pyenv/bin/uv sync --locked --all-extras --dev`, then `.venv/bin/python -m pytest -n 8`. A bare `uv sync` uninstalls the `server` and `postgres` extras and 105 postgres tests then skip silently, producing a green run that proves nothing.
- **Never push.** Build the branch locally. The force-push to `tickets/DM-55822` is the user's to run.
- **`cloned` and `server-postgres` axes stay.** Restored in `820444f40` after review. Phase 3 must not remove them.
- **Ruff and mypy clean at every commit.** `pre-commit` runs on commit and will block otherwise.
- **`make_butler_repo` is public only for the duration of phase 1.** A `unittest.TestCase` cannot consume the `butler_repo` fixture, so phase 1 lifts the fixture body into a public context manager in `fixtures.py` and the classes call that. A commit at the end of phase 2, once the last class is gone, restores `fixtures.py` byte-identically from `refs/tags/dm55822-target`, so the branch adds nothing permanent to the module's API and the acceptance test still holds.
- **Everything that is dropped before merge lives in commits that touch nothing else.** `docs/superpowers/` is not shipped in `daf_butler` by convention, so its commits must be droppable with a `git rebase --onto`, never entangled with a code change. The same holds for `tests/_migration/`, which additionally deletes itself in a closing commit. A commit that mixes either with test code cannot be dropped and has to be rewritten by hand.
- Original file sizes for reference: `tests/test_butler.py` 3682 lines, `tests/test_datastore.py` 2467 lines.

---

## The state derivation

Five trees. Only `S1` is hand-written; the rest are derived, which is what guarantees `S4 == HEAD`.

| State | Content | How it is obtained |
| --- | --- | --- |
| `S0` | `main` | Given. |
| `S1` | Original unittest classes, setup delegated to `ButlerHarness` | **Hand-written.** The only creative work in this plan. |
| `S2` | Every test a pytest function, still in the two original files, no drops | Derived: `S3` with the five dedup commits reverted. |
| `S3` | `S2` with the agreed drops applied, still two files | Derived: `S4` with the twelve files concatenated back into two. |
| `S4` | Current `HEAD` tree | Given. Must not change. |

Each derived state is committed and tagged, so the rewrite can be restarted
from any of them:

| Tag | State | Collected | Result |
| --- | --- | --- | --- |
| `dm55822-target` | `S4`, the split tree the rewrite must reproduce | 517 | 497 passed, 11 skipped, 9 xfailed |
| `dm55822-S3` | `S3`, two files, drops applied | 517 | 497 passed, 11 skipped, 9 xfailed |
| `dm55822-S2-split` | `S2` still split, axes restored | 642 | 621 passed, 11 skipped, 10 xfailed |
| `dm55822-S2` | `S2`, two files, axes restored | 642 | 621 passed, 11 skipped, 10 xfailed |

Refer to them as `refs/tags/<name>`; a bare name is ambiguous if a branch of
the same name exists.

`tests/_migration/verify_motion.py <tag>` confirms the split is pure motion at
each boundary: 152 top-level definitions match byte for byte between `S3` and
`dm55822-target`, and 153 between `S2` and `dm55822-S2-split`.

The +125 executions `S2` holds over `S3` reconcile exactly against the three
reverted commits: 32 for the `explicit-root` axis, 6 more from the
`repo_layout` parametrization those six tests regain (matching `01f5e0a42`'s
claim of 38), 40 for `trash`, 40 for `posix-no-checksums`, 4 for
`chained-memory` and 3 for the outfile layouts.

Deriving `S2` from `S3` works because the dedup commits (`178b031c5`, `01f5e0a42`, `345827ba8`, `a3c8a6ca9`, `def1bb2fe`) operated on already-converted pytest code, so reverting them yields pytest-style tests, not unittest ones.

The recovered mapping file at `git show a56a91752^:tests/_migration/mapping.md` (818 lines, ~550 of them a per-test old-nodeid → new-nodeid table) is the reference for phases 2 through 4. It also documents every deliberate behavior change made during the original conversion. It answers dhirving's "I can't tell what changed" directly and was deleted before merge; Task 1 restores it.

---

### Task 1: Restore the migration mapping and record the plan

**Files:**
- Create: `tests/_migration/mapping.md` (recovered from `a56a91752^`)
- Create: `docs/superpowers/plans/2026-09-03-dm55822-history-rewrite.md` (this file)

**Interfaces:**
- Produces: `tests/_migration/mapping.md`, the old→new test mapping every later task consults.

- [ ] **Step 1: Recover the file**

```bash
mkdir -p tests/_migration
git show a56a91752^:tests/_migration/mapping.md > tests/_migration/mapping.md
git show a56a91752^:tests/_migration/README.md  > tests/_migration/README.md 2>/dev/null || true
wc -l tests/_migration/mapping.md   # expect 818
```

- [ ] **Step 2: Commit on the current branch tip**

This commit is scaffolding for the rewrite and does **not** survive into the final history. It exists so the mapping is recoverable if the rewrite is interrupted.

```bash
git add tests/_migration docs/superpowers/plans/2026-09-03-dm55822-history-rewrite.md
git commit -m "Restore the migration mapping and add the history-rewrite plan"
```

- [ ] **Step 3: Tag the current tip as the acceptance reference**

```bash
git tag dm55822-target
git rev-parse dm55822-target^{tree}   # record this; it is the acceptance value minus the two files added above
```

---

### Task 2: Derive S3 by concatenating the split files

**Files:**
- Create: `/private/tmp/.../scratchpad/S3/tests/test_butler.py`
- Create: `/private/tmp/.../scratchpad/S3/tests/test_datastore.py`

**Interfaces:**
- Consumes: the twelve `tests/test_butler_*.py` and `tests/test_datastore_*.py` files at `HEAD`.
- Produces: the two-file `S3` tree, input to Task 3.

- [ ] **Step 1: Establish the file → source mapping**

From `tests/_migration/mapping.md`, the twelve files and their origin:

| New file | Origin |
| --- | --- |
| `test_butler_put_get.py` | `ButlerPutGetTests` |
| `test_butler_config_repo.py` | `ButlerConfigTests`, `ButlerMakeRepoOutfile*TestCase` |
| `test_butler_lifecycle.py` | `ButlerTests` (lifecycle half), `TransactionTestError` |
| `test_butler_collections.py` | `ButlerTests` (collection-chain half) |
| `test_butler_ingest.py` | `FileDatastoreButlerTests` (ingest half) |
| `test_butler_import_export.py` | `FileDatastoreButlerTests` (import/export half) |
| `test_butler_transfers.py` | `DatastoreTransfers`, `TransferDatasetsInPlace` |
| `test_butler_null_datastore.py` | `NullDatastoreTestCase` (butler) |
| `test_datastore_file.py` | `DatastoreTests`, `CleanupPosixDatastoreTestCase` |
| `test_datastore_constraints.py` | `DatastoreConstraintsTests`, `ChainedDatastorePerStoreConstraintsTests` |
| `test_datastore_cache.py` | `DatastoreCacheTestCase` |
| `test_datastore_records.py` | `DatasetRefURIsTestCase`, `StoredFileInfoTestCase`, `TestDatastoreRecordTable` |
| `test_datastore_null.py` | `NullDatastoreTestCase` (datastore) |

- [ ] **Step 2: Concatenate**

Body order inside each concatenated file follows the *original* file's order, not alphabetical, so that the Task 6 split diff reads as pure motion. Merge the import blocks, dedupe, and let ruff sort.

```bash
S=/private/tmp/claude-501/-Users-timj-work-lsstsw-build-daf-butler/01583f60-5663-46e0-85e3-2de1fd3af33a/scratchpad
mkdir -p $S/S3/tests
# Assemble by hand: one licence header, one merged import block, then each
# file's body in original-file order with its own header and imports stripped.
```

- [ ] **Step 3: Verify S3 collects and passes**

Copy `S3` over the working tree on a scratch branch and run:

```bash
.venv/bin/python -m pytest tests/test_butler.py tests/test_datastore.py -q -n 8
```

Expected: the same pass/skip/xfail totals as `HEAD` (497 passed, 11 skipped, 9 xfailed), because concatenation changes no test.

- [ ] **Step 4: Verify the split is reversible**

The check that makes Task 6 trustworthy: splitting `S3` by the table above must reproduce `HEAD` exactly.

```bash
git diff --stat dm55822-target -- tests/   # after re-splitting, expect empty
```

---

### Task 3: Derive S2 by reverting the five dedup commits

**Files:**
- Modify: `$S/S2/tests/test_butler.py`, `$S/S2/tests/test_datastore.py`

**Interfaces:**
- Consumes: `S3` from Task 2.
- Produces: `S2`, the fully converted, undedup'd two-file tree.

- [ ] **Step 1: List what phase 3 removes**

From `mapping.md`'s deduplication table, and excluding the two already restored:

| Axis | Commit | Executions removed |
| --- | --- | --- |
| explicit-root repo layout | `01f5e0a42` | see commit |
| outfile layouts (`test_put_get[outfile*]`) | `345827ba8` | 9 contexts |
| datastore `trash` profile | `a3c8a6ca9` | 80 contexts |
| `posix-no-checksums` profile | `a3c8a6ca9` | 80 contexts |
| `test_constraints[*-chained-memory]` | `a3c8a6ca9` | 4 contexts |

`178b031c5` (cloned) and `def1bb2fe` (server-postgres) are **already reverted** by `820444f40` and must not be re-applied.

- [ ] **Step 2: Revert onto the concatenated form**

The dedup commits touch `tests/butler_test_support.py` (axis lists) and individual split files. Apply the axis-list reverts directly; apply the per-file test restorations to the concatenated file.

```bash
git show 01f5e0a42 -- tests/butler_test_support.py | git apply -R
git show 345827ba8 -- tests/butler_test_support.py | git apply -R
git show a3c8a6ca9 -- tests/butler_test_support.py | git apply -R
# then hand-restore the test bodies each commit deleted, into the concatenated files
```

- [ ] **Step 3: Verify S2 is green and larger than S3**

```bash
.venv/bin/python -m pytest tests/test_butler.py tests/test_datastore.py -q -n 8
```

Expected: all pass; collected count exceeds `S3`'s 517 by the executions in the Step 1 table.

---

### Task 4: Phase 1 — hand-build S1, setup into fixtures

This is the only task with no derivable answer, and the largest. It produces the tree dhirving asked for in his step 1: "setup is moved to the fixtures, keeping the test classes as-is, just modifying the setup logic."

**Files:**
- Create: `python/lsst/daf/butler/tests/fixtures.py` (from `HEAD`, unchanged)
- Create: `tests/conftest.py` (from `HEAD`, unchanged)
- Modify: `tests/test_butler.py` — `setUp`/`setUpClass`/class attributes only
- Modify: `tests/test_datastore.py` — same
- Modify: `pyproject.toml` — the `pytest-timeout` dev dependency and `[tool.ruff]` per-file ignores
- Test: the whole suite, unchanged

**Interfaces:**
- Consumes: `main`'s `tests/test_butler.py` and `tests/test_datastore.py`.
- Produces: `ButlerHarness(butler_repo, storage_class_factory, exit_stack, default_run)` with `create_butler(run, storage_class, dataset_type_name, metrics=None) -> tuple[Butler, DatasetType]`, `create_empty_butler(run=None, writeable=None, metrics=None, cleanup=True) -> Butler`, `are_uris_equivalent(uri1, uri2) -> bool`, `remove_dataset_out_of_band(butler, ref) -> None`, and the `prediction_supported` / `trust_mode_supported` class flags. Subclasses `ClonedButlerHarness` and `ServerButlerHarness`. Also `DATASTORE_PROFILES`, `ButlerRepo`, `add_dataset_type`, `make_example_metrics`, `DEFAULT_RUN`.

The key fact that makes this possible: `ButlerHarness` and friends in `fixtures.py` are **plain classes, not fixtures**. A `unittest.TestCase` can build one in `setUp` and delegate to it. Parametrized pytest fixtures cannot drive a `TestCase`, but nothing here needs them to — the class hierarchy still supplies the axis.

- [ ] **Step 1: Land the fixture module unchanged**

```bash
git checkout dm55822-target -- python/lsst/daf/butler/tests/fixtures.py tests/conftest.py
```

`fixtures.py` at `HEAD` is already the extracted setup logic. It does not need rewriting for this phase; the classes consume it as-is.

- [ ] **Step 2: Convert one class group and run it**

Work one group at a time, committing each. Start with the datastore constraint classes, the smallest self-contained group. For each class, replace the bespoke `setUp` with harness construction:

```python
class PosixDatastoreConstraintsTestCase(DatastoreConstraintsTests, unittest.TestCase):
    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")

    def setUp(self) -> None:
        self.harness = make_datastore_harness(self, profile="posix")
```

The test method bodies do **not** change in this phase. Where a body reaches for `self.storageClassFactory`, add a forwarding property on the base class rather than editing the body:

```python
    @property
    def storageClassFactory(self) -> StorageClassFactory:
        return self.harness.storage_class_factory
```

- [ ] **Step 3: Run that group**

```bash
.venv/bin/python -m pytest tests/test_datastore.py -q -n 8 -k Constraints
```

Expected: identical pass/skip counts to `main` for the same selection. Record `main`'s numbers first:

```bash
git stash && git checkout main -- tests/ && .venv/bin/python -m pytest tests/test_datastore.py -q -n 8 -k Constraints
```

- [ ] **Step 4: Commit the group**

```bash
git add tests/test_datastore.py
git commit -m "Move the datastore constraint setup into the harness"
```

- [ ] **Step 5: Repeat for each remaining group**

One commit each, in this order. Sizes are the original class-body line spans.

| Commit | Classes | `main` lines |
| --- | --- | --- |
| datastore constraints | `DatastoreConstraintsTests` and its five subclasses, `ChainedDatastorePerStoreConstraintsTests` | 1479-1669 |
| datastore core | `DatastoreTestsBase`, `DatastoreTests`, `Posix*`, `NoChecksums`, `Trash`, `Cleanup*`, `InMemory*`, `Chained*` | 115-1479 |
| datastore cache | `DatastoreCacheTestCase` | 1669-2154 |
| datastore records | `DatasetRefURIsTestCase`, `StoredFileInfoTestCase`, `TestDatastoreRecordTable`, `NullDatastoreTestCase` | 2154-2467 |
| butler put/get | `ButlerPutGetTests` | 166-622 |
| butler lifecycle and collections | `ButlerTests` | 622-1830 |
| butler file datastore | `FileDatastoreButlerTests` | 1830-2360 |
| butler concrete classes | the twelve `*TestCase` classes | 2360-2884 |
| butler transfers | `DatastoreTransfers` and subclasses, `TransferDatasetsInPlace` | 2884-3516 |
| butler server | `ButlerServerTests`, `ButlerServerSqliteTests`, `ButlerServerPostgresTests` | 3574-3682 |

- [ ] **Step 6: Verify S1 against main's totals**

```bash
.venv/bin/python -m pytest tests/test_butler.py tests/test_datastore.py -q -n 8
```

Expected: the same collected, passed, skipped and xfailed counts as `main`. Any difference is a bug in this phase, not a deduplication — nothing is dropped until phase 3.

---

### Task 5: Phase 2 — convert to pytest style, in place

**Files:**
- Modify: `tests/test_butler.py`, `tests/test_datastore.py`
- Create: `tests/butler_test_support.py`

**Interfaces:**
- Consumes: `S1` from Task 4; the `S2` target from Task 3.
- Produces: `S2`.

Each commit converts one group from `unittest` classes to module-level functions with fixture injection, **in the same file**. A file mid-phase holds a mix of converted functions and unconverted classes; pytest collects both, so every commit stays green. The original branch already proved this pattern works (see `mapping.md`: `test_butler.py` kept a one-line `runPutGetTest` forwarding to the shared helper "so the classes still awaiting conversion share the single copy").

- [ ] **Step 1: Land the shared assertion helpers**

```bash
git checkout dm55822-target -- tests/butler_test_support.py
```

Then add the forwarding shims to `tests/test_butler.py` so the still-unconverted classes use the same helper bodies:

```python
    def runPutGetTest(self, storageClass, datasetTypeName):
        return run_put_get_test(self, storageClass, datasetTypeName)

    def assertGetComponents(self, butler, datasetRef, components, reference, collections=None):
        return assert_get_components(butler, datasetRef, components, reference, collections)
```

- [ ] **Step 2: Convert one group**

Take the group's target text verbatim from `S2` (Task 3), which is already the reviewed, converted form. Delete the corresponding classes. The diff a reviewer sees is method-body → function-body, side by side.

- [ ] **Step 3: Run the whole file after each group**

```bash
.venv/bin/python -m pytest tests/test_butler.py tests/test_datastore.py -q -n 8
```

Expected: total unchanged from `S1` throughout the phase. Parametrization replaces class inheritance one-for-one, so the count must not move.

- [ ] **Step 4: Commit and repeat**

Same group order as Task 4 Step 5. Commit messages name the group: `"Convert the datastore constraint tests to pytest style"`.

- [ ] **Step 5: Verify S2 exactly**

```bash
diff -u $S/S2/tests/test_butler.py tests/test_butler.py
diff -u $S/S2/tests/test_datastore.py tests/test_datastore.py
```

Expected: no output. If there is output, the last group's conversion diverged from the derived target and must be corrected now, before phase 3 builds on it.

---

### Task 6: Phase 3 — apply the agreed drops

**Files:**
- Modify: `tests/butler_test_support.py`, `tests/test_butler.py`, `tests/test_datastore.py`

**Interfaces:**
- Consumes: `S2`.
- Produces: `S3`.

Each drop is its own commit, so dhirving can accept or reject them individually. That is the point of isolating this phase; two of the original seven drops were already rejected.

- [ ] **Step 1: One commit per axis**

Re-apply, in this order, each with the coverage evidence from `mapping.md` in the commit message:

1. `"Drop the explicit-root repo layout axis"` (from `01f5e0a42`)
2. `"Drop the outfile layout axes"` (from `345827ba8`)
3. `"Drop the datastore trash profile axis"` (from `a3c8a6ca9`)
4. `"Drop the no-checksum datastore axis"` (from `a3c8a6ca9`)
5. `"Drop the chained-memory constraint axis"` (from `a3c8a6ca9`)

- [ ] **Step 2: State the evidence and its limit in every message**

Each message gives the marginal-coverage numbers **and** says what coverage cannot see. dhirving rejected two drops precisely because zero marginal coverage does not mean redundant. A message that cites only the number invites the same objection.

- [ ] **Step 3: Run after each commit**

```bash
.venv/bin/python -m pytest tests/test_butler.py tests/test_datastore.py -q -n 8
```

Expected: the collected count falls by exactly the executions named in that commit; passes stay green.

- [ ] **Step 4: Verify S3 exactly**

```bash
diff -u $S/S3/tests/test_butler.py tests/test_butler.py
diff -u $S/S3/tests/test_datastore.py tests/test_datastore.py
```

Expected: no output.

---

### Task 7: Phase 4 — split into the twelve files

**Files:**
- Create: the twelve `tests/test_butler_*.py` and `tests/test_datastore_*.py`
- Delete: `tests/test_butler.py`, `tests/test_datastore.py`

**Interfaces:**
- Consumes: `S3`.
- Produces: `S4`, which must equal `dm55822-target`.

- [ ] **Step 1: Split `test_butler.py`, preserving one file's history**

`git mv` the original to the largest derived file first, so git records a rename rather than a delete-plus-add and the reviewer keeps blame for the biggest chunk.

```bash
git mv tests/test_butler.py tests/test_butler_lifecycle.py
git commit -m "Split test_butler.py into seven files by subject area"
```

Then create the other six from it in the same commit.

- [ ] **Step 2: Split `test_datastore.py` the same way**

```bash
git mv tests/test_datastore.py tests/test_datastore_file.py
git commit -m "Split test_datastore.py into five files by subject area"
```

- [ ] **Step 3: Drop the migration scaffolding**

```bash
git rm -r tests/_migration
git commit -m "Remove the migration scaffolding"
```

Paste `mapping.md` into DM-55822 before this commit; it is the record of every deliberate behavior change and is the answer to the review's central question.

- [ ] **Step 4: The acceptance test**

```bash
git diff --stat dm55822-target -- tests/ python/ pyproject.toml doc/
```

Expected: **empty**. Any output means the rewrite changed the deliverable, which it must not.

- [ ] **Step 5: Full suite, final state**

```bash
~/pyenv/bin/uv sync --locked --all-extras --dev
.venv/bin/python -m pytest tests/ -q -n 8
```

Expected: 497 passed, 11 skipped, 9 xfailed for the migrated files, and no regression elsewhere in `tests/`.

---

### Task 8: Assemble and hand over

**Files:**
- Modify: none. Git plumbing only.

- [ ] **Step 1: Confirm every commit is green**

```bash
git rebase --exec '.venv/bin/python -m pytest tests/test_butler*.py tests/test_datastore*.py -q -n 8' main
```

This runs the suite at every commit. It is slow (roughly 30 s per commit) and it is the check that makes the four-phase history worth having.

- [ ] **Step 2: Write the PR body**

Say what changed and why the history now reads the way it does: four phases, the filenames held constant until the last one, `mapping.md` pasted into the ticket, and the two restored axes called out by name so dhirving can see his review was applied.

- [ ] **Step 3: Hand the force-push to the user**

Do not push. Report the branch is ready and give the command:

```bash
git push --force-with-lease origin tickets/DM-55822
```

`--force-with-lease` rather than `--force`, so a push that would discard someone else's commit fails instead of succeeding.

---

## Settled by the user

- **The branch as it stands today does not satisfy this.** Seven commits mix `docs/superpowers/` with code: `9b0bd671d`, `508ac7710`, `7f90ed18f`, `1a7124266`, `dfe98fd9e`, `430950f56`, `125842515`. Dropping the docs from the current history would mean rewriting all seven by hand. The rewrite fixes this as a side effect, and every new commit must be checked against it:

```bash
for c in $(git log --format=%h main..HEAD); do
  files=$(git show --stat --format='' --name-only $c)
  d=$(echo "$files" | grep -c '^docs/superpowers/')
  o=$(echo "$files" | grep -vc '^docs/superpowers/')
  [ "$d" -gt 0 ] && [ "$o" -gt 0 ] && echo "MIXED: $c $(git log -1 --format=%s $c)"
done
```

- **`docs/superpowers/` stays on the PR but never merges.** It is not shipped in `daf_butler`. Keep each docs change in a commit of its own so the set can be dropped in one rebase before merge. The existing branch already has this property for its first five commits; the rewrite must preserve it.
- **Acronym names are fixed up front, not in phase 2.** `testAbsoluteURITransferDirect` and its two siblings had become `test_absolute_u_r_i_transfer_*`; they are now `test_absolute_uri_transfer_*`. A sweep for other single-letter runs across the migrated files and `mapping.md` found no others, and `DatasetRefURIsTestCase`'s methods converted cleanly. Doing this before the rewrite means `dm55822-target` is a clean reference for the phase-4 split.
