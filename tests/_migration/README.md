# DM-55822 migration scaffolding

Everything in this directory is temporary and is deleted in the closing commit
of the branch, after `mapping.md` has been pasted into the ticket.

## What this is for

The branch converts `tests/test_butler.py` and `tests/test_datastore.py` from a
`unittest` subclass lattice to pytest fixture axes, and deletes the duplicate
test executions the lattice produced.

Deciding which duplicates are safe to delete requires evidence, not judgement.
`coverage_tool.py` supplies it, from a single instrumented baseline run taken
before anything changed.

## The baseline database is not committed

With per-test coverage contexts over the whole suite it is large and binary.

| Artifact | Location |
| --- | --- |
| Baseline coverage database | `~/dm55822/baseline.coverage` |
| Baseline pytest report | `~/dm55822/baseline-report.txt` |
| Committed tripwire | `baseline_summary.json` in this directory |

`baseline_summary.json` holds per-file covered line and arc counts. Re-running
`coverage_tool.py summary` against the out-of-repository database must reproduce
it exactly; if it does not, that database is not the right one.

## Baseline provenance

Commit `de89a4fce`. Nothing in `tests/` or `python/lsst/daf/butler/` had changed
at that point; the branch had touched only `docs/` and this directory.

Result:

```
2059 passed, 30 skipped, 10 xfailed, 0 errors, 1440 subtests passed in 490.34s
```

Those are the numbers every later run is compared against. The skip figure to
use is the **30** from the summary line, not the count of `SKIPPED` reason lines
in the `-rs` block — with `-rs` pytest aggregates skips as
`SKIPPED [N] <location>: <reason>`, and there are only 11 such lines.

For reference, the two files in scope on their own, without coverage
instrumentation, were `535 passed, 10 skipped, 10 xfailed, 271 subtests` in
86.16s.

Environment, matching what `.github/workflows/build.yaml` installs:

```bash
uv sync --locked --all-extras --dev
```

Command:

```bash
env -u PYTHONPATH -u DYLD_LIBRARY_PATH uv run --all-extras --dev pytest tests/ \
    -p no:randomly --cov=lsst.daf.butler --cov-branch --cov-context=test \
    --cov-report= -q -rs
```

`PYTHONPATH` and `DYLD_LIBRARY_PATH` must be cleared. Otherwise a configured
EUPS stack shadows the virtual environment and collection dies on
`dlopen ... libsphgeom.dylib`.

`-p no:randomly` matters: a stable order keeps context names reproducible
between the baseline and later runs.

## Tool

```bash
# Lines and arcs covered ONLY by contexts matching a substring.
uv run --all-extras --dev python tests/_migration/coverage_tool.py marginal <db> <pattern>

# Anything covered in baseline but not in new. Exits non-zero if non-empty.
uv run --all-extras --dev python tests/_migration/coverage_tool.py gate <baseline-db> <new-db>

# Per-file covered line and arc counts.
uv run --all-extras --dev python tests/_migration/coverage_tool.py summary <db> <out.json>
```

The measured set is `lsst.daf.butler` **excluding** `lsst/daf/butler/tests/` and
`lsst/daf/butler/registry/tests/`. The branch adds `fixtures.py` under the first
of those, and test-support coverage is not comparable across a rewrite that
renames every file.

### Two behaviors worth knowing

`marginal` distinguishes *no contexts matched the pattern* (exit 2, loud) from
*the matched contexts cover nothing unique* (exit 0, "0 unique"). Those mean
opposite things and must never be confused: the first is a typo, the second is
a licence to delete.

`_load` refuses a database with an empty line or arc set. In branch mode
coverage.py records only arcs and leaves `line_bits` empty, so lines are derived
from arcs — verified to reproduce `CoverageData.lines()` exactly. Without that
derivation the line half of the gate would silently compare two empty sets and
always pass.

### `uv run` must always carry `--all-extras --dev`

A bare `uv run` re-syncs the environment to the default dependency groups and
silently uninstalls the extras, `fastapi` among them.

Nothing errors. The suite simply reports a much larger skip count and a mass of
setup errors, and the resulting baseline understates covered lines — which makes
the gate easier to pass rather than harder, so it fails safe in the wrong
direction. The first baseline attempt on this branch was discarded for exactly
this reason: 1392 passed, 472 skipped, **231 errors**.

## Decided: docstrings on converted tests

The repo ignores `D102`, missing docstring in a public *method*, in both the
ruff `ignore` list and pydocstyle `add-ignore`. Test methods therefore never
needed a docstring.

`D103`, the equivalent for a public *function*, is **not** ignored. Converting a
test method to a module-level function therefore makes a docstring mandatory.
In `tests/test_datastore_cache.py` that meant writing 10 of them for 15 tests.

**Decision: `"tests/*" = ["D103"]` is added to `per-file-ignores`.** The project
has already decided that methods do not need docstrings; a test does not acquire
that requirement merely by ceasing to be a method. Several hundred formulaic
docstrings restating the function name would be noise, not information.

Docstrings are still written where a test's intent is not obvious from its name.
The ten already written for `tests/test_datastore_cache.py` are accurate and are
kept.

`D101` still applies to classes, and `PT` is unaffected, so the ratchet that
stops unittest idioms returning is untouched.
