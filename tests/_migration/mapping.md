# DM-55822 test mapping

Migration scaffolding. Deleted before merge, after being pasted into the ticket.

The diff is too large to review by reading, and test count cannot serve as a
check because deduplication reduces it deliberately.
This file records what happened to every test in the pre-migration
`tests/test_butler.py` and `tests/test_datastore.py`, so a reviewer can check
the dropped list against the coverage evidence.

The coverage gate proves nothing was lost.
This file explains why each removal was safe.

## Conventions

- **Moved:** new nodeid, when a test simply changed file or name.
- **Parametrized:** new nodeid with the parameter set, when several old nodeids
  collapse into one function.
- **Dropped:** `dropped: <axis> — marginal set empty (N lines, N arcs checked)`,
  citing the `coverage_tool.py marginal` output that justified it.
- **Findings:** anything noticed but deliberately not fixed here, because no
  library change may ride on this branch.

## Renames and behavior notes

| Item | Note |
| --- | --- |

## Findings for separate tickets

| Finding | Where | Why not fixed here |
| --- | --- | --- |

## Test mapping

| Original nodeid | New nodeid(s) or disposition |
| --- | --- |
