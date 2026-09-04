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
    """Return True if ``path`` is library code the gate measures."""
    return "/lsst/daf/butler/" in path and not any(e in path for e in EXCLUDED)


def _load(
    db_path: str,
) -> tuple[dict[int, set[tuple[str, int]]], dict[int, set[tuple[str, int, int]]], dict[int, str]]:
    """Return per-context line and arc sets, plus the id to name map."""
    conn = sqlite3.connect(db_path)
    # Store paths relative to the package root. The baseline and the run under
    # test can come from different checkouts of the same source, and an
    # absolute path would then differ for every file.
    files = {i: _relative(p) for i, p in conn.execute("select id, path from file") if _included(p)}
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

    # In branch mode coverage.py records only arcs and derives line coverage
    # from them, leaving line_bits empty. Deriving the same way keeps the line
    # half of the gate meaningful instead of silently comparing empty sets.
    # Verified to reproduce CoverageData.lines() exactly.
    if not lines and arcs:
        for ctx_id, ctx_arcs in arcs.items():
            lines[ctx_id] = {(path, n) for path, f, t in ctx_arcs for n in (f, t) if n > 0}

    if not lines or not arcs:
        raise SystemExit(
            f"{db_path}: no {'lines' if not lines else 'arcs'} for the measured set. "
            "A gate over an empty set always passes, so this is refused."
        )

    return lines, arcs, contexts


def _matches(name: str, pattern: str) -> bool:
    """Return True if a context name matches the (substring) pattern."""
    return pattern.strip("%") in name


def marginal(db_path: str, pattern: str) -> int:
    """Print lines and arcs covered only by contexts matching ``pattern``.

    Parameters
    ----------
    db_path : `str`
        Path to a coverage database recorded with per-test contexts.
    pattern : `str`
        Substring matched against context names.

    Returns
    -------
    status : `int`
        0 on success, 2 if no context matched the pattern.
    """
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


def gate(baseline_db: str, new_db: str) -> int:
    """Report anything covered in the baseline but not in the new run.

    Parameters
    ----------
    baseline_db : `str`
        Path to the pre-migration coverage database.
    new_db : `str`
        Path to the coverage database being checked.

    Returns
    -------
    status : `int`
        0 if nothing was lost, 1 otherwise.
    """
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


def _relative(path: str) -> str:
    """Strip the machine-specific prefix so the tripwire is portable."""
    marker = "/lsst/daf/butler/"
    i = path.find(marker)
    return "lsst/daf/butler/" + path[i + len(marker) :] if i >= 0 else path


def summary(db_path: str, out_path: str) -> int:
    """Write per-file covered line and arc counts as a tripwire.

    Parameters
    ----------
    db_path : `str`
        Path to the coverage database to summarize.
    out_path : `str`
        Path of the JSON file to write.

    Returns
    -------
    status : `int`
        Always 0.
    """
    lines, arcs, _ = _load(db_path)
    per_file: dict[str, dict[str, int]] = defaultdict(lambda: {"lines": 0, "arcs": 0})
    for path, _ in set().union(*lines.values()) if lines else set():
        per_file[_relative(path)]["lines"] += 1
    for path, _, _ in set().union(*arcs.values()) if arcs else set():
        per_file[_relative(path)]["arcs"] += 1
    with open(out_path, "w") as fh:
        json.dump(dict(sorted(per_file.items())), fh, indent=2, sort_keys=True)
    print(f"wrote {out_path} covering {len(per_file)} files")
    return 0


if __name__ == "__main__":
    cmd, *rest = sys.argv[1:]
    sys.exit({"marginal": marginal, "gate": gate, "summary": summary}[cmd](*rest))
