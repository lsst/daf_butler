"""Check that the file split moves code without editing it.

Compares every top-level definition in the concatenated files against its
counterpart in the split files at a given git ref. Any definition that is
missing, extra, or textually different means the split is not pure motion, and
a reviewer reading it as motion would miss a real change.
"""

from __future__ import annotations

import ast
import pathlib
import subprocess
import sys

GROUPS = {
    "tests/test_butler.py": [
        "test_butler_config_repo",
        "test_butler_put_get",
        "test_butler_lifecycle",
        "test_butler_collections",
        "test_butler_ingest",
        "test_butler_import_export",
        "test_butler_transfers",
        "test_butler_null_datastore",
    ],
    "tests/test_datastore.py": [
        "test_datastore_file",
        "test_datastore_constraints",
        "test_datastore_cache",
        "test_datastore_null",
        "test_datastore_records",
    ],
}
"""Concatenated file to the split files it is expected to account for."""

DEFAULT_TARGET = "dm55822-target"
"""Git ref holding the split files to compare against, absent an argument."""


def definitions(src: str) -> dict[str, str]:
    """Map each top-level definition to its exact source text.

    Parameters
    ----------
    src : `str`
        Python source to parse.

    Returns
    -------
    defs : `dict` [`str`, `str`]
        Definition name to the source segment that defines it, including any
        decorators.
    """
    tree = ast.parse(src)
    out: dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            out[node.name] = ast.get_source_segment(src, node, padded=True) or ""
    return out


def main() -> int:
    """Compare each concatenated file against the split files.

    Takes the git ref to compare against as the sole argument, defaulting to
    `DEFAULT_TARGET`. Each derived state has its own ref, so the same check
    runs at every phase boundary.

    Returns
    -------
    status : `int`
        Zero when every definition matches, one otherwise.
    """
    target = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_TARGET
    status = 0
    for combined, members in GROUPS.items():
        have = definitions(pathlib.Path(combined).read_text())
        want: dict[str, tuple[str, str]] = {}
        for member in members:
            blob = subprocess.run(
                ["git", "show", f"refs/tags/{target}:tests/{member}.py"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout
            for name, text in definitions(blob).items():
                want[name] = (member, text)

        missing = sorted(set(want) - set(have))
        extra = sorted(set(have) - set(want))
        changed = sorted(name for name in set(have) & set(want) if have[name] != want[name][1])

        print(f"=== {combined} vs {target}: {len(have)} definitions, {len(members)} files")
        for label, names in (("MISSING", missing), ("EXTRA", extra), ("CHANGED", changed)):
            if names:
                status = 1
                print(f"  {label} ({len(names)}):")
                for name in names[:20]:
                    where = want[name][0] if name in want else "not in the split files"
                    print(f"    {name}  [{where}]")
        if not (missing or extra or changed):
            print("  pure motion: every definition matches byte for byte")
    return status


if __name__ == "__main__":
    sys.exit(main())
