"""Concatenate the split test files back into the two pre-migration files.

Derives the state the deduplication commits act on from the final split tree.
The inverse of the file split, so splitting the output again must reproduce the
tree this was run against.
"""

from __future__ import annotations

import ast
import pathlib
import sys

GROUPS = {
    "tests/test_butler.py": (
        "Tests for Butler.",
        [
            "tests/test_butler_config_repo.py",
            "tests/test_butler_put_get.py",
            "tests/test_butler_lifecycle.py",
            "tests/test_butler_collections.py",
            "tests/test_butler_ingest.py",
            "tests/test_butler_import_export.py",
            "tests/test_butler_transfers.py",
            "tests/test_butler_null_datastore.py",
        ],
    ),
    "tests/test_datastore.py": (
        "Tests for the datastore implementations themselves.",
        [
            "tests/test_datastore_file.py",
            "tests/test_datastore_constraints.py",
            "tests/test_datastore_cache.py",
            "tests/test_datastore_null.py",
            "tests/test_datastore_records.py",
        ],
    ),
}
"""Target file to its module docstring and the files that make it up.

Source order follows the pre-migration file, so that the split reads as motion.
"""

PREAMBLE_TYPES = (ast.Import, ast.ImportFrom)
"""Node types that belong to the import block rather than the body."""


def is_type_checking_block(node: ast.stmt) -> bool:
    """Return whether a statement is an ``if TYPE_CHECKING:`` guard.

    Parameters
    ----------
    node : `ast.stmt`
        Top-level statement to classify.

    Returns
    -------
    guard : `bool`
        `True` if the statement is the type-checking guard.
    """
    return isinstance(node, ast.If) and isinstance(node.test, ast.Name) and node.test.id == "TYPE_CHECKING"


def split_file(path: str) -> tuple[list[str], list[ast.stmt], list[str], str]:
    """Separate a source file into its licence header, imports and body.

    Parameters
    ----------
    path : `str`
        File to read.

    Returns
    -------
    header : `list` [`str`]
        Licence header lines, up to the module docstring.
    preamble : `list` [`ast.stmt`]
        Import statements and any type-checking guard.
    body : `list` [`str`]
        Every line after the last preamble statement.
    source : `str`
        The whole file, so callers can recover exact source segments.
    """
    src = pathlib.Path(path).read_text()
    lines = src.splitlines(keepends=True)
    tree = ast.parse(src)

    imports: list[ast.stmt] = []
    type_checking: list[ast.stmt] = []
    last_end = 0
    for node in tree.body:
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
            last_end = node.end_lineno  # module docstring
        elif isinstance(node, PREAMBLE_TYPES):
            imports.append(node)
            last_end = node.end_lineno
        elif is_type_checking_block(node):
            type_checking.append(node)
            last_end = node.end_lineno
        else:
            break

    header_end = next(
        i for i, line in enumerate(lines) if line.startswith('"""') or line.startswith("from __future__")
    )
    return lines[:header_end], imports + type_checking, lines[last_end:], src


def render_imports(nodes: list[ast.stmt], sources: dict[int, str]) -> str:
    """Merge import statements from several files into one block.

    Duplicates collapse. A statement carrying a ``noqa`` comment, or a
    type-checking guard, is emitted verbatim rather than rebuilt, so its
    suppression survives.

    Parameters
    ----------
    nodes : `list` [`ast.stmt`]
        Import statements gathered from every source file.
    sources : `dict` [`int`, `str`]
        Exact source text of each node, keyed by `id`.

    Returns
    -------
    block : `str`
        The merged import block.
    """
    plain: set[str] = set()
    from_mod: dict[str, set[str]] = {}
    verbatim: list[str] = []
    future: set[str] = set()

    for node in nodes:
        text = sources[id(node)]
        if "noqa" in text or is_type_checking_block(node):
            verbatim.append(text)
            continue
        if isinstance(node, ast.Import):
            for alias in node.names:
                plain.add(f"import {alias.name}" + (f" as {alias.asname}" if alias.asname else ""))
        else:
            assert isinstance(node, ast.ImportFrom)
            module = "." * node.level + (node.module or "")
            target = future if module == "__future__" else from_mod.setdefault(module, set())
            for alias in node.names:
                target.add(alias.name + (f" as {alias.asname}" if alias.asname else ""))

    out: list[str] = []
    if future:
        out.append(f"from __future__ import {', '.join(sorted(future))}\n")
        out.append("\n")
    out.extend(sorted(s + "\n" for s in plain))
    for module in sorted(from_mod):
        names = ", ".join(sorted(from_mod[module]))
        out.append(f"from {module} import {names}\n")
    out.extend(v if v.endswith("\n") else v + "\n" for v in verbatim)
    return "".join(out)


def dedupe_module_constants(text: str) -> str:
    """Drop repeated definitions of a constant the source files shared.

    Every source file carries its own ``TESTDIR``. They must agree: a
    disagreement means the split edited code rather than moving it, and has to
    be resolved before the rewrite can proceed.

    Parameters
    ----------
    text : `str`
        Concatenated file content.

    Returns
    -------
    text : `str`
        The same content with only the first definition of each constant.

    Raises
    ------
    SystemExit
        Raised if two source files disagree about a constant's value.
    """
    out: list[str] = []
    first: dict[str, str] = {}
    for line in text.splitlines(keepends=True):
        if line.startswith("TESTDIR = "):
            if "TESTDIR" in first:
                if first["TESTDIR"] != line:
                    raise SystemExit(f"TESTDIR differs between sources:\n  {first['TESTDIR']}  {line}")
                continue
            first["TESTDIR"] = line
        out.append(line)
    return "".join(out)


def main() -> int:
    """Write each concatenated file over its split counterparts.

    Returns
    -------
    status : `int`
        Process exit status.
    """
    for target, (docstring, sources) in GROUPS.items():
        header: list[str] = []
        all_imports: list[ast.stmt] = []
        node_src: dict[int, str] = {}
        bodies: list[str] = []

        for path in sources:
            hdr, imports, body, src = split_file(path)
            if not header:
                header = hdr
            for node in imports:
                node_src[id(node)] = ast.get_source_segment(src, node) or ""
                all_imports.append(node)
            bodies.append("".join(body).strip("\n"))

        text = (
            "".join(header)
            + f'"""{docstring}"""\n\n'
            + render_imports(all_imports, node_src)
            + "\n\n"
            + "\n\n\n".join(bodies)
            + "\n"
        )
        text = dedupe_module_constants(text)
        pathlib.Path(target).write_text(text)
        print(f"wrote {target}: {len(text.splitlines())} lines from {len(sources)} files")
    return 0


if __name__ == "__main__":
    sys.exit(main())
