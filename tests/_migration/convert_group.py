"""Convert one group of test classes to their pytest form, in place.

Phase 2 of the history rewrite replaces `unittest` classes with the pytest
functions they became, one group at a time, without moving them to another
file. The converted text is not written by hand: it is taken from the derived
target state, so the end of the phase reproduces that state exactly and each
commit shows a reviewer the classes and the functions side by side.

Usage::

    python tests/_migration/convert_group.py <file> <class> [<class> ...]

``<file>`` is the working file, for example ``tests/test_datastore.py``. Each
``<class>`` is a class to remove. The functions to add are looked up in
``mapping.md``, which records what every original test became.
"""

from __future__ import annotations

import ast
import pathlib
import re
import subprocess
import sys

TARGET = "dm55822-S2"
"""Git tag holding the converted two-file state this phase reproduces."""

MAPPING = pathlib.Path(__file__).parent / "mapping.md"
"""File recording the original node id of every converted test."""

QUOTED = re.compile(r"`([^`]+)`")
"""A backtick-quoted item inside a table cell."""


def _function_name(item: str) -> str | None:
    """Return the function name a quoted mapping item names, if any.

    An item is a whole node id, a bare parametrized test name, or just the
    file prefix of the ids that follow it, which names no function.

    Parameters
    ----------
    item : `str`
        Backtick-quoted text from the mapping's "new node id" cell.

    Returns
    -------
    name : `str` or `None`
        The function name, or `None` if the item names none.
    """
    tail = item.rsplit("::", 1)[-1].split("[")[0].strip()
    return tail or None


def read_mapping() -> dict[str, set[str]]:
    """Map each original ``file::Class`` to the new function names it became.

    Returns
    -------
    groups : `dict` [`str`, `set` [`str`]]
        Keys are ``tests/test_x.py::ClassName``; values are the bare names of
        the functions those tests became.
    """
    groups: dict[str, set[str]] = {}
    for line in MAPPING.read_text().splitlines():
        if not line.startswith("|"):
            continue
        cells = line.split("|")
        if len(cells) < 4:
            continue
        old_items = QUOTED.findall(cells[1])
        if len(old_items) != 1:
            continue
        parts = old_items[0].split("::")
        if len(parts) != 3 or not parts[0].startswith("tests/"):
            continue
        key = f"{parts[0].strip()}::{parts[1].strip()}"

        # The new cell may hold several node ids, and a leading one may carry
        # the file name for those that follow it.
        names = {name for item in QUOTED.findall(cells[2]) if (name := _function_name(item)) is not None}
        if names:
            groups.setdefault(key, set()).update(names)
    return groups


def definitions(src: str) -> dict[str, tuple[int, int, str]]:
    """Locate every top-level definition in a source file.

    Parameters
    ----------
    src : `str`
        Python source.

    Returns
    -------
    defs : `dict` [`str`, `tuple` [`int`, `int`, `str`]]
        Name to its (first line, last line, source text), 1-indexed and
        inclusive, with decorators included.
    """
    tree = ast.parse(src)
    out: dict[str, tuple[int, int, str]] = {}
    for node in tree.body:
        if not isinstance(node, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        first = min([node.lineno, *(d.lineno for d in node.decorator_list)])
        out[node.name] = (first, node.end_lineno, ast.get_source_segment(src, node, padded=True) or "")
    return out


def bindings(src: str) -> dict[str, tuple[int, int, str]]:
    """Locate every top-level statement, keyed by each name it binds.

    Covers assignments and their trailing docstrings as well as definitions,
    so a converted test can pull in the parametrize lists and fixtures it
    needs and not only the functions the mapping names.

    Parameters
    ----------
    src : `str`
        Python source.

    Returns
    -------
    binds : `dict` [`str`, `tuple` [`int`, `int`, `str`]]
        Bound name to its statement's (first line, last line, source text).
    """
    lines = src.splitlines(keepends=True)
    tree = ast.parse(src)
    out: dict[str, tuple[int, int, str]] = {}
    body = tree.body
    for i, node in enumerate(body):
        names: list[str] = []
        if isinstance(node, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            names = [node.name]
        elif isinstance(node, ast.Assign):
            names = [t.id for t in node.targets if isinstance(t, ast.Name)]
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names = [node.target.id]
        if not names:
            continue
        decorators = getattr(node, "decorator_list", [])
        first = min([node.lineno, *(d.lineno for d in decorators)])
        end = node.end_lineno
        # A bare string statement immediately after an assignment is its
        # docstring and belongs with it.
        if not isinstance(node, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            nxt = body[i + 1] if i + 1 < len(body) else None
            if (
                isinstance(nxt, ast.Expr)
                and isinstance(nxt.value, ast.Constant)
                and isinstance(nxt.value.value, str)
            ):
                end = nxt.end_lineno
        text = "".join(lines[first - 1 : end])
        for name in names:
            out[name] = (first, end, text)
    return out


def free_names(text: str) -> set[str]:
    """Return every bare name a snippet reads.

    Parameters
    ----------
    text : `str`
        Python source for one or more top-level statements.

    Returns
    -------
    names : `set` [`str`]
        Names loaded by the snippet, including attribute bases and decorators.
    """
    return {
        node.id
        for node in ast.walk(ast.parse(text))
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
    }


def import_lines(src: str) -> dict[str, str]:
    """Return the import statements of a file, keyed by the name each binds.

    Parameters
    ----------
    src : `str`
        Python source.

    Returns
    -------
    imports : `dict` [`str`, `str`]
        Bound name to the single-name import statement that provides it.
    """
    out: dict[str, str] = {}
    for node in ast.parse(src).body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                bound = alias.asname or alias.name.split(".")[0]
                out[bound] = f"import {alias.name}" + (f" as {alias.asname}" if alias.asname else "")
        elif isinstance(node, ast.ImportFrom) and node.module != "__future__":
            module = "." * node.level + (node.module or "")
            for alias in node.names:
                bound = alias.asname or alias.name
                spec = alias.name + (f" as {alias.asname}" if alias.asname else "")
                out[bound] = f"from {module} import {spec}"
    return out


def target_source(path: str) -> str:
    """Return a file's content at the target tag.

    Parameters
    ----------
    path : `str`
        Repository-relative path.

    Returns
    -------
    src : `str`
        File content at `TARGET`.
    """
    return subprocess.run(
        ["git", "show", f"refs/tags/{TARGET}:{path}"], capture_output=True, text=True, check=True
    ).stdout


def main() -> int:
    """Rewrite one group of classes into their converted form.

    Returns
    -------
    status : `int`
        Zero on success.
    """
    if len(sys.argv) < 3:
        print(__doc__)
        return 2
    path, class_names = sys.argv[1], sys.argv[2:]

    working = pathlib.Path(path)
    src = working.read_text()
    target = target_source(path)
    have = bindings(src)
    want = bindings(target)
    groups = read_mapping()

    missing = [c for c in class_names if c not in have]
    if missing:
        print(f"not present in {path}: {', '.join(missing)}", file=sys.stderr)
        return 1

    # Which functions this group became.
    wanted: set[str] = set()
    for name in class_names:
        wanted |= groups.get(f"{path}::{name}", set())
    unknown = sorted(wanted - set(want))
    if unknown:
        print(f"mapping names functions absent from {TARGET}: {', '.join(unknown)}", file=sys.stderr)
        return 1
    if not wanted:
        print(f"the mapping names no functions for {', '.join(class_names)}", file=sys.stderr)
        return 1

    # The functions pull in the parametrize lists, fixtures and helpers they
    # read, transitively, so a group arrives complete rather than in pieces.
    going = {c for c in class_names}
    needed = set(wanted)
    frontier = set(wanted)
    while frontier:
        reads: set[str] = set()
        for name in frontier:
            reads |= free_names(want[name][2])
        frontier = {r for r in reads if r in want and r not in needed and r not in going}
        needed |= frontier

    to_add = sorted(needed - (set(have) - going), key=lambda n: want[n][0])
    if not to_add:
        print(f"nothing to add for {', '.join(class_names)}", file=sys.stderr)
        return 1

    # Replace the span the classes occupy, so the diff shows the two forms
    # against each other rather than a deletion and an unrelated addition.
    spans = sorted((have[c][0], have[c][1]) for c in class_names)
    lines = src.splitlines(keepends=True)
    inserted = "\n\n".join(want[n][2].rstrip("\n") for n in to_add) + "\n"
    first, last = spans[0][0], spans[-1][1]
    for start, end in reversed(spans[1:]):
        del lines[start - 1 : end]
    lines[first - 1 : spans[0][1]] = [inserted]
    out = "".join(lines)

    # Carry over any import the added code needs and this file lacks.
    target_imports = import_lines(target)
    present = set(import_lines(out))
    wanted_imports = sorted(
        {
            target_imports[n]
            for n in free_names(inserted)
            if n in target_imports and n not in present and n not in want
        }
    )
    if wanted_imports:
        marker = "\nTESTDIR"
        at = out.index(marker) if marker in out else None
        block = "".join(f"{line}\n" for line in wanted_imports)
        out = out[:at] + "\n" + block + out[at:] if at is not None else block + out

    working.write_text(out)

    print(f"{path}: removed {len(class_names)} classes, added {len(to_add)} definitions")
    for name in to_add:
        kind = "test" if name in wanted else "support"
        print(f"    + {name}  ({kind})")
    for line in wanted_imports:
        print(f"    + {line}")
    print(f"  (spanned lines {first}..{last}; run ruff format and the tests)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
