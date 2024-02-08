# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ("NonemptyMapping",)

from collections.abc import Callable, Iterator, Mapping
from typing import Any, TypeVar, overload

_K = TypeVar("_K")
_T = TypeVar("_T")
_V = TypeVar("_V", covariant=True)


class NonemptyMapping(Mapping[_K, _V]):
    """A `Mapping` that implicitly adds values (like
    `~collections.defaultdict`) but treats any that evaluate to `False` as not
    present.

    Parameters
    ----------
    default_factory : `~collections.abc.Callable`
        A callable that takes no arguments and returns a new instance of the
        value type.

    Notes
    -----
    Unlike `~collections.defaultdict`, this class implements only
    `collections.abc.Mapping`, not `~collections.abc.MutableMapping`, and hence
    it can be modified only by invoking ``__getitem__`` with a key that does
    not exist.  It is expected that the value type will be a mutable container
    like `set` or `dict`, and that an empty nested container should be
    considered equivalent to the absence of a key.
    """

    def __init__(self, default_factory: Callable[[], _V]) -> None:
        self._mapping: dict[_K, _V] = {}
        self._next: _V = default_factory()
        self._default_factory = default_factory

    def __len__(self) -> int:
        return sum(bool(v) for v in self._mapping.values())

    def __iter__(self) -> Iterator[_K]:
        for key, values in self._mapping.items():
            if values:
                yield key

    def __getitem__(self, key: _K) -> _V:
        # We use setdefault with an existing empty inner container (_next),
        # since we expect that to usually return an existing object and we
        # don't want the overhead of making a new inner container each time.
        # When we do insert _next, we replace it.
        if (value := self._mapping.setdefault(key, self._next)) is self._next:
            self._next = self._default_factory()
        return value

    # We don't let Mapping implement `__contains__` or `get` for us, because we
    # don't want the side-effect of adding an empty inner container from
    # calling `__getitem__`.

    def __contains__(self, key: Any) -> bool:
        return bool(self._mapping.get(key))

    @overload
    def get(self, key: _K) -> _V | None: ...

    @overload
    def get(self, key: _K, default: _T) -> _V | _T: ...

    def get(self, key: _K, default: Any = None) -> Any:
        if value := self._mapping.get(key):
            return value
        return default
