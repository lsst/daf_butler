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

__all__ = ("Progress", "ProgressBar", "ProgressHandler")

import contextlib
import logging
from abc import ABC, abstractmethod
from collections.abc import Generator, Iterable, Iterator, Sized
from contextlib import AbstractContextManager, contextmanager
from typing import ClassVar, Protocol, TypeVar

_T = TypeVar("_T", covariant=True)
_K = TypeVar("_K")
_V = TypeVar("_V", bound=Iterable)


class ProgressBar(Iterable[_T], Protocol):
    """A structural interface for progress bars that wrap iterables.

    An object conforming to this interface can be obtained from the
    `Progress.bar` method.

    Notes
    -----
    This interface is intentionally defined as the intersection of the progress
    bar objects returned by the ``click`` and ``tqdm`` packages, allowing those
    to directly satisfy code that uses this interface.
    """

    def update(self, n: int = 1) -> None:
        """Increment the progress bar by the given amount.

        Parameters
        ----------
        n : `int`, optional
            Increment the progress bar by this many steps (defaults to ``1``).
            Note that this is a relative increment, not an absolute progress
            value.
        """
        pass


class Progress:
    """Public interface for reporting incremental progress in the butler and
    related tools.

    This class automatically creates progress bars (or not) depending on
    whether a handle (see `ProgressHandler`) has been installed and the given
    name and level are enabled.  When progress reporting is not enabled, it
    returns dummy objects that can be used just like progress bars by calling
    code.

    Parameters
    ----------
    name : `str`
        Name of the process whose progress is being reported.  This is in
        general the name of a group of progress bars, not necessarily a single
        one, and it should have the same form as a logger name.
    level : `int`, optional
        A `logging` level value (defaults to `logging.INFO`).  Progress
        reporting is enabled if a logger with ``name`` is enabled for this
        level, and a `ProgressHandler` has been installed.

    Notes
    -----
    The progress system inspects the level for a name using the Python built-in
    `logging` module, and may not respect level-setting done via the
    ``lsst.log`` interface.  But while `logging` may be necessary to control
    progress bar visibility, the progress system can still be used together
    with either system for actual logging.
    """

    def __init__(self, name: str, level: int = logging.INFO) -> None:
        self._name = name
        self._level = level

    # The active handler is held in a ContextVar to isolate unit tests run
    # by pytest-xdist.  If butler codes is ever used in a real multithreaded
    # or asyncio application _and_ we want progress bars, we'll have to set
    # up per-thread handlers or similar.
    _active_handler: ClassVar[ProgressHandler | None] = None

    @classmethod
    def set_handler(cls, handler: ProgressHandler | None) -> None:
        """Set the (global) progress handler to the given instance.

        This should only be called in very high-level code that can be
        reasonably confident that it will dominate its current process, e.g.
        at the initialization of a command-line script or Jupyter notebook.

        Parameters
        ----------
        handler : `ProgressHandler` or `None`
            Object that will handle all progress reporting.  May be set to
            `None` to disable progress reporting.
        """
        cls._active_handler = handler

    def is_enabled(self) -> bool:
        """Check whether this process should report progress.

        Returns
        -------
        enabled : `bool`
            `True` if there is a `ProgressHandler` set and a logger with the
            same name and level as ``self`` is enabled.
        """
        if self._active_handler is not None:
            logger = logging.getLogger(self._name)
            if logger.isEnabledFor(self._level):
                return True
        return False

    def at(self, level: int) -> Progress:
        """Return a copy of this progress interface with a different level.

        Parameters
        ----------
        level : `int`
            A `logging` level value.  Progress reporting is enabled if a logger
            with ``name`` is enabled for this level, and a `ProgressHandler`
            has been installed.

        Returns
        -------
        progress : `Progress`
            A new `Progress` object with the same name as ``self`` and the
            given ``level``.
        """
        return Progress(self._name, level)

    def bar(
        self,
        iterable: Iterable[_T] | None = None,
        desc: str | None = None,
        total: int | None = None,
        skip_scalar: bool = True,
    ) -> AbstractContextManager[ProgressBar[_T]]:
        """Return a new progress bar context manager.

        Parameters
        ----------
        iterable : `~collections.abc.Iterable`, optional
            An arbitrary Python iterable that will be iterated over when the
            returned `ProgressBar` is.  If not provided, whether the progress
            bar is iterable is handler-defined, but it may be updated manually.
        desc : `str`, optional
            A user-friendly description for this progress bar; usually appears
            next to it.  If not provided, ``self.name`` is used (which is not
            usually a user-friendly string, but may be appropriate for
            debug-level progress).
        total : `int`, optional
            The total number of steps in this progress bar.  If not provided,
            ``len(iterable)`` is used.  If that does not work, whether the
            progress bar works at all is handler-defined, and hence this mode
            should not be relied upon.
        skip_scalar : `bool`, optional
            If `True` and ``total`` is zero or one, do not report progress.

        Returns
        -------
        bar : `contextlib.AbstractContextManager` [ `ProgressBar` ]
            A context manager that returns an object satisfying the
            `ProgressBar` interface when it is entered.
        """
        if self.is_enabled():
            if desc is None:
                desc = self._name
            handler = self._active_handler
            assert handler, "Guaranteed by `is_enabled` check above."
            if skip_scalar:
                if total is None:
                    with contextlib.suppress(TypeError):
                        # static typing says len() won't but that's why
                        # we're doing it inside a try block.
                        total = len(iterable)  # type: ignore
                if total is not None and total <= 1:
                    return _NullProgressBar.context(iterable)
            return handler.get_progress_bar(iterable, desc=desc, total=total, level=self._level)
        return _NullProgressBar.context(iterable)

    def wrap(
        self,
        iterable: Iterable[_T],
        desc: str | None = None,
        total: int | None = None,
        skip_scalar: bool = True,
    ) -> Generator[_T, None, None]:
        """Iterate over an object while reporting progress.

        Parameters
        ----------
        iterable : `~collections.abc.Iterable`
            An arbitrary Python iterable to iterate over.
        desc : `str`, optional
            A user-friendly description for this progress bar; usually appears
            next to it.  If not provided, ``self.name`` is used (which is not
            usually a user-friendly string, but may be appropriate for
            debug-level progress).
        total : `int`, optional
            The total number of steps in this progress bar.  If not provided,
            ``len(iterable)`` is used.  If that does not work, whether the
            progress bar works at all is handler-defined, and hence this mode
            should not be relied upon.
        skip_scalar : `bool`, optional
            If `True` and ``total`` is zero or one, do not report progress.

        Yields
        ------
        element
            The same objects that iteration over ``iterable`` would yield.
        """
        with self.bar(iterable, desc=desc, total=total, skip_scalar=skip_scalar) as bar:
            yield from bar

    def iter_chunks(
        self,
        chunks: Iterable[_V],
        desc: str | None = None,
        total: int | None = None,
        skip_scalar: bool = True,
    ) -> Generator[_V, None, None]:
        """Wrap iteration over chunks of elements in a progress bar.

        Parameters
        ----------
        chunks : `~collections.abc.Iterable`
            An iterable whose elements are themselves iterable.
        desc : `str`, optional
            A user-friendly description for this progress bar; usually appears
            next to it.  If not provided, ``self.name`` is used (which is not
            usually a user-friendly string, but may be appropriate for
            debug-level progress).
        total : `int`, optional
            The total number of steps in this progress bar; defaults to the sum
            of the lengths of the chunks if this can be computed.  If this is
            provided or `True`, each element in ``chunks`` must be sized but
            ``chunks`` itself need not be (and may be a single-pass iterable).
        skip_scalar : `bool`, optional
            If `True` and there are zero or one chunks, do not report progress.

        Yields
        ------
        chunk
            The same objects that iteration over ``chunks`` would yield.

        Notes
        -----
        This attempts to display as much progress as possible given the
        limitations of the iterables, assuming that sized iterables are also
        multi-pass (as is true of all built-in collections and lazy iterators).
        In detail, if ``total`` is `None`:

        - if ``chunks`` and its elements are both sized, ``total`` is computed
          from them and full progress is reported, and ``chunks`` must be a
          multi-pass iterable.
        - if ``chunks`` is sized but its elements are not, a progress bar over
          the number of chunks is shown, and ``chunks`` must be a multi-pass
          iterable.
        - if ``chunks`` is not sized, the progress bar just shows when updates
          occur.

        If ``total`` is `True` or an integer, ``chunks`` need not be sized, but
        its elements must be, ``chunks`` must be a multi-pass iterable, and
        full progress is shown.

        If ``total`` is `False`, ``chunks`` and its elements need not be sized,
        and the progress bar just shows when updates occur.
        """
        if isinstance(chunks, Sized):
            n_chunks = len(chunks)
        else:
            n_chunks = None
            if total is None:
                total = False
        if skip_scalar and n_chunks == 1:
            yield from chunks
            return
        use_n_chunks = False
        if total is True or total is None:
            total = 0
            for c in chunks:
                if total is True or isinstance(c, Sized):
                    total += len(c)  # type: ignore
                else:
                    use_n_chunks = True
                    total = None
                    break
        if total is False:
            total = None
            use_n_chunks = True
        if use_n_chunks:
            with self.bar(desc=desc, total=n_chunks) as bar:  # type: ignore
                for chunk in chunks:
                    yield chunk
                    bar.update(1)
        else:
            with self.bar(desc=desc, total=total) as bar:  # type: ignore
                for chunk in chunks:
                    yield chunk
                    bar.update(len(chunk))  # type: ignore

    def iter_item_chunks(
        self,
        items: Iterable[tuple[_K, _V]],
        desc: str | None = None,
        total: int | None = None,
        skip_scalar: bool = True,
    ) -> Generator[tuple[_K, _V], None, None]:
        """Wrap iteration over chunks of items in a progress bar.

        Parameters
        ----------
        items : `~collections.abc.Iterable`
            An iterable whose elements are (key, value) tuples, where the
            values are themselves iterable.
        desc : `str`, optional
            A user-friendly description for this progress bar; usually appears
            next to it.  If not provided, ``self.name`` is used (which is not
            usually a user-friendly string, but may be appropriate for
            debug-level progress).
        total : `int`, optional
            The total number of steps in this progress bar; defaults to the sum
            of the lengths of the chunks if this can be computed.  If this is
            provided or `True`, each element in ``chunks`` must be sized but
            ``chunks`` itself need not be (and may be a single-pass iterable).
        skip_scalar : `bool`, optional
            If `True` and there are zero or one items, do not report progress.

        Yields
        ------
        chunk
            The same 2-tuples that iteration over ``items`` would yield.

        Notes
        -----
        This attempts to display as much progress as possible given the
        limitations of the iterables, assuming that sized iterables are also
        multi-pass (as is true of all built-in collections and lazy iterators).
        In detail, if ``total`` is `None`:

        - if ``chunks`` and its values elements are both sized, ``total`` is
          computed from them and full progress is reported, and ``chunks`` must
          be a multi-pass iterable.
        - if ``chunks`` is sized but its value elements are not, a progress bar
          over the number of chunks is shown, and ``chunks`` must be a
          multi-pass iterable.
        - if ``chunks`` is not sized, the progress bar just shows when updates
          occur.

        If ``total`` is `True` or an integer, ``chunks`` need not be sized, but
        its value elements must be, ``chunks`` must be a multi-pass iterable,
        and full progress is shown.

        If ``total`` is `False`, ``chunks`` and its values elements need not be
        sized, and the progress bar just shows when updates occur.
        """
        if isinstance(items, Sized):
            n_items = len(items)
            if skip_scalar and n_items == 1:
                yield from items
                return
        else:
            n_items = None
            if total is None:
                total = False
        use_n_items = False
        if total is True or total is None:
            total = 0
            for _, v in items:
                if total is True or isinstance(v, Sized):
                    total += len(v)  # type: ignore
                else:
                    use_n_items = True
                    total = None
                    break
        if total is False:
            total = None
            use_n_items = True
        if use_n_items:
            with self.bar(desc=desc, total=n_items) as bar:  # type: ignore
                for chunk in items:
                    yield chunk
                    bar.update(1)
        else:
            with self.bar(desc=desc, total=total) as bar:  # type: ignore
                for k, v in items:
                    yield k, v
                    bar.update(len(v))  # type: ignore


class ProgressHandler(ABC):
    """An interface for objects that can create progress bars."""

    @abstractmethod
    def get_progress_bar(
        self, iterable: Iterable[_T] | None, desc: str, total: int | None, level: int
    ) -> AbstractContextManager[ProgressBar[_T]]:
        """Create a new progress bar.

        Parameters
        ----------
        iterable : `~collections.abc.Iterable` or `None`
            An arbitrary Python iterable that will be iterated over when the
            returned `ProgressBar` is.  If `None`, whether the progress bar is
            iterable is handler-defined, but it may be updated manually.
        desc : `str`
            A user-friendly description for this progress bar; usually appears
            next to it.
        total : `int` or `None`
            The total number of steps in this progress bar.  If `None``,
            ``len(iterable)`` should be used.  If that does not work, whether
            the progress bar works at all is handler-defined.
        level : `int`
            A `logging` level value (defaults to `logging.INFO`) associated
            with the process reporting progress.  Handlers are not responsible
            for disabling progress reporting on levels, but may utilize level
            information to annotate them differently.
        """
        raise NotImplementedError()


class _NullProgressBar(Iterable[_T]):
    """A trivial implementation of `ProgressBar` that does nothing but pass
    through its iterable's elements.

    Parameters
    ----------
    iterable : `~collections.abc.Iterable` or `None`
        An arbitrary Python iterable that will be iterated over when ``self``
        is.
    """

    def __init__(self, iterable: Iterable[_T] | None):
        self._iterable = iterable

    @classmethod
    @contextmanager
    def context(cls, iterable: Iterable[_T] | None) -> Generator[_NullProgressBar[_T], None, None]:
        """Return a trivial context manager that wraps an instance of this
        class.

        This context manager doesn't actually do anything other than allow this
        do-nothing implementation to be used in `Progress.bar`.

        Parameters
        ----------
        iterable : `~collections.abc.Iterable` or `None`
            An arbitrary Python iterable that will be iterated over when the
            returned object is.

        Yields
        ------
        _NullProgressBar
            Progress bar that does nothing.
        """
        yield cls(iterable)

    def __iter__(self) -> Iterator[_T]:
        assert self._iterable is not None, "Cannot iterate over progress bar initialized without iterable."
        return iter(self._iterable)

    def update(self, n: int = 1) -> None:
        pass
