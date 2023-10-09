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

__all__ = ("ClickProgressHandler",)

from collections.abc import Iterable
from contextlib import AbstractContextManager
from typing import Any, TypeVar

import click

from ..progress import Progress, ProgressBar, ProgressHandler

_T = TypeVar("_T")


class ClickProgressHandler(ProgressHandler):
    """A `ProgressHandler` implementation that delegates to
    `click.progressbar`.

    Parameters
    ----------
    **kwargs
        Additional keyword arguments to pass to `click.progressbar`.  May not
        include ``iterable``, ``length``, or ``label``, as these are passed
        directly from `get_progress_bar` arguments.
    """

    def __init__(self, **kwargs: Any):
        self._kwargs = kwargs

    @classmethod
    def callback(cls, ctx: click.Context, params: click.Parameter, value: Any) -> None:
        """`click` callback that installs this handler as the global handler
        for progress bars.

        Should usually be called only by the `option` method.
        """
        if value:
            Progress.set_handler(cls())
        else:
            Progress.set_handler(None)

    @classmethod
    def option(cls, cmd: Any) -> Any:
        """`click` command decorator that adds a ``--progress`` option
        that installs a default-constructed instance of this progress handler.
        """
        return click.option(
            "--progress/--no-progress",
            help="Show a progress bar for slow operations when possible.",
            default=False,
            is_flag=True,
            callback=cls.callback,
        )(cmd)

    def get_progress_bar(
        self, iterable: Iterable[_T] | None, desc: str | None, total: int | None, level: int
    ) -> AbstractContextManager[ProgressBar[_T]]:
        # Docstring inherited.
        return click.progressbar(iterable=iterable, length=total, label=desc, **self._kwargs)  # type: ignore
