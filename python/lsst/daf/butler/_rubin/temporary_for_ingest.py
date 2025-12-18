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

__all__ = ("TemporaryForIngest",)

import dataclasses
import glob
from contextlib import contextmanager
from typing import TYPE_CHECKING, Self, cast

from lsst.resources import ResourcePath

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import TracebackType

    from .._butler import Butler
    from .._dataset_ref import DatasetRef
    from .._file_dataset import FileDataset
    from .._limited_butler import LimitedButler


@dataclasses.dataclass
class TemporaryForIngest:
    """A context manager for generating temporary paths that will be ingested
    as butler datasets.

    Notes
    -----
    Neither this class nor its `make_path` method run ingest automatically when
    their context manager is exited; the `ingest` method must always be called
    explicitly.
    """

    butler: Butler
    """Full butler to obtain a predicted path from and ingest into."""

    ref: DatasetRef
    """Description of the dataset to ingest."""

    dataset: FileDataset = dataclasses.field(init=False)
    """The dataset that will be passed to `Butler.ingest`."""

    @property
    def path(self) -> ResourcePath:
        """The temporary path.

        Guaranteed to be a local POSIX path.
        """
        return cast(ResourcePath, self.dataset.path)

    @property
    def ospath(self) -> str:
        """The temporary path as a complete filename."""
        return self.path.ospath

    @classmethod
    @contextmanager
    def make_path(cls, final_path: ResourcePath) -> Iterator[ResourcePath]:
        """Return a temporary path context manager given the predicted final
        path.

        Parameters
        ----------
        final_path : `lsst.resources.ResourcePath`
            Predicted final path.

        Returns
        -------
        context : `contextlib.AbstractContextManager`
            A context manager that yields the temporary
            `~lsst.resources.ResourcePath` when entered and deletes that file
            when exited.
        """
        # Always write to a temporary even if using a local file system -- that
        # gives us atomic writes. If a process is killed as the file is being
        # written we do not want it to remain in the correct place but in
        # corrupt state. For local files write to the output directory not
        # temporary dir.
        prefix = final_path.dirname() if final_path.isLocal else None
        if prefix is not None:
            prefix.mkdir()
        with ResourcePath.temporary_uri(
            suffix=cls._get_temporary_suffix(final_path), prefix=prefix
        ) as temporary_path:
            yield temporary_path

    def ingest(self, record_validation_info: bool = True) -> None:
        """Ingest the file into the butler.

        Parameters
        ----------
        record_validation_info : `bool`, optional
            Whether to- record the file size and checksum upon ingest.
        """
        self.butler.ingest(self.dataset, transfer="move", record_validation_info=record_validation_info)

    def __enter__(self) -> Self:
        from .._file_dataset import FileDataset

        final_path = self.butler.getURI(self.ref, predict=True).replace(fragment="")
        prefix = final_path.dirname() if final_path.isLocal else None
        if prefix is not None:
            prefix.mkdir()
        self._temporary_path_context = self.make_path(final_path)
        temporary_path = self._temporary_path_context.__enter__()
        self.dataset = FileDataset(temporary_path, [self.ref], formatter=None)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        return self._temporary_path_context.__exit__(exc_type, exc_value, traceback)

    @classmethod
    def find_orphaned_temporaries_by_path(cls, final_path: ResourcePath) -> list[ResourcePath]:
        """Search for temporary files that were not successfully ingested.

        Parameters
        ----------
        final_path : `lsst.resources.ResourcePath`
            Final path a successfully-ingested file would have.

        Returns
        -------
        paths : `list` [ `lsst.resources.ResourcePath` ]
            Files that look like temporaries that might have been created while
            trying to write the target dataset.

        Notes
        -----
        Orphaned files are only possible when a context manager is interrupted
        by a hard error that prevents any cleanup code from running (e.g.
        sudden loss of power).
        """
        if not final_path.isLocal:
            # We return true tempfile for non-local predicted paths, so orphans
            # are not our problem (the OS etc. will take care of them).
            return []
        return [
            ResourcePath(filename)
            for filename in glob.glob(
                f"{glob.escape(final_path.dirname().ospath)}*{glob.escape(cls._get_temporary_suffix(final_path))}"
            )
            if filename != final_path.ospath
        ]

    @classmethod
    def find_orphaned_temporaries_by_ref(cls, ref: DatasetRef, butler: LimitedButler) -> list[ResourcePath]:
        """Search for temporary files that were not successfully ingested.

        Parameters
        ----------
        ref : `..DatasetRef`
            A dataset reference the temporaries correspond to.
        butler : `lsst.daf.butler.LimitedButler`
            Butler that can be used to obtain a predicted URI for a dataset.

        Returns
        -------
        paths : `list` [ `lsst.resources.ResourcePath` ]
            Files that look like temporaries that might have been created while
            trying to write the target dataset.

        Notes
        -----
        Orphaned files are only possible when a context manager is interrupted
        by a hard error that prevents any cleanup code from running (e.g.
        sudden loss of power).
        """
        final_path = butler.getURI(ref, predict=True).replace(fragment="")
        return cls.find_orphaned_temporaries_by_path(final_path)

    @staticmethod
    def _get_temporary_suffix(path: ResourcePath) -> str:
        ext = path.getExtension()
        basename = path.basename().removesuffix(ext)
        return f"{basename}.tmp{ext}"
