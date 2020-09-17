# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

import contextlib
from io import StringIO
import os
from typing import (
    Generator,
    Iterable,
    Optional,
    Set,
    TextIO,
    Tuple,
    Union,
)
import unittest
import unittest.mock

from lsst.daf.butler import (
    DatasetRef,
    Datastore,
    FileDataset,
    Registry,
    YamlRepoExportBackend,
    YamlRepoImportBackend,
)
from lsst.daf.butler.transfers import RepoExportContext
from lsst.daf.butler.registry import RegistryConfig


TESTDIR = os.path.abspath(os.path.dirname(__file__))


def _mock_export(refs: Iterable[DatasetRef], *,
                 directory: Optional[str] = None,
                 transfer: Optional[str] = None) -> Iterable[FileDataset]:
    """A mock of `Datastore.export` that satisifies the requirement that the
    refs passed in are included in the `FileDataset` objects returned.

    This can be used to construct a `Datastore` mock that can be used in
    repository export via::

        datastore = unittest.mock.Mock(spec=Datastore)
        datastore.export = _mock_export

    """
    for ref in refs:
        yield FileDataset(refs=[ref],
                          path="mock/path",
                          formatter="lsst.daf.butler.formatters.json.JsonFormatter")


class TransfersTestCase(unittest.TestCase):
    """Tests for repository import/export functionality.
    """

    def makeRegistry(self) -> Registry:
        """Create a new `Registry` instance.

        The default implementation returns a SQLite in-memory database.
        """
        config = RegistryConfig()
        config["db"] = "sqlite://"
        return Registry.fromConfig(config, create=True)

    def runImport(self, file: Union[str, TextIO],
                  *,
                  registry: Optional[Registry] = None,
                  datastore: Optional[Datastore] = None,
                  directory: Optional[str] = None,
                  transfer: Optional[str] = None,
                  skip_dimensions: Optional[Set[str]] = None) -> Tuple[Registry, Datastore]:
        """Import repository data from an export file.

        Parameters
        ----------
        file: `str` or `TextIO`
            Name of the (YAML) file that describes the data to import, or an
            open file-like object pointing to it.
        stream: `
        registry: `Registry`, optional
            Registry instance to load datsets into.  If not provided,
            `makeRegistry` is called.
        datastore: `Datastore`, optional
            Datastore instance to load datasets into (may be a mock).  If not
            provided, a mock is created.
        directory: `str`, optional
            Directory containing files to import.  Ignored if ``datastore`` is
            a mock.
        transfer: `str`, optional
            Transfer mode.  See `Datastore.ingest`.
        skip_dimensions: `Set` [ `str` ], optional
            Set of dimension element names for which records should not be
            imported.

        Returns
        -------
        registry: `Registry`
            The `Registry` that datasets were loaded into.
        datastore: `Datastore`
            The `Datastore` instance or mock that datasets were loaded into.
        """
        if registry is None:
            registry = self.makeRegistry()
        if datastore is None:
            datastore = unittest.mock.Mock(spec=Datastore)
        if isinstance(file, str):
            with open(file, 'r') as stream:
                backend = YamlRepoImportBackend(stream, registry)
                backend.register()
                backend.load(datastore, directory=directory, transfer=transfer,
                             skip_dimensions=skip_dimensions)
        else:
            backend = YamlRepoImportBackend(file, registry)
            backend.register()
            backend.load(datastore, directory=directory, transfer=transfer, skip_dimensions=skip_dimensions)
        return (registry, datastore)

    @contextlib.contextmanager
    def runExport(self, *,
                  registry: Optional[Registry] = None,
                  datastore: Optional[Datastore] = None,
                  stream: Optional[TextIO] = None,
                  directory: Optional[str] = None,
                  transfer: Optional[str] = None) -> Generator[RepoExportContext, None, None]:
        """Export repository data to an export file.

        Parameters
        ----------
        registry: `Registry`, optional
            Registry instance to load datasets from.  If not provided,
            `makeRegistry` is called.
        datastore: `Datastore`, optional
            Datastore instance to load datasets from (may be a mock).  If not
            provided, a mock is created.
        directory: `str`, optional
            Directory to contain exported file.  Ignored if ``datastore`` is
            a mock.
        stream : `TextIO`, optional
            Writeable file-like object pointing at the export file to write.
        transfer: `str`, optional
            Transfer mode.  See `Datastore.ingest`.

        Yields
        ------
        context : `RepoExportContext`
            A helper object that can be used to export repo data.  This is
            wrapped in a context manager via the `contextlib.contextmanager`
            decorator.
        """
        if registry is None:
            registry = self.makeRegistry()
        if datastore is None:
            datastore = unittest.mock.Mock(spec=Datastore)
            datastore.export = _mock_export
        backend = YamlRepoExportBackend(stream)
        try:
            helper = RepoExportContext(registry, datastore, backend=backend,
                                       directory=directory, transfer=transfer)
            yield helper
        except BaseException:
            raise
        else:
            helper._finish()

    def testReadBackwardsCompatibility(self):
        """Test that we can read an export file written by a previous version
        and commit to the daf_butler git repo.

        Notes
        -----
        At present this export file includes only dimension data, not datasets,
        which greatly limits the usefulness of this test.  We should address
        this at some point, but I think it's best to wait for the changes to
        the export format required for CALIBRATION collections to land.
        """
        registry, _ = self.runImport(os.path.join(TESTDIR, "data", "registry", "hsc-rc2-subset.yaml"))
        # Spot-check a few things, but the most important test is just that
        # the above does not raise.
        self.assertGreaterEqual(
            set(record.id for record in registry.queryDimensionRecords("detector", instrument="HSC")),
            set(range(104)),  # should have all science CCDs; may have some focus ones.
        )
        self.assertGreaterEqual(
            {
                (record.id, record.physical_filter)
                for record in registry.queryDimensionRecords("visit", instrument="HSC")
            },
            {
                (27136, 'HSC-Z'),
                (11694, 'HSC-G'),
                (23910, 'HSC-R'),
                (11720, 'HSC-Y'),
                (23900, 'HSC-R'),
                (22646, 'HSC-Y'),
                (1248, 'HSC-I'),
                (19680, 'HSC-I'),
                (1240, 'HSC-I'),
                (424, 'HSC-Y'),
                (19658, 'HSC-I'),
                (344, 'HSC-Y'),
                (1218, 'HSC-R'),
                (1190, 'HSC-Z'),
                (23718, 'HSC-R'),
                (11700, 'HSC-G'),
                (26036, 'HSC-G'),
                (23872, 'HSC-R'),
                (1170, 'HSC-Z'),
                (1876, 'HSC-Y'),
            }
        )

    def testAllDatasetsRoundTrip(self):
        """Test exporting all datasets from a repo and then importing them all
        back in again.
        """
        # Import data to play with.
        registry1, _ = self.runImport(os.path.join(TESTDIR, "data", "registry", "base.yaml"))
        self.runImport(os.path.join(TESTDIR, "data", "registry", "datasets.yaml"), registry=registry1)
        # Export all datasets.
        exportStream = StringIO()
        with self.runExport(stream=exportStream, registry=registry1) as exporter:
            exporter.saveDatasets(
                registry1.queryDatasets(..., collections=...)
            )
        # Import it all again.
        importStream = StringIO(exportStream.getvalue())
        registry2, _ = self.runImport(importStream)
        # Check that it all round-tripped.  Use unresolved() to make comparison
        # not care about dataset_id values, which may be rewritten.
        self.assertCountEqual(
            [ref.unresolved() for ref in registry1.queryDatasets(..., collections=...)],
            [ref.unresolved() for ref in registry2.queryDatasets(..., collections=...)],
        )


if __name__ == "__main__":
    unittest.main()
