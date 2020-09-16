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

import astropy.time

from lsst.daf.butler import (
    CollectionType,
    DatasetRef,
    Datastore,
    FileDataset,
    Registry,
    Timespan,
    YamlRepoExportBackend,
    YamlRepoImportBackend,
)
from lsst.daf.butler.transfers import RepoExportContext
from lsst.daf.butler.registry import CollectionSearch, RegistryConfig


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

    def testCollectionTransfers(self):
        """Test exporting and then importing collections of various types.
        """
        # Populate a registry with some datasets.
        registry1, _ = self.runImport(os.path.join(TESTDIR, "data", "registry", "base.yaml"))
        self.runImport(os.path.join(TESTDIR, "data", "registry", "datasets.yaml"), registry=registry1)
        # Add some more collections.
        registry1.registerRun("run1")
        registry1.registerCollection("tag1", CollectionType.TAGGED)
        registry1.registerCollection("calibration1", CollectionType.CALIBRATION)
        registry1.registerCollection("chain1", CollectionType.CHAINED)
        registry1.registerCollection("chain2", CollectionType.CHAINED)
        registry1.setCollectionChain("chain1", ["tag1", "run1", "chain2"])
        registry1.setCollectionChain("chain2", [("calibration1", ["bias"]), "run1"])
        # Associate some datasets into the TAGGED and CALIBRATION collections.
        flats1 = list(registry1.queryDatasets("flat", collections=...))
        registry1.associate("tag1", flats1)
        t1 = astropy.time.Time('2020-01-01T01:00:00', format="isot", scale="tai")
        t2 = astropy.time.Time('2020-01-01T02:00:00', format="isot", scale="tai")
        t3 = astropy.time.Time('2020-01-01T03:00:00', format="isot", scale="tai")
        bias2a = registry1.findDataset("bias", instrument="Cam1", detector=2, collections="imported_g")
        bias3a = registry1.findDataset("bias", instrument="Cam1", detector=3, collections="imported_g")
        bias2b = registry1.findDataset("bias", instrument="Cam1", detector=2, collections="imported_r")
        bias3b = registry1.findDataset("bias", instrument="Cam1", detector=3, collections="imported_r")
        registry1.certify("calibration1", [bias2a, bias3a], Timespan(t1, t2))
        registry1.certify("calibration1", [bias2b], Timespan(t2, None))
        registry1.certify("calibration1", [bias3b], Timespan(t2, t3))
        # Export all collections.
        exportStream = StringIO()
        with self.runExport(stream=exportStream, registry=registry1) as exporter:
            # Sort results to put chain1 before chain2, which is intentionally
            # not topological order.
            for collection in sorted(registry1.queryCollections()):
                exporter.saveCollection(collection)
            exporter.saveDatasets(flats1)
            exporter.saveDatasets([bias2a, bias2b, bias3a, bias3b])
        # Import them into a new registry.
        importStream = StringIO(exportStream.getvalue())
        registry2, _ = self.runImport(importStream)
        # Check that it all round-tripped, starting with the collections
        # themselves.
        self.assertIs(registry2.getCollectionType("run1"), CollectionType.RUN)
        self.assertIs(registry2.getCollectionType("tag1"), CollectionType.TAGGED)
        self.assertIs(registry2.getCollectionType("calibration1"), CollectionType.CALIBRATION)
        self.assertIs(registry2.getCollectionType("chain1"), CollectionType.CHAINED)
        self.assertIs(registry2.getCollectionType("chain2"), CollectionType.CHAINED)
        self.assertEqual(
            registry2.getCollectionChain("chain1"),
            CollectionSearch.fromExpression(["tag1", "run1", "chain2"]),
        )
        self.assertEqual(
            registry2.getCollectionChain("chain2"),
            CollectionSearch.fromExpression([("calibration1", ["bias"]), "run1"]),
        )
        # Check that tag collection contents are the same.
        self.maxDiff = None
        self.assertCountEqual(
            [ref.unresolved() for ref in registry1.queryDatasets(..., collections="tag1")],
            [ref.unresolved() for ref in registry2.queryDatasets(..., collections="tag1")],
        )
        # Check that calibration collection contents are the same.
        self.assertCountEqual(
            [(assoc.ref.unresolved(), assoc.timespan)
             for assoc in registry1.queryDatasetAssociations("bias", collections="calibration1")],
            [(assoc.ref.unresolved(), assoc.timespan)
             for assoc in registry2.queryDatasetAssociations("bias", collections="calibration1")],
        )


if __name__ == "__main__":
    unittest.main()
