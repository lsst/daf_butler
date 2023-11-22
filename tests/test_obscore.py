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

import gc
import os
import tempfile
import unittest
import warnings
from abc import abstractmethod
from typing import cast

import astropy.time
import sqlalchemy
from lsst.daf.butler import (
    CollectionType,
    Config,
    DataCoordinate,
    DatasetRef,
    DatasetType,
    StorageClassFactory,
)
from lsst.daf.butler.registry import RegistryConfig, _RegistryFactory
from lsst.daf.butler.registry.obscore import (
    DatasetTypeConfig,
    ObsCoreConfig,
    ObsCoreLiveTableManager,
    ObsCoreSchema,
)
from lsst.daf.butler.registry.obscore._schema import _STATIC_COLUMNS
from lsst.daf.butler.registry.sql_registry import SqlRegistry
from lsst.daf.butler.tests.utils import TestCaseMixin, makeTestTempDir, removeTestTempDir
from lsst.sphgeom import Box, ConvexPolygon, LonLat, UnitVector3d

try:
    import testing.postgresql  # type: ignore
except ImportError:
    testing = None

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class ObsCoreTests(TestCaseMixin):
    """Base class for testing obscore manager functionality."""

    root: str

    def make_registry(
        self, collections: list[str] | None = None, collection_type: str | None = None
    ) -> SqlRegistry:
        """Create new empty Registry."""
        config = self.make_registry_config(collections, collection_type)
        registry = _RegistryFactory(config).create_from_config(butlerRoot=self.root)
        self.initialize_registry(registry)
        return registry

    @abstractmethod
    def make_registry_config(
        self, collections: list[str] | None = None, collection_type: str | None = None
    ) -> RegistryConfig:
        """Make Registry configuration."""
        raise NotImplementedError()

    def initialize_registry(self, registry: SqlRegistry) -> None:
        """Populate Registry with the things that we need for tests."""
        registry.insertDimensionData("instrument", {"name": "DummyCam"})
        registry.insertDimensionData(
            "physical_filter", {"instrument": "DummyCam", "name": "d-r", "band": "r"}
        )
        for detector in (1, 2, 3, 4):
            registry.insertDimensionData(
                "detector", {"instrument": "DummyCam", "id": detector, "full_name": f"detector{detector}"}
            )

        for exposure in (1, 2, 3, 4):
            registry.insertDimensionData(
                "exposure",
                {
                    "instrument": "DummyCam",
                    "id": exposure,
                    "obs_id": f"exposure{exposure}",
                    "physical_filter": "d-r",
                },
            )

        registry.insertDimensionData("visit_system", {"instrument": "DummyCam", "id": 1, "name": "default"})

        for visit in (1, 2, 3, 4, 9):
            visit_start = astropy.time.Time(f"2020-01-01 08:0{visit}:00", scale="tai")
            visit_end = astropy.time.Time(f"2020-01-01 08:0{visit}:45", scale="tai")
            registry.insertDimensionData(
                "visit",
                {
                    "instrument": "DummyCam",
                    "id": visit,
                    "name": f"visit{visit}",
                    "physical_filter": "d-r",
                    "datetime_begin": visit_start,
                    "datetime_end": visit_end,
                },
            )
            registry.insertDimensionData(
                "visit_system_membership",
                {"instrument": "DummyCam", "visit": visit, "visit_system": 1},
            )

        # Only couple of exposures are linked to visits.
        for visit in (1, 2):
            registry.insertDimensionData(
                "visit_definition",
                {
                    "instrument": "DummyCam",
                    "exposure": visit,
                    "visit": visit,
                },
            )

        # map visit and detector to region
        self.regions: dict[tuple[int, int], ConvexPolygon] = {}
        for visit in (1, 2, 3, 4):
            for detector in (1, 2, 3, 4):
                lon = visit * 90 - 88
                lat = detector * 2 - 5
                region = ConvexPolygon(
                    [
                        UnitVector3d(LonLat.fromDegrees(lon - 1.0, lat - 1.0)),
                        UnitVector3d(LonLat.fromDegrees(lon + 1.0, lat - 1.0)),
                        UnitVector3d(LonLat.fromDegrees(lon + 1.0, lat + 1.0)),
                        UnitVector3d(LonLat.fromDegrees(lon - 1.0, lat + 1.0)),
                    ]
                )
                registry.insertDimensionData(
                    "visit_detector_region",
                    {
                        "instrument": "DummyCam",
                        "visit": visit,
                        "detector": detector,
                        "region": region,
                    },
                )
                self.regions[(visit, detector)] = region

        # Visit 9 has non-polygon region
        for detector in (1, 2, 3, 4):
            lat = detector * 2 - 5
            region = Box.fromDegrees(17.0, lat - 1.0, 19.0, lat + 1.0)
            registry.insertDimensionData(
                "visit_detector_region",
                {
                    "instrument": "DummyCam",
                    "visit": 9,
                    "detector": detector,
                    "region": region,
                },
            )

        # Add few dataset types
        storage_class_factory = StorageClassFactory()
        storage_class = storage_class_factory.getStorageClass("StructuredDataDict")

        self.dataset_types: dict[str, DatasetType] = {}

        dimensions = registry.dimensions.conform(["instrument", "physical_filter", "detector", "exposure"])
        self.dataset_types["raw"] = DatasetType("raw", dimensions, storage_class)

        dimensions = registry.dimensions.conform(["instrument", "physical_filter", "detector", "visit"])
        self.dataset_types["calexp"] = DatasetType("calexp", dimensions, storage_class)

        dimensions = registry.dimensions.conform(["instrument", "physical_filter", "detector", "visit"])
        self.dataset_types["no_obscore"] = DatasetType("no_obscore", dimensions, storage_class)

        dimensions = registry.dimensions.conform(["instrument", "physical_filter", "detector"])
        self.dataset_types["calib"] = DatasetType("calib", dimensions, storage_class, isCalibration=True)

        for dataset_type in self.dataset_types.values():
            registry.registerDatasetType(dataset_type)

        # Add few run collections.
        for run in (1, 2, 3, 4, 5, 6):
            registry.registerRun(f"run{run}")

        # Add few chained collections, run6 is not in any chained collections.
        registry.registerCollection("chain12", CollectionType.CHAINED)
        registry.setCollectionChain("chain12", ("run1", "run2"))
        registry.registerCollection("chain34", CollectionType.CHAINED)
        registry.setCollectionChain("chain34", ("run3", "run4"))
        registry.registerCollection("chain-all", CollectionType.CHAINED)
        registry.setCollectionChain("chain-all", ("chain12", "chain34", "run5"))

        # And a tagged collection
        registry.registerCollection("tagged", CollectionType.TAGGED)

    def make_obscore_config(
        self, collections: list[str] | None = None, collection_type: str | None = None
    ) -> Config:
        """Make configuration for obscore manager."""
        obscore_config = Config(os.path.join(TESTDIR, "config", "basic", "obscore.yaml"))
        if collections is not None:
            obscore_config["collections"] = collections
        if collection_type is not None:
            obscore_config["collection_type"] = collection_type
        return obscore_config

    def _insert_dataset(
        self, registry: SqlRegistry, run: str, dataset_type: str, do_import: bool = False, **kwargs
    ) -> DatasetRef:
        """Insert or import one dataset into a specified run collection."""
        data_id = {"instrument": "DummyCam", "physical_filter": "d-r"}
        data_id.update(kwargs)
        coordinate = DataCoordinate.standardize(data_id, universe=registry.dimensions)
        if do_import:
            ds_type = self.dataset_types[dataset_type]
            ref = DatasetRef(ds_type, coordinate, run=run)
            [ref] = registry._importDatasets([ref])
        else:
            [ref] = registry.insertDatasets(dataset_type, [data_id], run=run)
        return ref

    def _insert_datasets(self, registry: SqlRegistry, do_import: bool = False) -> list[DatasetRef]:
        """Inset a small bunch of datasets into every run collection."""
        return [
            self._insert_dataset(registry, "run1", "raw", detector=1, exposure=1, do_import=do_import),
            self._insert_dataset(registry, "run2", "calexp", detector=2, visit=2, do_import=do_import),
            self._insert_dataset(registry, "run3", "raw", detector=3, exposure=3, do_import=do_import),
            self._insert_dataset(registry, "run4", "calexp", detector=4, visit=4, do_import=do_import),
            self._insert_dataset(registry, "run5", "calexp", detector=4, visit=4, do_import=do_import),
            # This dataset type is not configured, will not be in obscore.
            self._insert_dataset(registry, "run5", "no_obscore", detector=1, visit=1, do_import=do_import),
            self._insert_dataset(registry, "run6", "raw", detector=1, exposure=4, do_import=do_import),
        ]

    def test_config_errors(self):
        """Test for handling various configuration problems."""
        # This raises pydantic ValidationError, which wraps ValueError
        exception_re = "'collections' must have one element"
        with self.assertRaisesRegex(ValueError, exception_re):
            self.make_registry(None, "TAGGED")

        with self.assertRaisesRegex(ValueError, exception_re):
            self.make_registry([], "TAGGED")

        with self.assertRaisesRegex(ValueError, exception_re):
            self.make_registry(["run1", "run2"], "TAGGED")

        # Invalid regex.
        with self.assertRaisesRegex(ValueError, "Failed to compile regex"):
            self.make_registry(["+run"], "RUN")

    def test_schema(self):
        """Check how obscore schema is constructed"""
        config = ObsCoreConfig(obs_collection="", dataset_types={}, facility_name="FACILITY")
        schema = ObsCoreSchema(config, [])
        table_spec = schema.table_spec
        self.assertEqual(list(table_spec.fields.names), [col.name for col in _STATIC_COLUMNS])

        # extra columns from top-level config
        config = ObsCoreConfig(
            obs_collection="",
            extra_columns={"c1": 1, "c2": "string", "c3": {"template": "{calib_level}", "type": "float"}},
            dataset_types={},
            facility_name="FACILITY",
        )
        schema = ObsCoreSchema(config, [])
        table_spec = schema.table_spec
        self.assertEqual(
            list(table_spec.fields.names),
            [col.name for col in _STATIC_COLUMNS] + ["c1", "c2", "c3"],
        )
        self.assertEqual(table_spec.fields["c1"].dtype, sqlalchemy.BigInteger)
        self.assertEqual(table_spec.fields["c2"].dtype, sqlalchemy.String)
        self.assertEqual(table_spec.fields["c3"].dtype, sqlalchemy.Float)

        # extra columns from per-dataset type configs
        config = ObsCoreConfig(
            obs_collection="",
            extra_columns={"c1": 1},
            dataset_types={
                "raw": DatasetTypeConfig(
                    name="raw",
                    dataproduct_type="image",
                    calib_level=1,
                    extra_columns={"c2": "string"},
                ),
                "calexp": DatasetTypeConfig(
                    dataproduct_type="image",
                    calib_level=2,
                    extra_columns={"c3": 1e10},
                ),
            },
            facility_name="FACILITY",
        )
        schema = ObsCoreSchema(config, [])
        table_spec = schema.table_spec
        self.assertEqual(
            list(table_spec.fields.names),
            [col.name for col in _STATIC_COLUMNS] + ["c1", "c2", "c3"],
        )
        self.assertEqual(table_spec.fields["c1"].dtype, sqlalchemy.BigInteger)
        self.assertEqual(table_spec.fields["c2"].dtype, sqlalchemy.String)
        self.assertEqual(table_spec.fields["c3"].dtype, sqlalchemy.Float)

        # Columns with the same names as in static list in configs, types
        # are not overriden.
        config = ObsCoreConfig(
            version=0,
            obs_collection="",
            extra_columns={"t_xel": 1e10},
            dataset_types={
                "raw": DatasetTypeConfig(
                    dataproduct_type="image",
                    calib_level=1,
                    extra_columns={"target_name": 1},
                ),
                "calexp": DatasetTypeConfig(
                    dataproduct_type="image",
                    calib_level=2,
                    extra_columns={"em_xel": "string"},
                ),
            },
            facility_name="FACILITY",
        )
        schema = ObsCoreSchema(config, [])
        table_spec = schema.table_spec
        self.assertEqual(list(table_spec.fields.names), [col.name for col in _STATIC_COLUMNS])
        self.assertEqual(table_spec.fields["t_xel"].dtype, sqlalchemy.Integer)
        self.assertEqual(table_spec.fields["target_name"].dtype, sqlalchemy.String)
        self.assertEqual(table_spec.fields["em_xel"].dtype, sqlalchemy.Integer)

    def test_insert_existing_collection(self):
        """Test insert and import registry methods, with various restrictions
        on collection names.
        """
        # First item is collections, second item is expected record count.
        test_data = (
            (None, 6),
            (["run1", "run2"], 2),
            (["run[34]"], 2),
            (["[rR]un[^6]"], 5),
        )

        for collections, count in test_data:
            for do_import in (False, True):
                registry = self.make_registry(collections)
                obscore = registry.obsCoreTableManager
                assert obscore is not None
                self._insert_datasets(registry, do_import)

                with obscore.query() as result:
                    rows = list(result)
                    self.assertEqual(len(rows), count)

                # Also check `query` method with COUNT(*)
                with obscore.query([sqlalchemy.sql.func.count()]) as result:
                    scalar = result.scalar_one()
                    self.assertEqual(scalar, count)

    def test_drop_datasets(self):
        """Test for dropping datasets after obscore insert."""
        collections = None
        registry = self.make_registry(collections)
        obscore = registry.obsCoreTableManager
        assert obscore is not None
        refs = self._insert_datasets(registry)

        with obscore.query() as result:
            rows = list(result)
            self.assertEqual(len(rows), 6)

        # drop single dataset
        registry.removeDatasets(ref for ref in refs if ref.run == "run1")
        with obscore.query() as result:
            rows = list(result)
            self.assertEqual(len(rows), 5)

        # drop whole run collection
        registry.removeCollection("run6")
        with obscore.query() as result:
            rows = list(result)
            self.assertEqual(len(rows), 4)

    def test_associate(self):
        """Test for associating datasets to TAGGED collection."""
        collections = ["tagged"]
        registry = self.make_registry(collections, "TAGGED")
        obscore = registry.obsCoreTableManager
        assert obscore is not None
        refs = self._insert_datasets(registry)

        with obscore.query() as result:
            rows = list(result)
            self.assertEqual(len(rows), 0)

        # Associate datasets that are already in obscore, changes nothing.
        registry.associate("tagged", (ref for ref in refs if ref.run == "run1"))
        with obscore.query() as result:
            rows = list(result)
            self.assertEqual(len(rows), 1)

        # Associate datasets that are not in obscore
        registry.associate("tagged", (ref for ref in refs if ref.run == "run3"))
        with obscore.query() as result:
            rows = list(result)
            self.assertEqual(len(rows), 2)

        # Disassociate them
        registry.disassociate("tagged", (ref for ref in refs if ref.run == "run3"))
        with obscore.query() as result:
            rows = list(result)
            self.assertEqual(len(rows), 1)

        # Non-associated dataset, should be OK and not throw.
        registry.disassociate("tagged", (ref for ref in refs if ref.run == "run2"))
        with obscore.query() as result:
            rows = list(result)
            self.assertEqual(len(rows), 1)

        registry.disassociate("tagged", (ref for ref in refs if ref.run == "run1"))
        with obscore.query() as result:
            rows = list(result)
            self.assertEqual(len(rows), 0)

    @unittest.skip("Temporary, while deprecation warnings are present.")
    def test_region_type_warning(self) -> None:
        """Test that non-polygon region generates one or more warnings."""
        collections = None
        registry = self.make_registry(collections)

        with warnings.catch_warnings(record=True) as warning_records:
            self._insert_dataset(registry, "run2", "calexp", detector=2, visit=9)
            self.assertEqual(len(warning_records), 1)
            for record in warning_records:
                self.assertRegex(
                    str(record.message),
                    "Unexpected region type: .*lsst.sphgeom._sphgeom.Box.*",
                )

    def test_update_exposure_region(self) -> None:
        """Test for update_exposure_regions method."""
        registry = self.make_registry(["run1"])
        obscore = registry.obsCoreTableManager
        assert obscore is not None

        # Exposure 4 is not associated with any visit.
        for detector in (1, 2, 3, 4):
            self._insert_dataset(registry, "run1", "raw", detector=detector, exposure=4)

        # All spatial columns should be None.
        with obscore.query() as result:
            rows = list(result)
            self.assertEqual(len(rows), 4)
            for row in rows:
                self.assertIsNone(row.s_ra)
                self.assertIsNone(row.s_dec)
                self.assertIsNone(row.s_region)

        # Assign Region from visit 4.
        count = obscore.update_exposure_regions(
            "DummyCam", [(4, 1, self.regions[(4, 1)]), (4, 2, self.regions[(4, 2)])]
        )
        self.assertEqual(count, 2)

        with obscore.query(["s_ra", "s_dec", "s_region", "lsst_detector"]) as result:
            rows = list(result)
            self.assertEqual(len(rows), 4)
            for row in rows:
                if row.lsst_detector in (1, 2):
                    self.assertIsNotNone(row.s_ra)
                    self.assertIsNotNone(row.s_dec)
                    self.assertIsNotNone(row.s_region)
                else:
                    self.assertIsNone(row.s_ra)
                    self.assertIsNone(row.s_dec)
                    self.assertIsNone(row.s_region)


class SQLiteObsCoreTest(ObsCoreTests, unittest.TestCase):
    """Unit test for obscore with SQLite backend."""

    def setUp(self):
        self.root = makeTestTempDir(TESTDIR)

    def tearDown(self):
        removeTestTempDir(self.root)

    def make_registry_config(
        self, collections: list[str] | None = None, collection_type: str | None = None
    ) -> RegistryConfig:
        # docstring inherited from a base class
        _, filename = tempfile.mkstemp(dir=self.root, suffix=".sqlite3")
        config = RegistryConfig()
        config["db"] = f"sqlite:///{filename}"
        config["managers", "obscore"] = {
            "cls": "lsst.daf.butler.registry.obscore.ObsCoreLiveTableManager",
            "config": self.make_obscore_config(collections, collection_type),
        }
        return config


@unittest.skipUnless(testing is not None, "testing.postgresql module not found")
class PostgresObsCoreTest(ObsCoreTests, unittest.TestCase):
    """Unit test for obscore with PostgreSQL backend."""

    @classmethod
    def _handler(cls, postgresql):
        engine = sqlalchemy.engine.create_engine(postgresql.url())
        with engine.begin() as connection:
            connection.execute(sqlalchemy.text("CREATE EXTENSION btree_gist;"))

    @classmethod
    def setUpClass(cls):
        # Create the postgres test server.
        cls.postgresql = testing.postgresql.PostgresqlFactory(
            cache_initialized_db=True, on_initialized=cls._handler
        )
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        # Clean up any lingering SQLAlchemy engines/connections
        # so they're closed before we shut down the server.
        gc.collect()
        cls.postgresql.clear_cache()
        super().tearDownClass()

    def setUp(self):
        self.root = makeTestTempDir(TESTDIR)
        self.server = self.postgresql()
        self.count = 0

    def tearDown(self):
        removeTestTempDir(self.root)
        self.server = self.postgresql()

    def make_registry_config(
        self, collections: list[str] | None = None, collection_type: str | None = None
    ) -> RegistryConfig:
        # docstring inherited from a base class
        self.count += 1
        config = RegistryConfig()
        config["db"] = self.server.url()
        # Use unique namespace for each instance, some tests may use sub-tests.
        config["namespace"] = f"namespace{self.count}"
        config["managers", "obscore"] = {
            "cls": "lsst.daf.butler.registry.obscore.ObsCoreLiveTableManager",
            "config": self.make_obscore_config(collections, collection_type),
        }
        return config


@unittest.skipUnless(testing is not None, "testing.postgresql module not found")
class PostgresPgSphereObsCoreTest(PostgresObsCoreTest):
    """Unit test for obscore with PostgreSQL backend and pgsphere plugin."""

    @classmethod
    def _handler(cls, postgresql):
        super()._handler(postgresql)
        engine = sqlalchemy.engine.create_engine(postgresql.url())
        with engine.begin() as connection:
            try:
                connection.execute(sqlalchemy.text("CREATE EXTENSION pg_sphere"))
            except sqlalchemy.exc.DatabaseError as exc:
                raise unittest.SkipTest(f"pg_sphere extension does not exist: {exc}") from None

    def make_obscore_config(
        self, collections: list[str] | None = None, collection_type: str | None = None
    ) -> Config:
        """Make configuration for obscore manager."""
        obscore_config = super().make_obscore_config(collections, collection_type)
        obscore_config["spatial_plugins"] = {
            "pgsphere": {
                "cls": "lsst.daf.butler.registry.obscore.pgsphere.PgSphereObsCorePlugin",
                "config": {
                    "region_column": "pgs_region",
                    "position_column": "pgs_center",
                },
            }
        }
        return obscore_config

    def test_spatial(self):
        """Test that pgsphere plugin fills spatial columns."""
        collections = None
        registry = self.make_registry(collections)
        obscore = registry.obsCoreTableManager
        assert obscore is not None
        self._insert_datasets(registry)

        # select everything
        with obscore.query() as result:
            rows = list(result)
            self.assertEqual(len(rows), 6)

        db = registry._db
        assert registry.obsCoreTableManager is not None
        table = cast(ObsCoreLiveTableManager, registry.obsCoreTableManager).table

        # It's not easy to generate spatial queries in sqlalchemy, use plain
        # text queries for testing.

        # position matching visit=1, there is a single dataset
        query = f"SELECT * FROM {table.key} WHERE pgs_center <-> '(2d,0d)'::spoint < .1"
        with db.query(sqlalchemy.text(query)) as results:
            self.assertEqual(len(list(results)), 1)

        # position matching visit=4, there are two datasets
        query = f"SELECT * FROM {table.key} WHERE pgs_center <-> '(272d,0d)'::spoint < .1"
        with db.query(sqlalchemy.text(query)) as results:
            self.assertEqual(len(list(results)), 2)

        # position matching visit=1, there is a single dataset
        query = f"SELECT * FROM {table.key} WHERE '(2d,-3d)'::spoint @ pgs_region"
        with db.query(sqlalchemy.text(query)) as results:
            self.assertEqual(len(list(results)), 1)

        # position matching visit=4, there are two datasets
        query = f"SELECT * FROM {table.key} WHERE '(272d,3d)'::spoint @ pgs_region"
        with db.query(sqlalchemy.text(query)) as results:
            self.assertEqual(len(list(results)), 2)


if __name__ == "__main__":
    unittest.main()
