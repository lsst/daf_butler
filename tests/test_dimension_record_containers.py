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

import os
import unittest

import pyarrow as pa
import pyarrow.parquet as pq
from lsst.daf.butler import DimensionRecordSet, DimensionRecordTable, YamlRepoImportBackend
from lsst.daf.butler.registry import RegistryConfig, _RegistryFactory

DIMENSION_DATA_FILES = [
    os.path.normpath(os.path.join(os.path.dirname(__file__), "data", "registry", "base.yaml")),
    os.path.normpath(os.path.join(os.path.dirname(__file__), "data", "registry", "spatial.yaml")),
]


class DimensionRecordContainersTestCase(unittest.TestCase):
    """Tests for the DimensionRecordTable class."""

    @classmethod
    def setUpClass(cls):
        # Create an in-memory SQLite database and Registry just to import the
        # YAML data.
        config = RegistryConfig()
        config["db"] = "sqlite://"
        registry = _RegistryFactory(config).create_from_config()
        for data_file in DIMENSION_DATA_FILES:
            with open(data_file) as stream:
                backend = YamlRepoImportBackend(stream, registry)
            backend.register()
            backend.load(datastore=None)
        cls.records = {
            element: tuple(list(registry.queryDimensionRecords(element)))
            for element in ("visit", "skymap", "patch")
        }
        cls.universe = registry.dimensions
        cls.data_ids = list(registry.queryDataIds(["visit", "patch"]).expanded())

    def test_record_table_schema_visit(self):
        """Test that the Arrow schema for 'visit' has the right types,
        including dictionary encoding.
        """
        schema = DimensionRecordTable.make_arrow_schema(self.universe["visit"])
        self.assertEqual(schema.field("instrument").type, pa.dictionary(pa.int32(), pa.string()))
        self.assertEqual(schema.field("id").type, pa.uint64())
        self.assertEqual(schema.field("physical_filter").type, pa.dictionary(pa.int32(), pa.string()))
        self.assertEqual(schema.field("name").type, pa.string())
        self.assertEqual(schema.field("observation_reason").type, pa.dictionary(pa.int32(), pa.string()))

    def test_record_table_schema_skymap(self):
        """Test that the Arrow schema for 'skymap' has the right types,
        including dictionary encoding.
        """
        schema = DimensionRecordTable.make_arrow_schema(self.universe["skymap"])
        self.assertEqual(schema.field("name").type, pa.string())
        self.assertEqual(schema.field("hash").type, pa.binary())

    def test_empty_record_table_visit(self):
        """Test methods on a table that was initialized with no records.

        We use 'visit' records for this test because they have both timespans
        and regions, and those are the tricky column types for interoperability
        with Arrow.
        """
        table = DimensionRecordTable(self.universe["visit"])
        self.assertEqual(len(table), 0)
        self.assertEqual(list(table), [])
        self.assertEqual(table.element, self.universe["visit"])
        with self.assertRaises(IndexError):
            table[0]
        self.assertEqual(len(table.column("instrument")), 0)
        self.assertEqual(len(table.column("id")), 0)
        self.assertEqual(len(table.column("physical_filter")), 0)
        self.assertEqual(len(table.column("name")), 0)
        self.assertEqual(len(table.column("day_obs")), 0)
        table.extend(self.records["visit"])
        self.assertCountEqual(table, self.records["visit"])
        self.assertEqual(
            table.to_arrow().schema, DimensionRecordTable.make_arrow_schema(self.universe["visit"])
        )

    def test_empty_record_table_skymap(self):
        """Test methods on a table that was initialized with no records.

        We use 'skymap' records for this test because that's the only one with
        a "hash" column.
        """
        table = DimensionRecordTable(self.universe["skymap"])
        self.assertEqual(len(table), 0)
        self.assertEqual(list(table), [])
        self.assertEqual(table.element, self.universe["skymap"])
        with self.assertRaises(IndexError):
            table[0]
        self.assertEqual(len(table.column("name")), 0)
        self.assertEqual(len(table.column("hash")), 0)
        table.extend(self.records["skymap"])
        self.assertCountEqual(table, self.records["skymap"])
        self.assertEqual(
            table.to_arrow().schema, DimensionRecordTable.make_arrow_schema(self.universe["skymap"])
        )

    def test_full_record_table_visit(self):
        """Test methods on a table that was initialized with an iterable.

        We use 'visit' records for this test because they have both timespans
        and regions, and those are the tricky column types for interoperability
        with Arrow.
        """
        table = DimensionRecordTable(self.universe["visit"], self.records["visit"])
        self.assertEqual(len(table), 2)
        self.assertEqual(table[0], self.records["visit"][0])
        self.assertEqual(table[1], self.records["visit"][1])
        self.assertEqual(list(table), list(self.records["visit"]))
        self.assertEqual(table.element, self.universe["visit"])
        self.assertEqual(table.column("instrument")[0].as_py(), "Cam1")
        self.assertEqual(table.column("instrument")[1].as_py(), "Cam1")
        self.assertEqual(table.column("id")[0].as_py(), 1)
        self.assertEqual(table.column("id")[1].as_py(), 2)
        self.assertEqual(table.column("name")[0].as_py(), "1")
        self.assertEqual(table.column("name")[1].as_py(), "2")
        self.assertEqual(table.column("physical_filter")[0].as_py(), "Cam1-G")
        self.assertEqual(table.column("physical_filter")[1].as_py(), "Cam1-R1")
        self.assertEqual(table.column("day_obs")[0].as_py(), 20210909)
        self.assertEqual(table.column("day_obs")[1].as_py(), 20210909)
        self.assertEqual(list(table[:1]), list(self.records["visit"][:1]))
        self.assertEqual(list(table[1:]), list(self.records["visit"][1:]))
        table.extend(self.records["visit"])
        self.assertCountEqual(table, self.records["visit"] + self.records["visit"])

    def test_full_record_table_skymap(self):
        """Test methods on a table that was initialized with an iterable.

        We use 'skymap' records for this test because that's the only one with
        a "hash" column.
        """
        table = DimensionRecordTable(self.universe["skymap"], self.records["skymap"])
        self.assertEqual(len(table), 1)
        self.assertEqual(table[0], self.records["skymap"][0])
        self.assertEqual(list(table), list(self.records["skymap"]))
        self.assertEqual(table.element, self.universe["skymap"])
        self.assertEqual(table.column("name")[0].as_py(), "SkyMap1")
        self.assertEqual(table.column("hash")[0].as_py(), b"notreallyahashofanything!")
        table.extend(self.records["skymap"])
        self.assertCountEqual(table, self.records["skymap"] + self.records["skymap"])

    def test_record_table_parquet_visit(self):
        """Test round-tripping a dimension record table through Parquet.

        We use 'visit' records for this test because they have both timespans
        and regions, and those are the tricky column types for interoperability
        with Arrow.
        """
        table1 = DimensionRecordTable(self.universe["visit"], self.records["visit"])
        stream = pa.BufferOutputStream()
        pq.write_table(table1.to_arrow(), stream)
        table2 = DimensionRecordTable(
            universe=self.universe, table=pq.read_table(pa.BufferReader(stream.getvalue()))
        )
        self.assertEqual(list(table1), list(table2))

    def test_record_table_parquet_skymap(self):
        """Test round-tripping a dimension record table through Parquet.

        We use 'skymap' records for this test because that's the only one with
        a "hash" column.
        """
        table1 = DimensionRecordTable(self.universe["skymap"], self.records["skymap"])
        stream = pa.BufferOutputStream()
        pq.write_table(table1.to_arrow(), stream)
        table2 = DimensionRecordTable(
            universe=self.universe, table=pq.read_table(pa.BufferReader(stream.getvalue()))
        )
        self.assertEqual(list(table1), list(table2))

    def test_record_chunk_init(self):
        """Test constructing a DimensionRecordTable from an iterable in chunks.

        We use 'patch' records for this test because there are enough of them
        to have multiple chunks.
        """
        table1 = DimensionRecordTable(self.universe["patch"], self.records["patch"], batch_size=5)
        self.assertEqual(len(table1), 12)
        self.assertEqual([len(batch) for batch in table1.to_arrow().to_batches()], [5, 5, 2])
        self.assertEqual(list(table1), list(self.records["patch"]))

    def test_record_set_const(self):
        """Test attributes and methods of `DimensionRecordSet` that do not
        modify the set.

        We use 'patch' records for this test because there are enough of them
        to do nontrivial set-operation tests.
        """
        element = self.universe["patch"]
        records = self.records["patch"]
        set1 = DimensionRecordSet(element, records[:7])
        self.assertEqual(set1, DimensionRecordSet("patch", records[:7], universe=self.universe))
        # DimensionRecordSets do not compare as equal with other set types,
        # even with the same content.
        self.assertNotEqual(set1, set(records[:7]))
        with self.assertRaises(TypeError):
            DimensionRecordSet("patch", records[:7])
        self.assertEqual(set1.element, self.universe["patch"])
        self.assertEqual(len(set1), 7)
        self.assertEqual(list(set1), list(records[:7]))
        self.assertIn(records[4], set1)
        self.assertIn(records[5].dataId, set1)
        self.assertNotIn(self.records["visit"][0], set1)
        self.assertTrue(set1.issubset(DimensionRecordSet(element, records[:8])))
        self.assertFalse(set1.issubset(DimensionRecordSet(element, records[1:6])))
        with self.assertRaises(ValueError):
            set1.issubset(DimensionRecordSet(self.universe["tract"]))
        self.assertTrue(set1.issuperset(DimensionRecordSet(element, records[1:6])))
        self.assertFalse(set1.issuperset(DimensionRecordSet(element, records[:8])))
        with self.assertRaises(ValueError):
            set1.issuperset(DimensionRecordSet(self.universe["tract"]))
        self.assertTrue(set1.isdisjoint(DimensionRecordSet(element, records[7:])))
        self.assertFalse(set1.isdisjoint(DimensionRecordSet(element, records[5:8])))
        with self.assertRaises(ValueError):
            set1.isdisjoint(DimensionRecordSet(self.universe["tract"]))
        self.assertEqual(
            set1.intersection(DimensionRecordSet(element, records[5:])),
            DimensionRecordSet(element, records[5:7]),
        )
        self.assertEqual(
            set1.intersection(DimensionRecordSet(element, records[5:])),
            DimensionRecordSet(element, records[5:7]),
        )
        with self.assertRaises(ValueError):
            set1.intersection(DimensionRecordSet(self.universe["tract"]))
        self.assertEqual(
            set1.difference(DimensionRecordSet(element, records[5:])),
            DimensionRecordSet(element, records[:5]),
        )
        with self.assertRaises(ValueError):
            set1.difference(DimensionRecordSet(self.universe["tract"]))
        self.assertEqual(
            set1.union(DimensionRecordSet(element, records[5:9])),
            DimensionRecordSet(element, records[:9]),
        )
        with self.assertRaises(ValueError):
            set1.union(DimensionRecordSet(self.universe["tract"]))
        self.assertEqual(set1.find(records[0].dataId), records[0])
        with self.assertRaises(LookupError):
            set1.find(self.records["patch"][8].dataId)
        with self.assertRaises(ValueError):
            set1.find(self.records["visit"][0].dataId)
        self.assertEqual(set1.find_with_required_values(records[0].dataId.required_values), records[0])


if __name__ == "__main__":
    unittest.main()
