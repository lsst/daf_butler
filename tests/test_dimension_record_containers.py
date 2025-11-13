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

import copy
import json
import unittest

import pyarrow as pa
import pyarrow.parquet as pq
import pydantic

from lsst.daf.butler import (
    DataCoordinate,
    DimensionDataAttacher,
    DimensionDataExtractor,
    DimensionRecordSet,
    DimensionRecordSetDeserializer,
    DimensionRecordTable,
    SerializableDimensionData,
    SerializedKeyValueDimensionRecord,
)
from lsst.daf.butler.arrow_utils import TimespanArrowType
from lsst.daf.butler.tests.utils import create_populated_sqlite_registry

DIMENSION_DATA_FILES = [
    "resource://lsst.daf.butler/tests/registry_data/base.yaml",
    "resource://lsst.daf.butler/tests/registry_data/spatial.yaml",
]


class DimensionRecordContainersTestCase(unittest.TestCase):
    """Tests for the DimensionRecordTable class."""

    @classmethod
    def setUpClass(cls):
        # Create an in-memory SQLite database and Registry just to import the
        # YAML data.
        cls.butler = create_populated_sqlite_registry(*DIMENSION_DATA_FILES)
        cls.enterClassContext(cls.butler)
        cls.records = {
            element: tuple(list(cls.butler.registry.queryDimensionRecords(element)))
            for element in ("visit", "skymap", "patch")
        }
        cls.universe = cls.butler.dimensions
        cls.data_ids = list(cls.butler.registry.queryDataIds(["visit", "patch"]).expanded())

    def assert_record_sets_equal(self, a: DimensionRecordSet, b: DimensionRecordSet) -> None:
        self.assertEqual(a, b)  # only checks data ID equality
        self.assertCountEqual(list(a), list(b))

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
        self.assertListEqual([x.region for x in table], [x.region for x in self.records["visit"]])
        self.assertListEqual([x.timespan for x in table], [x.timespan for x in self.records["visit"]])
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
        original_records = list(self.records["visit"])
        table1 = DimensionRecordTable(self.universe["visit"], self.records["visit"])
        stream = pa.BufferOutputStream()
        pq.write_table(table1.to_arrow(), stream)
        table2 = DimensionRecordTable(
            universe=self.universe, table=pq.read_table(pa.BufferReader(stream.getvalue()))
        )
        self.assertEqual(list(table1), list(table2))
        self.assertEqual(original_records, list(table2))
        self.assertListEqual([x.region for x in table1], [x.region for x in table2])
        self.assertListEqual([x.region for x in original_records], [x.region for x in table2])
        self.assertListEqual([x.timespan for x in table1], [x.timespan for x in table2])
        self.assertListEqual([x.timespan for x in original_records], [x.timespan for x in table2])

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

    def test_arrow_timespan_nulls(self):
        """Test that null values for Timespan can be round-tripped through
        arrow.
        """
        schema = pa.schema([pa.field("timespan", TimespanArrowType(), nullable=True)])
        table = pa.Table.from_pylist([{"timespan": None}], schema=schema)
        output_value = table.to_pylist()[0]["timespan"]
        self.assertIsNone(output_value)

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

    def test_record_set_add(self):
        """Test DimensionRecordSet.add."""
        set1 = DimensionRecordSet("patch", self.records["patch"][:2], universe=self.universe)
        set1.add(self.records["patch"][2])
        with self.assertRaises(ValueError):
            set1.add(self.records["visit"][0])
        self.assertEqual(set1, DimensionRecordSet("patch", self.records["patch"][:3], universe=self.universe))
        set1.add(self.records["patch"][2])
        self.assertEqual(list(set1), list(self.records["patch"][:3]))

    def test_record_set_find_or_add(self):
        """Test DimensionRecordSet.find and find_with_required_values with
        a 'or_add' callback.
        """
        set1 = DimensionRecordSet("patch", self.records["patch"][:2], universe=self.universe)
        set1.find(self.records["patch"][2].dataId, or_add=lambda _c, _r: self.records["patch"][2])
        with self.assertRaises(ValueError):
            set1.find(self.records["visit"][0].dataId, or_add=lambda _c, _r: self.records["visit"][0])
        self.assertEqual(set1, DimensionRecordSet("patch", self.records["patch"][:3], universe=self.universe))

        set1.find_with_required_values(
            self.records["patch"][3].dataId.required_values, or_add=lambda _c, _r: self.records["patch"][3]
        )
        self.assertEqual(set1, DimensionRecordSet("patch", self.records["patch"][:4], universe=self.universe))

    def test_record_set_update_from_data_coordinates(self):
        """Test DimensionRecordSet.update_from_data_coordinates."""
        set1 = DimensionRecordSet("patch", self.records["patch"][:2], universe=self.universe)
        set1.update_from_data_coordinates(self.data_ids)
        for data_id in self.data_ids:
            self.assertIn(data_id.records["patch"], set1)

    def test_record_set_discard(self):
        """Test DimensionRecordSet.discard."""
        set1 = DimensionRecordSet("patch", self.records["patch"][:2], universe=self.universe)
        set2 = copy.deepcopy(set1)
        # These discards should do nothing.
        set1.discard(self.records["patch"][2])
        self.assertEqual(set1, set2)
        set1.discard(self.records["patch"][2].dataId)
        self.assertEqual(set1, set2)
        with self.assertRaises(ValueError):
            set1.discard(self.records["visit"][0])
        self.assertEqual(set1, set2)
        with self.assertRaises(ValueError):
            set1.discard(self.records["visit"][0].dataId)
        self.assertEqual(set1, set2)
        # These ones should remove a record from each set.
        set1.discard(self.records["patch"][1])
        set2.discard(self.records["patch"][1].dataId)
        self.assertEqual(set1, set2)
        self.assertNotIn(self.records["patch"][1], set1)
        self.assertNotIn(self.records["patch"][1], set2)

    def test_record_set_remove(self):
        """Test DimensionRecordSet.remove."""
        set1 = DimensionRecordSet("patch", self.records["patch"][:2], universe=self.universe)
        set2 = copy.deepcopy(set1)
        # These removes should raise with strong exception safety.
        with self.assertRaises(KeyError):
            set1.remove(self.records["patch"][2])
        self.assertEqual(set1, set2)
        with self.assertRaises(KeyError):
            set1.remove(self.records["patch"][2].dataId)
        self.assertEqual(set1, set2)
        with self.assertRaises(ValueError):
            set1.remove(self.records["visit"][0])
        self.assertEqual(set1, set2)
        with self.assertRaises(ValueError):
            set1.remove(self.records["visit"][0].dataId)
        self.assertEqual(set1, set2)
        # These ones should remove a record from each set.
        set1.remove(self.records["patch"][1])
        set2.remove(self.records["patch"][1].dataId)
        self.assertEqual(set1, set2)
        self.assertNotIn(self.records["patch"][1], set1)
        self.assertNotIn(self.records["patch"][1], set2)

    def test_record_set_pop(self):
        """Test DimensionRecordSet.pop."""
        set1 = DimensionRecordSet("patch", self.records["patch"][:2], universe=self.universe)
        set2 = copy.deepcopy(set1)
        record1 = set1.pop()
        set2.remove(record1)
        self.assertNotIn(record1, set1)
        self.assertEqual(set1, set2)
        record2 = set1.pop()
        set2.remove(record2)
        self.assertNotIn(record2, set1)
        self.assertEqual(set1, set2)
        self.assertFalse(set1)

    def test_record_key_value_serialization(self):
        """Test DimensionRecord.serialize_key_value, deserialize_key, and
        deserialize_value.
        """
        for element, records in self.records.items():
            with self.subTest(element=element):
                for record1 in records:
                    raw1 = record1.serialize_key_value()
                    self.assertIsInstance(raw1, list)
                    raw2 = json.loads(json.dumps(raw1))
                    self.assertEqual(raw1, raw2)
                    key, raw_value = record1.definition.RecordClass.deserialize_key(raw2)
                    self.assertEqual(key, record1.dataId.required_values)
                    record2 = record1.definition.RecordClass.deserialize_value(key, raw_value)
                    self.assertEqual(record1, record2)

    def test_record_set_serialization(self):
        """Test DimensionRecordSet.serialize_records and deserialize_records,
        as well as DimensionRecordSetDeserializer.
        """
        adapter = pydantic.TypeAdapter(list[SerializedKeyValueDimensionRecord])
        for element, records in self.records.items():
            with self.subTest(element=element):
                rs1 = DimensionRecordSet(element, records, universe=self.universe)
                raw = adapter.validate_json(adapter.dump_json(rs1.serialize_records()))
                rs2 = DimensionRecordSet(element, universe=self.universe)
                rs2.deserialize_records(raw)
                self.assert_record_sets_equal(rs1, rs2)
                deserializer = DimensionRecordSetDeserializer.from_raw(self.universe[element], raw)
                self.assertEqual(len(deserializer), len(records))
                for record1 in rs1:
                    record2 = deserializer[record1.dataId.required_values]
                    self.assertEqual(record1, record2)

    def test_extract_serialize_attach(self):
        """Test serializing expanded data IDs by first extracting their
        dimension records, serializing them in normalized form, and then
        attaching the records to deserialized data IDs.
        """
        dimensions = self.universe.conform(["visit", "patch", "htm5"])
        expanded_data_ids_1: list[DataCoordinate] = []
        pixelization = dimensions.universe.skypix_dimensions["htm5"].pixelization
        for data_id in self.data_ids:
            for htm_begin, htm_end in pixelization.envelope(data_id.records["patch"].region):
                for htm_id in range(htm_begin, htm_end):
                    new_data_id = self.butler.registry.expandDataId(data_id, htm5=htm_id)
                    self.assertEqual(new_data_id.dimensions, dimensions)
                    expanded_data_ids_1.append(new_data_id)
        extractor = DimensionDataExtractor.from_dimension_group(
            dimensions,
            ignore=["tract"],
            ignore_cached=True,
            include_skypix=False,
        )
        extractor.update(expanded_data_ids_1)
        thin_data_ids = [
            DataCoordinate.from_full_values(dimensions, data_id.full_values)
            for data_id in expanded_data_ids_1
        ]
        model1 = SerializableDimensionData.from_record_sets(extractor.records.values())
        self.assertEqual(model1.root.keys(), {"visit", "patch", "day_obs"})
        # Delete one patch record from the model to probe more logic branches
        # in the deserialization; like the tract records, we'll recover it by
        # querying the butler.
        del model1.root["patch"][-1]
        model2 = SerializableDimensionData.model_validate_json(model1.model_dump_json())
        with self.butler.query() as query:
            attacher1 = DimensionDataAttacher(
                deserializers=model2.make_deserializers(self.universe),
                cache=query._driver._dimension_record_cache,
                dimensions=dimensions,
            )
            expanded_data_ids_2 = attacher1.attach(dimensions, thin_data_ids, query=query)
        # This equality check is necessary but not sufficient for this test,
        # because DataCoordinate equality doesn't look at records.
        self.assertEqual(expanded_data_ids_1, expanded_data_ids_2)
        for a, b in zip(expanded_data_ids_1, expanded_data_ids_2):
            self.assertEqual(dict(a.records), dict(b.records))
        # Caching in the attacher should ensure that there is only one copy
        # of each dimension record in the new set.
        all_values = {(data_id["tract"], data_id["patch"]) for data_id in expanded_data_ids_2}
        all_identities = {
            id(data_id.records["patch"]): data_id.records["patch"] for data_id in expanded_data_ids_2
        }
        self.assertGreater(len(all_values), 1)
        self.assertEqual(len(all_identities), len(all_values), msg=str(all_identities))
        # Serialize and round-trip again, via attacher.serialized(), and attach
        # all records again.
        model3 = attacher1.serialized()
        attacher2 = DimensionDataAttacher(
            deserializers=model3.make_deserializers(self.universe), dimensions=dimensions
        )
        expanded_data_ids_4 = attacher2.attach(dimensions, thin_data_ids)
        self.assertEqual(expanded_data_ids_1, expanded_data_ids_4)
        for a, b in zip(expanded_data_ids_1, expanded_data_ids_4):
            self.assertEqual(dict(a.records), dict(b.records))
        # Attach data IDs with the original attacher again, after clearing out
        # its deserializers.  The cached records should all be present, and it
        # should work without any butler queries.
        attacher1.deserializers.clear()
        expanded_data_ids_3 = attacher1.attach(dimensions, thin_data_ids)
        self.assertEqual(expanded_data_ids_1, expanded_data_ids_3)
        for a, b in zip(expanded_data_ids_1, expanded_data_ids_3):
            self.assertEqual(dict(a.records), dict(b.records))
        # Delete a record and try again to check error-handling.
        removed_patch_data_id = next(iter(attacher1.records["patch"])).dataId
        attacher1.records["patch"].remove(removed_patch_data_id)
        with self.assertRaisesRegex(LookupError, r"No dimension record.*patch.*"):
            attacher1.attach(dimensions, thin_data_ids)


if __name__ == "__main__":
    unittest.main()
