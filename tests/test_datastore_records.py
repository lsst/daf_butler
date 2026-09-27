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

"""Tests for the record types a datastore stores alongside its files."""

from __future__ import annotations

import pickle
import uuid

import pytest

from lsst.daf.butler import DimensionUniverse, StorageClassFactory
from lsst.daf.butler.datastore import DatasetRefURIs
from lsst.daf.butler.datastore.record_data import (
    DatastoreRecordData,
    DatastoreRecordTable,
    SerializedDatastoreRecordData,
)
from lsst.daf.butler.datastore.stored_file_info import (
    StoredFileInfo,
    StoredFileInfoTable,
    make_datastore_path_relative,
)
from lsst.daf.butler.tests import DatasetTestHelper
from lsst.resources import ResourcePath
from lsst.utils.introspection import get_full_type_name


def test_sequence_access() -> None:
    """Verify that DatasetRefURIs can be treated like a two-item tuple."""
    uris = DatasetRefURIs()

    assert len(uris) == 2
    assert uris[0] is None
    assert uris[1] == {}

    primary_uri = ResourcePath("1/2/3")
    component_uri = ResourcePath("a/b/c")

    # affirm that DatasetRefURIs does not support MutableSequence functions.
    # The type: ignore comments are the point of the test: the assignments are
    # not allowed, statically or at run time.
    with pytest.raises(TypeError):
        uris[0] = primary_uri  # type: ignore[index]
    with pytest.raises(TypeError):
        uris[1] = {"foo": component_uri}  # type: ignore[index]

    # but DatasetRefURIs can be set by property name:
    uris.primaryURI = primary_uri
    uris.componentURIs = {"foo": component_uri}
    assert uris.primaryURI == primary_uri
    assert uris[0] == primary_uri

    primary, components = uris
    assert primary == primary_uri
    assert components == {"foo": component_uri}


def test_repr() -> None:
    """Verify __repr__ output."""
    uris = DatasetRefURIs(ResourcePath("/1/2/3"), {"comp": ResourcePath("/a/b/c")})
    assert (
        repr(uris)
        == 'DatasetRefURIs(ResourcePath("file:///1/2/3"), {\'comp\': ResourcePath("file:///a/b/c")})'
    )


def test_stored_file_info() -> None:
    helper = DatasetTestHelper()
    storage_class = StorageClassFactory().getStorageClass("StructuredDataDict")
    ref = helper.makeDatasetRef("metric", DimensionUniverse().empty, storage_class, {})

    record = dict(
        storage_class="StructuredDataDict",
        formatter="lsst.daf.butler.Formatter",
        path="a/b/c.txt",
        component="component",
        checksum=None,
        file_size=5,
    )
    info = StoredFileInfo.from_record(record)

    assert info.to_record() == record

    ref2 = helper.makeDatasetRef("metric", DimensionUniverse().empty, storage_class, {})
    rebased = info.rebase(ref2)
    assert rebased.rebase(ref) == info

    with pytest.raises(TypeError):
        rebased.update(formatter=42)

    with pytest.raises(ValueError, match="Unexpected keyword arguments"):
        rebased.update(something=42, new="42")

    # Check that pickle works on StoredFileInfo.
    pickled_info = pickle.dumps(info)
    unpickled_info = pickle.loads(pickled_info)
    assert unpickled_info == info


def test_make_datastore_path_relative() -> None:
    assert make_datastore_path_relative("a/relative/path") == "a/relative/path"
    assert make_datastore_path_relative("path/with#fragment") == "path/with#fragment"
    assert make_datastore_path_relative("http://server.com/some/path") == "some/path"
    assert make_datastore_path_relative("http://server.com/some/path#frag") == "some/path#frag"


def test_datastore_record_data_json_types() -> None:
    """Test that we don't round-trip checksums to UUIDs when deserializing
    datastore record data.
    """
    test_json = """
        {
            "dataset_ids": [
                "74478304-abf1-4a9c-9eb2-926090a84446"
            ],
            "records": {
                "lsst.daf.butler.datastore.stored_file_info.StoredFileInfo": {
                "74478304abf14a9c9eb2926090a84446": {
                    "file_datastore_records": [
                    {
                        "formatter": "lsst.daf.butler.formatters.yaml.YamlFormatter",
                        "path": "gain_factors/base-2025-158/gain_factors_spx_base-2025-158.yaml",
                        "storage_class": "GainFactors",
                        "component": "__NULL_STRING__",
                        "checksum": "cab515f6-ab67-0484-393f-aaa525dd526f",
                        "file_size": 5412
                    }
                    ]
                }
                }
            }
        }
    """
    id_str = "74478304abf14a9c9eb2926090a84446"
    s = SerializedDatastoreRecordData.model_validate_json(test_json)
    assert isinstance(
        s.records[get_full_type_name(StoredFileInfo)][id_str]["file_datastore_records"][0]["checksum"],
        str,
    )
    dataset_id = uuid.UUID(id_str)
    d = DatastoreRecordData.from_simple(s)
    stored = d.records[dataset_id]["file_datastore_records"][0]
    assert isinstance(stored, StoredFileInfo)
    assert isinstance(stored.checksum, str)


def test_empty_datastore_records_table() -> None:
    file_info_table = StoredFileInfoTable.from_records([])
    assert len(file_info_table) == 0

    assert len(DatastoreRecordTable.from_stored_file_info_table("datastore_name", file_info_table)) == 0

    assert len(DatastoreRecordTable.create_empty()) == 0
    assert len(DatastoreRecordTable.combine([])) == 0
    # Doesn't throw because there is no mismatch in datastore names.
    DatastoreRecordTable.create_empty().validate_datastore_names("arbitrary_name")


def test_stored_file_info_table_records() -> None:
    uuid1 = uuid.UUID("019e1892-7b9b-736d-8248-0e031723646c")
    uuid2 = uuid.UUID("019e1895-9ec3-7431-bed9-8ae60096103f")
    uuid3 = uuid.UUID("13d13272-454c-4bc4-94d5-3e322982eee8")
    checksum = (
        "021ced8799518305c451cde3e921515ef315ee7ba8937"
        "a92697a20c571c776afa4102744bc28d2d99d35f44e073cde80cf96e387f65f3967cca45b0d015f5a6b"
    )
    input_records = [
        {
            "dataset_id": uuid1,
            "path": "a/relative/path.fits",
            "formatter": "lsst.obs.base.formatters.fitsExposure.FitsExposureFormatter",
            "storage_class": "ExposureF",
            "component": "__NULL_STRING__",
            "checksum": None,
            "file_size": 123,
        },
        {
            "dataset_id": uuid2,
            "path": "file:///an/absolute/path.fits",
            "formatter": "lsst.obs.base.formatters.fitsExposure.FitsExposureFormatter",
            "storage_class": "ExposureF",
            "component": "comp",
            "checksum": checksum,
            "file_size": -1,
        },
    ]
    table = StoredFileInfoTable.from_records(input_records)
    assert len(table) == 2

    def _check_records(records: list[dict]) -> None:
        rec0 = records[0]
        assert rec0["dataset_id"] == uuid1
        assert rec0["path"] == "a/relative/path.fits"
        assert rec0["formatter"] == "lsst.obs.base.formatters.fitsExposure.FitsExposureFormatter"
        assert rec0["storage_class"] == "ExposureF"
        assert rec0["component"] is None
        assert rec0["checksum"] is None
        assert rec0["file_size"] == 123
        rec1 = records[1]
        assert rec1["dataset_id"] == uuid2
        assert rec1["path"] == "file:///an/absolute/path.fits"
        assert rec1["formatter"] == "lsst.obs.base.formatters.fitsExposure.FitsExposureFormatter"
        assert rec1["storage_class"] == "ExposureF"
        assert rec1["component"] == "comp"
        assert rec1["checksum"] == checksum
        assert rec1["file_size"] is None

    arrow_records = table.to_arrow().to_pylist()
    assert len(arrow_records) == 2
    _check_records(arrow_records)
    assert table.to_records() == input_records

    datastore_table = DatastoreRecordTable.from_stored_file_info_table("name_of_datastore", table)
    datastore_arrow_records = datastore_table.to_arrow().to_pylist()
    assert len(datastore_arrow_records) == 2
    _check_records(datastore_arrow_records)
    assert datastore_arrow_records[0]["datastore_name"] == "name_of_datastore"
    assert datastore_arrow_records[1]["datastore_name"] == "name_of_datastore"
    _check_records(datastore_table.to_stored_file_info_table().to_arrow().to_pylist())
    # Check round-tripping to_arrow() through from_arrow()
    _check_records(
        datastore_table.from_arrow(datastore_table.to_arrow())
        .to_stored_file_info_table()
        .to_arrow()
        .to_pylist()
    )

    with pytest.raises(ValueError, match="do not match known datastores"):
        datastore_table.validate_datastore_names(["not_the_same_datastore"])
    datastore_table.validate_datastore_names(["not_the_same_datastore", "name_of_datastore"])

    second_table = DatastoreRecordTable.from_stored_file_info_table(
        "other_datastore_name",
        StoredFileInfoTable.from_records(
            [
                {
                    "dataset_id": uuid3,
                    "path": "a/relative/path2.fits",
                    "formatter": "lsst.obs.lsst.rawFormatter.LsstCamRawFormatter",
                    "storage_class": "Exposure",
                    "component": "__NULL_STRING__",
                    "checksum": None,
                    "file_size": 1000,
                },
            ]
        ),
    )
    combined_table = DatastoreRecordTable.combine([datastore_table, second_table])
    combined_records = combined_table.to_arrow().to_pylist()
    _check_records(combined_records)
    rec2 = combined_records[2]
    assert rec2["dataset_id"] == uuid3
    assert rec2["path"] == "a/relative/path2.fits"
    assert rec2["formatter"] == "lsst.obs.lsst.rawFormatter.LsstCamRawFormatter"
    assert rec2["storage_class"] == "Exposure"
    assert rec2["component"] is None
    assert rec2["checksum"] is None
    assert rec2["file_size"] == 1000

    assert len(datastore_table.filter_by_datastore_name("unknown_datastore")) == 0
    filtered_table = combined_table.filter_by_datastore_name("name_of_datastore")
    assert len(filtered_table) == 2
    _check_records(filtered_table.to_arrow().to_pylist())
    assert filtered_table.to_stored_file_info_table().to_records() == input_records
