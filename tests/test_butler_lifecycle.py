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

"""Tests for constructing, closing, pickling and stringifying a Butler."""

from __future__ import annotations

import contextlib
import os
import pathlib
import pickle
import unittest.mock
import uuid
import warnings
import weakref
from collections.abc import Iterator, Mapping
from typing import Any

import pytest
from butler_test_support import (
    AXIS_NAMES,
    BUTLER_TESTS_AXES,
    FILE_DATASTORE_AXES,
    assert_get_components,
    run_put_get_test,
)

from lsst.daf.butler import (
    Butler,
    ButlerMetrics,
    ButlerRepoIndex,
    Config,
    DataCoordinate,
    DatasetProvenance,
    DatasetRef,
    DatasetType,
    DimensionRecord,
    FileDataset,
)
from lsst.daf.butler.direct_butler import DirectButler
from lsst.daf.butler.registry import DataIdValueError
from lsst.daf.butler.registry.sql_registry import SqlRegistry
from lsst.daf.butler.tests import MetricsExampleModel
from lsst.daf.butler.tests.fixtures import (
    ButlerHarness,
    ServerButlerHarness,
    add_dataset_type,
    get_test_data_path,
    make_example_metrics,
)
from lsst.daf.butler.tests.utils import create_populated_sqlite_registry
from lsst.resources import ResourcePath
from lsst.utils.introspection import get_full_type_name

PICKLE_AXES = [
    pytest.param(
        *param.values,
        id=param.id,
        marks=[
            *param.marks,
            pytest.mark.xfail(reason="Pickling not yet implemented for RemoteButler/HybridButler."),
        ],
    )
    if isinstance(param.id, str) and param.id.startswith("server")
    else param
    for param in BUTLER_TESTS_AXES
]
"""BUTLER_TESTS_AXES with the server axes marked as expected to fail."""


class TransactionTestError(Exception):
    """Specific error for testing transactions, to prevent misdiagnosing
    that might otherwise occur when a standard exception is used.
    """


@pytest.fixture(autouse=True, scope="module")
def _clean_environment() -> Iterator[None]:
    """Remove external environment variables that affect these tests.

    Only this file needs it: the repository index variable is read by the
    constructor and repository-alias tests here and nowhere else in the
    migrated set.
    """
    saved = os.environ.pop("DAF_BUTLER_REPOSITORY_INDEX", None)
    yield
    if saved is not None:
        os.environ["DAF_BUTLER_REPOSITORY_INDEX"] = saved


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_constructor(butler_harness: ButlerHarness, butler_client: str) -> None:
    """Independent test of constructor."""
    if butler_client == "server":
        # RemoteButler constructor is tested in test_server.py and
        # test_remote_butler.py.
        return

    config_file = butler_harness.config_file
    default_run = butler_harness.default_run
    stack = contextlib.ExitStack()
    butler = butler_harness.create_empty_butler(run=default_run)
    assert isinstance(butler, Butler)

    # Check that butler.yaml is added automatically.
    if config_file.endswith(end := "/butler.yaml"):
        config_dir = config_file[: -len(end)]
        butler = stack.enter_context(Butler.from_config(config_dir, run=default_run))
        assert isinstance(butler, Butler)

        # Even with a ResourcePath.
        butler = stack.enter_context(
            Butler.from_config(ResourcePath(config_dir, forceDirectory=True), run=default_run)
        )
        assert isinstance(butler, Butler)

    collections = set(butler.collections.query("*"))
    assert collections == {default_run}

    # Check that some special characters can be included in run name.
    special_run = "u@b.c-A"
    with Butler.from_config(butler=butler, run=special_run) as butler_special:
        collections = set(butler_special.registry.queryCollections("*@*"))
        assert collections == {special_run}

    with Butler.from_config(butler=butler, collections=["other"]) as butler2:
        assert butler2.collections.defaults == ("other",)
        assert butler2.run is None
        assert type(butler._datastore) is type(butler2._datastore)
        assert butler._datastore.config == butler2._datastore.config

    # Test that we can use an environment variable to find this
    # repository.
    butler_index = Config()
    butler_index["label"] = config_file
    for suffix in (".yaml", ".json"):
        # Ensure that the content differs so that we know that
        # we aren't reusing the cache.
        bad_label = f"file://bucket/not_real{suffix}"
        butler_index["bad_label"] = bad_label
        with ResourcePath.temporary_uri(suffix=suffix) as temp_file:
            butler_index.dumpToUri(temp_file)
            with unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_REPOSITORY_INDEX": str(temp_file)}):
                assert Butler.get_known_repos() == {"label", "bad_label"}
                uri = Butler.get_repo_uri("bad_label")
                assert uri == ResourcePath(bad_label)
                uri = Butler.get_repo_uri("label")
                butler = Butler.from_config(uri, writeable=False)
                assert isinstance(butler, Butler)
                butler.close()
                butler = Butler.from_config("label", writeable=False)
                assert isinstance(butler, Butler)
                butler.close()
                with pytest.raises(FileNotFoundError, match="aliases:.*bad_label"):
                    Butler.from_config("not_there", writeable=False)
                with pytest.raises(FileNotFoundError, match="resolved from alias 'bad_label'"):
                    Butler.from_config("bad_label")
                with pytest.raises(FileNotFoundError):
                    # Should ignore aliases.
                    Butler.from_config(ResourcePath("label", forceAbsolute=False))
                with pytest.raises(KeyError, match="not known to") as exc_info:
                    Butler.get_repo_uri("missing")
                assert Butler.get_repo_uri("missing", True) == ResourcePath("missing", forceAbsolute=False)
                assert "not known to" in str(exc_info.value)
                # Should report no failure.
                assert ButlerRepoIndex.get_failure_reason() == ""
    with ResourcePath.temporary_uri(suffix=suffix) as temp_file:
        # Now with empty configuration.
        butler_index = Config()
        butler_index.dumpToUri(temp_file)
        with (
            unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_REPOSITORY_INDEX": str(temp_file)}),
            pytest.raises(FileNotFoundError, match="(no known aliases)"),
        ):
            Butler.from_config("label")
    with ResourcePath.temporary_uri(suffix=suffix) as temp_file:
        # Now with bad contents.
        with open(temp_file.ospath, "w") as fh:
            print("'", file=fh)
        with (
            unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_REPOSITORY_INDEX": str(temp_file)}),
            pytest.raises(FileNotFoundError, match="(no known aliases:.*could not be read)"),
        ):
            Butler.from_config("label")
    with unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_REPOSITORY_INDEX": "file://not_found/x.yaml"}):
        with pytest.raises(FileNotFoundError):
            Butler.get_repo_uri("label")
        assert Butler.get_known_repos() == set()

        with pytest.raises(FileNotFoundError, match="index file not found"):
            Butler.from_config("label")

        # Check that we can create Butler when the alias file is not found.
        butler = butler_harness.create_empty_butler(writeable=False)
        assert isinstance(butler, Butler)
    with pytest.raises(RuntimeError, match="No repository index defined") as runtime_info:
        # No environment variable set.
        Butler.get_repo_uri("label")
    assert Butler.get_repo_uri("label", True) == ResourcePath("label", forceAbsolute=False)
    assert "No repository index defined" in str(runtime_info.value)
    with pytest.raises(FileNotFoundError, match="no known aliases.*No repository index"):
        # No aliases registered.
        Butler.from_config("not_there")
    assert Butler.get_known_repos() == set()
    stack.close()


def test_path_constructor(butler_harness: ButlerHarness) -> None:
    """Independent test of constructor using PathLike."""
    config_file = butler_harness.config_file
    butler = butler_harness.create_empty_butler(run=butler_harness.default_run)
    assert isinstance(butler, Butler)

    with contextlib.ExitStack() as stack:
        # And again with a Path object with the butler yaml
        path = pathlib.Path(config_file)
        butler = stack.enter_context(Butler.from_config(path, writeable=False))
        assert isinstance(butler, Butler)

        # And again with a Path object without the butler yaml. The guard
        # matters for any layout whose config is not named butler.yaml.
        if config_file.endswith("butler.yaml"):
            path = pathlib.Path(os.path.dirname(config_file))
            butler = stack.enter_context(Butler.from_config(path, writeable=False))
            assert isinstance(butler, Butler)


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_close(butler_harness: ButlerHarness) -> None:
    butler = butler_harness.create_empty_butler(cleanup=False)
    # A RemoteButler has no _closed flag, so only the direct case can check it.
    direct_butler = butler if isinstance(butler, DirectButler) else None
    if direct_butler is not None:
        assert not direct_butler._closed

    with butler as butler_from_context_manager:
        assert butler is butler_from_context_manager
    if direct_butler is not None:
        assert direct_butler._closed
        with pytest.raises(RuntimeError, match="has been closed"):
            butler.get_dataset_type("raw")

    # Close may be called multiple times.
    butler.close()
    if direct_butler is not None:
        assert direct_butler._closed


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_garbage_collection(butler_harness: ButlerHarness) -> None:
    """Test that Butler does not have any circular references that prevent
    it from being garbage collected immediately when it goes out of scope.
    """
    butler = butler_harness.create_empty_butler(cleanup=False)
    is_direct_butler = isinstance(butler, DirectButler)
    butler_ref = weakref.ref(butler)
    # Narrowed with isinstance rather than the flag so that no second strong
    # reference to the butler outlives the `del` below.
    if isinstance(butler, DirectButler):
        registry_ref = weakref.ref(butler._registry)
        managers_ref = weakref.ref(butler._registry._managers)
        datastore_ref = weakref.ref(butler._datastore)
        db_ref = weakref.ref(butler._registry._db)
        engine_ref = weakref.ref(butler._registry._db._engine)

    with warnings.catch_warnings():
        # Hide warnings from unclosed database handles.
        warnings.simplefilter("ignore", ResourceWarning)
        del butler
        assert butler_ref() is None, "Butler should have been garbage collected"
        if is_direct_butler:
            assert registry_ref() is None, "SqlRegistry should have been garbage collected"
            assert managers_ref() is None, "Registry managers should have been garbage collected"
            assert datastore_ref() is None, "Datastore should have been garbage collected"
            assert db_ref() is None, "Database should have been garbage collected"
            # SQLAlchemy has internal reference cycles, so the Engine instance
            # is not cleaned up promptly even if we release our reference to
            # it.  Explicitly clean it up here to avoid file handles leaking.
            engine = engine_ref()
            if engine is not None:
                engine.dispose()


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_daf_butler_repositories(butler_harness: ButlerHarness, butler_client: str) -> None:
    # butler_harness is requested but unused: the original built a repository
    # in setUp for every one of these runs, and the axis parametrization
    # needs the whole fixture closure.
    if butler_client == "server":
        # Loading of RemoteButler via repository index is tested in
        # test_server.py.
        return

    with unittest.mock.patch.dict(
        os.environ,
        {"DAF_BUTLER_REPOSITORIES": "label: 'https://someuri.com'\notherLabel: 'https://otheruri.com'\n"},
    ):
        assert str(Butler.get_repo_uri("label")) == "https://someuri.com"

    with (
        unittest.mock.patch.dict(
            os.environ,
            {
                "DAF_BUTLER_REPOSITORIES": "label: https://someuri.com",
                "DAF_BUTLER_REPOSITORY_INDEX": "https://someuri.com",
            },
        ),
        pytest.raises(RuntimeError, match="Only one of the environment variables"),
    ):
        Butler.get_repo_uri("label")

    with (
        unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_REPOSITORIES": "invalid"}),
        pytest.raises(ValueError, match="Repository index not in expected format"),
    ):
        Butler.get_repo_uri("label")


@pytest.mark.parametrize(AXIS_NAMES, PICKLE_AXES, indirect=True)
def test_pickle(butler_harness: ButlerHarness) -> None:
    """Test pickle support."""
    butler = butler_harness.create_empty_butler(run=butler_harness.default_run)
    assert isinstance(butler, DirectButler), "Expect DirectButler in configuration"
    with pickle.loads(pickle.dumps(butler)) as butler_out:
        assert isinstance(butler_out, DirectButler)
        assert butler_out._config == butler._config
        assert list(butler_out.collections.defaults) == list(butler.collections.defaults)
        assert butler_out.run == butler.run


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_transaction(butler_harness: ButlerHarness, butler_client: str) -> None:
    if butler_client == "server":
        # Transactions will never be supported for RemoteButler.
        return

    butler = butler_harness.create_empty_butler(run=butler_harness.default_run)
    dataset_type_name = "test_metric"
    dimensions = butler.dimensions.conform(["instrument", "visit"])
    dimension_entries: tuple[tuple[str, Mapping[str, Any]], ...] = (
        ("instrument", {"instrument": "DummyCam"}),
        ("physical_filter", {"instrument": "DummyCam", "name": "d-r", "band": "R"}),
        ("day_obs", {"instrument": "DummyCam", "id": 20250101}),
        (
            "visit",
            {
                "instrument": "DummyCam",
                "id": 42,
                "name": "fortytwo",
                "physical_filter": "d-r",
                "day_obs": 20250101,
            },
        ),
    )
    storage_class = butler_harness.storage_class_factory.getStorageClass("StructuredData")
    metric = make_example_metrics()
    data_id = {"instrument": "DummyCam", "visit": 42}
    # Create and register a DatasetType
    dataset_type = add_dataset_type(dataset_type_name, dimensions, storage_class, butler.registry)
    with pytest.raises(TransactionTestError), butler.transaction():  # noqa: PT012
        # Add needed Dimensions
        for args in dimension_entries:
            butler.registry.insertDimensionData(*args)
        # Store a dataset
        ref = butler.put(metric, dataset_type_name, data_id)
        assert isinstance(ref, DatasetRef)
        # Test get of a ref.
        metric_out = butler.get(ref)
        assert metric == metric_out
        # Test get
        metric_out = butler.get(dataset_type_name, data_id)
        assert metric == metric_out
        # Check we can get components
        assert_get_components(butler, ref, ("summary", "data", "output"), metric)
        raise TransactionTestError("This should roll back the entire transaction")

    with pytest.raises(DataIdValueError):
        butler.registry.expandDataId(data_id)
    # Should raise LookupError for missing data ID value
    with pytest.raises(LookupError):
        butler.get(dataset_type_name, data_id)
    # Also check explicitly if Dataset entry is missing
    assert butler.find_dataset(dataset_type, data_id, collections=butler.collections.defaults) is None
    # Direct retrieval should not find the file in the Datastore
    with pytest.raises(FileNotFoundError):
        butler.get(ref)


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_stringification(butler_harness: ButlerHarness, butler_client: str, registry_backend: str) -> None:
    if butler_client == "server":
        assert isinstance(butler_harness, ServerButlerHarness)
        assert (
            str(butler_harness.server_instance.remote_butler)
            == "RemoteButler(https://test.example/api/butler/repo/testrepo/)"
        )
        return

    profile = butler_harness.profile
    # The registry string is a property of the backend, not the datastore.
    registry_str = "PostgreSQL@test" if registry_backend == "postgres" else "/gen3.sqlite3"

    butler = butler_harness.create_empty_butler(run=butler_harness.default_run)
    butler_str = str(butler)

    for test_str in profile.datastore_str:
        assert test_str in butler_str
    assert registry_str in butler_str

    datastore_name = butler._datastore.name
    if profile.datastore_name is not None:
        for test_str in profile.datastore_name:
            assert test_str in datastore_name


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_butler_rewrite_data_id(butler_harness: ButlerHarness) -> None:
    """Test that dataIds can be rewritten based on dimension records."""
    default_run = butler_harness.default_run
    butler = butler_harness.create_empty_butler(run=default_run)

    storage_class = butler_harness.storage_class_factory.getStorageClass("StructuredDataDict")
    dataset_type_name = "random_data"

    # Create dimension records.
    butler.registry.insertDimensionData("instrument", {"name": "DummyCamComp"})
    butler.registry.insertDimensionData(
        "physical_filter", {"instrument": "DummyCamComp", "name": "d-r", "band": "R"}
    )
    butler.registry.insertDimensionData(
        "detector", {"instrument": "DummyCamComp", "id": 1, "full_name": "det1"}
    )

    dimensions = butler.dimensions.conform(["instrument", "exposure"])
    dataset_type = DatasetType(dataset_type_name, dimensions, storage_class)
    butler.registry.registerDatasetType(dataset_type)

    n_exposures = 5
    dayobs = 20210530

    # Create records for multiple day_obs but same seq_num to test that
    # we are constraining gets properly when day_obs/seq_num is used
    # for an exposure. Second day is year in future but is not used.
    for day_obs in (dayobs, dayobs + 1_00_00):
        butler.registry.insertDimensionData("day_obs", {"instrument": "DummyCamComp", "id": day_obs})

        for i in range(n_exposures):
            group_name = f"group_{day_obs}_{i}"
            butler.registry.insertDimensionData("group", {"instrument": "DummyCamComp", "name": group_name})
            butler.registry.insertDimensionData(
                "exposure",
                {
                    "instrument": "DummyCamComp",
                    "id": day_obs + i,
                    "obs_id": f"exp_{day_obs}_{i}",
                    "seq_num": i,
                    "day_obs": day_obs,
                    "physical_filter": "d-r",
                    "group": group_name,
                },
            )

    # Write some data.
    for i in range(n_exposures):
        metric = {"something": i, "other": "metric", "list": [2 * x for x in range(i)]}

        # Use the seq_num for the put to test rewriting.
        data_id = {"seq_num": i, "day_obs": dayobs, "instrument": "DummyCamComp", "physical_filter": "d-r"}
        ref = butler.put(metric, dataset_type_name, dataId=data_id)

        # Check that the exposure is correct in the dataId
        assert ref.dataId["exposure"] == dayobs + i

        # and check that we can get the dataset back with the same dataId
        new_metric = butler.get(dataset_type_name, dataId=data_id)
        assert new_metric == metric

    # Check that we can find the datasets using the day_obs or the
    # exposure.day_obs.
    datasets_1 = list(
        butler.registry.queryDatasets(
            dataset_type,
            collections=default_run,
            where="day_obs = :dayObs AND instrument = :instr",
            bind={"dayObs": dayobs, "instr": "DummyCamComp"},
        )
    )
    datasets_2 = list(
        butler.registry.queryDatasets(
            dataset_type,
            collections=default_run,
            where="exposure.day_obs = :dayObs AND instrument = :instr",
            bind={"dayObs": dayobs, "instr": "DummyCamComp"},
        )
    )
    assert datasets_1 == datasets_2


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_transfer_dimension_records_from(butler_harness: ButlerHarness) -> None:
    source_butler = butler_harness.create_empty_butler(writeable=True)
    source_butler.import_(filename=get_test_data_path("lsstcam-subset.yaml"))

    visit_id = 2025120200439
    exposure_id = visit_id
    with create_populated_sqlite_registry() as target_butler:
        target_butler.transfer_dimension_records_from(
            source_butler,
            [
                # Should trigger the lookup of visit and all its associated
                # "populated_by" records (visit_detector_region,
                # visit_definition, etc.)
                DataCoordinate.standardize(
                    {"instrument": "LSSTCam", "visit": visit_id, "detector": 10},
                    universe=source_butler.dimensions,
                ),
                # Shouldn't add any records to the lookup.
                DataCoordinate.make_empty(source_butler.dimensions),
            ],
        )

        def _fetch_record(dimension: str) -> DimensionRecord:
            records = target_butler.query_dimension_records(dimension)
            assert len(records) == 1
            return records[0]

        visit = _fetch_record("visit")
        assert visit.id == visit_id
        assert visit.day_obs == 20251202
        assert visit.target_name == "lowdust"
        assert visit.seq_num == 439
        original_visit = source_butler.query_dimension_records("visit", instrument="LSSTCam", visit=visit_id)[
            0
        ]
        assert visit.region == original_visit.region
        assert visit.timespan == original_visit.timespan

        visit_detector_region = _fetch_record("visit_detector_region")
        assert visit_detector_region.instrument == "LSSTCam"
        assert visit_detector_region.detector == 10
        assert visit_detector_region.visit == visit_id
        original_visit_detector_region = source_butler.query_dimension_records(
            "visit_detector_region", instrument="LSSTCam", visit=visit_id, detector=10
        )[0]
        assert visit_detector_region.region == original_visit_detector_region.region

        visit_definition = _fetch_record("visit_definition")
        assert visit_definition.instrument == "LSSTCam"
        assert visit_definition.exposure == 2025120200439
        assert visit_definition.visit == visit_id

        # The matching exposure record should have been pulled in via
        # visit -> visit_definition.
        exposure = _fetch_record("exposure")
        assert exposure.instrument == "LSSTCam"
        assert exposure.id == 2025120200439
        assert exposure.obs_id == "MC_O_20251202_000439"
        original_exposure = source_butler.query_dimension_records(
            "exposure", instrument="LSSTCam", exposure=exposure_id
        )[0]
        assert exposure.timespan == original_exposure.timespan

        group = _fetch_record("group")
        assert group.instrument == "LSSTCam"
        assert group.name == "2025-12-03T07:58:10.858"

        visit_system_memberships = target_butler.query_dimension_records("visit_system_membership")
        visit_system_memberships.sort(key=lambda record: record.visit_system)
        assert len(visit_system_memberships) == 2
        assert visit_system_memberships[0].visit_system == 0
        assert visit_system_memberships[1].visit_system == 2
        assert visit_system_memberships[0].visit == visit_id
        assert visit_system_memberships[1].visit == visit_id

        visit_systems = target_butler.query_dimension_records("visit_system")
        visit_systems.sort(key=lambda record: record.id)
        assert visit_systems[0].id == 0
        assert visit_systems[1].id == 2
        assert visit_systems[0].name == "one-to-one"
        assert visit_systems[1].name == "by-seq-start-end"


@pytest.mark.parametrize(AXIS_NAMES, FILE_DATASTORE_AXES, indirect=True)
def test_butler_metrics(butler_harness: ButlerHarness) -> None:
    """Test that metrics are collected."""
    run = "test_run"
    metrics = ButlerMetrics()
    butler, dataset_type = butler_harness.create_butler(
        run, "MetricsExampleModelProvenance", "prov_metric", metrics=metrics
    )
    data = MetricsExampleModel(
        summary={"AM1": 5.2, "AM2": 30.6},
        output={"a": [1, 2, 3], "b": {"blue": 5, "red": "green"}},
        data=[563, 234, 456.7, 752, 8, 9, 27],
    )

    data_ref = butler.put(data, dataset_type, visit=424, instrument="DummyCamComp")
    butler.get(data_ref)
    butler.get(data_ref)
    assert metrics.n_get == 2
    assert metrics.time_in_get > 0.0
    assert metrics.n_put == 1
    assert metrics.time_in_put > 0.0

    deferred = butler.getDeferred(data_ref)
    deferred.get()
    assert metrics.n_get == 3

    with butler.record_metrics() as new:
        data_ref_2 = butler.put(data, dataset_type, visit=425, instrument="DummyCamComp")
        butler.get(data_ref)

        butler.pruneDatasets([data_ref, data_ref_2], purge=True, unstore=True)
        with ResourcePath.temporary_uri(suffix=".json") as tmp_file:
            tmp_file.write(data.model_dump_json().encode())
            refs = [
                DatasetRef(dataset_type, data_ref_2.dataId, run),
                DatasetRef(dataset_type, data_ref.dataId, run),
            ]
            datasets = [FileDataset(path=tmp_file, refs=refs)]
            butler.ingest(*datasets, transfer="copy")

    assert new.n_get == 1
    assert new.n_put == 1
    assert new.n_ingest == 2


def test_pytype_coercion(butler_harness: ButlerHarness) -> None:
    """Test python type coercion on Butler.get and put."""
    # Store some data with the normal example storage class.
    storage_class = butler_harness.storage_class_factory.getStorageClass("StructuredDataNoComponents")
    dataset_type_name = "test_metric"
    butler = run_put_get_test(butler_harness, storage_class, dataset_type_name)

    data_id = {"instrument": "DummyCamComp", "visit": 423}
    metric = butler.get(dataset_type_name, dataId=data_id)
    assert get_full_type_name(metric) == "lsst.daf.butler.tests.MetricsExample"

    dataset_type_ori = butler.get_dataset_type(dataset_type_name)
    assert dataset_type_ori.storageClass.name == "StructuredDataNoComponents"

    # Now need to hack the registry dataset type definition.
    # There is no API for this.
    registry = butler._registry  # type: ignore[attr-defined]
    assert isinstance(registry, SqlRegistry)
    manager = registry._managers.datasets
    assert hasattr(manager, "_db")
    assert hasattr(manager, "_static")
    manager._db.update(
        manager._static.dataset_type,
        {"name": dataset_type_name},
        {dataset_type_name: dataset_type_name, "storage_class": "StructuredDataNoComponentsModel"},
    )

    # Force reset of dataset type cache
    butler.registry.refresh()

    dataset_type_new = butler.get_dataset_type(dataset_type_name)
    assert dataset_type_new.name == dataset_type_ori.name
    assert dataset_type_new.storageClass.name == "StructuredDataNoComponentsModel"

    metric_model = butler.get(dataset_type_name, dataId=data_id)
    assert type(metric_model) is not type(metric)
    assert get_full_type_name(metric_model) == "lsst.daf.butler.tests.MetricsExampleModel"

    # Put the model and read it back to show that everything now
    # works as normal.
    metric_ref = butler.put(metric_model, dataset_type_name, dataId=data_id, visit=424)
    metric_model_new = butler.get(metric_ref)
    assert metric_model_new == metric_model

    # Hack the storage class again to something that will fail on the
    # get with no conversion class.
    manager._db.update(
        manager._static.dataset_type,
        {"name": dataset_type_name},
        {dataset_type_name: dataset_type_name, "storage_class": "StructuredDataListYaml"},
    )
    butler.registry.refresh()

    with pytest.raises(ValueError, match="no valid converter found to convert"):
        butler.get(dataset_type_name, dataId=data_id)


def test_provenance(butler_harness: ButlerHarness) -> None:
    """Test that provenance is attached on put."""
    run = "test_run"
    butler, dataset_type = butler_harness.create_butler(run, "MetricsExampleModelProvenance", "prov_metric")
    metric = MetricsExampleModel(
        summary={"AM1": 5.2, "AM2": 30.6},
        output={"a": [1, 2, 3], "b": {"blue": 5, "red": "green"}},
        data=[563, 234, 456.7, 752, 8, 9, 27],
    )
    # Provenance can be attached to the object being put. Whether
    # it is or not is dependent on the formatter. For this test we
    # copy on adding provenance to ensure they differ.
    assert metric.dataset_id is None
    metric_ref = butler.put(metric, dataset_type, visit=424, instrument="DummyCamComp")
    assert metric.dataset_id is None
    metric_2 = butler.get(metric_ref)
    assert metric_2.data == metric.data
    assert metric_2.dataset_id == metric_ref.id
    assert metric_2.provenance is None

    # Put with provenance.
    prov = DatasetProvenance(quantum_id=uuid.uuid4())
    prov.add_input(metric_ref)
    prov.add_extra_provenance(metric_ref.id, {"answer": 42})
    metric_ref2 = butler.put(metric, dataset_type, visit=423, instrument="DummyCamComp", provenance=prov)
    metric_3 = butler.get(metric_ref2)
    assert metric_3.provenance == prov

    # Check that we can extract provenance from dict form.
    prov_dict = prov.to_flat_dict(metric_ref2)
    prov_from_prov, ref_from_prov = DatasetProvenance.from_flat_dict(prov_dict, butler)
    assert ref_from_prov == metric_ref2
    # Direct __eq__ of the provenance does not work because one side
    # includes dimension records.
    assert {ref.id for ref in prov_from_prov.inputs} == {ref.id for ref in prov.inputs}
    assert prov_from_prov.quantum_id == prov.quantum_id
    assert prov_from_prov.extras == prov.extras

    # Force a bad ID into the dict.
    prov_dict["id"] = uuid.uuid4()
    with pytest.raises(ValueError, match="Dataset associated with this provenance"):
        DatasetProvenance.from_flat_dict(prov_dict, butler)
    del prov_dict["id"]
    prov_dict["input 0 id"] = uuid.uuid4()
    # The added key separates on spaces while the rest of the header separates
    # on ".", so the separator check rejects it before the unknown input ID is
    # ever looked up. See DM-55822's mapping notes.
    with pytest.raises(ValueError, match="Inconsistent values found for separators"):
        DatasetProvenance.from_flat_dict(prov_dict, butler)

    # Check that simple types can be reconstructed with non-standard
    # separators.
    prov_dict = prov.to_flat_dict(metric_ref2, prefix="XYZ", sep="😎", simple_types=True)
    prov_from_prov, ref_from_prov = DatasetProvenance.from_flat_dict(prov_dict, butler)
    assert ref_from_prov == metric_ref2
    assert {ref.id for ref in prov_from_prov.inputs} == {ref.id for ref in prov.inputs}

    with pytest.raises(ValueError, match="No provenance information found in header"):
        DatasetProvenance.from_flat_dict({"unknown": 42}, butler)
