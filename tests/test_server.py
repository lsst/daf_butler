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

import asyncio
import os.path
import tempfile
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import DEFAULT, AsyncMock, NonCallableMock, patch

from lsst.daf.butler.tests.dict_convertible_model import DictConvertibleModel

try:
    # Failing to import any of these should disable the tests.
    import safir.dependencies.logger
    from fastapi.testclient import TestClient

    import lsst.daf.butler.remote_butler._query_results
    import lsst.daf.butler.remote_butler.server.handlers._query_streaming
    from lsst.daf.butler.remote_butler import RemoteButler
    from lsst.daf.butler.remote_butler._authentication import _EXPLICIT_BUTLER_ACCESS_TOKEN_ENVIRONMENT_KEY
    from lsst.daf.butler.remote_butler.server import create_app
    from lsst.daf.butler.remote_butler.server._dependencies import butler_factory_dependency
    from lsst.daf.butler.remote_butler.server_models import QueryCollectionsRequestModel
    from lsst.daf.butler.tests.server import TEST_REPOSITORY_NAME, UnhandledServerError, create_test_server

    reason_text = ""
except ImportError as e:
    create_test_server = None
    reason_text = str(e)


from lsst.daf.butler import (
    Butler,
    DataCoordinate,
    DatasetNotFoundError,
    DatasetRef,
    LabeledButlerFactory,
    MissingDatasetTypeError,
    NoDefaultCollectionError,
    StorageClassFactory,
)
from lsst.daf.butler.datastore import DatasetRefURIs
from lsst.daf.butler.registry import RegistryDefaults
from lsst.daf.butler.tests import DatastoreMock, addDatasetType
from lsst.daf.butler.tests.utils import MetricsExample, MetricTestRepo, mock_env
from lsst.resources import ResourcePath
from lsst.resources.http import HttpResourcePath

TESTDIR = os.path.abspath(os.path.dirname(__file__))


@unittest.skipIf(create_test_server is None, f"Server dependencies not installed: {reason_text}")
class ButlerClientServerTestCase(unittest.TestCase):
    """Test for Butler client/server."""

    @classmethod
    def setUpClass(cls):
        server_instance = cls.enterClassContext(create_test_server(TESTDIR))
        cls.client = server_instance.client
        cls.butler = server_instance.remote_butler
        cls.butler_without_error_propagation = server_instance.remote_butler_without_error_propagation

        cls.storageClassFactory = StorageClassFactory()

        cls.repo = MetricTestRepo.create_from_butler(
            server_instance.direct_butler, server_instance.config_file_path
        )
        # Add a file with corrupted data for testing error conditions
        cls.dataset_with_corrupted_data = _create_corrupted_dataset(cls.repo)
        # All of the datasets that come with MetricTestRepo are disassembled
        # composites.  Add a simple dataset for testing the common case.
        cls.simple_dataset_ref = _create_simple_dataset(server_instance.direct_butler)

        # Populate the test server.
        # The DatastoreMock is required because the datasets referenced in
        # these imports do not point at real files.
        direct_butler = server_instance.direct_butler
        DatastoreMock.apply(direct_butler)
        direct_butler.import_(filename=os.path.join(TESTDIR, "data", "registry", "base.yaml"))
        direct_butler.import_(filename=os.path.join(TESTDIR, "data", "registry", "datasets.yaml"))

    def test_health_check(self):
        try:
            import importlib.metadata

            importlib.metadata.metadata("lsst.daf.butler")
        except ModuleNotFoundError:
            raise self.skipTest("Standard python package metadata not available. Butler not pip installed.")
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["name"], "butler")

    def test_static_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "temp.txt"), "w") as fh:
                fh.write("test data 123")

            with mock_env({"DAF_BUTLER_SERVER_STATIC_FILES_PATH": tmpdir}):
                with create_test_server(TESTDIR) as server:
                    response = server.client.get("/api/butler/configs/temp.txt")
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.text, "test data 123")

    def test_dimension_universe(self):
        universe = self.butler.dimensions
        self.assertEqual(universe.namespace, "daf_butler")

    def test_get_dataset_type(self):
        bias_type = self.butler.get_dataset_type("bias")
        self.assertEqual(bias_type.name, "bias")

        with self.assertRaises(MissingDatasetTypeError):
            self.butler_without_error_propagation.get_dataset_type("not_bias")

    def test_find_dataset(self):
        storage_class = self.storageClassFactory.getStorageClass("Exposure")

        ref = self.butler.find_dataset("bias", collections="imported_g", detector=1, instrument="Cam1")
        self.assertIsInstance(ref, DatasetRef)
        self.assertEqual(ref.id, uuid.UUID("e15ab039-bc8b-4135-87c5-90902a7c0b22"))
        self.assertFalse(ref.dataId.hasRecords())

        # Try again with variation of parameters.
        ref_new = self.butler.find_dataset(
            "bias",
            {"detector": 1},
            collections="imported_g",
            instrument="Cam1",
            dimension_records=True,
        )
        self.assertEqual(ref_new, ref)
        self.assertTrue(ref_new.dataId.hasRecords())

        ref_new = self.butler.find_dataset(
            ref.datasetType,
            DataCoordinate.standardize(detector=1, instrument="Cam1", universe=self.butler.dimensions),
            collections="imported_g",
            storage_class=storage_class,
        )
        self.assertEqual(ref_new, ref)

        ref2 = self.butler.get_dataset(ref.id)
        self.assertEqual(ref2, ref)

        # Use detector name to find it.
        ref3 = self.butler.find_dataset(
            ref.datasetType,
            collections="imported_g",
            instrument="Cam1",
            full_name="Aa",
        )
        self.assertEqual(ref2, ref3)

        # Try expanded refs.
        self.assertFalse(ref.dataId.hasRecords())
        expanded = self.butler.get_dataset(ref.id, dimension_records=True)
        self.assertTrue(expanded.dataId.hasRecords())

        # The test datasets are all Exposure so storage class conversion
        # can not be tested until we fix that. For now at least test the
        # code paths.
        bias = self.butler.get_dataset(ref.id, storage_class=storage_class)
        self.assertEqual(bias.datasetType.storageClass, storage_class)

        # Unknown dataset should not fail.
        self.assertIsNone(self.butler.get_dataset(uuid.uuid4()))
        self.assertIsNone(self.butler.get_dataset(uuid.uuid4(), storage_class="NumpyArray"))

    def test_instantiate_via_butler_http_search(self):
        """Ensure that the primary Butler constructor's automatic search logic
        correctly locates and reads the configuration file and ends up with a
        RemoteButler pointing to the correct URL
        """

        # This is kind of a fragile test.  Butler's search logic does a lot of
        # manipulations involving creating new ResourcePaths, and ResourcePath
        # doesn't use httpx so we can't easily inject the TestClient in there.
        # We don't have an actual valid HTTP URL to give to the constructor
        # because the test instance of the server is accessed via ASGI.
        #
        # Instead we just monkeypatch the HTTPResourcePath 'read' method and
        # hope that all ResourcePath HTTP reads during construction are going
        # to the server under test.
        def override_read(http_resource_path):
            return self.client.get(http_resource_path.geturl()).content

        server_url = f"https://test.example/api/butler/repo/{TEST_REPOSITORY_NAME}/"

        with patch.object(HttpResourcePath, "read", override_read):
            # RegistryDefaults.finish() needs to download the dimension
            # universe from the server, which will fail because there is no
            # server here.  So mock it out.
            with patch.object(RegistryDefaults, "finish"):
                # Add access key to environment variables. RemoteButler
                # instantiation will throw an error if access key is not
                # available.
                with mock_env({_EXPLICIT_BUTLER_ACCESS_TOKEN_ENVIRONMENT_KEY: "fake-access-token"}):
                    butler = Butler(
                        server_url,
                        collections=["collection1", "collection2"],
                        run="collection2",
                    )
                    self.assertIsInstance(butler, RemoteButler)
                    self.assertEqual(butler._connection.server_url, server_url)
                    self.assertEqual(butler.collections.defaults, ("collection1", "collection2"))
                    self.assertEqual(butler.run, "collection2")
                    # A butler created this way uses the default cache config.
                    self.assertFalse(butler._use_disabled_datastore_cache)

                butler_factory = LabeledButlerFactory({"server": server_url})
                factory_created_butler = butler_factory.create_butler(label="server", access_token="token")
                self.assertIsInstance(factory_created_butler, RemoteButler)
                self.assertTrue(factory_created_butler._use_disabled_datastore_cache)
                self.assertEqual(factory_created_butler._connection.server_url, server_url)

    def test_get(self):
        dataset_type = "test_metric_comp"
        data_id = {"instrument": "DummyCamComp", "visit": 423}
        collections = "ingest/run"
        # Test get() of a DatasetRef.
        ref = self.butler.find_dataset(dataset_type, data_id, collections=collections)
        metric = self.butler.get(ref)
        self.assertIsInstance(metric, MetricsExample)
        self.assertEqual(metric.summary, MetricTestRepo.METRICS_EXAMPLE_SUMMARY)

        # Test get() by DataId.
        data_id_metric = self.butler.get(dataset_type, dataId=data_id, collections=collections)
        self.assertEqual(metric, data_id_metric)
        # Test get() by DataId dict augmented with kwargs.
        kwarg_metric = self.butler.get(
            dataset_type, dataId={"instrument": "DummyCamComp"}, collections=collections, visit=423
        )
        self.assertEqual(metric, kwarg_metric)
        # Test get() by DataId DataCoordinate augmented with kwargs.
        coordinate = DataCoordinate.make_empty(self.butler.dimensions)
        kwarg_data_coordinate_metric = self.butler.get(
            dataset_type, dataId=coordinate, collections=collections, instrument="DummyCamComp", visit=423
        )
        self.assertEqual(metric, kwarg_data_coordinate_metric)
        # Test get() of a non-existent DataId.
        invalid_data_id = {"instrument": "NotAValidlInstrument", "visit": 423}
        with self.assertRaises(DatasetNotFoundError):
            self.butler_without_error_propagation.get(
                dataset_type, dataId=invalid_data_id, collections=collections
            )

        # Test get() by DataId with default collections.
        butler_with_default_collection = self.butler.clone(collections="ingest/run")
        default_collection_metric = butler_with_default_collection.get(dataset_type, dataId=data_id)
        self.assertEqual(metric, default_collection_metric)

        # Test get() by DataId with no collections specified.
        with self.assertRaises(NoDefaultCollectionError):
            self.butler_without_error_propagation.get(dataset_type, dataId=data_id)

        # Test looking up a non-existent ref
        invalid_ref = ref.replace(id=uuid.uuid4())
        with self.assertRaises(DatasetNotFoundError):
            self.butler_without_error_propagation.get(invalid_ref)

        with self.assertRaises(RuntimeError):
            self.butler_without_error_propagation.get(self.dataset_with_corrupted_data)

        # Test storage class override
        new_sc = self.storageClassFactory.getStorageClass("MetricsConversion")

        def check_sc_override(converted):
            self.assertNotEqual(type(metric), type(converted))
            self.assertIsInstance(converted, new_sc.pytype)
            self.assertEqual(metric, converted)

        check_sc_override(self.butler.get(ref, storageClass=new_sc))

        # Test storage class override via DatasetRef.
        check_sc_override(self.butler.get(ref.overrideStorageClass("MetricsConversion")))
        # Test storage class override via DatasetType.
        check_sc_override(
            self.butler.get(
                ref.datasetType.overrideStorageClass(new_sc), dataId=data_id, collections=collections
            )
        )

        # Test component override via DatasetRef.
        component_ref = ref.makeComponentRef("summary")
        component_data = self.butler.get(component_ref)
        self.assertEqual(component_data, MetricTestRepo.METRICS_EXAMPLE_SUMMARY)

        # Test overriding both storage class and component via DatasetRef.
        converted_component_data = self.butler.get(component_ref, storageClass="DictConvertibleModel")
        self.assertIsInstance(converted_component_data, DictConvertibleModel)
        self.assertEqual(converted_component_data.content, MetricTestRepo.METRICS_EXAMPLE_SUMMARY)

        # Test component override via DatasetType.
        dataset_type_component_data = self.butler.get(
            component_ref.datasetType, component_ref.dataId, collections=collections
        )
        self.assertEqual(dataset_type_component_data, MetricTestRepo.METRICS_EXAMPLE_SUMMARY)

    def test_getURIs_no_components(self):
        # This dataset does not have components, and should return one URI.
        def check_uri(uri: ResourcePath):
            self.assertIsNotNone(uris.primaryURI)
            self.assertEqual(uris.primaryURI.scheme, "https")
            self.assertEqual(uris.primaryURI.read(), b"123")

        uris = self.butler.getURIs(self.simple_dataset_ref)
        self.assertEqual(len(uris.componentURIs), 0)
        check_uri(uris.primaryURI)

        check_uri(self.butler.getURI(self.simple_dataset_ref))

    def test_getURIs_multiple_components(self):
        # This dataset has multiple components, so we should get back multiple
        # URIs.
        dataset_type = "test_metric_comp"
        data_id = {"instrument": "DummyCamComp", "visit": 423}
        collections = "ingest/run"

        def check_uris(uris: DatasetRefURIs):
            self.assertIsNone(uris.primaryURI)
            self.assertEqual(len(uris.componentURIs), 3)
            path = uris.componentURIs["summary"]
            self.assertEqual(path.scheme, "https")
            data = path.read()
            self.assertEqual(data, b"AM1: 5.2\nAM2: 30.6\n")

        uris = self.butler.getURIs(dataset_type, dataId=data_id, collections=collections)
        check_uris(uris)

        # Calling getURI on a multi-file dataset raises an exception
        with self.assertRaises(RuntimeError):
            self.butler.getURI(dataset_type, dataId=data_id, collections=collections)

        # getURIs does NOT respect component overrides on the DatasetRef,
        # instead returning the parent's URIs.  Unclear if this is "correct"
        # from a conceptual point of view, but this matches DirectButler
        # behavior.
        ref = self.butler.find_dataset(dataset_type, data_id=data_id, collections=collections)
        componentRef = ref.makeComponentRef("summary")
        componentUris = self.butler.getURIs(componentRef)
        check_uris(componentUris)

    def test_auth_check(self):
        # This is checking that the unit-test middleware for validating the
        # authentication headers is working.  It doesn't test actual server
        # functionality -- in a real deployment, the authentication headers are
        # handled by GafaelfawrIngress, not our app.
        with self.assertRaises(UnhandledServerError) as cm:
            self.client.get("/v1/dataset_type/int")
        self.assertEqual(cm.exception.__cause__.status_code, 401)

    def test_exception_logging(self):
        app = create_app()

        def raise_error():
            raise RuntimeError("An unhandled error")

        app.dependency_overrides[butler_factory_dependency] = raise_error
        client = TestClient(app, raise_server_exceptions=False)

        with patch.object(safir.dependencies.logger, "logger_dependency") as mock_logger_dep:
            mock_logger = NonCallableMock(["aerror"])

            async def noop():
                pass

            mock_logger.aerror.return_value = noop()

            async def get_logger():
                return mock_logger

            mock_logger_dep.return_value = get_logger()
            client.get(
                "/api/butler/repo/something/v1/dataset_type/int",
                headers={"X-Auth-Request-User": "user-name", "X-Butler-Client-Request-Id": "request-id"},
            )
            mock_logger_dep.assert_called_once()

            mock_logger.aerror.assert_called_once()
            args, kwargs = mock_logger.aerror.call_args
            self.assertIsInstance(kwargs["exc_info"], RuntimeError)
            self.assertEqual(kwargs["clientRequestId"], "request-id")
            self.assertEqual(kwargs["user"], "user-name")

    def test_query_keepalive(self):
        """Test that long-running queries stream keep-alive messages to stop
        the HTTP connection from closing before they are able to return
        results.
        """
        # Normally it takes 15 seconds for a timeout -- mock it to trigger
        # immediately instead.
        with patch.object(
            lsst.daf.butler.remote_butler.server.handlers._query_streaming, "_timeout"
        ) as mock_timeout:
            # Hook into QueryDriver to track the number of keep-alives we have
            # seen.
            with patch.object(
                lsst.daf.butler.remote_butler._query_results, "_received_keep_alive"
            ) as mock_keep_alive:
                mock_timeout.side_effect = _timeout_twice()
                with self.butler.query() as query:
                    datasets = list(query.datasets("bias", "imported_g"))
                self.assertEqual(len(datasets), 3)
                self.assertGreaterEqual(mock_timeout.call_count, 3)
                self.assertGreaterEqual(mock_keep_alive.call_count, 2)

    def test_query_retries(self):
        """Test that the server will send HTTP status 503 to put backpressure
        on clients if it is overloaded, and that the client will retry if this
        happens.
        """
        query_event = threading.Event()
        retry_event = asyncio.Event()

        async def block_first_request() -> None:
            # Signal the unit tests that we have reached the critical section
            # in the server, where the first client has reserved the query
            # slot.
            query_event.set()
            # Block inside the query, until the 2nd client has been forced to
            # retry.
            await retry_event.wait()

        async def block_second_request() -> None:
            # Release the first client, so it can finish its query and prevent
            # this client from being blocked on the next go-round.
            retry_event.set()

        def do_query(butler: Butler) -> list[DatasetRef]:
            return butler.query_datasets("bias", "imported_g")

        with (
            patch.object(
                lsst.daf.butler.remote_butler.server.handlers._query_streaming,
                "_MAXIMUM_CONCURRENT_STREAMING_QUERIES",
                new=1,
            ),
            patch.object(
                lsst.daf.butler.remote_butler.server.handlers._query_streaming, "_QUERY_RETRY_SECONDS", new=1
            ),
            patch.object(
                lsst.daf.butler.remote_butler.server.handlers._query_streaming,
                "_block_query_for_unit_test",
                new=AsyncMock(wraps=block_first_request),
            ) as mock_first_client,
            patch.object(
                lsst.daf.butler.remote_butler.server.handlers._query_streaming,
                "_block_retry_for_unit_test",
                new=AsyncMock(wraps=block_second_request),
            ) as mock_second_client,
            ThreadPoolExecutor(max_workers=1) as exec1,
            ThreadPoolExecutor(max_workers=1) as exec2,
        ):
            first_butler = self.butler
            second_butler = self.butler.clone()

            # Run the first client up until the server starts executing its
            # query.
            future1 = exec1.submit(do_query, first_butler)
            event_reached = query_event.wait(60)
            if not event_reached:
                raise TimeoutError("Server did not execute query logic as expected.")

            # Start the second client, which will trigger the retry logic and
            # release the first client to finish its query.
            future2 = exec2.submit(do_query, second_butler)

            result1 = future1.result(60)
            result2 = future2.result(60)
            self.assertEqual(len(result1), 3)
            self.assertEqual(len(result2), 3)
            # The original thread should have gone through this section, and
            # then the 2nd thread after it retries.
            self.assertEqual(mock_first_client.await_count, 2)
            # We should have triggered the retry logic at least once, but it
            # might occur multiple times depending how long the first client
            # takes to finish.
            self.assertGreaterEqual(mock_second_client.await_count, 1)

    # TODO DM-46204: This can be removed once the RSP recommended image has
    # been upgraded to a version that contains DM-46129.
    def test_deprecated_collection_endpoints(self):
        # These REST endpoints are no longer used by Butler client so they need
        # to be checked separately until they can be removed.
        json = self.butler._connection.get(
            "collection_info",
            params={"name": "imported_g", "include_doc": True, "include_parents": True},
        ).json()
        self.assertEqual(json["name"], "imported_g")
        self.assertEqual(json["type"], 1)

        json = self.butler._connection.post(
            "query_collections",
            QueryCollectionsRequestModel(
                search=["imported_*"], collection_types=[1], flatten_chains=False, include_chains=False
            ),
        ).json()
        self.assertCountEqual(json["collections"], ["imported_g", "imported_r"])


def _create_corrupted_dataset(repo: MetricTestRepo) -> DatasetRef:
    run = "corrupted-run"
    ref = repo.addDataset({"instrument": "DummyCamComp", "visit": 423}, run=run)
    uris = repo.butler.getURIs(ref)
    oneOfTheComponents = list(uris.componentURIs.values())[0]
    oneOfTheComponents.write("corrupted data")
    return ref


def _create_simple_dataset(butler: Butler) -> DatasetRef:
    dataset_type = addDatasetType(butler, "test_int", {"instrument", "visit"}, "int")
    ref = butler.put(123, dataset_type, dataId={"instrument": "DummyCamComp", "visit": 423}, run="ingest/run")
    return ref


def _timeout_twice():
    """Return a mock side-effect function that raises a timeout error the first
    two times it is called.
    """
    count = 0

    def timeout(*args):
        nonlocal count
        count += 1
        if count <= 2:
            raise TimeoutError()
        return DEFAULT

    return timeout


if __name__ == "__main__":
    unittest.main()
