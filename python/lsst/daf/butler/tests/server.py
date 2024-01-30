import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient
from lsst.daf.butler import Butler, Config, LabeledButlerFactory
from lsst.daf.butler.remote_butler import RemoteButler, RemoteButlerFactory
from lsst.daf.butler.remote_butler.server import create_app
from lsst.daf.butler.remote_butler.server._dependencies import butler_factory_dependency
from lsst.resources.s3utils import clean_test_environment_for_s3, getS3Client

from ..direct_butler import DirectButler
from .hybrid_butler import HybridButler
from .server_utils import add_auth_header_check_middleware

try:
    # moto v5
    from moto import mock_aws  # type: ignore
except ImportError:
    # moto v4 and earlier
    from moto import mock_s3 as mock_aws  # type: ignore

__all__ = ("create_test_server", "TestServerInstance", "TEST_REPOSITORY_NAME")


TEST_REPOSITORY_NAME = "testrepo"


@dataclass(frozen=True)
class TestServerInstance:
    """Butler instances and other data associated with a temporary server
    instance.
    """

    config_file_path: str
    """Path to the Butler config file used by the server."""
    client: TestClient
    """HTTPX client connected to the temporary server."""
    remote_butler: RemoteButler
    """`RemoteButler` connected to the temporary server."""
    remote_butler_without_error_propagation: RemoteButler
    """`RemoteButler` connected to the temporary server.

    By default, the TestClient instance raises any unhandled exceptions
    from the server as if they had originated in the client to ease debugging.
    However, this can make it appear that error propagation is working
    correctly when in a real deployment the server exception would cause a 500
    Internal Server Error.  This instance of the butler is set up so that any
    unhandled server exceptions do return a 500 status code."""
    direct_butler: Butler
    """`DirectButler` instance connected to the same repository as the
    temporary server.
    """
    hybrid_butler: HybridButler
    """`HybridButler` instance connected to the temporary server."""


@contextmanager
def create_test_server(test_directory: str) -> Iterator[TestServerInstance]:
    """Create a temporary Butler server instance for testing.

    Parameters
    ----------
    test_directory : `str`
        Path to the ``tests/`` directory at the root of the repository,
        containing Butler test configuration files.

    Returns
    -------
    instance : `TestServerInstance`
        Object containing Butler instances connected to the server and
        associated information.
    """
    # Set up a mock S3 environment using Moto.  Moto also monkeypatches the
    # `requests` library so that any HTTP requests to presigned S3 URLs get
    # redirected to the mocked S3.
    # Note that all files are stored in memory.
    with clean_test_environment_for_s3():
        with mock_aws():
            base_config_path = os.path.join(test_directory, "config/basic/server.yaml")
            # Create S3 buckets used for the datastore in server.yaml.
            for bucket in ["mutable-bucket", "immutable-bucket"]:
                getS3Client().create_bucket(Bucket=bucket)

            with TemporaryDirectory() as root:
                Butler.makeRepo(root, config=Config(base_config_path), forceConfigRoot=False)
                config_file_path = os.path.join(root, "butler.yaml")

                app = create_app()
                add_auth_header_check_middleware(app)
                # Override the server's Butler initialization to point at our
                # test repo
                server_butler_factory = LabeledButlerFactory({TEST_REPOSITORY_NAME: config_file_path})
                app.dependency_overrides[butler_factory_dependency] = lambda: server_butler_factory

                client = TestClient(app)
                client_without_error_propagation = TestClient(app, raise_server_exceptions=False)

                remote_butler = _make_remote_butler(client)
                remote_butler_without_error_propagation = _make_remote_butler(
                    client_without_error_propagation
                )

                direct_butler = Butler.from_config(config_file_path, writeable=True)
                assert isinstance(direct_butler, DirectButler)
                hybrid_butler = HybridButler(remote_butler, direct_butler)

                yield TestServerInstance(
                    config_file_path=config_file_path,
                    client=client,
                    direct_butler=direct_butler,
                    remote_butler=remote_butler,
                    remote_butler_without_error_propagation=remote_butler_without_error_propagation,
                    hybrid_butler=hybrid_butler,
                )


def _make_remote_butler(client: TestClient) -> RemoteButler:
    remote_butler_factory = RemoteButlerFactory(
        f"https://test.example/api/butler/repo/{TEST_REPOSITORY_NAME}", client
    )
    return remote_butler_factory.create_butler_for_access_token("fake-access-token")