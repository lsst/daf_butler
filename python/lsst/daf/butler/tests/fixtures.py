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

"""Pytest fixtures for testing Butler against several configurations.

The configuration a Butler test runs against has four independent axes:
the registry backend, the datastore, the Butler client, and where the
repository configuration sits relative to the repository root.

Each axis is a fixture with a single default value, so a test written plainly
runs once. A test that is sensitive to an axis opts into more values with
indirect parametrization, which keeps the multiplication visible in the diff
rather than implicit in a class hierarchy::

    @pytest.mark.parametrize(
        "registry_backend", ["sqlite", "postgres"], indirect=True
    )
    def test_ingest_date_handling(butler): ...

Consuming packages activate these by adding the following to their
``conftest.py``, and by supplying a ``test_directory`` fixture naming the
directory that holds their Butler test configuration::

    pytest_plugins = ["lsst.daf.butler.tests.fixtures"]
"""

from __future__ import annotations

__all__ = [
    "DATASTORE_PROFILES",
    "ButlerHarness",
    "ButlerRepo",
    "ClonedButlerHarness",
    "DatastoreProfile",
    "ServerButlerHarness",
    "add_dataset_type",
    "get_test_data_path",
    "make_example_metrics",
]

import contextlib
import dataclasses
import os
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, cast

import astropy.time
import pytest

from lsst.resources import ResourcePath

from .. import Butler, Config, DatasetRef, DatasetType, StorageClass, StorageClassFactory
from ..datastores.fileDatastore import FileDatastore
from ..direct_butler import DirectButler
from ..repo_relocation import BUTLER_ROOT_TAG
from ._examplePythonTypes import MetricsExample
from ._repo_template_cache import make_repo_for_test
from .utils import makeTestTempDir, removeTestTempDir

if TYPE_CHECKING:
    from .._butler_metrics import ButlerMetrics
    from ..dimensions import DimensionGroup
    from ..registry import Registry


@dataclasses.dataclass(frozen=True)
class DatastoreProfile:
    """Everything that varies between datastore configurations."""

    config_file: str
    """Path, relative to the test directory, of the Butler config to use."""

    full_config_key: str | None
    """Key expected in the full config but not in the limited one, or `None`
    if this configuration has no such key."""

    validation_can_fail: bool
    """Whether ``validateConfiguration`` can fail for this datastore."""

    datastore_str: list[str]
    """Fragments expected in the datastore's string representation."""

    datastore_name: list[str] | None
    """Expected datastore names, or `None` if they can only be computed once
    the repository root is known."""

    prediction_supported: bool = True
    """Whether ``getURIs`` supports prediction mode."""

    trust_mode_supported: bool = True
    """Whether the datastore supports trust mode."""


DATASTORE_PROFILES: dict[str, DatastoreProfile] = {
    "posix": DatastoreProfile(
        config_file="config/basic/butler.yaml",
        full_config_key=".datastore.formatters",
        validation_can_fail=True,
        datastore_str=["/tmp"],
        datastore_name=[f"FileDatastore@{BUTLER_ROOT_TAG}"],
    ),
    "in_memory": DatastoreProfile(
        config_file="config/basic/butler-inmemory.yaml",
        full_config_key=None,
        validation_can_fail=False,
        datastore_str=["datastore='InMemory"],
        datastore_name=["InMemoryDatastore@"],
    ),
    "chained": DatastoreProfile(
        config_file="config/basic/butler-chained.yaml",
        full_config_key=".datastore.datastores.1.formatters",
        validation_can_fail=True,
        datastore_str=["datastore='InMemory", "/FileDatastore_1/,", "/FileDatastore_2/'"],
        datastore_name=[
            "InMemoryDatastore@",
            f"FileDatastore@{BUTLER_ROOT_TAG}/FileDatastore_1",
            "SecondDatastore",
        ],
    ),
    "remote_test": DatastoreProfile(
        config_file="config/basic/butler-remotetest-store.yaml",
        full_config_key=None,
        validation_can_fail=True,
        # Both are computed from the generated root URI; see
        # _make_remote_test_repo.
        datastore_str=[],
        datastore_name=None,
    ),
}
"""Configuration that varies between datastores, keyed by datastore type."""


def make_example_metrics() -> MetricsExample:
    """Return an example dataset suitable for tests.

    Returns
    -------
    metrics : `MetricsExample`
        The example dataset.
    """
    return MetricsExample(
        {"AM1": 5.2, "AM2": 30.6},
        {"a": [1, 2, 3], "b": {"blue": 5, "red": "green"}},
        [563, 234, 456.7, 752, 8, 9, 27],
    )


def get_test_data_path(filename: str) -> ResourcePath:
    """Return the URI of a file in the packaged registry test data.

    Parameters
    ----------
    filename : `str`
        Name of the file within ``tests/registry_data``.

    Returns
    -------
    uri : `ResourcePath`
        URI of the requested file.
    """
    return ResourcePath(f"resource://lsst.daf.butler/tests/registry_data/{filename}")


def add_dataset_type(
    dataset_type_name: str,
    dimensions: DimensionGroup,
    storage_class: StorageClass | str,
    registry: Registry,
) -> DatasetType:
    """Create a `DatasetType` and register it.

    Parameters
    ----------
    dataset_type_name : `str`
        Name of the dataset type to create.
    dimensions : `DimensionGroup`
        Dimensions of the dataset type.
    storage_class : `StorageClass` or `str`
        Storage class of the dataset type.
    registry : `Registry`
        Registry to register the dataset type with.

    Returns
    -------
    dataset_type : `DatasetType`
        The registered dataset type.
    """
    dataset_type = DatasetType(dataset_type_name, dimensions, storage_class)
    registry.registerDatasetType(dataset_type)
    return dataset_type


@dataclasses.dataclass
class ButlerRepo:
    """A Butler repository built for one test, and where its pieces live.

    Not named ``TestRepo``: pytest tries to collect anything named ``Test*`` as
    a test class and warns that it cannot, because this has a constructor.
    """

    config_file: str
    """Path or URI of the config a Butler should be opened from."""

    root: str
    """Temporary directory holding the repository."""

    profile: DatastoreProfile
    """Effective datastore profile, which some layouts override."""

    dir1: str | None = None
    """Repository root, when the layout separates it from the config."""

    dir2: str | None = None
    """Directory holding the config, when the layout separates the two."""


def _make_config(test_directory: str, profile: DatastoreProfile) -> Config:
    """Load the Butler config for a datastore profile."""
    return Config(os.path.join(test_directory, profile.config_file))


def _apply_registry_backend(config: Config, registry_backend: str, request: pytest.FixtureRequest) -> None:
    """Patch a Butler config for the requested registry backend.

    The postgres instance is resolved lazily rather than declared as a fixture
    parameter, so that a sqlite test never starts a postgres server.
    """
    if registry_backend == "postgres":
        request.getfixturevalue("postgres_instance").patch_butler_config(config)
    elif registry_backend != "sqlite":
        raise ValueError(f"Unknown registry backend {registry_backend!r}")


def _make_remote_test_repo(root: str, config: Config) -> ButlerRepo:
    """Build a repository whose datastore root reports itself as not local."""
    from lsst.resources.tests import make_remote_test_uri

    # The space in the directory name is deliberate. It ensures the URI has to
    # be percent-encoded correctly on the way in and decoded on the way out.
    root_path = os.path.join(root, "butler root")
    os.makedirs(root_path)
    rooturi = make_remote_test_uri(root_path)
    config.update({"datastore": {"datastore": {"root": str(rooturi)}}})

    # The registry database has to live on a real local file system.
    reg_dir = os.path.join(root, "registry")
    os.makedirs(reg_dir)
    config["registry", "db"] = f"sqlite:///{reg_dir}/gen3.sqlite3"

    profile = dataclasses.replace(
        DATASTORE_PROFILES["remote_test"],
        datastore_str=[f"datastore='{rooturi}'"],
        datastore_name=[f"FileDatastore@{rooturi}"],
    )
    make_repo_for_test(rooturi, config=config, forceConfigRoot=False)
    config_file = str(rooturi.join("butler.yaml", forceDirectory=False))
    return ButlerRepo(config_file=config_file, root=root, profile=profile)


def _make_explicit_root_repo(root: str, config: Config, profile: DatastoreProfile) -> ButlerRepo:
    """Build a repository whose config lives outside the repository root."""
    dir1 = os.path.join(root, "dir1")
    make_repo_for_test(dir1, config=config)

    dir2 = os.path.join(root, "dir2")
    os.makedirs(dir2, exist_ok=True)
    config_file1 = os.path.join(dir1, "butler.yaml")
    moved = Config(config_file1)
    moved["root"] = dir1
    config_file2 = os.path.join(dir2, "butler2.yaml")
    moved.dumpToUri(config_file2)
    os.remove(config_file1)

    # This layout deliberately does not use butler.yaml as the config name, so
    # the makeRepo check does not apply, and the datastore is under dir1.
    effective = dataclasses.replace(profile, full_config_key=None, datastore_str=["dir1"])
    return ButlerRepo(config_file=config_file2, root=root, profile=effective, dir1=dir1, dir2=dir2)


def _make_outfile_repo(
    root: str, root2: str, config: Config, profile: DatastoreProfile, layout: str
) -> ButlerRepo:
    """Build a repository whose config was written outside it by makeRepo."""
    match layout:
        case "outfile":
            outfile: str = os.path.join(root2, "different.yaml")
        case "outfile_dir":
            outfile = root2
        case "outfile_uri":
            outfile = ResourcePath(os.path.join(root2, "something.yaml")).geturl()
        case _:
            raise ValueError(f"Unknown outfile layout {layout!r}")
    make_repo_for_test(root, config=config, outfile=outfile)
    return ButlerRepo(config_file=outfile, root=root, profile=profile, dir2=root2)


class ButlerHarness:
    """A Butler plus the hooks whose behavior depends on the client kind.

    Parameters
    ----------
    repo : `ButlerRepo`
        The repository this harness opens Butlers against.
    storage_class_factory : `StorageClassFactory`
        Factory holding the test storage class definitions.
    exit_stack : `contextlib.ExitStack`
        Stack that closes any Butler this harness opens.
    default_run : `str`
        Run collection new Butlers default to.
    """

    prediction_supported = True
    """Whether ``getURIs`` supports prediction mode."""

    trust_mode_supported = True
    """Whether the datastore supports trust mode."""

    def __init__(
        self,
        repo: ButlerRepo,
        storage_class_factory: StorageClassFactory,
        exit_stack: contextlib.ExitStack,
        default_run: str,
    ) -> None:
        self.repo = repo
        self.profile = repo.profile
        self.config_file = repo.config_file
        self.root = repo.root
        self.storage_class_factory = storage_class_factory
        self.default_run = default_run
        self._exit_stack = exit_stack

    def create_empty_butler(
        self,
        run: str | None = None,
        writeable: bool | None = None,
        metrics: ButlerMetrics | None = None,
        cleanup: bool = True,
    ) -> Butler:
        """Create a Butler for the test repository, without inserting test
        data.

        Parameters
        ----------
        run : `str`, optional
            Run collection the Butler defaults to.
        writeable : `bool`, optional
            Whether the Butler should be writeable.
        metrics : `ButlerMetrics`, optional
            Metrics object the Butler should record into.
        cleanup : `bool`, optional
            Whether to close the Butler when the test ends.

        Returns
        -------
        butler : `Butler`
            The new Butler.
        """
        butler = Butler.from_config(self.config_file, run=run, writeable=writeable, metrics=metrics)
        if cleanup:
            self._exit_stack.enter_context(butler)
        assert isinstance(butler, DirectButler), "Expect DirectButler in configuration"
        return butler

    def create_butler(
        self,
        run: str,
        storage_class: StorageClass | str,
        dataset_type_name: str,
        metrics: ButlerMetrics | None = None,
    ) -> tuple[Butler, DatasetType]:
        """Create a Butler for the test repository and insert some test data.

        Parameters
        ----------
        run : `str`
            Run collection to insert datasets into.
        storage_class : `StorageClass` or `str`
            Storage class for the new dataset type.
        dataset_type_name : `str`
            Name of the dataset type to register.
        metrics : `ButlerMetrics`, optional
            Metrics object the Butler should record into.

        Returns
        -------
        butler : `Butler`
            The new Butler.
        dataset_type : `DatasetType`
            The registered dataset type.
        """
        butler = self.create_empty_butler(run=run, metrics=metrics)

        collections = set(butler.collections.query("*"))
        assert collections == {run}
        # Create and register a DatasetType
        dimensions = butler.dimensions.conform(["instrument", "visit"])

        dataset_type = add_dataset_type(dataset_type_name, dimensions, storage_class, butler.registry)

        # Add needed Dimensions
        butler.registry.insertDimensionData("instrument", {"name": "DummyCamComp"})
        butler.registry.insertDimensionData(
            "physical_filter", {"instrument": "DummyCamComp", "name": "d-r", "band": "R"}
        )
        butler.registry.insertDimensionData(
            "visit_system", {"instrument": "DummyCamComp", "id": 1, "name": "default"}
        )
        butler.registry.insertDimensionData("day_obs", {"instrument": "DummyCamComp", "id": 20200101})
        visit_start = astropy.time.Time("2020-01-01 08:00:00.123456789", scale="tai")
        visit_end = astropy.time.Time("2020-01-01 08:00:36.66", scale="tai")
        butler.registry.insertDimensionData(
            "visit",
            {
                "instrument": "DummyCamComp",
                "id": 423,
                "name": "fourtwentythree",
                "physical_filter": "d-r",
                "datetime_begin": visit_start,
                "datetime_end": visit_end,
                "day_obs": 20200101,
            },
        )

        # Add more visits for some later tests
        for visit_id in (424, 425):
            butler.registry.insertDimensionData(
                "visit",
                {
                    "instrument": "DummyCamComp",
                    "id": visit_id,
                    "name": f"fourtwentyfour_{visit_id}",
                    "physical_filter": "d-r",
                    "day_obs": 20200101,
                },
            )
        return butler, dataset_type

    def are_uris_equivalent(self, uri1: ResourcePath, uri2: ResourcePath) -> bool:
        """Return `True` if two URIs refer to the same resource.

        Parameters
        ----------
        uri1, uri2 : `ResourcePath`
            URIs to compare.

        Returns
        -------
        equivalent : `bool`
            Whether the URIs are equivalent.
        """
        return uri1 == uri2

    def remove_dataset_out_of_band(self, butler: Butler, ref: DatasetRef) -> None:
        """Simulate an external actor removing a file outside Butler's
        knowledge.

        Parameters
        ----------
        butler : `Butler`
            Butler holding the dataset.
        ref : `DatasetRef`
            Dataset to remove.
        """
        uri = butler.getURI(ref)
        uri.remove()
        datastore = cast(FileDatastore, butler._datastore)
        datastore.cacheManager.remove_from_cache(ref)


class ClonedButlerHarness(ButlerHarness):
    """Harness that hands out cloned Butlers, to check that `Butler.clone`
    leaves a working Butler behind.
    """

    def create_butler(
        self,
        run: str,
        storage_class: StorageClass | str,
        dataset_type_name: str,
        metrics: ButlerMetrics | None = None,
    ) -> tuple[Butler, DatasetType]:
        # Docstring inherited.
        butler, dataset_type = super().create_butler(run, storage_class, dataset_type_name, metrics=metrics)
        return butler.clone(run=run, metrics=metrics), dataset_type


class ServerButlerHarness(ButlerHarness):
    """Harness backed by a RemoteButler talking to a test server.

    Parameters
    ----------
    server_instance : `TestServerInstance`
        The running test server.
    *args, **kwargs
        Forwarded to `ButlerHarness`.
    """

    prediction_supported = False
    trust_mode_supported = False

    def __init__(self, server_instance: Any, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.server_instance = server_instance

    def create_empty_butler(
        self,
        run: str | None = None,
        writeable: bool | None = None,
        metrics: ButlerMetrics | None = None,
        cleanup: bool = True,
    ) -> Butler:
        # Docstring inherited.
        return self.server_instance.hybrid_butler.clone(run=run, metrics=metrics)

    def are_uris_equivalent(self, uri1: ResourcePath, uri2: ResourcePath) -> bool:
        # Docstring inherited.
        # S3 pre-signed URLs may end up with differing expiration times in the
        # query parameters, so ignore query parameters when comparing.
        return uri1.scheme == uri2.scheme and uri1.netloc == uri2.netloc and uri1.path == uri2.path

    def remove_dataset_out_of_band(self, butler: Butler, ref: DatasetRef) -> None:
        # Docstring inherited.
        # Can't delete a file via S3 signed URLs, so we need to reach in
        # through DirectButler to delete the dataset.
        uri = self.server_instance.direct_butler.getURI(ref)
        uri.remove()


DEFAULT_RUN = "ingésτ😺"
"""Default run collection name, deliberately non-ASCII."""


@pytest.fixture
def registry_backend(request: pytest.FixtureRequest) -> str:  # numpydoc ignore=PR01
    """Registry backend for this test: ``sqlite`` or ``postgres``."""
    return getattr(request, "param", "sqlite")


@pytest.fixture
def datastore_type(request: pytest.FixtureRequest) -> str:  # numpydoc ignore=PR01
    """Datastore configuration for this test: a key of `DATASTORE_PROFILES`."""
    return getattr(request, "param", "posix")


@pytest.fixture
def butler_client(request: pytest.FixtureRequest) -> str:  # numpydoc ignore=PR01
    """Butler client for this test: ``direct``, ``cloned`` or ``server``."""
    return getattr(request, "param", "direct")


@pytest.fixture
def repo_layout(request: pytest.FixtureRequest) -> str:  # numpydoc ignore=PR01
    """Where the config sits relative to the repository root.

    One of ``in_repo``, ``explicit_root``, ``outfile``, ``outfile_dir`` or
    ``outfile_uri``.
    """
    return getattr(request, "param", "in_repo")


SENTINEL_STORAGE_CLASS = "StructuredDataDictJson"
"""Storage class used to detect whether the test configs are still loaded."""


@pytest.fixture
def storage_class_factory(test_directory: str) -> StorageClassFactory:  # numpydoc ignore=PR01
    """Storage classes from the test configurations.

    `StorageClassFactory` is a singleton, so loading every profile's config
    here matches what the per-class ``setUpClass`` methods did collectively.

    This is function-scoped and reloads only when the sentinel class is absent,
    which costs a dict lookup in the normal case. It cannot simply be
    session-scoped: some tests call `StorageClassFactory.reset` to undo
    converters they installed, and because the factory is a singleton that
    reset would empty it for every later test that shares a session-scoped
    instance. Reloading unconditionally is not an option either, at roughly
    28 ms a time across the suite.
    """
    factory = StorageClassFactory()
    if SENTINEL_STORAGE_CLASS not in factory:
        for profile in DATASTORE_PROFILES.values():
            factory.addFromConfig(os.path.join(test_directory, profile.config_file))
    return factory


@pytest.fixture(scope="session")
def postgres_instance() -> Iterator[Any]:
    """One postgres server per session, matching the previous setUpClass."""
    from .postgresql import setup_postgres_test_db

    with setup_postgres_test_db() as instance:
        yield instance


@pytest.fixture
def butler_repo(
    request: pytest.FixtureRequest,
    test_directory: str,
    registry_backend: str,
    datastore_type: str,
    repo_layout: str,
) -> Iterator[ButlerRepo]:  # numpydoc ignore=PR01
    """Build a Butler repository for the requested axis combination."""
    profile = DATASTORE_PROFILES[datastore_type]
    config = _make_config(test_directory, profile)
    _apply_registry_backend(config, registry_backend, request)

    root = makeTestTempDir(test_directory)
    root2: str | None = None
    try:
        if datastore_type == "remote_test":
            if repo_layout != "in_repo":
                raise ValueError("The remote_test datastore only supports the in_repo layout.")
            yield _make_remote_test_repo(root, config)
        elif repo_layout == "in_repo":
            make_repo_for_test(root, config=config)
            yield ButlerRepo(config_file=os.path.join(root, "butler.yaml"), root=root, profile=profile)
        elif repo_layout == "explicit_root":
            yield _make_explicit_root_repo(root, config, profile)
        else:
            root2 = makeTestTempDir(test_directory)
            yield _make_outfile_repo(root, root2, config, profile, repo_layout)
    finally:
        removeTestTempDir(root)
        if root2 is not None:
            removeTestTempDir(root2)


@pytest.fixture
def butler_harness(
    request: pytest.FixtureRequest,
    test_directory: str,
    butler_repo: ButlerRepo,
    butler_client: str,
    registry_backend: str,
    storage_class_factory: StorageClassFactory,
) -> Iterator[ButlerHarness]:  # numpydoc ignore=PR01
    """Yield a `ButlerHarness` for the requested client kind."""
    with contextlib.ExitStack() as exit_stack:
        args = (butler_repo, storage_class_factory, exit_stack, DEFAULT_RUN)
        match butler_client:
            case "direct":
                yield ButlerHarness(*args)
            case "cloned":
                yield ClonedButlerHarness(*args)
            case "server":
                from .server import create_test_server

                postgres = (
                    request.getfixturevalue("postgres_instance") if registry_backend == "postgres" else None
                )
                server = exit_stack.enter_context(create_test_server(test_directory, postgres=postgres))
                yield ServerButlerHarness(server, *args)
            case _:
                raise ValueError(f"Unknown butler client {butler_client!r}")


@pytest.fixture
def butler(butler_harness: ButlerHarness) -> Butler:  # numpydoc ignore=PR01
    """Return the Butler under test, for tests that need nothing else."""
    return butler_harness.create_empty_butler(run=butler_harness.default_run)
