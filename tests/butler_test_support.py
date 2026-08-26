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
"""Axis combinations and shared assertions for the Butler test suite.

This is a plain module rather than part of ``conftest.py`` because the
parametrize lists have to be importable at collection time, which a fixture
cannot provide. ``conftest.py`` calls `pytest.register_assert_rewrite` on it so
that the assertions here still report the values that failed.
"""

from __future__ import annotations

import logging
import tempfile
import uuid
from typing import TYPE_CHECKING, Any

import pytest

from lsst.daf.butler import DataCoordinate, DatasetNotFoundError, DatasetRef, DatasetType
from lsst.daf.butler.datastores.file_datastore.retrieve_artifacts import ZipIndex
from lsst.daf.butler.registry import ConflictingDefinitionError
from lsst.daf.butler.tests import MetricsExample
from lsst.daf.butler.tests.fixtures import make_example_metrics
from lsst.daf.butler.tests.server_available import butler_server_import_error, butler_server_is_available
from lsst.resources import ResourcePath

if TYPE_CHECKING:
    from lsst.daf.butler import Butler, StorageClass
    from lsst.daf.butler.tests.fixtures import ButlerHarness

AXIS_NAMES = ("registry_backend", "datastore_type", "butler_client", "repo_layout")
"""Fixture names an axis combination sets, in the order the lists below use."""

_SERVER_MARKS = (
    pytest.mark.server,
    pytest.mark.skipif(not butler_server_is_available, reason=butler_server_import_error),
)

FILE_DATASTORE_AXES = [
    pytest.param("sqlite", "posix", "direct", "in_repo", id="posix"),
    pytest.param("postgres", "posix", "direct", "in_repo", id="postgres", marks=pytest.mark.postgres),
    pytest.param("sqlite", "chained", "direct", "in_repo", id="chained"),
    pytest.param("sqlite", "remote_test", "direct", "in_repo", id="remote-test"),
    pytest.param("sqlite", "posix", "server", "in_repo", id="server-sqlite", marks=_SERVER_MARKS),
    pytest.param(
        "postgres",
        "posix",
        "server",
        "in_repo",
        id="server-postgres",
        marks=(*_SERVER_MARKS, pytest.mark.postgres),
    ),
]
"""Axis combinations covering every datastore that inherits FileDatastore.

The cloned client and the explicit-root layout are deliberately absent:
DM-55822 measured each one's marginal coverage as zero unique lines and zero
unique arcs, so they are represented by the single ``test_cloned_put_get``
and ``test_file_locations`` respectively.
"""

BUTLER_TESTS_AXES = [
    *FILE_DATASTORE_AXES,
    pytest.param("sqlite", "in_memory", "direct", "in_repo", id="in-memory"),
]
"""Axis combinations covering every datastore, including the ephemeral one."""

PUT_GET_AXES = [
    *BUTLER_TESTS_AXES,
    pytest.param("sqlite", "posix", "direct", "outfile", id="outfile"),
    pytest.param("sqlite", "posix", "direct", "outfile_dir", id="outfile-dir"),
    pytest.param("sqlite", "posix", "direct", "outfile_uri", id="outfile-uri"),
]
"""Axis combinations for the tests that also cover the outfile layouts."""


def make_datastore_metrics(use_none: bool = False) -> MetricsExample:
    """Return the example dataset the datastore tests use.

    Deliberately not `~lsst.daf.butler.tests.fixtures.make_example_metrics`:
    the datastore tests were written against a different data array, and one of
    them needs the array to be absent.

    Parameters
    ----------
    use_none : `bool`, optional
        If `True`, leave the data array unset.

    Returns
    -------
    metrics : `~lsst.daf.butler.tests.MetricsExample`
        The example dataset.
    """
    array = None if use_none else [563, 234, 456.7, 105, 2054, -1045]
    return MetricsExample(
        {"AM1": 5.2, "AM2": 30.6},
        {"a": [1, 2, 3], "b": {"blue": 5, "red": "green"}},
        array,
    )


def records_from(caplog: pytest.LogCaptureFixture, logger_name: str, level: int) -> list[logging.LogRecord]:
    """Return the records a given logger and its children emitted.

    `caplog` collects from every logger, so a test that cares which logger
    spoke has to filter, as ``unittest.TestCase.assertLogs`` did implicitly.

    Parameters
    ----------
    caplog : `pytest.LogCaptureFixture`
        Fixture holding the captured records.
    logger_name : `str`
        Name of the logger of interest; its children match too.
    level : `int`
        Lowest level to include.

    Returns
    -------
    records : `list` [`logging.LogRecord`]
        The matching records, in the order they were emitted.
    """
    return [
        record for record in caplog.records if record.name.startswith(logger_name) and record.levelno >= level
    ]


def assert_get_components(
    butler: Butler,
    dataset_ref: DatasetRef,
    components: tuple[str, ...],
    reference: Any,
    collections: Any = None,
) -> None:
    """Check that every component of a composite reads back as expected.

    Parameters
    ----------
    butler : `~lsst.daf.butler.Butler`
        Butler holding the composite.
    dataset_ref : `~lsst.daf.butler.DatasetRef`
        Reference to the composite dataset.
    components : `tuple` [`str`]
        Names of the components to check.
    reference : `object`
        Object whose attributes give the expected component values.
    collections : `~typing.Any`, optional
        Collections to search, passed straight through to `Butler.get`.
    """
    dataset_type = dataset_ref.datasetType
    data_id = dataset_ref.dataId
    deferred = butler.getDeferred(dataset_ref)

    for component in components:
        comp_type_name = dataset_type.componentTypeName(component)
        result = butler.get(comp_type_name, data_id, collections=collections)
        assert result == getattr(reference, component)
        result_deferred = deferred.get(component=component)
        assert result_deferred == result


def run_put_get_test(harness: ButlerHarness, storage_class: StorageClass, dataset_type_name: str) -> Butler:
    """Exercise put, get and artifact retrieval against one repository.

    This is the shared body of the put/get suite, called both by the put/get
    tests themselves and by tests elsewhere that need a populated repository.

    Parameters
    ----------
    harness : `~lsst.daf.butler.tests.fixtures.ButlerHarness`
        Harness supplying the Butler under test.
    storage_class : `~lsst.daf.butler.StorageClass`
        Storage class to register the dataset type with.
    dataset_type_name : `str`
        Name of the dataset type to create.

    Returns
    -------
    butler : `~lsst.daf.butler.Butler`
        The Butler, left with one dataset in the default run so that callers
        can keep testing against it.
    """
    # New datasets will be added to run and tag, but we will only look in
    # tag when looking up datasets.
    run = harness.default_run
    butler, dataset_type = harness.create_butler(run, storage_class, dataset_type_name)
    assert butler.run is not None

    # Create and store a dataset
    metric = make_example_metrics()
    data_id = butler.registry.expandDataId({"instrument": "DummyCamComp", "visit": 423})

    # Dataset should not exist if we haven't added it
    with pytest.raises(DatasetNotFoundError):
        butler.get(dataset_type_name, data_id)

    # Put and remove the dataset once as a DatasetRef, once as a dataId,
    # and once with a DatasetType

    # Keep track of any collections we add and do not clean up
    expected_collections = {run}

    counter = 0
    ref = DatasetRef(dataset_type, data_id, id=uuid.UUID(int=1), run="put_run_1")
    args: tuple[DatasetRef] | tuple[str | DatasetType, DataCoordinate]
    for args in ((ref,), (dataset_type_name, data_id), (dataset_type, data_id)):
        # A failure on the first iteration would otherwise cascade into the
        # others, which would fail immediately because the dataset already
        # exists. Work around this by using a distinct run collection each
        # time.
        counter += 1
        this_run = f"put_run_{counter}"
        butler.collections.register(this_run)
        expected_collections.update({this_run})

        kwargs: dict[str, Any] = {}
        if not isinstance(args[0], DatasetRef):
            kwargs["run"] = this_run
        ref = butler.put(metric, *args, **kwargs)
        assert isinstance(ref, DatasetRef), f"put with args {args!r}"

        # Test get of a ref.
        metric_out = butler.get(ref)
        assert metric == metric_out, f"get with args {args!r}"
        # Test get
        metric_out = butler.get(ref.datasetType.name, data_id, collections=this_run)
        assert metric == metric_out, f"get by name with args {args!r}"
        # Test get with a datasetRef
        metric_out = butler.get(ref)
        assert metric == metric_out, f"get by ref with args {args!r}"
        # Test getDeferred with dataId
        metric_out = butler.getDeferred(ref.datasetType.name, data_id, collections=this_run).get()
        assert metric == metric_out, f"getDeferred by name with args {args!r}"
        # Test getDeferred with a ref
        metric_out = butler.getDeferred(ref).get()
        assert metric == metric_out, f"getDeferred by ref with args {args!r}"

        # Check we can get components
        if storage_class.isComposite():
            assert_get_components(butler, ref, ("summary", "data", "output"), metric, collections=this_run)

        primary_uri, secondary_uris = butler.getURIs(ref)
        n_uris = len(secondary_uris)
        if primary_uri:
            n_uris += 1

        # Can the artifacts themselves be retrieved?
        if not butler._datastore.isEphemeral:
            # Create a temporary directory to hold the retrieved artifacts.
            with tempfile.TemporaryDirectory(
                prefix="butler-artifacts-", ignore_cleanup_errors=True
            ) as artifact_root:
                root_uri = ResourcePath(artifact_root, forceDirectory=True)

                for preserve_path in (True, False):
                    destination = root_uri.join(f"{preserve_path}_{counter}/")
                    log = logging.getLogger("lsst.x")
                    log.debug("Using destination %s for args %s", destination, args)
                    # Use copy so that we can test that overwrite protection
                    # works (using "auto" for File URIs would use hard links
                    # and subsequent transfer would work because it knows they
                    # are the same file).
                    transferred = butler.retrieveArtifacts(
                        [ref], destination, preserve_path=preserve_path, transfer="copy"
                    )
                    assert len(transferred) > 0
                    artifacts = list(ResourcePath.findFileResources([destination]))
                    # Filter out the index file.
                    artifacts = [a for a in artifacts if a.basename() != ZipIndex.index_name]
                    assert set(transferred) == set(artifacts)

                    for artifact in transferred:
                        path_in_destination = artifact.relative_to(destination)
                        assert path_in_destination is not None

                        # When path is not preserved there should not be any
                        # path separators.
                        num_seps = path_in_destination.count("/")
                        if preserve_path:
                            assert num_seps > 0
                        else:
                            assert num_seps == 0

                    assert len(artifacts) == n_uris, (
                        "Comparing expected artifacts vs actual:"
                        f" {artifacts} vs {primary_uri} and {secondary_uris}"
                    )

                    if preserve_path:
                        # No need to run these twice. FileDatastore and
                        # RemoteButler reject a move with different wording.
                        with pytest.raises(
                            ValueError,
                            match="Can not move artifacts out of datastore"
                            "|Only 'copy' and 'auto' transfer modes are supported",
                        ):
                            butler.retrieveArtifacts([ref], destination, transfer="move")

                        with pytest.raises(
                            ValueError, match="^Destination location must refer to a directory"
                        ):
                            butler.retrieveArtifacts(
                                [ref], ResourcePath("/some/file.txt", forceDirectory=False)
                            )

                        with pytest.raises(FileExistsError):
                            butler.retrieveArtifacts([ref], destination)

                        transferred_again = butler.retrieveArtifacts(
                            [ref], destination, preserve_path=preserve_path, overwrite=True
                        )
                        assert set(transferred_again) == set(transferred)

        # Now remove the dataset completely.
        butler.pruneDatasets([ref], purge=True, unstore=True)
        # Lookup with original args should still fail.
        kwargs = {"collections": this_run}
        if isinstance(args[0], DatasetRef):
            kwargs = {}  # Prevent warning from being issued.
        assert not butler.exists(*args, **kwargs)
        # get() should still fail.
        with pytest.raises((FileNotFoundError, DatasetNotFoundError)):
            butler.get(ref)
        # Registry shouldn't be able to find it by dataset_id anymore.
        assert butler.get_dataset(ref.id) is None

        # Do explicit registry removal since we know they are empty
        butler.collections.x_remove(this_run)
        expected_collections.remove(this_run)

    # Create DatasetRef for put using default run.
    ref_in = DatasetRef(dataset_type, data_id, id=uuid.UUID(int=1), run=butler.run)

    # Check that getDeferred fails with standalone ref.
    with pytest.raises(LookupError):
        butler.getDeferred(ref_in)

    # Put the dataset again, since the last thing we did was remove it
    # and we want to use the default collection.
    ref = butler.put(metric, ref_in)

    # Get with parameters
    stop = 4
    sliced = butler.get(ref, parameters={"slice": slice(stop)})
    assert metric != sliced
    assert metric.summary == sliced.summary
    assert metric.output == sliced.output
    assert metric.data is not None  # for mypy
    assert metric.data[:stop] == sliced.data
    # getDeferred with parameters
    sliced = butler.getDeferred(ref, parameters={"slice": slice(stop)}).get()
    assert metric != sliced
    assert metric.summary == sliced.summary
    assert metric.output == sliced.output
    assert metric.data[:stop] == sliced.data
    # getDeferred with deferred parameters
    sliced = butler.getDeferred(ref).get(parameters={"slice": slice(stop)})
    assert metric != sliced
    assert metric.summary == sliced.summary
    assert metric.output == sliced.output
    assert metric.data[:stop] == sliced.data

    if storage_class.isComposite():
        # Check that components can be retrieved
        metric_out = butler.get(ref.datasetType.name, data_id)
        comp_name_s = ref.datasetType.componentTypeName("summary")
        comp_name_d = ref.datasetType.componentTypeName("data")
        summary = butler.get(comp_name_s, data_id)
        assert summary == metric.summary
        data = butler.get(comp_name_d, data_id)
        assert data == metric.data

        if "counter" in storage_class.derivedComponents:
            count = butler.get(ref.datasetType.componentTypeName("counter"), data_id)
            assert count == len(data)

            count = butler.get(
                ref.datasetType.componentTypeName("counter"), data_id, parameters={"slice": slice(stop)}
            )
            assert count == stop

        comp_ref = butler.find_dataset(comp_name_s, data_id, collections=butler.collections.defaults)
        assert comp_ref is not None
        summary = butler.get(comp_ref)
        assert summary == metric.summary

    # Create a Dataset type that has the same name but is inconsistent.
    inconsistent_dataset_type = DatasetType(
        dataset_type_name, dataset_type.dimensions, harness.storage_class_factory.getStorageClass("Config")
    )

    # Getting with a dataset type that does not match registry fails
    with pytest.raises(
        ValueError,
        match="(Supplied dataset type .* inconsistent with registry)"
        "|(The new storage class .* is not compatible with the existing storage class)",
    ):
        butler.get(inconsistent_dataset_type, data_id)

    # Combining a DatasetRef with a dataId should fail
    with pytest.raises(ValueError, match="DatasetRef given, cannot use dataId as well"):
        butler.get(ref, data_id)
    # Getting with an explicit ref should fail if the id doesn't match.
    with pytest.raises((FileNotFoundError, DatasetNotFoundError)):
        butler.get(DatasetRef(ref.datasetType, ref.dataId, id=uuid.UUID(int=101), run=butler.run))

    # Getting a dataset with unknown parameters should fail
    with pytest.raises(KeyError, match="Parameter 'unsupported' not understood"):
        butler.get(ref, parameters={"unsupported": True})

    # Check we have a collection
    collections = set(butler.collections.query("*"))
    assert collections == expected_collections

    # Clean up to check that we can remove something that may have
    # already had a component removed
    butler.pruneDatasets([ref], unstore=True, purge=True)

    # Add the same ref again, so we can check that duplicate put fails.
    ref = butler.put(metric, dataset_type, data_id)

    # Repeat put will fail.
    with pytest.raises(ConflictingDefinitionError, match="A database constraint failure was triggered"):
        butler.put(metric, dataset_type, data_id)

    # Remove the datastore entry.
    butler.pruneDatasets([ref], unstore=True, purge=False, disassociate=False)

    # Put will still fail
    with pytest.raises(ConflictingDefinitionError, match="A database constraint failure was triggered"):
        butler.put(metric, dataset_type, data_id)

    # Repeat the same sequence with resolved ref.
    butler.pruneDatasets([ref], unstore=True, purge=True)
    ref = butler.put(metric, ref_in)

    # Repeat put will fail.
    with pytest.raises(ConflictingDefinitionError, match="Datastore already contains dataset"):
        butler.put(metric, ref_in)

    # Remove the datastore entry.
    butler.pruneDatasets([ref], unstore=True, purge=False, disassociate=False)

    # In case of resolved ref this write will succeed.
    ref = butler.put(metric, ref_in)

    # Leave the dataset in place since some downstream tests require
    # something to be present

    return butler
