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

"""Tests for Butler configuration and for creating repositories."""

from __future__ import annotations

import contextlib
import logging
import os
import tempfile

import pytest
from butler_test_support import (
    AXIS_NAMES,
    BUTLER_TESTS_AXES,
    FILE_DATASTORE_AXES,
    records_from,
)

from lsst.daf.butler import Butler, ButlerConfig, Config, DatasetType
from lsst.daf.butler.datastore.file_templates import FileTemplate, FileTemplateValidationError
from lsst.daf.butler.direct_butler import DirectButler
from lsst.daf.butler.tests.fixtures import (
    DATASTORE_PROFILES,
    ButlerHarness,
    ButlerRepo,
    make_example_metrics,
)
from lsst.resources import ResourcePath

BUTLER_LOGGER = "lsst.daf.butler"
"""Root of the loggers the config search path test watches."""

FILE_TEMPLATE_LOGGER = "lsst.daf.butler.datastore.file_templates"
"""Logger that reports a template referring to a missing record field."""

OUTFILE_LAYOUTS = ["outfile", "outfile_dir", "outfile_uri"]
"""Repository layouts where makeRepo wrote the config outside the repo.

DM-55822 measured each layout's marginal coverage as zero unique lines and
zero unique arcs, so they no longer rerun the put/get suite. They stay on
``test_config_existence``, which is the test that distinguishes an outfile
naming a file, a directory and a URI.
"""


def test_search_path(test_directory: str, caplog: pytest.LogCaptureFixture) -> None:
    """Test that searchPaths brings in an overriding config directory."""
    config_file = os.path.join(test_directory, "config", "basic", "butler.yaml")

    with caplog.at_level(logging.DEBUG, logger=BUTLER_LOGGER):
        caplog.clear()
        config1 = ButlerConfig(config_file)
        records = records_from(caplog, BUTLER_LOGGER, logging.DEBUG)
    assert records
    assert "testConfigs" not in "\n".join(record.getMessage() for record in records)

    override_directory = os.path.join(test_directory, "config", "testConfigs")
    with caplog.at_level(logging.DEBUG, logger=BUTLER_LOGGER):
        caplog.clear()
        config2 = ButlerConfig(config_file, searchPaths=[override_directory])
        records = records_from(caplog, BUTLER_LOGGER, logging.DEBUG)
    assert records
    assert "testConfigs" in "\n".join(record.getMessage() for record in records)

    key = ("datastore", "records", "table")
    assert config1[key] != config2[key]
    assert config2[key] == "override_record"


@pytest.mark.parametrize("repo_layout", ["explicit_root"], indirect=True)
def test_file_locations(butler_repo: ButlerRepo) -> None:
    """Test that a yaml file in one location can refer to a root in another."""
    dir1, dir2 = butler_repo.dir1, butler_repo.dir2
    assert dir1 is not None
    assert dir2 is not None
    assert dir1 != dir2
    assert os.path.exists(os.path.join(dir2, "butler2.yaml"))
    assert not os.path.exists(os.path.join(dir1, "butler.yaml"))
    assert os.path.exists(os.path.join(dir1, "gen3.sqlite3"))


@pytest.mark.parametrize("repo_layout", OUTFILE_LAYOUTS, indirect=True)
def test_config_existence(butler_repo: ButlerRepo, repo_layout: str) -> None:
    """Test that a config file makeRepo wrote outside the repo points back."""
    config_file = butler_repo.config_file
    if repo_layout == "outfile_dir":
        # Append the yaml file else the Config constructor does not know the
        # file type.
        config_file = os.path.join(config_file, "butler.yaml")

    c = Config(config_file)
    uri_config = ResourcePath(c["root"])
    uri_expected = ResourcePath(butler_repo.root, forceDirectory=True)
    assert uri_config.geturl() == uri_expected.geturl()
    assert ":" not in uri_config.path, "Check for URI concatenated with normal path"


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_make_repo(
    butler_harness: ButlerHarness,
    test_directory: str,
    datastore_type: str,
    registry_backend: str,
    butler_client: str,
) -> None:
    """Test that we can write butler configuration to a new repository via
    the Butler.makeRepo interface and then instantiate a butler from the
    repo root.
    """
    if butler_client == "server":
        # Only applies to DirectButler.
        return
    if registry_backend == "postgres":
        # This test assumes that it is using sqlite and that the config file
        # on disk is acceptable to sqlite.
        pytest.skip("Postgres config is not compatible with this test.")

    full_config_key = butler_harness.profile.full_config_key
    if full_config_key is None:
        # Do not run the test if we know this datastore configuration does
        # not support a file system root.
        return

    source_config = os.path.join(test_directory, DATASTORE_PROFILES[datastore_type].config_file)
    root = butler_harness.root

    # create two separate directories
    root1 = tempfile.mkdtemp(dir=root)
    root2 = tempfile.mkdtemp(dir=root)

    with contextlib.ExitStack() as stack:
        # This test asserts on repository creation itself, so it must not go
        # through the caching helper.
        assert not Butler.has_repo_config(root1)
        butler_config = Butler.makeRepo(root1, config=Config(source_config))
        assert Butler.has_repo_config(root1)
        limited = Config(source_config)
        butler1 = stack.enter_context(Butler.from_config(butler_config))
        assert isinstance(butler1, DirectButler), "Expect DirectButler in configuration"
        butler_config = Butler.makeRepo(root2, standalone=True, config=Config(source_config))
        full = Config(butler_harness.config_file)
        butler2 = stack.enter_context(Butler.from_config(butler_config))
        assert isinstance(butler2, DirectButler), "Expect DirectButler in configuration"
        # Butlers should have the same configuration regardless of whether
        # defaults were expanded.
        assert butler1._config == butler2._config
        # Config files loaded directly should not be the same.
        assert limited != full
        # Make sure "limited" doesn't have a few keys we know it should be
        # inheriting from defaults.
        assert full_config_key in full
        assert full_config_key not in limited

        # Collections don't appear until something is put in them
        collections1 = set(butler1.registry.queryCollections())
        assert collections1 == set()
        assert set(butler2.registry.queryCollections()) == collections1

        # Check that a config with no associated file name will not
        # work properly with relocatable Butler repo
        butler_config.configFile = None
        with pytest.raises(ValueError, match="Required to replace <butlerRoot>"):
            Butler.from_config(butler_config)

        with pytest.raises(FileExistsError):
            Butler.makeRepo(root, standalone=True, config=Config(source_config), overwrite=False)


@pytest.mark.parametrize(AXIS_NAMES, FILE_DATASTORE_AXES, indirect=True)
def test_put_templates(
    butler_harness: ButlerHarness, butler_client: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that datasets land at the paths the file templates describe."""
    if butler_client == "server":
        # The Butler server instance is configured with different file naming
        # templates than this test is expecting.
        return

    storage_class = butler_harness.storage_class_factory.getStorageClass("StructuredDataNoComponents")
    default_run = butler_harness.default_run
    butler = butler_harness.create_empty_butler(run=default_run)

    # Add needed Dimensions
    butler.registry.insertDimensionData("instrument", {"name": "DummyCamComp"})
    butler.registry.insertDimensionData(
        "physical_filter", {"instrument": "DummyCamComp", "name": "d-r", "band": "R"}
    )
    butler.registry.insertDimensionData("day_obs", {"instrument": "DummyCamComp", "id": 20250101})
    butler.registry.insertDimensionData(
        "visit",
        {
            "instrument": "DummyCamComp",
            "id": 423,
            "name": "v423",
            "physical_filter": "d-r",
            "day_obs": 20250101,
        },
    )
    butler.registry.insertDimensionData(
        "visit",
        {
            "instrument": "DummyCamComp",
            "id": 425,
            "name": "v425",
            "physical_filter": "d-r",
            "day_obs": 20250101,
        },
    )

    # Create and store a dataset
    metric = make_example_metrics()

    # Create two almost-identical DatasetTypes (both will use default
    # template)
    dimensions = butler.dimensions.conform(["instrument", "visit"])
    butler.registry.registerDatasetType(DatasetType("metric1", dimensions, storage_class))
    butler.registry.registerDatasetType(DatasetType("metric2", dimensions, storage_class))
    butler.registry.registerDatasetType(DatasetType("metric3", dimensions, storage_class))

    data_id1 = {"instrument": "DummyCamComp", "visit": 423}
    data_id2 = {"instrument": "DummyCamComp", "visit": 423, "physical_filter": "d-r"}

    # Put with exactly the data ID keys needed
    ref = butler.put(metric, "metric1", data_id1)
    uri = butler.getURI(ref)
    assert uri.exists()
    assert uri.unquoted_path.endswith(f"{default_run}/metric1/??#?/d-r/DummyCamComp_423.pickle")

    # Check the template based on dimensions
    if hasattr(butler._datastore, "templates"):
        butler._datastore.templates.validateTemplates([ref])

    # Put with extra data ID keys (physical_filter is an optional
    # dependency); should not change template (at least the way we're
    # defining them  to behave now; the important thing is that they
    # must be consistent).
    ref = butler.put(metric, "metric2", data_id2)
    uri = butler.getURI(ref)
    assert uri.exists()
    assert uri.unquoted_path.endswith(f"{default_run}/metric2/d-r/DummyCamComp_v423.pickle")

    # Check the template based on dimensions
    if hasattr(butler._datastore, "templates"):
        butler._datastore.templates.validateTemplates([ref])

    # Use a template that has a typo in dimension record metadata.
    # Easier to test with a butler that has a ref with records attached.
    template = FileTemplate("a/{visit.name}/{id}_{visit.namex:?}.fits")
    with caplog.at_level(logging.INFO, logger=FILE_TEMPLATE_LOGGER):
        caplog.clear()
        path = template.format(ref)
        assert records_from(caplog, FILE_TEMPLATE_LOGGER, logging.INFO)
    assert path == f"a/v423/{ref.id}_fits"

    # Without the "?" the same typo is an error rather than a warning.
    template = FileTemplate("a/{visit.name}/{id}_{visit.namex}.fits")
    with pytest.raises(KeyError):
        template.format(ref)

    # Now use a file template that will not result in unique filenames
    with pytest.raises(FileTemplateValidationError):
        butler.put(metric, "metric3", data_id1)
