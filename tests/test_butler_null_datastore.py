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


"""Test that Butler can fall back to a null datastore."""

from __future__ import annotations

import contextlib
import os

import pytest

from lsst.daf.butler import Butler, Config, DatasetRef, DatasetType, StorageClassFactory
from lsst.daf.butler.datastore import NullDatastore
from lsst.daf.butler.tests.fixtures import ButlerRepo


def test_fallback(butler_repo: ButlerRepo, storage_class_factory: StorageClassFactory) -> None:
    """Test that a broken datastore config still yields a usable registry."""
    # Read the butler config and mess with the datastore section.
    config_path = os.path.join(butler_repo.root, "butler.yaml")
    bad_config = Config(config_path)
    bad_config["datastore", "cls"] = "lsst.not.a.datastore.Datastore"
    bad_config.dumpToUri(config_path)

    with pytest.raises(RuntimeError):
        Butler(butler_repo.root, without_datastore=False)

    with pytest.raises(RuntimeError):
        Butler.from_config(butler_repo.root, without_datastore=False)

    with contextlib.closing(
        Butler.from_config(butler_repo.root, writeable=True, without_datastore=True)
    ) as butler:
        assert isinstance(butler._datastore, NullDatastore)

        # Check that registry is working.
        butler.collections.register("MYRUN")
        collections = butler.collections.query("*")
        assert "MYRUN" in set(collections)

        # Create a ref.
        dimensions = butler.dimensions.conform([])
        storageClass = storage_class_factory.getStorageClass("StructuredDataDict")
        datasetTypeName = "metric"
        datasetType = DatasetType(datasetTypeName, dimensions, storageClass)
        butler.registry.registerDatasetType(datasetType)
        ref = DatasetRef(datasetType, {}, run="MYRUN")

        # Check that datastore will complain.
        with pytest.raises(FileNotFoundError):
            butler.get(ref)
        with pytest.raises(FileNotFoundError):
            butler.getURI(ref)
