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

"""Tests for the null datastore, which refuses almost everything."""

from __future__ import annotations

import pytest

from lsst.daf.butler import DimensionUniverse, StorageClassFactory
from lsst.daf.butler.datastore import NullDatastore
from lsst.daf.butler.tests import DatasetTestHelper
from lsst.resources import ResourcePath


def test_basics() -> None:
    helper = DatasetTestHelper()
    storage_class = StorageClassFactory().getStorageClass("StructuredDataDict")
    ref = helper.makeDatasetRef("metric", DimensionUniverse().empty, storage_class, {})

    null = NullDatastore(None, None)

    assert not null.exists(ref)
    assert not null.knows(ref)
    knows = null.knows_these([ref])
    assert not knows[ref]
    null.validateConfiguration([ref])

    with pytest.raises(FileNotFoundError):
        null.get(ref)
    with pytest.raises(NotImplementedError):
        null.put("", ref)
    with pytest.raises(FileNotFoundError):
        null.getURI(ref)
    with pytest.raises(FileNotFoundError):
        null.getURIs(ref)
    with pytest.raises(FileNotFoundError):
        null.getManyURIs([ref])
    with pytest.raises(NotImplementedError):
        null.getLookupKeys()
    with pytest.raises(NotImplementedError):
        null.import_records({})
    with pytest.raises(NotImplementedError):
        null.export_records([])
    with pytest.raises(NotImplementedError):
        null.export_predicted_records([])
    with pytest.raises(NotImplementedError):
        null.export([ref])
    with pytest.raises(NotImplementedError):
        null.transfer(null, ref)
    with pytest.raises(NotImplementedError):
        null.emptyTrash()
    with pytest.raises(NotImplementedError):
        null.trash(ref)
    with pytest.raises(NotImplementedError):
        null.forget([ref])
    with pytest.raises(NotImplementedError):
        null.remove(ref)
    with pytest.raises(NotImplementedError):
        null.retrieveArtifacts([ref], ResourcePath("."))
    with pytest.raises(NotImplementedError):
        null.transfer_from({}, [ref])
    with pytest.raises(NotImplementedError):
        null.ingest()
