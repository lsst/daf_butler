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

from __future__ import annotations

__all__ = ("app", "factory_dependency")

import logging
from functools import cache
from typing import Any

from fastapi import Depends, FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from lsst.daf.butler import (
    Butler,
    DataCoordinate,
    SerializedDataCoordinate,
    SerializedDatasetRef,
    SerializedDatasetType,
)

from ._factory import Factory
from ._server_models import FindDatasetModel

BUTLER_ROOT = "ci_hsc_gen3/DATA"

log = logging.getLogger(__name__)

app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1000)


@cache
def _make_global_butler() -> Butler:
    return Butler.from_config(BUTLER_ROOT)


def factory_dependency() -> Factory:
    return Factory(butler=_make_global_butler())


def unpack_dataId(butler: Butler, data_id: SerializedDataCoordinate | None) -> DataCoordinate | None:
    """Convert the serialized dataId back to full DataCoordinate.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler to use for registry and universe.
    data_id : `SerializedDataCoordinate` or `None`
        The serialized form.

    Returns
    -------
    dataId : `DataCoordinate` or `None`
        The DataId usable by registry.
    """
    if data_id is None:
        return None
    return DataCoordinate.from_simple(data_id, registry=butler.registry)


@app.get("/butler/v1/universe", response_model=dict[str, Any])
def get_dimension_universe(factory: Factory = Depends(factory_dependency)) -> dict[str, Any]:
    """Allow remote client to get dimensions definition."""
    butler = factory.create_butler()
    return butler.dimensions.dimensionConfig.toDict()


@app.get(
    "/butler/v1/dataset_type/{dataset_type_name}",
    summary="Retrieve this dataset type definition.",
    response_model=SerializedDatasetType,
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def get_dataset_type(
    dataset_type_name: str, factory: Factory = Depends(factory_dependency)
) -> SerializedDatasetType:
    """Return the dataset type."""
    butler = factory.create_butler()
    datasetType = butler.get_dataset_type(dataset_type_name)
    return datasetType.to_simple()


# Not yet supported: TimeSpan is not yet a pydantic model.
# collections parameter assumes client-side has resolved regexes.
@app.post(
    "/butler/v1/find_dataset/{dataset_type}",
    summary="Retrieve this dataset definition from collection, dataset type, and dataId",
    response_model=SerializedDatasetRef,
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def find_dataset(
    dataset_type: str,
    query: FindDatasetModel,
    factory: Factory = Depends(factory_dependency),
) -> SerializedDatasetRef | None:
    collection_query = query.collections if query.collections else None

    butler = factory.create_butler()
    ref = butler.find_dataset(
        dataset_type, dataId=unpack_dataId(butler, query.dataId), collections=collection_query
    )
    return ref.to_simple() if ref else None
