# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

import astropy.time

from .._butler import Butler
from ..core import Timespan
from ..registry import CollectionType


def certifyCalibrations(
    repo: str,
    input_collection: str,
    output_collection: str,
    dataset_type_name: str,
    begin_date: str | None,
    end_date: str | None,
    search_all_inputs: bool,
) -> None:
    """Certify a set of calibrations with a validity range.

    Parameters
    ----------
    repo : `str`
        URI to the location of the repo or URI to a config file describing the
        repo and its location.
    inputCollection : `str`
       Data collection to pull calibrations from.  Usually an existing
        `~CollectionType.RUN` or `~CollectionType.CHAINED` collection, and may
        _not_ be a `~CollectionType.CALIBRATION` collection or a nonexistent
        collection.
    outputCollection : `str`
        Data collection to store final calibrations.  If it already exists, it
        must be a `~CollectionType.CALIBRATION` collection.  If not, a new
        `~CollectionType.CALIBRATION` collection with this name will be
        registered.
    datasetTypeName : `str`
        Name of the dataset type to certify.
    begin_date : `str`, optional
        ISO-8601 date (TAI) this calibration should start being used.
    end_date : `str`, optional
        ISO-8601 date (TAI) this calibration should stop being used.
    search_all_inputs : `bool`, optional
        Search all children of the inputCollection if it is a CHAINED
        collection, instead of just the most recent one.
    """
    butler = Butler(repo, writeable=True)
    registry = butler.registry
    timespan = Timespan(
        begin=astropy.time.Time(begin_date, scale="tai") if begin_date is not None else None,
        end=astropy.time.Time(end_date, scale="tai") if end_date is not None else None,
    )
    if not search_all_inputs:
        if registry.getCollectionType(input_collection) is CollectionType.CHAINED:
            input_collection = next(iter(registry.getCollectionChain(input_collection)))

    refs = set(registry.queryDatasets(dataset_type_name, collections=[input_collection]))
    if not refs:
        raise RuntimeError(f"No inputs found for dataset {dataset_type_name} in {input_collection}.")
    registry.registerCollection(output_collection, type=CollectionType.CALIBRATION)
    registry.certify(output_collection, refs, timespan)
