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

import astropy.time

from .._butler import Butler
from .._collection_type import CollectionType
from .._timespan import Timespan


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
    input_collection : `str`
        Data collection to pull calibrations from.  Usually an existing
        `~CollectionType.RUN` or `~CollectionType.CHAINED` collection, and may
        _not_ be a `~CollectionType.CALIBRATION` collection or a nonexistent
        collection.
    output_collection : `str`
        Data collection to store final calibrations.  If it already exists, it
        must be a `~CollectionType.CALIBRATION` collection.  If not, a new
        `~CollectionType.CALIBRATION` collection with this name will be
        registered.
    dataset_type_name : `str`
        Name of the dataset type to certify.
    begin_date : `str`, optional
        ISO-8601 date (TAI) this calibration should start being used.
    end_date : `str`, optional
        ISO-8601 date (TAI) this calibration should stop being used.
    search_all_inputs : `bool`, optional
        Search all children of the inputCollection if it is a CHAINED
        collection, instead of just the most recent one.
    """
    butler = Butler.from_config(repo, writeable=True, without_datastore=True)
    timespan = Timespan(
        begin=astropy.time.Time(begin_date, scale="tai") if begin_date is not None else None,
        end=astropy.time.Time(end_date, scale="tai") if end_date is not None else None,
    )
    if not search_all_inputs:
        collection_info = butler.collections.get_info(input_collection)
        if collection_info.type is CollectionType.CHAINED:
            input_collection = collection_info.children[0]

    with butler.query() as query:
        results = query.datasets(dataset_type_name, collections=input_collection)
        refs = set(results)
        if not refs:
            explanation = "\n".join(results.explain_no_results())
            raise RuntimeError(
                f"No inputs found for dataset {dataset_type_name} in {input_collection}. {explanation}"
            )
    butler.collections.register(output_collection, type=CollectionType.CALIBRATION)
    butler.registry.certify(output_collection, refs, timespan)
