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

__all__ = ("transfer_datasets_to_datastore",)

from collections.abc import Iterable

from .._butler import Butler
from .._butler_config import ButlerConfig
from .._dataset_ref import DatasetRef
from .._file_dataset import FileDataset
from .._standalone_datastore import instantiate_standalone_datastore
from ..direct_butler import DirectButler


def transfer_datasets_to_datastore(
    source_butler: Butler, output_config: ButlerConfig, refs: Iterable[DatasetRef]
) -> list[FileDataset]:
    """Copy datasets into a `Datastore` without writing entries into the
    associated registry database.

    Parameters
    ----------
    source_butler : `Butler`
        Butler instance from which to copy datasets.
    output_config : `ButlerConfig`
        Butler configuration specifying the configuration for the target
        datastore.
    refs : `~collections.abc.Iterable` [ `DatasetRef` ]
        List of datasets to copy into the target datastore.

    Returns
    -------
    file_datasets : `list` [ `FileDataset` ]
        `FileDataset` instances representing the artifacts copied to
        the target datastore.
    """
    refs = list(refs)
    if len(refs) == 0:
        return []

    if not isinstance(source_butler, DirectButler):
        raise ValueError("Butler must be an instance of DirectButler")

    # Refs must contain dimension records to allow for filename template
    # expansion in the datastore.
    refs = source_butler._registry.expand_refs(list(refs))
    # Read out the absolute URLs of the datasets we are about to transfer.
    datasets = list(source_butler._datastore.export(refs, directory=None, transfer="direct"))

    # Set up a temporary, initially empty, in-memory DB for the target
    # Datastore.
    dimension_universe = datasets[0].refs[0].dimensions.universe
    datastore, db = instantiate_standalone_datastore(output_config, dimension_universe)
    try:
        # Write the files to their destination in the target datastore.
        datastore.ingest(*datasets, transfer="copy", record_validation_info=False)
        # Read out the relative URLs of the files, for their location
        # in the target datastore.
        return list(datastore.export(refs, directory=None, transfer=None))
    finally:
        db.dispose()
