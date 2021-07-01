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

__all__ = ("ingest_files",)

import logging
from typing import Optional, Tuple, Dict, Any
from collections import defaultdict

from astropy.table import Table

from lsst.utils import doImport

from .. import Butler, DatasetIdGenEnum
from ..core import FileDataset, DatasetRef, ButlerURI, DataCoordinate

log = logging.getLogger(__name__)


def ingest_files(repo: str, dataset_type: str, run: str, table_file: str,
                 data_id: Tuple[str, ...] = (),
                 formatter: Optional[str] = None,
                 id_generation_mode: str = "UNIQUE",
                 prefix: Optional[str] = None,
                 transfer: str = "auto") -> None:
    """Ingest files from a table.

    Parameters
    ----------
    repo : `str`
        URI string of the Butler repo to use.
    dataset_type : `str`
        The name of the dataset type for the files to be ingested.
    formatter : `str`
        Fully-qualified python class name for the `Formatter` to use
        to read the ingested files.
    run : `str`
        The run in which the files should be ingested.
    table_file : `str`
        Path to a table file to read.
    id_generation_mode : `str`
        Mode to use for generating IDs.  Should map to `DatasetGenIdEnum`.
    prefix : `str`
        Prefix to use when resolving relative paths in table files. The default
        is to use the current working directory.
    transfer : `str`
        Transfer mode to use for ingest.
    """

    # Check that the formatter can be imported -- validate this as soon
    # as possible before we read a potentially large table file.
    if formatter:
        doImport(formatter)
    else:
        formatter = None

    # Force null prefix to None for API compatibility.
    if not prefix:
        prefix = None

    # Convert the dataset ID gen more string to enum
    id_gen_mode = DatasetIdGenEnum.__members__[id_generation_mode]

    # Create the butler with the relevant run attached.
    butler = Butler(repo, run=run)

    datasetType = butler.registry.getDatasetType(dataset_type)

    universe = butler.registry.dimensions

    # Convert any additional k=v strings in the dataId tuple to dict
    # form.
    common_data_id: Dict[str, Any] = {}
    for id_str in data_id:
        dimension_str, value = id_str.split("=")

        try:
            dimension = universe.getStaticDimensions()[dimension_str]
        except KeyError:
            raise ValueError(f"DataID dimension '{dimension_str}' is not known to this universe.") from None

        # Cast the value to the right python type (since they will be
        # strings at this point).
        value = dimension.primaryKey.getPythonType()(value)

        common_data_id[dimension_str] = value

    # Read the table and determine the dimensions (the first column
    # is the file URI).
    table = Table.read(table_file)

    dimensions = table.colnames
    dimensions.pop(0)

    # Handle multiple dataIds per file by grouping by file.
    refs_by_file = defaultdict(list)
    n_datasets = 0
    for row in table:
        dataId: Dict[str, Any] = {k: row[k] for k in dimensions}

        # The command line can override a column.
        dataId.update(common_data_id)

        standardized = DataCoordinate.standardize(dataId, graph=datasetType.dimensions)

        ref = DatasetRef(datasetType, standardized, conform=False)

        # Convert path to absolute (because otherwise system will
        # assume relative to datastore root and that is almost certainly
        # never the right default here).
        path = ButlerURI(row[0], root=prefix, forceAbsolute=True)

        refs_by_file[path].append(ref)
        n_datasets += 1

    datasets = [FileDataset(path=file,
                            refs=refs,
                            formatter=formatter,) for file, refs in refs_by_file.items()]

    log.info("Ingesting %d dataset(s) from %d file(s)", n_datasets, len(datasets))

    butler.ingest(*datasets, transfer=transfer, run=run, idGenerationMode=id_gen_mode)
