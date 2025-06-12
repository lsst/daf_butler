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

from urllib.parse import quote

from lsst.daf.butler import Butler, DatasetId
from lsst.daf.butler.dimensions import DataCoordinate, SerializedDataId
from lsst.daf.butler.registry import RegistryDefaults


def set_default_data_id(butler: Butler, data_id: SerializedDataId) -> None:  # numpydoc ignore=PR01
    """Set the default data ID values used for lookups in the given Butler."""
    butler.registry.defaults = RegistryDefaults.from_data_id(
        DataCoordinate.standardize(data_id, universe=butler.dimensions)
    )


def generate_file_download_uri(
    root_uri: str, repository: str, dataset_id: DatasetId, component: str | None = None
) -> str:  # numpydoc ignore=PR01
    """Generate a URL pointing to the file download redirect endpoint."""
    root_uri = root_uri.rstrip("/")
    uri = f"{root_uri}/api/butler/repo/{quote(repository)}/v1/dataset/{quote(str(dataset_id))}/download"
    if component:
        return uri + f"?component={quote(component)}"
    else:
        return uri
