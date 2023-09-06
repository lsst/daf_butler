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

"""Support for reading Astropy tables with the Arrow formatter."""
from __future__ import annotations

from typing import Any

import astropy.table as atable
from lsst.daf.butler.formatters.parquet import ArrowAstropySchema
from lsst.utils.introspection import get_full_type_name

from .arrowtable import ArrowTableDelegate

__all__ = ["ArrowAstropyDelegate"]


class ArrowAstropyDelegate(ArrowTableDelegate):
    """Delegate that understands the ``ArrowAstropy`` storage class."""

    _datasetType = atable.Table

    def getComponent(self, composite: atable.Table, componentName: str) -> Any:
        """Get a component from an astropy table stored via ArrowAstropy.

        Parameters
        ----------
        composite : `~astropy.table.Table`
            Astropy table to access component.
        componentName : `str`
            Name of component to retrieve.

        Returns
        -------
        component : `object`
            The component.

        Raises
        ------
        AttributeError
            The component can not be found.
        """
        match componentName:
            case "columns":
                return list(composite.columns.keys())
            case "schema":
                return ArrowAstropySchema(composite)
            case "rowcount":
                return len(composite)

        raise AttributeError(
            f"Do not know how to retrieve component {componentName} from {get_full_type_name(composite)}"
        )

    def _getColumns(self, inMemoryDataset: atable.Table) -> list[str]:
        return inMemoryDataset.columns.keys()

    def _selectColumns(self, inMemoryDataset: atable.Table, columns: list[str]) -> atable.Table:
        return inMemoryDataset[columns]
