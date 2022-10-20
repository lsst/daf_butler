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

__all__ = ["MissingDatabaseError", "RegionTypeWarning", "SpatialObsCorePlugin"]

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Optional

from lsst.daf.butler import DatasetId
from lsst.sphgeom import Region
from lsst.utils import doImportType

if TYPE_CHECKING:
    from ...core import ddl
    from ..interfaces import Database
    from ._config import SpatialPluginConfig
    from ._records import Record


class MissingDatabaseError(Exception):
    """Exception raised when database is not provided but plugin implementation
    requires it.
    """


class RegionTypeWarning(Warning):
    """Warning category for unsupported region types."""


class SpatialObsCorePlugin(ABC):
    """Interface for classes that implement support for spatial columns and
    indices in obscore table.
    """

    @classmethod
    @abstractmethod
    def initialize(
        cls, *, name: str, config: Mapping[str, Any], db: Optional[Database]
    ) -> SpatialObsCorePlugin:
        """Construct an instance of the plugin.

        Parameters
        ----------
        name : `str`
            Arbitrary name given to this plugin (usually key in
            configuration).
        config : `dict` [ `str`, `Any` ]
            Plugin configuration dictionary.
        db : `Database`, optional
            Interface to the underlying database engine and namespace. In some
            contexts the database may not be available and will be set to
            `None`. If plugin class requires access to the database, it should
            raise a `MissingDatabaseError` exception when ``db`` is `None`.

        Returns
        -------
        manager : `ObsCoreTableManager`
            Plugin instance.

        Raises
        ------
        MissingDatabaseError
            Raised if plugin requires access to database, but ``db`` is `None`.
        """
        raise NotImplementedError()

    @abstractmethod
    def extend_table_spec(self, table_spec: ddl.TableSpec) -> None:
        """Update obscore table specification with any new columns that are
        needed for this plugin.

        Parameters
        ----------
        table : `ddl.TableSpec`
            ObsCore table specification.

        Notes
        -----
        Table specification is updated in place. Plugins should not remove
        columns, normally updates are limited to adding new columns or indices.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_records(self, dataset_id: DatasetId, region: Optional[Region]) -> Optional[Record]:
        """Return data for obscore records corresponding to a given region.

        Parameters
        ----------
        dataset_id : `DatasetId`
            ID of the corresponding dataset.
        region : `Region`, optional
            Spatial region, can be `None` if dataset has no associated region.

        Returns
        -------
        record : `dict` [ `str`, `Any` ] or `None`
            Data to store in the main obscore table with column values
            corresponding to a region or `None` if there is nothing to store.
        """
        raise NotImplementedError()

    @classmethod
    def load_plugins(
        cls, config: Mapping[str, SpatialPluginConfig], db: Optional[Database]
    ) -> Sequence[SpatialObsCorePlugin]:
        """Load all plugins based on plugin configurations.

        Parameters
        ----------
        config : `Mapping` [ `str`, `SpatialPluginConfig` ]
            Configuration for plugins. The key is an arbitrary name and the
            value is an object describing plugin class and its configuration
            options.
        db : `Database`, optional
            Interface to the underlying database engine and namespace.

        Raises
        ------
        MissingDatabaseError
            Raised if one of the plugins requires access to database, but
            ``db`` is `None`.
        """
        # We have to always load default plugin even if it does not appear in
        # configuration.
        from .default_spatial import DefaultSpatialObsCorePlugin

        spatial_plugins: list[SpatialObsCorePlugin] = []
        has_default = False
        for plugin_name, plugin_config in config.items():
            class_name = plugin_config.cls
            plugin_class = doImportType(class_name)
            if not issubclass(plugin_class, SpatialObsCorePlugin):
                raise TypeError(
                    f"Spatial ObsCore plugin {class_name} is not a subclass of SpatialObsCorePlugin"
                )
            plugin = plugin_class.initialize(name=plugin_name, config=plugin_config.config, db=db)
            spatial_plugins.append(plugin)
            if plugin_class is DefaultSpatialObsCorePlugin:
                has_default = True

        if not has_default:
            # Always create default spatial plugin.
            spatial_plugins.insert(
                0, DefaultSpatialObsCorePlugin.initialize(name="default", config={}, db=db)
            )

        return spatial_plugins
