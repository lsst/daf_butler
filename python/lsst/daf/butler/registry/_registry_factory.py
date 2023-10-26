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

__all__ = ("_RegistryFactory",)

from typing import TYPE_CHECKING

from lsst.resources import ResourcePathExpression

from .._config import Config
from ..dimensions import DimensionConfig
from ._config import RegistryConfig
from ._defaults import RegistryDefaults
from .sql_registry import SqlRegistry

if TYPE_CHECKING:
    from .._butler_config import ButlerConfig


class _RegistryFactory:
    """Interface for creating and initializing Registry instances.

    Parameters
    ----------
    config : `RegistryConfig` or `str`, optional
        Registry configuration, if missing then default configuration will
        be loaded from registry.yaml.

    Notes
    -----
    Each registry implementation can have its own constructor parameters.
    The assumption is that an instance of a specific subclass will be
    constructed from configuration using ``RegistryClass.fromConfig()`` or
    ``RegistryClass.createFromConfig()``.

    This class will look for a ``cls`` entry in registry configuration object
    (defaulting to ``SqlRegistry``), import that class, and call one of the
    above methods on the imported class.
    """

    def __init__(self, config: ButlerConfig | RegistryConfig | Config | str | None):
        if not isinstance(config, RegistryConfig):
            if isinstance(config, str | Config) or config is None:
                config = RegistryConfig(config)
            else:
                raise ValueError(f"Incompatible Registry configuration: {config}")
        self._config = config

        registry_cls_name = config.get("cls")
        # Check that config does not specify unknown registry type, and allow
        # both old and new location of SqlRegistry.
        if registry_cls_name not in (
            "lsst.daf.butler.registries.sql.SqlRegistry",
            "lsst.daf.butler.registry.sql_registry.SqlRegistry",
            None,
        ):
            raise TypeError(
                f"Registry class obtained from config {registry_cls_name} is not a SqlRegistry class."
            )
        self._registry_cls = SqlRegistry

    def create_from_config(
        self,
        dimensionConfig: DimensionConfig | str | None = None,
        butlerRoot: ResourcePathExpression | None = None,
    ) -> SqlRegistry:
        """Create registry database and return `SqlRegistry` instance.

        This method initializes database contents, database must be empty
        prior to calling this method.

        Parameters
        ----------
        dimensionConfig : `DimensionConfig` or `str`, optional
            Dimensions configuration, if missing then default configuration
            will be loaded from dimensions.yaml.
        butlerRoot : convertible to `lsst.resources.ResourcePath`, optional
            Path to the repository root this `Registry` will manage.

        Returns
        -------
        registry : `SqlRegistry`
            A new `SqlRegistry` instance.
        """
        return self._registry_cls.createFromConfig(self._config, dimensionConfig, butlerRoot)

    def from_config(
        self,
        butlerRoot: ResourcePathExpression | None = None,
        writeable: bool = True,
        defaults: RegistryDefaults | None = None,
    ) -> SqlRegistry:
        """Create `SqlRegistry` subclass instance from ``config``.

        Registry database must be initialized prior to calling this method.

        Parameters
        ----------
        butlerRoot : convertible to `lsst.resources.ResourcePath`, optional
            Path to the repository root this `Registry` will manage.
        writeable : `bool`, optional
            If `True` (default) create a read-write connection to the database.
        defaults : `~lsst.daf.butler.registry.RegistryDefaults`, optional
            Default collection search path and/or output `~CollectionType.RUN`
            collection.

        Returns
        -------
        registry : `SqlRegistry` (subclass)
            A new `SqlRegistry` subclass instance.
        """
        return self._registry_cls.fromConfig(self._config, butlerRoot, writeable, defaults)
