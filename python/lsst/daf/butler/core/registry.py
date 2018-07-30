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

from abc import ABCMeta, abstractmethod

from .utils import doImport
from .config import Config, ConfigSubset
from .schema import SchemaConfig

__all__ = ("RegistryConfig", "Registry")


class RegistryConfig(ConfigSubset):
    component = "registry"
    requiredKeys = ("cls",)
    defaultConfigFile = "registry.yaml"


class Registry(metaclass=ABCMeta):
    """Registry interface.

    Parameters
    ----------
    registryConfig : `RegistryConfig`
        Registry configuration.
    schemaConfig : `SchemaConfig`, optional
        Schema configuration.
    """

    defaultConfigFile = None
    """Path to configuration defaults. Relative to $DAF_BUTLER_DIR/config or
    absolute path. Can be None if no defaults specified.
    """

    @classmethod
    @abstractmethod
    def setConfigRoot(cls, root, config, full):
        """Set any filesystem-dependent config options for this Registry to
        be appropriate for a new empty repository with the given root.

        Parameters
        ----------
        root : `str`
            Filesystem path to the root of the data repository.
        config : `Config`
            A `Config` to update. Only the subset understood by
            this component will be updated. Will not expand
            defaults.
        full : `Config`
            A complete config with all defaults expanded that can be
            converted to a `RegistryConfig`. Read-only and will not be
            modified by this method.
            Repository-specific options that should not be obtained
            from defaults when Butler instances are constructed
            should be copied from `full` to `Config`.
        """
        Config.overrideConfigParameters(RegistryConfig, config, full,
                                        tocopy=("skypix.cls", "skypix.level"))

    @staticmethod
    def fromConfig(registryConfig, schemaConfig=None, create=False):
        """Create `Registry` subclass instance from `config`.

        Uses ``registry.cls`` from `config` to determine which subclass to
        instantiate.

        Parameters
        ----------
        registryConfig : `ButlerConfig`, `RegistryConfig`, `Config` or `str`
            Registry configuration
        schemaConfig : `SchemaConfig`, `Config` or `str`, optional.
            Schema configuration. Can be read from supplied registryConfig
            if the relevant component is defined and ``schemaConfig`` is
            `None`.
        create : `bool`
            Assume empty Registry and create a new one.

        Returns
        -------
        registry : `Registry` (subclass)
            A new `Registry` subclass instance.
        """
        if schemaConfig is None:
            # Try to instantiate a schema configuration from the supplied
            # registry configuration.
            schemaConfig = SchemaConfig(registryConfig)
        elif not isinstance(schemaConfig, SchemaConfig):
            if isinstance(schemaConfig, str) or isinstance(schemaConfig, Config):
                schemaConfig = SchemaConfig(schemaConfig)
            else:
                raise ValueError("Incompatible Schema configuration: {}".format(schemaConfig))

        if not isinstance(registryConfig, RegistryConfig):
            if isinstance(registryConfig, str) or isinstance(registryConfig, Config):
                registryConfig = RegistryConfig(registryConfig)
            else:
                raise ValueError("Incompatible Registry configuration: {}".format(registryConfig))

        cls = doImport(registryConfig['cls'])
        return cls(registryConfig, schemaConfig, create=create)

    def __init__(self, registryConfig, schemaConfig=None, create=False):
        assert isinstance(registryConfig, RegistryConfig)
        self.config = registryConfig
        self._pixelization = None

    @property
    def pixelization(self):
        """Object that interprets SkyPix DataUnit values (`sphgeom.Pixelization`)."""
        if self._pixelization is None:
            pixelizationCls = doImport(self.config["skypix.cls"])
            self._pixelization = pixelizationCls(level=self.config["skypix.level"])
        return self._pixelization

    #  TODO Add back all interfaces (copied from SqlRegistry) once that is stabalized
