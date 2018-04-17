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

from lsst.daf.butler.core.utils import doImport

from .config import Config

__all__ = ("RegistryConfig", "Registry")


class RegistryConfig(Config):
    pass


class Registry(metaclass=ABCMeta):
    """Registry interface.

    Parameters
    ----------
    config : `RegistryConfig`
    """

    @staticmethod
    def fromConfig(config):
        """Create `Registry` subclass instance from `config`.

        Uses ``registry.cls`` from `config` to determine which subclass to instantiate.

        Parameters
        ----------
        config : `RegistryConfig`, `Config` or `str`
            Registry configuration

        Returns
        -------
        registry : `Registry` (subclass)
            A new `Registry` subclass instance.
        """
        if not isinstance(config, RegistryConfig):
            if isinstance(config, str):
                config = Config(config)
            if isinstance(config, Config):
                config = RegistryConfig(config['registry'])
            else:
                raise ValueError("Incompatible Registry configuration: {}".format(config))
        cls = doImport(config['cls'])
        return cls(config=config)

    def __init__(self, config):
        assert isinstance(config, RegistryConfig)
        self.config = config
