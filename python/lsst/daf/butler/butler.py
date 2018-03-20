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

"""
Butler top level classes.
"""

from .core.config import Config
from .core.datastore import Datastore
from .core.registry import Registry

__all__ = ("ButlerConfig", "Butler")


class ButlerConfig(Config):
    """Contains the configuration for a `Butler`
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.validate()

    def validate(self):
        for k in ['run', 'datastore.cls', 'registry.cls']:
            if k not in self:
                raise ValueError("Missing ButlerConfig parameter: {0}".format(k))


class Butler(object):
    """Main entry point for the data access system.

    Attributes
    ----------
    config : `str` or `Config`
        (filename to) configuration.
    datastore : `Datastore`
        Datastore to use for storage.
    registry : `Registry`
        Registry to use for lookups.

    Parameters
    ----------
    config : `Config`
        Configuration.
    """

    def __init__(self, config):
        self.config = ButlerConfig(config)
        self.datastore = Datastore.fromConfig(self.config)
        self.registry = Registry.fromConfig(self.config)
        self.run = self.registry.getRun(self.config['run'])
        if self.run is None:
            self.run = self.registry.makeRun(self.config['run'])

    def put(self, obj, datasetType, dataId, producer=None):
        """Store and register a dataset.

        Parameters
        ----------
        obj : `object`
            The dataset.
        datasetType : `DatasetType` instance or `str`
            The `DatasetType`.
        dataId : `dict`
            An identifier with `DataUnit` names and values.
        producer : `Quantum`, optional
            The producer.

        Returns
        -------
        ref : `DatasetRef`
            A reference to the stored dataset.
        """
        datasetType = self.registry.getDatasetType(datasetType)
        ref = self.registry.addDataset(datasetType, dataId, run=self.run, producer=producer)
        # self.datastore.put(obj, ref)
        return ref

    def getDirect(self, ref):
        """Retrieve a stored dataset.

        Unlike `Butler.get`, this method allows datasets outside the Butler's collection to be read as
        long as the `DatasetRef` that identifies them can be obtained separately.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to an already stored dataset.

        Returns
        -------
        obj : `object`
            The dataset.
        """
        # Currently a direct pass-through to `Datastore.get` but this should
        # change for composites.
        return self.datastore.get(ref)

    def get(self, datasetType, dataId):
        """Retrieve a stored dataset.

        Parameters
        ----------
        datasetType : `DatasetType` instance or `str`
            The `DatasetType`.
        dataId : `dict`
            A `dict` of `DataUnit` name, value pairs that label the `DatasetRef`
            within a Collection.

        Returns
        -------
        obj : `object`
            The dataset.
        """
        datasetType = self.registry.getDatasetType(datasetType)
        ref = self.registry.find(datasetType, dataId)
        return self.getDirect(ref)
