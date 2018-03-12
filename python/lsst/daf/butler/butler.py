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

    def getDirect(self, ref, parameters=None):
        """Load a `Dataset` or a slice thereof from a `DatasetRef`.

        Unlike `Butler.get`, this method allows `Datasets` outside the Butler's `Collection` to be read as
        long as the `DatasetRef` that identifies them can be obtained separately.

        Parameters
        ----------
        ref : `DatasetRef`
            A pointer to the `Dataset` to load.
        parameters : `dict`
            `StorageClass`-specific parameters that can be used to obtain a slice of the `Dataset`.

        Returns
        -------
        inMemoryDataset : `InMemoryDataset`
            The requested `Dataset`.
        """
        parent = self.datastore.get(ref.uri, ref.datasetType.storageClass, parameters) if ref.uri else None
        children = {name: self.datastore.get(childRef, parameters)
                    for name, childRef in ref.components.items()}
        return ref.datasetType.storageClass.assemble(parent, children)

    def get(self, ref, parameters=None):
        """Load a `Dataset` or a slice thereof from the Butler's `Collection`.

        Parameters
        ----------
        ref : `DatasetRef`
            The `Dataset` to retrieve.
        parameters : `dict`
            A dictionary of `StorageClass`-specific parameters that can be
            used to obtain a slice of the `Dataset`.

        Returns
        -------
        dataset : `InMemoryDataset`
            The requested `Dataset`.
        """
        ref = self.registry.find(self.run.collection, ref)
        if ref:
            return self.getDirect(ref, parameters)
        else:
            return None  # No Dataset found

    def put(self, ref, inMemoryDataset, producer=None):
        """Write a `Dataset`.

        Parameters
        ----------
        ref : `DatasetRef`
            The `Dataset` being stored.
        inMemoryDataset : `InMemoryDataset`
            The `Dataset` to store.
        producer : `Quantum`
            The producer of this `Dataset`.  May be ``None`` for some
            `Registry` instances.
            ``producer.run`` must match ``self.config['run']``.

        Returns
        -------
        datasetRef : `DatasetRef`
            The registered (and stored) dataset.
        """
        ref = self.registry.expand(ref)
        run = self.run
        assert(producer is None or run == producer.run)
        storageHint = ref.makeStorageHint(run)
        uri, components = self.datastore.put(inMemoryDataset, ref.datasetType.storageClass,
                                             storageHint, ref.datasetType.name)
        return self.registry.addDataset(ref, uri, components, producer=producer, run=run)

    def markInputUsed(self, quantum, ref):
        """Mark a `Dataset` as having been "actually" (not just
        predicted-to-be) used by a `Quantum`.

        Parameters
        ----------
        quantum : `Quantum`
            The dependent `Quantum`.
        ref : `DatasetRef`
            The `Dataset` that is a true dependency of ``quantum``.
        """
        ref = self.registry.find(self.run.collection, ref)
        self.registry.markInputUsed(ref, quantum)

    def unlink(self, *refs):
        """Remove dataset from collection.

        Remove the `Dataset`\ s associated with the given `DatasetRef`\ s
        from the `Butler`\ 's collection, and signal that they may be deleted
        from storage if they are not referenced by any other collection.

        Parameters
        ----------
        refs : `list` of `DatasetRef`
            List of refs for `Dataset`\ s to unlink.
        """
        refs = [self.registry.find(self.run.collection, ref) for ref in refs]
        for ref in self.registry.disassociate(self.run.collection, refs, remove=True):
            self.datastore.remove(ref.uri)
