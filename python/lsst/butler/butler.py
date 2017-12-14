#
# LSST Data Management System
#
# Copyright 2008-2017  AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#

from .datastore import Datastore
from .registry import Registry
from .datasets import DatasetLabel, DatasetHandle


class ButlerConfig(object):
    """Contains the configuration for a `Butler`.

    Attributes
    ----------
    collection : `CollectionTag`
        The input collection.
    run : `Run`
        The run used for all outputs.
        May be ``None`` to construct a read-only Butler.
        The `Run`s `CollectionTag` is always used as the input collection when a `Run` is provided.
    templates : `dict`
        Maps `DatasetType` names to path templates, used to override `DatasetType.template` as obtained from the `Registry` when present.
    """

    def __init__(self, collection=None, run=None, templates=None):
        """Constructor.

        Parameters
        ----------
        collection : `CollectionTag`
            The input collection.
        run : `Run`
            The run used for all outputs.
            May be ``None`` to construct a read-only Butler.
            The `Run`s `CollectionTag` is always used as the input collection when a `Run` is provided.
        templates : `dict`
            Maps `DatasetType` names to path templates, used to override `DatasetType.template` as obtained from the `Registry` when present.
        """
        assert collection or run
        self.run = run
        if run:
            self.collection = run.tag
        else:
            self.collection = collection
        self.templates = templates


class Butler(object):
    """Main entry point for the data access system.

    Attributes
    ----------
    config : `ButlerConfiguration`
        Configuration.
    datastore : `Datastore`
        Datastore to use for storage.
    registry : `Registry`
        Registry to use for lookups.
    """

    def __init__(self, config, datastore, registry):
        """Constructor.

        Parameters
        ----------
        config : `ButlerConfiguration`
            Configuration.
        datastore : `Datastore`
            Datastore to use for storage.
        registry : `Registry`
            Registry to use for lookups.
        """
        assert isinstance(config, ButlerConfig)
        assert isinstance(datastore, Datastore)
        assert isinstance(registry, Registry)
        self.config = config
        self.datastore = datastore
        self.registry = registry

    def getDirect(self, handle, parameters=None):
        """Load a `Dataset` or a slice thereof from a `DatasetHandle`.

        Unlike `Butler.get`, this method allows `Datasets` outside the Butler's `Collection` to be read as
        long as the `DatasetHandle` that identifies them can be obtained separately.

        Parameters
        ----------
        handle : `DatasetHandle`
            A pointer to the `Dataset` to load.
        parameters : `dict`
            `StorageClass`-specific parameters that can be used to obtain a slice of the `Dataset`.

        Returns
        -------
        inMemoryDataset : `InMemoryDataset`
            The requested `Dataset`.
        """
        assert isinstance(handle, DatasetHandle)
        parent = self.datastore.get(handle.uri, handle.type.storageClass, parameters) if handle.uri else None
        children = {name: self.datastore.get(childHandle, parameters)
                    for name, childHandle in handle.components.items()}
        return handle.type.storageClass.assemble(parent, children)

    def get(self, label, parameters=None):
        """Load a `Dataset` or a slice thereof from the Butler's `Collection`.

        Parameters
        ----------
        label : `DatasetLabel`
            Identifies the `Dataset` to retrieve.
        parameters : `dict`
            A dictionary of `StorageClass`-specific parameters that can be used to obtain a slice of the `Dataset`.

        Returns
        -------
        dataset : `InMemoryDataset`
            The requested `Dataset`.
        """
        assert isinstance(label, DatasetLabel)
        handle = self.registry.find(self.config.collection, label)
        if handle:
            return self.getDirect(handle, parameters)
        else:
            return None  # No Dataset found

    def put(self, label, inMemoryDataset, producer=None):
        """Write a `Dataset`.

        Parameters
        ----------
        label : `DatasetLabel`
            Identifies the `Dataset` being stored.
        inMemoryDataset : `InMemoryDataset`
            The `Dataset` to store.
        producer : `Quantum`
            Identifies the producer of this `Dataset`.  May be ``None`` for some `Registries`.
            ``producer.run`` must match ``self.config.run``.

        Returns
        -------
        datasetHandle : `DatasetHandle`
            A handle that identifies the registered (and stored) dataset.
        """
        ref = self.registry.expand(label)
        run = self.config.run
        assert(producer is None or run == producer.run)
        # template = self.config.templates.get(ref.type.name, None)
        # path = ref.makePath(run, template)
        path = "temp.fits"  # TODO: Implement this!
        uri, components = self.datastore.put(inMemoryDataset, ref.type.storageClass, path, ref.type.name)
        return self.registry.addDataset(ref, uri, components, producer=producer, run=run)

    def markInputUsed(self, quantum, ref):
        """Mark a `Dataset` as having been "actually" (not just predicted-to-be) used by a `Quantum`.

        Parameters
        ----------
        quantum : `Quantum`
            The dependent `Quantum`.
        ref : `DatasetRef`
            The `Dataset` that is a true dependency of ``quantum``.
        """
        handle = self.registry.find(self.config.collection, ref)
        self.registry.markInputUsed(handle, quantum)

    def unlink(self, *labels):
        """Remove the `Dataset`s associated with the given `DatasetLabel`s from the Butler's `Collection`,
        and signal that they may be deleted from storage if they are not referenced by any other `Collection`.

        Parameters
        ----------
        labels : [`DatasetLabel`]
            List of labels for `Dataset`s to unlink.
        """
        handles = [self.registry.find(self.config.collection, label)
                   for label in labels]
        for handle in self.registry.disassociate(self.config.collection, handles, remove=True):
            self.datastore.remove(handle.uri)
