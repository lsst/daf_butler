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

import os

from .core.config import Config
from .core.datastore import Datastore
from .core.registry import Registry
from .core.storageClass import StorageClassFactory
from .core.utils import doImport

__all__ = ("ButlerConfig", "Butler")


class ButlerConfig(Config):
    """Contains the configuration for a `Butler`

    The configuration is read and merged with default configurations for
    the particular classes. The defaults are read from
    ``$DAF_BUTLER_DIR/config`` using names specified by the appropriate classes
    for registry and datastore, and including the standard schema and storage
    class definitions.

    Parameters
    ----------
    other : `str`
        Path to butler configuration YAML file.
    """

    def __init__(self, other):

        # Create an empty config for us to populate
        super().__init__()

        # Find the butler configs
        self.defaultsDir = None
        if "DAF_BUTLER_DIR" in os.environ:
            self.defaultsDir = os.path.join(os.environ["DAF_BUTLER_DIR"], "config")

        # Storage classes
        storageClasses = Config(os.path.join(self.defaultsDir, "storageClasses.yaml"))
        self.update(storageClasses)

        # Default schema
        schema = Config(os.path.join(self.defaultsDir, "registry", "default_schema.yaml"))
        self.update(schema)

        # Read the supplied config so that we can work out which other
        # defaults to use.
        butlerConfig = Config(other)

        # Check that fundamental keys are present
        self._validate(butlerConfig)

        # Look for class specific defaults
        for section in ("datastore", "registry"):
            k = f"{section}.cls"
            print("Checking {}: {}".format(k, butlerConfig[k]))
            cls = doImport(butlerConfig[k])
            defaultsPath = cls.defaults
            if defaultsPath is not None:
                if not os.path.isabs(defaultsPath):
                    defaultsPath = os.path.join(self.defaultsDir, defaultsPath)
                c = Config(defaultsPath)
                # Merge into baseline
                self.update(c)

        # Now that we have all the defaults we can merge the externally
        # provided config into the defaults.
        self.update(butlerConfig)

    def _validate(self, config=None):
        """Check a butler config contains mandatory keys.

        Parameters
        ----------
        config : `Config`, optional
            By default checks itself, but if ``config`` is given, this
            config will be checked instead.
        """
        if config is None:
            config = self
        for k in ['datastore.cls', 'registry.cls']:
            if k not in config:
                raise ValueError(f"Missing ButlerConfig parameter: {k}")


class Butler:
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
        self.registry = Registry.fromConfig(self.config)
        self.datastore = Datastore.fromConfig(self.config, self.registry)
        self.storageClasses = StorageClassFactory()
        self.storageClasses.addFromConfig(self.config)
        self.run = self.registry.getRun(collection=self.config['run'])
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

        # Look up storage class to see if this is a composite
        storageClass = datasetType.storageClass

        # Check to see if this storage class has a disassembler
        if storageClass.assemblerClass.disassemble is not None and storageClass.components:
            components = storageClass.assembler().disassemble(obj)
            for component, info in components.items():
                compTypeName = datasetType.componentTypeName(component)
                compRef = self.put(info.component, compTypeName, dataId, producer)
                self.registry.attachComponent(component, ref, compRef)
        else:
            # This is an entity without a disassembler.
            # If it is a composite we still need to register the components
            for component in storageClass.components:
                compTypeName = datasetType.componentTypeName(component)
                compDatasetType = self.registry.getDatasetType(compTypeName)
                compRef = self.registry.addDataset(compDatasetType, dataId, run=self.run, producer=producer)
                self.registry.attachComponent(component, ref, compRef)
            self.datastore.put(obj, ref)

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
        # if the ref exists in the store we return it directly
        if self.datastore.exists(ref):
            return self.datastore.get(ref)
        elif ref.components:
            # Reconstruct the composite
            components = {}
            for compName, compRef in ref.components.items():
                components[compName] = self.datastore.get(compRef)

            # Assemble the components
            return ref.datasetType.storageClass.assembler().assemble(components)
        else:
            # single entity in datastore
            raise ValueError("Unable to locate ref {} in datastore {}".format(ref.id, self.datastore.name))

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
        ref = self.registry.find(self.run.collection, datasetType, dataId)
        return self.getDirect(ref)
