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

"""Support for Storage Classes."""

import builtins
import itertools
import logging

from .utils import doImport, Singleton, getFullTypeName
from .composites import CompositeAssembler
from .config import ConfigSubset

log = logging.getLogger(__name__)

__all__ = ("StorageClass", "StorageClassFactory", "StorageClassConfig")


class StorageClassConfig(ConfigSubset):
    component = "storageClasses"
    defaultConfigFile = "storageClasses.yaml"


class StorageClass:
    """Class describing how a label maps to a particular Python type.

    Parameters
    ----------
    name : `str`
        Name to use for this class.
    pytype : `type`
        Python type (or name of type) to associate with the `StorageClass`
    components : `dict`, optional
        `dict` mapping name of a component to another `StorageClass`.
    assembler : `str`, optional
        Fully qualified name of class supporting assembly and disassembly
        of a `pytype` instance.
    """
    defaultAssembler = CompositeAssembler
    defaultAssemblerName = getFullTypeName(defaultAssembler)

    def __init__(self, name, pytype=None, components=None, assembler=None):
        self.name = name
        self._pytypeName = pytype
        if pytype is None:
            self._pytypeName = "object"
            self._pytype = object
        self._components = components if components is not None else {}
        # if the assembler is not None also set it and clear the default
        # assembler
        if assembler is not None:
            self._assemblerClassName = assembler
            self._assembler = None
        elif components is not None:
            # We set a default assembler for composites so that a class is
            # guaranteed to support something if it is a composite.
            log.debug("Setting default assembler for %s", self.name)
            self._assembler = self.defaultAssembler
            self._assemblerClassName = self.defaultAssemblerName
        else:
            self._assembler = None
            self._assemblerClassName = None
        # The types are created on demand and cached
        self._pytype = None

    @property
    def components(self):
        """Component names mapped to associated `StorageClass`
        """
        return self._components

    @property
    def pytype(self):
        """Python type associated with this `StorageClass`."""
        if self._pytype is not None:
            return self._pytype
        # Handle case where we did get a python type not string
        if not isinstance(self._pytypeName, str):
            pytype = self._pytypeName
            self._pytypeName = self._pytypeName.__name__
        elif hasattr(builtins, self._pytypeName):
            pytype = getattr(builtins, self._pytypeName)
        else:
            pytype = doImport(self._pytypeName)
        self._pytype = pytype
        return self._pytype

    @property
    def assemblerClass(self):
        """Class to use to (dis)assemble an object from components."""
        if self._assembler is not None:
            return self._assembler
        if self._assemblerClassName is None:
            return None
        self._assembler = doImport(self._assemblerClassName)
        return self._assembler

    def assembler(self):
        """Return an instance of an assembler.

        Returns
        -------
        assembler : `CompositeAssembler`
            Instance of the assembler associated with this `StorageClass`.
            Assembler is constructed with this `StorageClass`.
        """
        cls = self.assemblerClass
        return cls(storageClass=self)

    def validateInstance(self, instance):
        """Check that the supplied Python object has the expected Python type

        Parameters
        ----------
        instance : `object`
            Object to check.

        Returns
        -------
        isOk : `bool`
            True if the supplied instance object can be handled by this
            `StorageClass`, False otherwise.
        """
        return isinstance(instance, self.pytype)

    def __eq__(self, other):
        """Equality checks name, pytype name, assembler name, and components"""
        if self.name != other.name:
            return False

        if type(self) != type(other):
            return False

        # We must compare pytype and assembler by name since we do not want
        # to trigger an import of external module code here
        if self._assemblerClassName != other._assemblerClassName:
            return False
        if self._pytypeName != other._pytypeName:
            return False

        # Ensure we have the same component keys in each
        if set(self.components.keys()) != set(other.components.keys()):
            return False

        # Ensure that all the components have the same type
        for k in self.components:
            if self.components[k] != other.components[k]:
                return False

        # If we got to this point everything checks out
        return True

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        components = list(self.components.keys() if self.components else "[]")
        return "{}({}, pytype={}, components={})".format(type(self).__qualname__,
                                                         self.name,
                                                         self.pytype,
                                                         components)


class StorageClassFactory(metaclass=Singleton):
    """Factory for `StorageClass` instances.

    This class is a singleton, with each instance sharing the pool of
    StorageClasses. Since code can not know whether it is the first
    time the instance has been created, the constructor takes no arguments.
    To populate the factory with storage classes, a call to
    `~StorageClassFactory.addFromConfig()` should be made.

    Parameters
    ----------
    config : `StorageClassConfig` or `str`, optional
        Load configuration. In a ButlerConfig` the relevant configuration
        is located in the ``storageClasses`` section.
    """

    def __init__(self, config=None):
        self._storageClasses = {}
        self._configs = []

        if config is not None:
            self.addFromConfig(config)

    def __contains__(self, storageClassOrName):
        """Indicates whether the storage class exists in the factory.

        Parameters
        ----------
        storageClassOrName : `str` or `StorageClass`
            If `str` is given existence of the named StorageClass
            in the factory is checked. If `StorageClass` is given
            existence and equality are checked.

        Returns
        -------
        in : `bool`
            True if the supplied string is present, or if the supplied
            `StorageClass` is present and identical.
        """
        if isinstance(storageClassOrName, str):
            return storageClassOrName in self._storageClasses
        elif isinstance(storageClassOrName, StorageClass):
            if storageClassOrName.name in self._storageClasses:
                return storageClassOrName == self._storageClasses[storageClassOrName.name]
        return False

    def addFromConfig(self, config):
        """Add more `StorageClass` definitions from a config file.

        Parameters
        ----------
        config : `StorageClassConfig`, `Config` or `str`
            Storage class configuration. Can contain a ``storageClasses``
            key if part of a global configuration.
        """
        sconfig = StorageClassConfig(config)
        self._configs.append(sconfig)

        # Since we can not assume that we will get definitions of
        # components before the definitions of the composites, we create
        # two lists
        composites = {}
        simple = {}
        for name, info in sconfig.items():
            if "components" in info:
                composites[name] = info
            else:
                simple[name] = info

        for name in itertools.chain(simple, composites):
            info = sconfig[name]

            # Always create the storage class so we can ensure that
            # we are not trying to overwrite with a different definition
            components = None
            if "components" in info:
                components = {}
                for cname, ctype in info["components"].items():
                    components[cname] = self.getStorageClass(ctype)

            # Extract scalar items from dict that are needed for StorageClass Constructor
            storageClassKwargs = {k: info[k] for k in ("pytype", "assembler") if k in info}

            # Fill in other items
            storageClassKwargs["components"] = components

            # Create the new storage class and register it
            newStorageClass = StorageClass(name, **storageClassKwargs)
            self.registerStorageClass(newStorageClass)

    def getStorageClass(self, storageClassName):
        """Get a StorageClass instance associated with the supplied name.

        Parameters
        ----------
        storageClassName : `str`
            Name of the storage class to retrieve.

        Returns
        -------
        instance : `StorageClass`
            Instance of the correct `StorageClass`.
        """
        return self._storageClasses[storageClassName]

    def registerStorageClass(self, storageClass):
        """Store the `StorageClass` in the factory.

        Will be indexed by `StorageClass.name` and will return instances
        of the supplied `StorageClass`.

        Parameters
        ----------
        storageClass : `StorageClass`
            Type of the Python `StorageClass` to register.

        Raises
        ------
        ValueError
            If a storage class has already been registered with
            storageClassName and the previous definition differs.
        """
        if storageClass.name in self._storageClasses:
            existing = self.getStorageClass(storageClass.name)
            if existing != storageClass:
                raise ValueError(f"New definition for StorageClass {storageClass.name} differs from current"
                                 " definition")
        else:
            self._storageClasses[storageClass.name] = storageClass

    def unregisterStorageClass(self, storageClassName):
        """Remove the named StorageClass from the factory.

        Parameters
        ----------
        storageClassName : `str`
            Name of storage class to remove.

        Raises
        ------
        KeyError
            The named storage class is not registered.
        """
        del self._storageClasses[storageClassName]
