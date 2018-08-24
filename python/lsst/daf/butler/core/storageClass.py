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
import logging

from .utils import doImport, Singleton, getFullTypeName
from .assembler import CompositeAssembler
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
    _cls_name = "BaseStorageClass"
    _cls_components = None
    _cls_assembler = None
    _cls_pytype = None
    defaultAssembler = CompositeAssembler
    defaultAssemblerName = getFullTypeName(defaultAssembler)

    def __init__(self, name=None, pytype=None, components=None, assembler=None):
        if name is None:
            name = self._cls_name
        if pytype is None:
            pytype = self._cls_pytype
        if components is None:
            components = self._cls_components
        if assembler is None:
            assembler = self._cls_assembler
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

        if not isinstance(other, StorageClass):
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
        return "{}({}, pytype={}, assembler={}, components={})".format(type(self).__qualname__,
                                                                       self.name,
                                                                       self._pytypeName,
                                                                       self._assemblerClassName,
                                                                       list(self.components.keys()))


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

        Notes
        -----
        The two different checks (one for "key" and one for "value") based on
        the type of the given argument mean that it is possible for
        StorageClass.name to be in the factory but StorageClass to not be
        in the factory.
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
        # components or parents before their classes are defined
        # we have a helper function that we can call recursively
        # to extract definitions from the configuration.
        def processStorageClass(name, sconfig):
            # Maybe we've already processed this through recursion
            if name not in sconfig:
                return
            info = sconfig.pop(name)

            # Always create the storage class so we can ensure that
            # we are not trying to overwrite with a different definition
            components = None
            if "components" in info:
                components = {}
                for cname, ctype in info["components"].items():
                    if ctype not in self:
                        processStorageClass(ctype, sconfig)
                    components[cname] = self.getStorageClass(ctype)

            # Extract scalar items from dict that are needed for StorageClass Constructor
            storageClassKwargs = {k: info[k] for k in ("pytype", "assembler") if k in info}

            # Fill in other items
            storageClassKwargs["components"] = components

            # Create the new storage class and register it
            baseClass = StorageClass
            if "inheritsFrom" in info:
                baseName = info["inheritsFrom"]
                if baseName not in self:
                    processStorageClass(baseName, sconfig)
                baseClass = type(self.getStorageClass(baseName))

            newStorageClassType = self.makeNewStorageClass(name, baseClass, **storageClassKwargs)
            newStorageClass = newStorageClassType()
            print("Registering {}".format(newStorageClass))
            self.registerStorageClass(newStorageClass)

        for name in list(sconfig.keys()):
            processStorageClass(name, sconfig)

    @staticmethod
    def makeNewStorageClass(name, baseClass, **kwargs):
        """Create a new Python class as a subclass of `StorageClass`.

        Parameters
        ----------
        name : `str`
            Name to use for this class.
        baseClass : `type`
            Base class for this `StorageClass`.

        Returns
        -------
        newtype : `type` subclass of `StorageClass`
            Newly created Python type.
        """

        # convert the arguments to use different internal names
        clsargs = {f"_cls_{k}": v for k, v in kwargs.items() if v is not None}
        clsargs["_cls_name"] = name

        return type(f"StorageClass{name}", (baseClass,), clsargs)

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

        Raises
        ------
        KeyError
            The requested storage class name is not registered.
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
                raise ValueError(f"New definition for StorageClass {storageClass.name} ({storageClass}) "
                                 f"differs from current definition ({existing})")
        else:
            self._storageClasses[storageClass.name] = storageClass

    def _unregisterStorageClass(self, storageClassName):
        """Remove the named StorageClass from the factory.

        Parameters
        ----------
        storageClassName : `str`
            Name of storage class to remove.

        Raises
        ------
        KeyError
            The named storage class is not registered.

        Notes
        -----
        This method is intended to simplify testing of StorageClassFactory
        functionality and it is not expected to be required for normal usage.
        """
        del self._storageClasses[storageClassName]
