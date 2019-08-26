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

__all__ = ("Formatter", "FormatterFactory")

from abc import ABCMeta, abstractmethod
import logging
from typing import ClassVar, Set, FrozenSet, Union, Optional, Dict, Any, Tuple, Type

from .configSupport import processLookupConfigs, LookupKey
from .mappingFactory import MappingFactory
from .utils import getFullTypeName
from .fileDescriptor import FileDescriptor
from .location import Location
from .config import Config
from .dimensions import DimensionUniverse
from .storageClass import StorageClass
from .datasets import DatasetType, DatasetRef

log = logging.getLogger(__name__)

# Define a new special type for functions that take "entity"
Entity = Union[DatasetType, DatasetRef, StorageClass, str]


class Formatter(metaclass=ABCMeta):
    """Interface for reading and writing Datasets with a particular
    `StorageClass`.

    Parameters
    ----------
    fileDescriptor : `FileDescriptor`, optional
        Identifies the file to read or write, and the associated storage
        classes and parameter information.  Its value can be `None` if the
        caller will never call `Formatter.read` or `Formatter.write`.
    """

    unsupportedParameters: ClassVar[Optional[Union[FrozenSet[str], Set[str]]]] = frozenset()
    """Set of parameters not understood by this `Formatter`. An empty set means
    all parameters are supported.  `None` indicates that no parameters
    are supported (`frozenset`).
    """

    def __init__(self, fileDescriptor: FileDescriptor):
        if not isinstance(fileDescriptor, FileDescriptor):
            raise TypeError("File descriptor must be a FileDescriptor")
        self._fileDescriptor = fileDescriptor

    def __str__(self):
        return f"{self.name()}@{self.fileDescriptor.location.path}"

    def __repr__(self):
        return f"{self.name()}({self.fileDescriptor!r})"

    @property
    def fileDescriptor(self) -> FileDescriptor:
        """FileDescriptor associated with this formatter
        (`FileDescriptor`, read-only)"""
        return self._fileDescriptor

    @classmethod
    def name(cls) -> str:
        """Returns the fully qualified name of the formatter.

        Returns
        -------
        name : `str`
            Fully-qualified name of formatter class.
        """
        return getFullTypeName(cls)

    @abstractmethod
    def read(self, component: Optional[str] = None) -> object:
        """Read a Dataset.

        Parameters
        ----------
        component : `str`, optional
            Component to read from the file. Only used if the `StorageClass`
            for reading differed from the `StorageClass` used to write the
            file.

        Returns
        -------
        inMemoryDataset : `object`
            The requested Dataset.
        """
        raise NotImplementedError("Type does not support reading")

    @abstractmethod
    def write(self, inMemoryDataset: Any) -> str:
        """Write a Dataset.

        Parameters
        ----------
        inMemoryDataset : `object`
            The Dataset to store.

        Returns
        -------
        path : `str`
            The path to where the Dataset was stored within the datastore.
        """
        raise NotImplementedError("Type does not support writing")

    def fromBytes(self, serializedDataset: bytes,
                  component: Optional[str] = None) -> object:
        """Reads serialized data into a Dataset or its component.

        Parameters
        ----------
        serializedDataset : `bytes`
            Bytes object to unserialize.
        component : `str`, optional
            Component to read from the Dataset. Only used if the `StorageClass`
            for reading differed from the `StorageClass` used to write the
            file.

        Returns
        -------
        inMemoryDataset : `object`
            The requested data as a Python object. The type of object
            is controlled by the specific formatter.
        """
        raise NotImplementedError("Type does not support reading from bytes.")

    def toBytes(self, inMemoryDataset: Any) -> bytes:
        """Serialize the Dataset to bytes based on formatter.

        Parameters
        ----------
        inMemoryDataset : `object`
            The Python object to serialize.

        Returns
        -------
        serializedDataset : `bytes`
            Bytes representing the serialized dataset.
        """
        raise NotImplementedError("Type does not support writing to bytes.")

    @classmethod
    @abstractmethod
    def predictPathFromLocation(cls, location: Location) -> str:
        """Return the path that would be returned by write, without actually
        writing.

        Parameters
        ----------
        location : `Location`
            Location of file for which path prediction is required.

        Returns
        -------
        path : `str`
            Path within datastore that would be associated with this location.
        """
        raise NotImplementedError("Type does not support writing")

    def predictPath(self) -> str:
        """Return the path that would be returned by write, without actually
        writing.

        Uses the `FileDescriptor` associated with the instance.

        Returns
        -------
        path : `str`
            Path within datastore that would be associated with the location
            stored in this `Formatter`.
        """
        return self.predictPathFromLocation(self.fileDescriptor.location)

    def segregateParameters(self, parameters: Optional[Dict[str, Any]] = None) -> Tuple[Dict, Dict]:
        """Segregate the supplied parameters into those understood by the
        formatter and those not understood by the formatter.

        Any unsupported parameters are assumed to be usable by associated
        assemblers.

        Parameters
        ----------
        parameters : `dict`, optional
            Parameters with values that have been supplied by the caller
            and which might be relevant for the formatter.  If `None`
            parameters will be read from the registered `FileDescriptor`.

        Returns
        -------
        supported : `dict`
            Those parameters supported by this formatter.
        unsupported : `dict`
            Those parameters not supported by this formatter.
        """

        if parameters is None:
            parameters = self.fileDescriptor.parameters

        if parameters is None:
            return {}, {}

        if self.unsupportedParameters is None:
            # Support none of the parameters
            return {}, parameters.copy()

        # Start by assuming all are supported
        supported = parameters.copy()
        unsupported = {}

        # And remove any we know are not supported
        for p in set(supported):
            if p in self.unsupportedParameters:
                unsupported[p] = supported.pop(p)

        return supported, unsupported


class FormatterFactory:
    """Factory for `Formatter` instances.
    """

    def __init__(self):
        self._mappingFactory = MappingFactory(Formatter)

    def __contains__(self, key):
        """Indicates whether the supplied key is present in the factory.

        Parameters
        ----------
        key : `LookupKey`, `str` or objects with ``name`` attribute
            Key to use to lookup in the factory whether a corresponding
            formatter is present.

        Returns
        -------
        in : `bool`
            `True` if the supplied key is present in the factory.
        """
        return key in self._mappingFactory

    def registerFormatters(self, config: Config, *, universe: DimensionUniverse) -> None:
        """Bulk register formatters from a config.

        Parameters
        ----------
        config : `Config`
            ``formatters`` section of a configuration.
        universe : `DimensionUniverse`, optional
            Set of all known dimensions, used to expand and validate any used
            in lookup keys.

        Notes
        -----
        The configuration can include one level of hierarchy where an
        instrument-specific section can be defined to override more general
        template specifications.  This is represented in YAML using a
        key of form ``instrument<name>`` which can then define templates
        that will be returned if a `DatasetRef` contains a matching instrument
        name in the data ID.

        The config is parsed using the function
        `~lsst.daf.butler.configSubset.processLookupConfigs`.
        """
        contents = processLookupConfigs(config, universe=universe)
        for key, f in contents.items():
            self.registerFormatter(key, f)

    def getLookupKeys(self) -> Set[LookupKey]:
        """Retrieve the look up keys for all the registry entries.

        Returns
        -------
        keys : `set` of `LookupKey`
            The keys available for matching in the registry.
        """
        return self._mappingFactory.getLookupKeys()

    def getFormatterClassWithMatch(self, entity: Entity) -> Tuple[LookupKey, Type]:
        """Get the matching formatter class along with the matching registry
        key.

        Parameters
        ----------
        entity : `DatasetRef`, `DatasetType`, `StorageClass`, or `str`
            Entity to use to determine the formatter to return.
            `StorageClass` will be used as a last resort if `DatasetRef`
            or `DatasetType` instance is provided.  Supports instrument
            override if a `DatasetRef` is provided configured with an
            ``instrument`` value for the data ID.

        Returns
        -------
        matchKey : `LookupKey`
            The key that resulted in the successful match.
        formatter : `type`
            The class of the registered formatter.
        """
        if isinstance(entity, str):
            names = (entity,)
        else:
            names = entity._lookupNames()
        matchKey, formatter = self._mappingFactory.getClassFromRegistryWithMatch(names)
        log.debug("Retrieved formatter %s from key '%s' for entity '%s'", getFullTypeName(formatter),
                  matchKey, entity)

        return matchKey, formatter

    def getFormatterClass(self, entity: Entity) -> Type:
        """Get the matching formatter class.

        Parameters
        ----------
        entity : `DatasetRef`, `DatasetType`, `StorageClass`, or `str`
            Entity to use to determine the formatter to return.
            `StorageClass` will be used as a last resort if `DatasetRef`
            or `DatasetType` instance is provided.  Supports instrument
            override if a `DatasetRef` is provided configured with an
            ``instrument`` value for the data ID.

        Returns
        -------
        formatter : `type`
            The class of the registered formatter.
        """
        _, formatter = self.getFormatterClassWithMatch(entity)
        return formatter

    def getFormatterWithMatch(self, entity: Entity, *args, **kwargs) -> Tuple[LookupKey, Formatter]:
        """Get a new formatter instance along with the matching registry
        key.

        Parameters
        ----------
        entity : `DatasetRef`, `DatasetType`, `StorageClass`, or `str`
            Entity to use to determine the formatter to return.
            `StorageClass` will be used as a last resort if `DatasetRef`
            or `DatasetType` instance is provided.  Supports instrument
            override if a `DatasetRef` is provided configured with an
            ``instrument`` value for the data ID.
        args : `tuple`
            Positional arguments to use pass to the object constructor.
        kwargs : `dict`
            Keyword arguments to pass to object constructor.

        Returns
        -------
        matchKey : `LookupKey`
            The key that resulted in the successful match.
        formatter : `Formatter`
            An instance of the registered formatter.
        """
        if isinstance(entity, str):
            names = (entity,)
        else:
            names = entity._lookupNames()
        matchKey, formatter = self._mappingFactory.getFromRegistryWithMatch(names, *args, **kwargs)
        log.debug("Retrieved formatter %s from key '%s' for entity '%s'", getFullTypeName(formatter),
                  matchKey, entity)

        return matchKey, formatter

    def getFormatter(self, entity: Entity, *args, **kwargs) -> Formatter:
        """Get a new formatter instance.

        Parameters
        ----------
        entity : `DatasetRef`, `DatasetType`, `StorageClass`, or `str`
            Entity to use to determine the formatter to return.
            `StorageClass` will be used as a last resort if `DatasetRef`
            or `DatasetType` instance is provided.  Supports instrument
            override if a `DatasetRef` is provided configured with an
            ``instrument`` value for the data ID.
        args : `tuple`
            Positional arguments to use pass to the object constructor.
        kwargs : `dict`
            Keyword arguments to pass to object constructor.

        Returns
        -------
        formatter : `Formatter`
            An instance of the registered formatter.
        """
        _, formatter = self.getFormatterWithMatch(entity, *args, **kwargs)
        return formatter

    def registerFormatter(self, type_: Union[LookupKey, str, StorageClass, DatasetType],
                          formatter: str) -> None:
        """Register a `Formatter`.

        Parameters
        ----------
        type_ : `LookupKey`, `str`, `StorageClass` or `DatasetType`
            Type for which this formatter is to be used.  If a `LookupKey`
            is not provided, one will be constructed from the supplied string
            or by using the ``name`` property of the supplied entity.
        formatter : `str`
            Identifies a `Formatter` subclass to use for reading and writing
            Datasets of this type.

        Raises
        ------
        ValueError
            Raised if the formatter does not name a valid formatter type.
        """
        self._mappingFactory.placeInRegistry(type_, formatter)
