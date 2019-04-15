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

from .configSupport import processLookupConfigs
from .mappingFactory import MappingFactory
from .utils import getFullTypeName

log = logging.getLogger(__name__)


class Formatter(metaclass=ABCMeta):
    """Interface for reading and writing Datasets with a particular
    `StorageClass`.
    """

    unsupportedParameters = frozenset()
    """Set of parameters not understood by this `Formatter`. An empty set means
    all parameters are supported.  `None` indicates that no parameters
    are supported.
    """

    @classmethod
    def name(cls):
        """Returns the fully qualified name of the formatter.
        """
        return getFullTypeName(cls)

    @abstractmethod
    def read(self, fileDescriptor, component=None):
        """Read a Dataset.

        Parameters
        ----------
        fileDescriptor : `FileDescriptor`
            Identifies the file to read, type to read it into and parameters
            to be used for reading.
        component : `str`, optional
            Component to read from the file. Only used if the `StorageClass`
            for reading differed from the `StorageClass` used to write the
            file.

        Returns
        -------
        inMemoryDataset : `InMemoryDataset`
            The requested Dataset.
        """
        raise NotImplementedError("Type does not support reading")

    @abstractmethod
    def write(self, inMemoryDataset, fileDescriptor):
        """Write a Dataset.

        Parameters
        ----------
        inMemoryDataset : `InMemoryDataset`
            The Dataset to store.
        fileDescriptor : `FileDescriptor`
            Identifies the file to write.

        Returns
        -------
        path : `str`
            The path to where the Dataset was stored.
        """
        raise NotImplementedError("Type does not support writing")

    @abstractmethod
    def predictPath(self, location):
        """Return the path that would be returned by write, without actually
        writing.

        location : `Location`
            The location to simulate writing to.
        """
        raise NotImplementedError("Type does not support writing")

    def segregateParameters(self, parameters):
        """Segregate the supplied parameters into those understood by the
        formatter and those not understood by the formatter.

        Any unsupported parameters are assumed to be usable by associated
        assemblers.

        Parameters
        ----------
        parameters : `dict`
            Parameters with values that have been supplied by the caller
            and which might be relevant for the formatter.

        Returns
        -------
        supported : `dict`
            Those parameters supported by this formatter.
        unsupported : `dict`
            Those parameters not supported by this formatter.
        """

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

    def normalizeDimensions(self, universe):
        """Normalize formatter lookups that use dimensions.

        Parameters
        ----------
        universe : `DimensionUniverse`
            The set of all known dimensions. If `None`, returns without
            action.

        Notes
        -----
        Goes through all registered formatters, and for keys that include
        dimensions, rewrites those keys to use a verified set of
        dimensions.

        Returns without action if the formatter keys have already been
        normalized.

        Raises
        ------
        ValueError
            Raised if a key exists where a dimension is not part of
            the ``universe``.
        """
        return self._mappingFactory.normalizeRegistryDimensions(universe)

    def registerFormatters(self, config, universe=None):
        """Bulk register formatters from a config.

        Parameters
        ----------
        config : `Config`
            ``formatters`` section of a configuration.
        universe : `DimensionUniverse`, optional
            The set of all known dimensions. If not `None`, any look up keys
            involving dimensions will be normalized.  The normalization flag
            will be cleared each time this method is called without a
            universe.

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
        contents = processLookupConfigs(config)
        for key, f in contents.items():
            self.registerFormatter(key, f)

        if universe is not None:
            self.normalizeDimensions(universe)
        else:
            # Trigger new normalization round since new formatters have
            # been added without a universe.
            self._mappingFactory.normalized = False

    def getLookupKeys(self):
        """Retrieve the look up keys for all the registry entries.

        Returns
        -------
        keys : `set` of `LookupKey`
            The keys available for matching in the registry.
        """
        return self._mappingFactory.getLookupKeys()

    def getFormatterWithMatch(self, entity):
        """Get a new formatter instance along with the matching registry
        key.

        Parameters
        ----------
        entity : `DatasetRef`, `DatasetType` or `StorageClass`, or `str`
            Entity to use to determine the formatter to return.
            `StorageClass` will be used as a last resort if `DatasetRef`
            or `DatasetType` instance is provided.  Supports instrument
            override if a `DatasetRef` is provided configured with an
            ``instrument`` value for the data ID.

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
            # Normalize the registry to a universe if not already done
            if not self._mappingFactory.normalized:
                try:
                    universe = entity.dimensions.universe
                except AttributeError:
                    pass
                else:
                    self._mappingFactory.normalizeRegistryDimensions(universe)

            names = entity._lookupNames()
        matchKey, formatter = self._mappingFactory.getFromRegistryWithMatch(*names)
        log.debug("Retrieved formatter from key '%s' for entity '%s'", matchKey, entity)

        return matchKey, formatter

    def getFormatter(self, entity):
        """Get a new formatter instance.

        Parameters
        ----------
        entity : `DatasetRef`, `DatasetType` or `StorageClass`, or `str`
            Entity to use to determine the formatter to return.
            `StorageClass` will be used as a last resort if `DatasetRef`
            or `DatasetType` instance is provided.  Supports instrument
            override if a `DatasetRef` is provided configured with an
            ``instrument`` value for the data ID.

        Returns
        -------
        formatter : `Formatter`
            An instance of the registered formatter.
        """
        _, formatter = self.getFormatterWithMatch(entity)
        return formatter

    def registerFormatter(self, type_, formatter):
        """Register a `Formatter`.

        Parameters
        ----------
        type_ : `LookupKey`, `str` or `StorageClass` or `DatasetType`
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
