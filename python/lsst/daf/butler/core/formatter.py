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

from __future__ import annotations

__all__ = ("Formatter", "FormatterFactory", "FormatterParameter")

import contextlib
import copy
import logging
from abc import ABCMeta, abstractmethod
from collections.abc import Mapping
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    ClassVar,
    Dict,
    Iterator,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

from lsst.utils.introspection import get_full_type_name

from .config import Config
from .configSupport import LookupKey, processLookupConfigs
from .datasets import DatasetRef, DatasetType
from .dimensions import DimensionUniverse
from .fileDescriptor import FileDescriptor
from .location import Location
from .mappingFactory import MappingFactory
from .storageClass import StorageClass

log = logging.getLogger(__name__)

# Define a new special type for functions that take "entity"
Entity = Union[DatasetType, DatasetRef, StorageClass, str]


if TYPE_CHECKING:
    from .dimensions import DataCoordinate


class Formatter(metaclass=ABCMeta):
    """Interface for reading and writing Datasets.

    The formatters are associated with a particular `StorageClass`.

    Parameters
    ----------
    fileDescriptor : `FileDescriptor`, optional
        Identifies the file to read or write, and the associated storage
        classes and parameter information.  Its value can be `None` if the
        caller will never call `Formatter.read` or `Formatter.write`.
    dataId : `DataCoordinate`
        Data ID associated with this formatter.
    writeParameters : `dict`, optional
        Any parameters to be hard-coded into this instance to control how
        the dataset is serialized.
    writeRecipes : `dict`, optional
        Detailed write Recipes indexed by recipe name.

    Notes
    -----
    All Formatter subclasses should share the base class's constructor
    signature.
    """

    unsupportedParameters: ClassVar[Optional[AbstractSet[str]]] = frozenset()
    """Set of read parameters not understood by this `Formatter`. An empty set
    means all parameters are supported.  `None` indicates that no parameters
    are supported. These param (`frozenset`).
    """

    supportedWriteParameters: ClassVar[Optional[AbstractSet[str]]] = None
    """Parameters understood by this formatter that can be used to control
    how a dataset is serialized. `None` indicates that no parameters are
    supported."""

    supportedExtensions: ClassVar[AbstractSet[str]] = frozenset()
    """Set of all extensions supported by this formatter.

    Only expected to be populated by Formatters that write files. Any extension
    assigned to the ``extension`` property will be automatically included in
    the list of supported extensions."""

    def __init__(
        self,
        fileDescriptor: FileDescriptor,
        dataId: DataCoordinate,
        writeParameters: Optional[Dict[str, Any]] = None,
        writeRecipes: Optional[Dict[str, Any]] = None,
    ):
        if not isinstance(fileDescriptor, FileDescriptor):
            raise TypeError("File descriptor must be a FileDescriptor")
        assert dataId is not None, "dataId is now required for formatter initialization"
        self._fileDescriptor = fileDescriptor
        self._dataId = dataId

        # Check that the write parameters are allowed
        if writeParameters:
            if self.supportedWriteParameters is None:
                raise ValueError(
                    f"This formatter does not accept any write parameters. Got: {', '.join(writeParameters)}"
                )
            else:
                given = set(writeParameters)
                unknown = given - self.supportedWriteParameters
                if unknown:
                    s = "s" if len(unknown) != 1 else ""
                    unknownStr = ", ".join(f"'{u}'" for u in unknown)
                    raise ValueError(f"This formatter does not accept parameter{s} {unknownStr}")

        self._writeParameters = writeParameters
        self._writeRecipes = self.validateWriteRecipes(writeRecipes)

    def __str__(self) -> str:
        return f"{self.name()}@{self.fileDescriptor.location.path}"

    def __repr__(self) -> str:
        return f"{self.name()}({self.fileDescriptor!r})"

    @property
    def fileDescriptor(self) -> FileDescriptor:
        """File descriptor associated with this formatter (`FileDescriptor`).

        Read-only property.
        """
        return self._fileDescriptor

    @property
    def dataId(self) -> DataCoordinate:
        """Return Data ID associated with this formatter (`DataCoordinate`)."""
        return self._dataId

    @property
    def writeParameters(self) -> Mapping[str, Any]:
        """Parameters to use when writing out datasets."""
        if self._writeParameters is not None:
            return self._writeParameters
        return {}

    @property
    def writeRecipes(self) -> Mapping[str, Any]:
        """Detailed write Recipes indexed by recipe name."""
        if self._writeRecipes is not None:
            return self._writeRecipes
        return {}

    @classmethod
    def validateWriteRecipes(cls, recipes: Optional[Mapping[str, Any]]) -> Optional[Mapping[str, Any]]:
        """Validate supplied recipes for this formatter.

        The recipes are supplemented with default values where appropriate.

        Parameters
        ----------
        recipes : `dict`
            Recipes to validate.

        Returns
        -------
        validated : `dict`
            Validated recipes.

        Raises
        ------
        RuntimeError
            Raised if validation fails.  The default implementation raises
            if any recipes are given.
        """
        if recipes:
            raise RuntimeError(f"This formatter does not understand these writeRecipes: {recipes}")
        return recipes

    @classmethod
    def name(cls) -> str:
        """Return the fully qualified name of the formatter.

        Returns
        -------
        name : `str`
            Fully-qualified name of formatter class.
        """
        return get_full_type_name(cls)

    @abstractmethod
    def read(self, component: Optional[str] = None) -> Any:
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
    def write(self, inMemoryDataset: Any) -> None:
        """Write a Dataset.

        Parameters
        ----------
        inMemoryDataset : `object`
            The Dataset to store.
        """
        raise NotImplementedError("Type does not support writing")

    @classmethod
    def can_read_bytes(cls) -> bool:
        """Indicate if this formatter can format from bytes.

        Returns
        -------
        can : `bool`
            `True` if the `fromBytes` method is implemented.
        """
        # We have no property to read so instead try to format from a byte
        # and see what happens
        try:
            # We know the arguments are incompatible
            cls.fromBytes(cls, b"")  # type: ignore
        except NotImplementedError:
            return False
        except Exception:
            # There will be problems with the bytes we are supplying so ignore
            pass
        return True

    def fromBytes(self, serializedDataset: bytes, component: Optional[str] = None) -> object:
        """Read serialized data into a Dataset or its component.

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

    @contextlib.contextmanager
    def _updateLocation(self, location: Optional[Location]) -> Iterator[Location]:
        """Temporarily replace the location associated with this formatter.

        Parameters
        ----------
        location : `Location`
            New location to use for this formatter. If `None` the
            formatter will not change but it will still return
            the old location. This allows it to be used in a code
            path where the location may not need to be updated
            but the with block is still convenient.

        Yields
        ------
        old : `Location`
            The old location that will be restored.

        Notes
        -----
        This is an internal method that should be used with care.
        It may change in the future. Should be used as a context
        manager to restore the location when the temporary is no
        longer required.
        """
        old = self._fileDescriptor.location
        try:
            if location is not None:
                self._fileDescriptor.location = location
            yield old
        finally:
            if location is not None:
                self._fileDescriptor.location = old

    def makeUpdatedLocation(self, location: Location) -> Location:
        """Return a new `Location` updated with this formatter's extension.

        Parameters
        ----------
        location : `Location`
            The location to update.

        Returns
        -------
        updated : `Location`
            A new `Location` with a new file extension applied.

        Raises
        ------
        NotImplementedError
            Raised if there is no ``extension`` attribute associated with
            this formatter.

        Notes
        -----
        This method is available to all Formatters but might not be
        implemented by all formatters. It requires that a formatter set
        an ``extension`` attribute containing the file extension used when
        writing files.  If ``extension`` is `None` the supplied file will
        not be updated. Not all formatters write files so this is not
        defined in the base class.
        """
        location = copy.deepcopy(location)
        try:
            # We are deliberately allowing extension to be undefined by
            # default in the base class and mypy complains.
            location.updateExtension(self.extension)  # type:ignore
        except AttributeError:
            raise NotImplementedError("No file extension registered with this formatter") from None
        return location

    @classmethod
    def validateExtension(cls, location: Location) -> None:
        """Check the extension of the provided location for compatibility.

        Parameters
        ----------
        location : `Location`
            Location from which to extract a file extension.

        Raises
        ------
        NotImplementedError
            Raised if file extensions are a concept not understood by this
            formatter.
        ValueError
            Raised if the formatter does not understand this extension.

        Notes
        -----
        This method is available to all Formatters but might not be
        implemented by all formatters. It requires that a formatter set
        an ``extension`` attribute containing the file extension used when
        writing files.  If ``extension`` is `None` only the set of supported
        extensions will be examined.
        """
        supported = set(cls.supportedExtensions)

        try:
            # We are deliberately allowing extension to be undefined by
            # default in the base class and mypy complains.
            default = cls.extension  # type: ignore
        except AttributeError:
            raise NotImplementedError("No file extension registered with this formatter") from None

        # If extension is implemented as an instance property it won't return
        # a string when called as a class property. Assume that
        # the supported extensions class property is complete.
        if default is not None and isinstance(default, str):
            supported.add(default)

        # Get the file name from the uri
        file = location.uri.basename()

        # Check that this file name ends with one of the supported extensions.
        # This is less prone to confusion than asking the location for
        # its extension and then doing a set comparison
        for ext in supported:
            if file.endswith(ext):
                return

        raise ValueError(
            f"Extension '{location.getExtension()}' on '{location}' "
            f"is not supported by Formatter '{cls.__name__}' (supports: {supported})"
        )

    def predictPath(self) -> str:
        """Return the path that would be returned by write.

        Does not write any data file.

        Uses the `FileDescriptor` associated with the instance.

        Returns
        -------
        path : `str`
            Path within datastore that would be associated with the location
            stored in this `Formatter`.
        """
        updated = self.makeUpdatedLocation(self.fileDescriptor.location)
        return updated.pathInStore.path

    def segregateParameters(self, parameters: Optional[Dict[str, Any]] = None) -> Tuple[Dict, Dict]:
        """Segregate the supplied parameters.

        This splits the parameters into those understood by the
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
    """Factory for `Formatter` instances."""

    defaultKey = LookupKey("default")
    """Configuration key associated with default write parameter settings."""

    writeRecipesKey = LookupKey("write_recipes")
    """Configuration key associated with write recipes."""

    def __init__(self) -> None:
        self._mappingFactory = MappingFactory(Formatter)

    def __contains__(self, key: Union[LookupKey, str]) -> bool:
        """Indicate whether the supplied key is present in the factory.

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

        The values for formatter entries can be either a simple string
        referring to a python type or a dict representing the formatter and
        parameters to be hard-coded into the formatter constructor. For
        the dict case the following keys are supported:

        - formatter: The python type to be used as the formatter class.
        - parameters: A further dict to be passed directly to the
            ``writeParameters`` Formatter constructor to seed it.
            These parameters are validated at instance creation and not at
            configuration.

        Additionally, a special ``default`` section can be defined that
        uses the formatter type (class) name as the keys and specifies
        default write parameters that should be used whenever an instance
        of that class is constructed.

        .. code-block:: yaml

           formatters:
             default:
               lsst.daf.butler.formatters.example.ExampleFormatter:
                 max: 10
                 min: 2
                 comment: Default comment
             calexp: lsst.daf.butler.formatters.example.ExampleFormatter
             coadd:
               formatter: lsst.daf.butler.formatters.example.ExampleFormatter
               parameters:
                 max: 5

        Any time an ``ExampleFormatter`` is constructed it will use those
        parameters. If an explicit entry later in the configuration specifies
        a different set of parameters, the two will be merged with the later
        entry taking priority.  In the example above ``calexp`` will use
        the default parameters but ``coadd`` will override the value for
        ``max``.

        Formatter configuration can also include a special section describing
        collections of write parameters that can be accessed through a
        simple label.  This allows common collections of options to be
        specified in one place in the configuration and reused later.
        The ``write_recipes`` section is indexed by Formatter class name
        and each key is the label to associate with the parameters.

        .. code-block:: yaml

           formatters:
             write_recipes:
               lsst.obs.base.formatters.fitsExposure.FixExposureFormatter:
                 lossless:
                   ...
                 noCompression:
                   ...

        By convention a formatter that uses write recipes will support a
        ``recipe`` write parameter that will refer to a recipe name in
        the ``write_recipes`` component.  The `Formatter` will be constructed
        in the `FormatterFactory` with all the relevant recipes and
        will not attempt to filter by looking at ``writeParameters`` in
        advance.  See the specific formatter documentation for details on
        acceptable recipe options.
        """
        allowed_keys = {"formatter", "parameters"}

        contents = processLookupConfigs(config, allow_hierarchy=True, universe=universe)

        # Extract any default parameter settings
        defaultParameters = contents.get(self.defaultKey, {})
        if not isinstance(defaultParameters, Mapping):
            raise RuntimeError(
                "Default formatter parameters in config can not be a single string"
                f" (got: {type(defaultParameters)})"
            )

        # Extract any global write recipes -- these are indexed by
        # Formatter class name.
        writeRecipes = contents.get(self.writeRecipesKey, {})
        if isinstance(writeRecipes, str):
            raise RuntimeError(
                f"The formatters.{self.writeRecipesKey} section must refer to a dict not '{writeRecipes}'"
            )

        for key, f in contents.items():
            # default is handled in a special way
            if key == self.defaultKey:
                continue
            if key == self.writeRecipesKey:
                continue

            # Can be a str or a dict.
            specificWriteParameters = {}
            if isinstance(f, str):
                formatter = f
            elif isinstance(f, Mapping):
                all_keys = set(f)
                unexpected_keys = all_keys - allowed_keys
                if unexpected_keys:
                    raise ValueError(f"Formatter {key} uses unexpected keys {unexpected_keys} in config")
                if "formatter" not in f:
                    raise ValueError(f"Mandatory 'formatter' key missing for formatter key {key}")
                formatter = f["formatter"]
                if "parameters" in f:
                    specificWriteParameters = f["parameters"]
            else:
                raise ValueError(f"Formatter for key {key} has unexpected value: '{f}'")

            # Apply any default parameters for this formatter
            writeParameters = copy.deepcopy(defaultParameters.get(formatter, {}))
            writeParameters.update(specificWriteParameters)

            kwargs: Dict[str, Any] = {}
            if writeParameters:
                kwargs["writeParameters"] = writeParameters

            if formatter in writeRecipes:
                kwargs["writeRecipes"] = writeRecipes[formatter]

            self.registerFormatter(key, formatter, **kwargs)

    def getLookupKeys(self) -> Set[LookupKey]:
        """Retrieve the look up keys for all the registry entries.

        Returns
        -------
        keys : `set` of `LookupKey`
            The keys available for matching in the registry.
        """
        return self._mappingFactory.getLookupKeys()

    def getFormatterClassWithMatch(self, entity: Entity) -> Tuple[LookupKey, Type[Formatter], Dict[str, Any]]:
        """Get the matching formatter class along with the registry key.

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
        formatter_kwargs : `dict`
            Keyword arguments that are associated with this formatter entry.
        """
        names = (LookupKey(name=entity),) if isinstance(entity, str) else entity._lookupNames()
        matchKey, formatter, formatter_kwargs = self._mappingFactory.getClassFromRegistryWithMatch(names)
        log.debug(
            "Retrieved formatter %s from key '%s' for entity '%s'",
            get_full_type_name(formatter),
            matchKey,
            entity,
        )

        return matchKey, formatter, formatter_kwargs

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
        _, formatter, _ = self.getFormatterClassWithMatch(entity)
        return formatter

    def getFormatterWithMatch(self, entity: Entity, *args: Any, **kwargs: Any) -> Tuple[LookupKey, Formatter]:
        """Get a new formatter instance along with the matching registry key.

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
        **kwargs
            Keyword arguments to pass to object constructor.

        Returns
        -------
        matchKey : `LookupKey`
            The key that resulted in the successful match.
        formatter : `Formatter`
            An instance of the registered formatter.
        """
        names = (LookupKey(name=entity),) if isinstance(entity, str) else entity._lookupNames()
        matchKey, formatter = self._mappingFactory.getFromRegistryWithMatch(names, *args, **kwargs)
        log.debug(
            "Retrieved formatter %s from key '%s' for entity '%s'",
            get_full_type_name(formatter),
            matchKey,
            entity,
        )

        return matchKey, formatter

    def getFormatter(self, entity: Entity, *args: Any, **kwargs: Any) -> Formatter:
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
        **kwargs
            Keyword arguments to pass to object constructor.

        Returns
        -------
        formatter : `Formatter`
            An instance of the registered formatter.
        """
        _, formatter = self.getFormatterWithMatch(entity, *args, **kwargs)
        return formatter

    def registerFormatter(
        self,
        type_: Union[LookupKey, str, StorageClass, DatasetType],
        formatter: str,
        *,
        overwrite: bool = False,
        **kwargs: Any,
    ) -> None:
        """Register a `Formatter`.

        Parameters
        ----------
        type_ : `LookupKey`, `str`, `StorageClass` or `DatasetType`
            Type for which this formatter is to be used.  If a `LookupKey`
            is not provided, one will be constructed from the supplied string
            or by using the ``name`` property of the supplied entity.
        formatter : `str` or class of type `Formatter`
            Identifies a `Formatter` subclass to use for reading and writing
            Datasets of this type.  Can be a `Formatter` class.
        overwrite : `bool`, optional
            If `True` an existing entry will be replaced by the new value.
            Default is `False`.
        **kwargs
            Keyword arguments to always pass to object constructor when
            retrieved.

        Raises
        ------
        ValueError
            Raised if the formatter does not name a valid formatter type and
            ``overwrite`` is `False`.
        """
        self._mappingFactory.placeInRegistry(type_, formatter, overwrite=overwrite, **kwargs)


# Type to use when allowing a Formatter or its class name
FormatterParameter = Union[str, Type[Formatter], Formatter]
