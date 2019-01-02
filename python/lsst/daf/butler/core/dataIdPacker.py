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

__all__ = ("DataIdPacker", "DataIdPackerDimensions")

from abc import ABCMeta, abstractmethod
from collections import defaultdict
from lsst.utils import doImport

from .dimensions import toNameSet, DimensionKeyDict


class DataIdPackerDimensions:
    """A helper class that holds the three categories of dimensions associated
    with `DataIdPacker` objects.

    Parameters
    ----------
    given : `DimensionSet`
        The dimensions that must be provided to construct a `DataIdPacker`.
        The packed ID values are only unique and reversible with these
        dimensions held fixed.
    covered : `DimensionSet`
        The dimensions that are packed into the ID.
    required : `DimensionSet`
        The dimensions that must be present in a `DataId` for it to be packed.
        Note that a `DataIdPacker` may cover a dimension without requiring it,
        but given dimensions are always required.
    """

    @classmethod
    def fromConfig(cls, universe, config):
        """Construct from a `Config` subset.

        Parameters
        ----------
        universe : `DimensionGraph`
            A graph containing all known `Dimension` instances.
        config : `Config`
            A config with "given", "covered", and "required" keys that map
            to lists of `Dimension` names.
        """
        return cls(
            given=universe.toSet().intersection(config["given"]),
            covered=universe.toSet().intersection(config["covered"]),
            required=universe.toSet().intersection(config["required"])
        )

    def __init__(self, given, covered, required):
        self._given = given
        self._covered = covered
        self._required = required
        if not self.given.issubset(self.required):
            raise ValueError(f"Given dimensions ({self.given}) are always required (got {self.required}).")

    @property
    def universe(self):
        """A graph containing all known dimensions `(DimensionGraph`).
        """
        return self._given.universe

    @property
    def given(self):
        """The dimensions that must be provided to construct a `DataIdPacker`
        (`DimensionSet`)

        The packed ID values are only unique and reversible with these
        dimensions held fixed.
        """
        return self._given

    @property
    def covered(self):
        """The dimensions that are packed into the ID (`DimensionSet`).
        """
        return self._covered

    @property
    def required(self):
        """The dimensions that must be present in a `DataId` for it to be
        packed (`DimensionSet`).

        Note that a `DataIdPacker` may cover a dimension without requiring it,
        but "given" dimensions are always required.
        """
        return self._required


class DataIdPacker(metaclass=ABCMeta):
    """An abstract base class for a bidirectional mappings between a `DataId`
    and a packed integer or `bytes` blob.

    Derived class constructors must accept at least the positional argments
    required by the base class contructor, but may accept additional keyword
    subclass-specific arguments as well (see `configure`).

    Parameters
    ----------
    dimensions : `DataIdPackerDimensions`
        Struct containing dimensions related to this `DataIdPacker`.
    dataId : `DataId`
        `DataId` that uniquely identifies (at least) the given dimensions
        for this `DataIdPacker`.  Note that this must be a true `DataId`, not
        a dict or other mapping.
    kwds
        Additional subclass-specific keyword arguments.

    Notes
    -----
    `DataIdPacker` subclass instances should generally be obtained from a
    `Registry`.  This involves:

     - One or more packers are configured in the ``dataIdPackers`` section of
       the `Registry` configuration.  In YAML form, that looks something like
       this:

        .. code: yaml
            dataIdPackers:
              VisitDetectorId:
                given: [Instrument]
                covered: [Visit, Detector]
                required: [Instrument, Visit, Detector]
                cls: lsst.daf.butler.instrument.ObservationDataIdPacker

       See `DataIdPackerDimensions` for a description of the ``given``,
       ``covered``, and ``required`` options.

     - Relevant packers are attached to a `DataId` via a call to
       `Registry.expandDataId` with ``packers=True``.

     - The packer is retrieved by name from the `DataId.packers` dict.

    The configured `DataIdPacker` subclass is only imported when a relevant
    `DataId` is expanded with ``packers=True``, so it is safe to configure a
    `Registry` to look for a packer in an extension package that may not always
    be available.

    To implement this lazy loading, `DataIdPacker` construction is split into
    two stages:

     - `DataIdPacker.configure` is called to retrieve the names of any
       dimension metadata fields that are used to construct the `DataIdPacker`,
       as well as additional keyword arguments to pass to its constructor.

     - Regular class construction sequence is invoked, passing the dimensions
       from the configuration, a `DataId` expanded to include the metadata
       identified in the first stage, and the additional keyword arguments
       obtained from the first stage.
    """

    # Override __new__ so subclasses aren't forced to call super().__init__
    # and have to support at least the standard signature.
    def __new__(cls, dimensions, dataId, **kwds):
        self = object.__new__(cls)
        self._dimensions = dimensions
        return self

    @classmethod
    @abstractmethod
    def configure(cls, dimensions):
        """Perform first-stage initialization for the `DataIdPacker`.

        Derived classes must override this method, and should at least
        validate that the given dimensions are appropriate for the class even
        if they return empty dictionaries.

        Parameters
        ----------
        dimensions : `DataIdPackerDimensions`
            The dimensions this `DataIdPacker` was configured to be associated
            with.

        Returns
        -------
        metadata : `dict`
            A dictionary mapping `Dimension` instance or name to a sequence of
            field names.  These field names must be present in the
            `DataId.entries` for a `DataIdPacker` to be constructed with that
            data ID given.
        kwds : `dict`
            A dictionary of additional keyword arguments to forward when
            invoking the `DataIdPacker` subclass constructor.
        """
        return {}, {}

    @property
    def dimensions(self):
        """The dimensions associated with the `DataIdPacker`
        (`DataIdPackerDimensions`).
        """
        return self._dimensions

    @property
    @abstractmethod
    def maxBits(self):
        """The maximum number of nonzero bits in the packed ID returned by
        `pack` (`int`).

        Must be implemented by all concrete subclassses.  May return `None` to
        indicate that there is no maximum.
        """
        raise NotImplementedError()

    @abstractmethod
    def pack(self, dataId, **kwds):
        """Pack the given data ID into a single integer or `bytes` blob.

        Must be implemented by all concrete derived classes.

        Parameters
        ----------
        dataId : `dict` or `DataId`
            Dictionary-like object identifying (at least) all required
            dimensions associated with this packer.  Subclasses should in
            general accept an arbitrary mapping and call the `DataId`
            constructor internally to standardize.  Values for any keys also
            present in the data ID passed at construction must be the same
            as the values in the data ID passed at construction.
        kwds
            Additional keyword arguments to pass to the `DataId` constructor.

        Returns
        -------
        packed : `int` or `bytes`
            Packed ID.
        """
        raise NotImplementedError()

    @abstractmethod
    def unpack(self, packedId):
        """Unpack an ID produced by `pack` into a full `DataId`.

        Must be implemented by all concrete derived classes.

        Parameters
        ----------
        packedId : `int` or `bytes`
            The result of a call to `pack` on either `self` or an
            identically-constructed one.

        Returns
        -------
        dataId : `DataId`
            Dictionary-like ID that uniquely identifies all covered
            dimensions.
        """
        raise NotImplementedError()


class DataIdPackerFactory:
    """A factory class for `DataIdPacker` instances that can report what
    metadata fields they need to be constructed.

    This class is intended for internal use by `DataIdPackerLibrary` only.

    Parameters
    ----------
    dimensions : `DataIdPackerDimensions`
        The dimensions associated with the `DataIdPacker`.
    cls : `type`
        Subclass of `DataIdPacker` to construct.
    metadata : `dict`, optional
        Dictionary that maps `Dimension` instance or name to a sequence of
        fields that must be included in `DataId.entries` in order to construct
        a `DataIdPacker` for it.
    kwds
        Additional keyword arguments forwarded to the constructor for ``cls``.
    """

    def __init__(self, dimensions, cls, metadata=None, **kwds):
        if metadata is None:
            metadata = {}
        self._dimensions = dimensions
        self._cls = cls
        self._metadata = metadata
        self._kwds = kwds

    @property
    def dimensions(self):
        """The dimensions associated with the `DataIdPacker`
        (`DataIdPackerDimensions`).
        """
        return self._dimensions

    @property
    def metadata(self):
        """The fields needed to construct a `DataIdPacker` instance (`dict`).

        This dictionary maps `Dimension` instance or name to a sequence of
        field names; these fields must be present in `DataId.entries` in order
        to construct a `DataIdPacker` for it.
        """
        return self._metadata

    def makePacker(self, dataId):
        """Construct a `DataIdPacker` instance for the given data ID.

        Parameters
        ----------
        dataId : `DataId`
            A validated `DataId` to pass to the `DataIdPacker` subclass
            constructor.
        """
        return self._cls(self.dimensions, dataId, **self._kwds)


class DataIdPackerLoader:
    """A helper class for importing `DataIdPacker` classes and retrieving the
    metadata fields they need.

    This class is intended for internal use by `DataIdPackerLibrary` only.

    Parameters
    ----------
    dimensions : `DataIdPackerDimensions`
        The dimensions associated with the `DataIdPacker`.
    clsName : `str`
        The fully-qualified name of a `DataIdPacker` subclass.
    """

    def __init__(self, dimensions, clsName):
        self._dimensions = dimensions
        self._clsName = clsName

    @property
    def dimensions(self):
        """The dimensions associated with the `DataIdPacker`
        (`DataIdPackerDimensions`).
        """
        return self._dimensions

    def loadFactory(self):
        """Import the `DataIdPacker` subclass and return a factory for its
        instances.
        """
        cls = doImport(self._clsName)
        metadata, kwds = cls.configure(self.dimensions)
        return DataIdPackerFactory(self.dimensions, cls=cls, metadata=metadata, **kwds)


class DataIdPackerLibrary:
    """A manager that manages `DataIdPacker` classes for `Registry.`

    This class is intended for internal use by `Registry` only.

    Parameters
    ----------
    universe : `DimensionGraph`
        All dimensions known to the `Registry`.
    config : `Config`
        The "dataIdPackers" subsection of a `RegistryConfig`.
    """

    def __init__(self, universe, config):
        self._metadata = DimensionKeyDict(keys=universe.elements, factory=set)
        self._loaders = defaultdict(dict)
        self._factories = defaultdict(dict)
        for name, subconfig in config.items():
            dimensions = DataIdPackerDimensions.fromConfig(universe.universe, subconfig)
            clsName = subconfig["cls"]
            self._loaders[dimensions.given][name] = DataIdPackerLoader(dimensions, clsName)

    def load(self, dimensions):
        """Import `DataIdPacker` classes for the given dimensions and return a
        dictionary of metadata fields needed to construct them.

        This must be called explicitly with
        ``dataId.dimensions(implied=True)`` before any calls to `extract` for
        a data ID.

        Parameters
        ----------
        dimensions : iterable over `Dimension` or `str`
            Dimensions that must be a superset of the "required" dimensions
            of any matched `DataIdPacker` classes.

        Returns
        -------
        metadata : `~collections.abc.Mapping`
            A mapping with `DimensionElement` keys and `~collections.abc.Set`
            of `str` values indicating the dimension metadata fields that must
            be included in a `DataId` to ensure relevant packers may be
            constructed.  May (as an optimization) include additional entries
            not relevant for the given dimensions and/or return an internal
            dict that should not be modified by the caller.

        Notes
        -----
        This is called early in the process of `DataId` expansion to import
        `DataIdPacker` classes that may be relevant for it; we defer those
        imports to this point avoid bringing down `Registry` construction just
        because some packer we weren't actually planning to use in a
        particular session is in an extension package not in ``sys.path``.
        Once the packer class is imported, we can retreive the dictionary of
        metadata entries it needs for instantiation, and hence ensure those
        entries are always loaded whenever data IDs are expanded.
        """
        # Standardize dimension input
        dimensions = toNameSet(dimensions)
        matched = []
        for given, loaders in self._loaders.items():
            if given.issubset(dimensions):
                for name, loader in loaders.items():
                    factory = loader.loadFactory()
                    # Retrieve the dict of metadata fields needed by this
                    # factory, and merge it with the fields needed by all of
                    # the other factories we know about.
                    self._metadata.updateValues(factory.metadata)
                    # Insert the factory we just created into the factories
                    # dict.
                    self._factories[loader.dimensions.given][name] = factory
                matched.append(given)

        # Remove the loaded factories from the loaders dict, so future calls
        # to loadMetadata will (once all factories are loaded) just return
        # already-loaded metadata.
        for given in matched:
            del self._loaders[given]

        return self._metadata

    def extract(self, dataId):
        """Return the `DataIdPacker` instances relevant to a data ID.

        A `DataIdPacker` is considered relevant for a data ID if the data ID
        uniquely identifies all of its "given" dimensions.  This always
        includes all packers with no given dimensions.

        Parameters
        ----------
        dataId : `DataId`
            Data ID to match.  Note that this must be a true `DataId`, not
            a dict or other mapping.

        Returns
        -------
        packers : `dict`
            Dictionary mapping configured packer name to `DataIdPacker`
            instance.
        """
        result = {}
        for given, factories in self._factories.items():
            if given.issubset(dataId.dimensions(implied=True)):
                for name, factory in factories.items():
                    result[name] = factory.makePacker(dataId)
        return result
